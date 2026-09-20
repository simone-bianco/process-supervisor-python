"""Finite real-Windows acceptance of a raw framed channel, not an MCP parser or launcher.

Each fixture owns its Job, child and temporary directories. No
Docker, application service, network or downloaded package is executed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from py_laravel_supervisor.framed_channel import FramedStdioChannel, _pipe_available
from py_laravel_supervisor.windows import (
    ManagedWindowsProcess, WindowsProcessError, close_handle, create_job,
    job_active_processes, spawn_process, terminate_job,
)


@unittest.skipUnless(os.name == 'nt', 'Owned Windows stdio fixture')
class McpFramedChannelTest(unittest.TestCase):
    def setUp(self):
        self.node = shutil.which('node.exe')
        self.assertIsNotNone(self.node, 'Use the installed Node; tests must never install it.')
        self.temporary = tempfile.TemporaryDirectory(prefix='mcp-framed-731-')
        self.root = Path(self.temporary.name)
        self.code, self.state = self.root / 'code', self.root / 'state'
        self.code.mkdir(); self.state.mkdir()
        self.job = create_job('Local\\McpFramed731-' + uuid.uuid4().hex)
        self.process = None
        self.channel = None

    def tearDown(self):
        try:
            if self.channel is not None:
                self.channel.close()
            elif self.process is not None:
                self.process.terminate_tree()
                self.process.wait(2)
            self.assertEqual(0, job_active_processes(self.job))
        finally:
            # These are only the fixture's original handles, never PID discovery.
            terminate_job(self.job)
            if self.channel is not None:
                try: self.channel._writes.put_nowait(None)
                except queue.Full: pass
                for thread in self.channel._threads: thread.join(timeout=1)
            if self.process is not None: self.process.close()
            close_handle(self.job)
            self.temporary.cleanup()

    def start(self, source: str, **limits) -> FramedStdioChannel:
        script = self.code / 'peer.cjs'
        script.write_text(source, encoding='utf-8')
        environment = {'SystemRoot': os.environ['SystemRoot'], 'WINDIR': os.environ['SystemRoot'],
            **{name: str(self.state) for name in ('HOME', 'USERPROFILE', 'TEMP', 'TMP', 'APPDATA', 'LOCALAPPDATA')}}
        self.process = spawn_process([self.node, '--preserve-symlinks', '--preserve-symlinks-main', str(script)],
            cwd=self.state, environment=environment, job_handles=[self.job], exact_job_handle=self.job,
            cleanup_job_handle=self.job, stdin_pipe=True, exact_environment=True)
        self.channel = FramedStdioChannel(self.process, **limits)
        return self.channel

    @staticmethod
    def frame(value) -> bytes:
        return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n'

    def test_notification_without_response_and_separate_multiple_response_frames_preserve_protocol_bytes(self):
        channel = self.start("""
let initialized=false;
require('readline').createInterface({input:process.stdin}).on('line', line => {
 const request=JSON.parse(line);
 if(request.method==='notifications/initialized'){initialized=true;return;}
 const frames=[{jsonrpc:'2.0',method:'notifications/message',params:{text:'東京 ü'}},
 {jsonrpc:'2.0',id:request.id,result:{initialized,text:request.params.text}}];
 process.stdout.write(frames.map(value=>JSON.stringify(value)+'\\n').join(''));
});
""")
        channel.assert_usable(); channel.assert_idle()
        channel.write(self.frame({'jsonrpc': '2.0', 'method': 'notifications/initialized'}), timeout=3)
        channel.assert_idle()
        request = {'jsonrpc': '2.0', 'id': 'original', 'method': 'tools/call', 'params': {'text': 'sample 東京'}}
        channel.write(self.frame(request), timeout=3)
        first = channel.read(timeout=3)
        second = channel.read(timeout=3)
        self.assertEqual(self.frame({'jsonrpc': '2.0', 'method': 'notifications/message', 'params': {'text': '東京 ü'}}), first)
        self.assertEqual(self.frame({'jsonrpc': '2.0', 'id': 'original', 'result': {'initialized': True, 'text': 'sample 東京'}}), second)
        channel.assert_idle(); channel.assert_usable()
        channel.close()
        self.assertEqual(0, job_active_processes(self.job))

    def test_idle_observes_kernel_bytes_even_before_the_reader_thread_has_processed_them(self):
        gate = threading.Event()
        def paused_reader(channel):
            channel._failed.wait(3)
            gate.set()
        with patch.object(FramedStdioChannel, '_stdout', paused_reader):
            channel = self.start("process.stdout.write('not-yet-buffered');process.stdin.resume();")
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and _pipe_available(self.process.stdout_fd) == 0:
            gate.wait(0.005)
        self.assertGreater(_pipe_available(self.process.stdout_fd), 0)
        self.assertEqual(0, channel.buffered_bytes)
        self.assertTrue(channel._frames.empty())
        with self.assertRaisesRegex(WindowsProcessError, 'idle'):
            channel.assert_idle()
        self.assertEqual(0, job_active_processes(self.job))
        self.assertTrue(gate.is_set())

    def test_partial_unsolicited_stdout_is_not_reported_idle(self):
        channel = self.start("process.stdout.write('{partial');process.stdin.resume();")
        observed = threading.Event()
        # This condition samples only this fixture's owned byte buffer, not a peer handoff or service state.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if channel.buffered_bytes:
                observed.set(); break
            observed.wait(0.005)
        self.assertTrue(observed.is_set(), 'The partial bytes must actually arrive before the idle assertion.')
        with self.assertRaisesRegex(WindowsProcessError, 'idle'):
            channel.assert_idle()
        self.assertEqual(0, job_active_processes(self.job))

    def test_idle_check_preserves_no_leftover_complete_response_for_the_next_request(self):
        channel = self.start("process.stdin.once('data',()=>process.stdout.write('first\\nsecond\\n'));process.stdin.resume();")
        channel.write(b'{}\n', timeout=2)
        self.assertEqual(b'first\n', channel.read(timeout=2))
        with self.assertRaisesRegex(WindowsProcessError, 'idle'):
            channel.assert_idle()
        self.assertEqual(0, job_active_processes(self.job))

    def test_write_backpressure_enforces_original_deadline_and_kills_only_owned_job(self):
        channel = self.start("setInterval(()=>{},1000);")
        started = time.monotonic()
        with self.assertRaisesRegex(WindowsProcessError, 'deadline'):
            channel.write(self.frame({'text': 'x' * 200000}), timeout=0.12)
        self.assertLess(time.monotonic() - started, 4)
        self.assertEqual(0, job_active_processes(self.job))
        self.assertTrue(all(not thread.is_alive() for thread in channel._threads))

    def test_read_timeout_after_successful_notification_write_does_not_replay(self):
        channel = self.start("const fs=require('fs');require('readline').createInterface({input:process.stdin}).on('line',s=>fs.appendFileSync('seen',s+'\\n'));")
        channel.write(b'notification\n', timeout=2)
        with self.assertRaisesRegex(WindowsProcessError, 'deadline'):
            channel.read(timeout=0.15)
        self.assertEqual('notification\n', (self.state / 'seen').read_text())
        self.assertEqual(0, job_active_processes(self.job))

    def test_malformed_or_oversized_outbound_frame_is_rejected_before_write_without_poisoning_channel(self):
        channel = self.start("require('readline').createInterface({input:process.stdin}).on('line',s=>process.stdout.write(s+'\\n'));")
        for frame in [b'no-newline', b'a\nb\n', b'bad\r\n', 'text\n', b'x' * 262145 + b'\n']:
            with self.subTest(value_type=type(frame).__name__, length=len(frame)):
                with self.assertRaises(ValueError): channel.write(frame, timeout=1)
        for timeout in [0, -1, 61, float('nan'), float('inf')]:
            with self.assertRaises(ValueError): channel.write(b'valid\n', timeout=timeout)
        channel.assert_idle()
        channel.write(b'valid\n', timeout=2)
        self.assertEqual(b'valid\n', channel.read(timeout=2))

    def test_partial_eof_or_stderr_flood_closes_without_disclosing_a_truncated_frame(self):
        channel = self.start("process.stdin.once('data',()=>{process.stdout.write('{truncated');process.exit(0);});")
        channel.write(b'{}\n', timeout=2)
        with self.assertRaises(WindowsProcessError): channel.read(timeout=2)
        self.assertEqual(0, job_active_processes(self.job))

    def test_competing_read_fails_fast_without_cancelling_original_reader(self):
        channel = self.start("require('readline').createInterface({input:process.stdin}).on('line',s=>setTimeout(()=>process.stdout.write(s+'\\n'),400));")
        channel.write(b'original\n', timeout=2)
        entered = threading.Event()
        read_result = []
        original_get = channel._frames.get
        def observed_get(*args, **kwargs):
            entered.set()
            return original_get(*args, **kwargs)
        def reader():
            try: read_result.append(channel.read(timeout=2))
            except WindowsProcessError as error: read_result.append(error)
        with patch.object(channel._frames, 'get', side_effect=observed_get):
            first = threading.Thread(target=reader)
            first.start()
            self.assertTrue(entered.wait(2))
            started = time.monotonic()
            with self.assertRaisesRegex(WindowsProcessError, 'active'):
                channel.read(timeout=0.05)
            self.assertLess(time.monotonic() - started, 0.25)
            first.join(timeout=3)
        self.assertFalse(first.is_alive())
        self.assertEqual([b'original\n'], read_result)
        channel.assert_usable()

    def test_close_failure_remains_retryable_for_the_exact_original_process(self):
        channel = self.start("process.stdin.resume();")
        original = ManagedWindowsProcess.terminate_tree
        invoked = False
        def fail_once(process):
            nonlocal invoked
            if process is self.process and not invoked:
                invoked = True
                raise WindowsProcessError('synthetic close failure')
            return original(process)
        with patch.object(ManagedWindowsProcess, 'terminate_tree', fail_once):
            with self.assertRaisesRegex(WindowsProcessError, 'synthetic close failure'): channel.close()
            self.assertFalse(channel._closed)
            self.assertIsNone(self.process.poll())
            channel.close()
        self.assertTrue(channel._closed)
        self.assertEqual(0, job_active_processes(self.job))


if __name__ == '__main__': unittest.main()
