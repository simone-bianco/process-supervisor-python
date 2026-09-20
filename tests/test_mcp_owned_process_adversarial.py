"""Independent, finite Windows P0 regressions. Each fixture owns its exact Job.

No MCP deployment, installer, application service or user's process is started.
These tests exercise byte/lifecycle isolation, not JSON-RPC or gateway authority.
"""
from __future__ import annotations

import json
import os
import queue
from pathlib import Path
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from py_laravel_supervisor.duplex import DuplexChannel
from py_laravel_supervisor.windows import (
    ManagedWindowsProcess,
    WindowsProcessError,
    close_handle,
    create_job,
    job_active_processes,
    spawn_process,
    terminate_job,
)


@unittest.skipUnless(os.name == "nt", "Windows owned Job regression")
class McpOwnedProcessAdversarialTest(unittest.TestCase):
    def setUp(self):
        self.node = shutil.which("node.exe")
        self.assertIsNotNone(self.node, "An already installed Node is required; tests do not install it.")
        self.temporary = tempfile.TemporaryDirectory(prefix="mcp-qa417-")
        self.root = Path(self.temporary.name)
        self.resources = []

    def tearDown(self):
        failures = []
        for item in reversed(self.resources):
            try:
                if item["channel"] is not None:
                    item["channel"].close()
            except BaseException as error:
                failures.append(error)
            finally:
                # Fault-injection tests may deliberately interrupt normal cleanup.
                # Clean only the exact test-owned Job; never discover/kill by PID.
                try:
                    terminate_job(item["job"])
                    if item["process"] is not None and item["process"].process_handle:
                        item["process"].wait(2)
                    self.assertEqual(0, job_active_processes(item["job"]))
                finally:
                    if item["process"] is not None:
                        if item["channel"] is not None:
                            try:
                                item["channel"]._writes.put_nowait(None)
                            except queue.Full:
                                pass
                        for thread in item["channel"]._threads if item["channel"] is not None else []:
                            thread.join(timeout=1)
                        item["process"].close()
                    close_handle(item["job"])
        self.temporary.cleanup()
        if failures:
            raise failures[0]

    def start(self, source: str, *, name: str = "first", **bounds):
        home = self.root / name
        code, state = home / "code", home / "state"
        code.mkdir(parents=True)
        state.mkdir()
        script = code / "peer.cjs"
        script.write_text(source, encoding="utf-8")
        job = create_job("Local\\McpQA417-" + uuid.uuid4().hex)
        owned = {"job": job, "process": None, "channel": None, "code": code, "state": state}
        self.resources.append(owned)
        environment = {"SystemRoot": os.environ["SystemRoot"], "WINDIR": os.environ["SystemRoot"],
                       **{key: str(state) for key in ("HOME", "USERPROFILE", "TEMP", "TMP", "APPDATA", "LOCALAPPDATA")}}
        process = spawn_process([self.node, "--preserve-symlinks", "--preserve-symlinks-main", str(script)],
                                cwd=state, environment=environment, job_handles=[job], exact_job_handle=job,
                                cleanup_job_handle=job, stdin_pipe=True, exact_environment=True)
        owned["process"] = process
        owned["channel"] = DuplexChannel(process, **bounds)
        return owned

    @staticmethod
    def frame(payload) -> bytes:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"

    def test_fragmented_unicode_and_exact_byte_budget_preserve_original_bytes(self):
        item = self.start("""
const fs = require('fs');
require('readline').createInterface({input:process.stdin}).on('line', line => {
 const response=Buffer.from(line+'\\n','utf8');
 for (let i=0;i<response.length;i+=3) fs.writeSync(1,response.subarray(i,i+3));
});
""", frame_bytes=1024)
        frame = self.frame({"text": "memoria 東京 ü"})
        self.assertEqual(frame, item["channel"].exchange(frame, timeout=3))
        exact = b'"' + b'x' * 1021 + b'"\n'
        self.assertEqual(1024, len(exact))
        self.assertEqual(exact, item["channel"].exchange(exact, timeout=3))

    def test_partial_stdout_eof_fails_without_replay_and_closes_the_job(self):
        item = self.start("process.stdin.once('data',()=>{process.stdout.write('{\"partial\":');process.exit(0);});")
        with self.assertRaises(WindowsProcessError):
            item["channel"].exchange(b"{}\n", timeout=2)
        self.assertEqual(0, job_active_processes(item["job"]))

    def test_a_single_oversized_response_is_never_truncated_into_success(self):
        item = self.start("process.stdin.once('data',()=>process.stdout.write('x'.repeat(1024)+'\\n'));", frame_bytes=1024)
        with self.assertRaises(WindowsProcessError):
            item["channel"].exchange(b"{}\n", timeout=2)
        self.assertEqual(0, job_active_processes(item["job"]))

    def test_unsolicited_frame_queue_overflow_is_bounded_and_not_reused(self):
        item = self.start("process.stdout.write(('unexpected\\n').repeat(64));process.stdin.resume();", frame_bytes=1024)
        self.assertTrue(item["channel"]._failed.wait(3), "The bounded reader must detect an overflowing unsolicited stream.")
        with self.assertRaises(WindowsProcessError):
            item["channel"].exchange(b"{}\n", timeout=1)
        self.assertEqual(0, job_active_processes(item["job"]))

    def test_stdin_backpressure_obeys_deadline_and_leaves_no_io_thread(self):
        item = self.start("setInterval(()=>{},1000);")
        payload = self.frame({"text": "x" * 200000})
        started = time.monotonic()
        with self.assertRaisesRegex(WindowsProcessError, "deadline"):
            item["channel"].exchange(payload, timeout=0.15)
        self.assertLess(time.monotonic() - started, 4, "Cleanup has a finite additional allowance, not an unbounded pipe write.")
        self.assertEqual(0, job_active_processes(item["job"]))
        self.assertTrue(all(not thread.is_alive() for thread in item["channel"]._threads))

    def test_input_rejection_does_not_dispatch_or_poison_the_next_valid_request(self):
        item = self.start("""
const fs=require('fs');
require('readline').createInterface({input:process.stdin}).on('line', line=>{
 fs.appendFileSync('received.txt',line+'\\n');process.stdout.write(line+'\\n');
});
""", frame_bytes=1024)
        for invalid in [b"{}", b"{}\n{}\n", b"{}\r\n", b"x" * 1025 + b"\n", "{}\n"]:
            with self.subTest(kind=type(invalid).__name__, size=len(invalid)):
                with self.assertRaises(ValueError):
                    item["channel"].exchange(invalid, timeout=1)
        self.assertFalse((item["state"] / "received.txt").exists())
        self.assertEqual(b"{}\n", item["channel"].exchange(b"{}\n", timeout=2))
        self.assertEqual("{}\n", (item["state"] / "received.txt").read_text(encoding="utf-8"))

    def test_trusted_processes_share_os_authority_but_keep_separate_job_lifetimes(self):
        source = """
const fs=require('fs');
require('readline').createInterface({input:process.stdin}).on('line', line=>{
 const request=JSON.parse(line);let outcome='allowed';
 try { fs.readFileSync(request.path); } catch(error) { outcome=error.code; }
 process.stdout.write(JSON.stringify({outcome})+'\\n');
});
"""
        first = self.start(source)
        second = self.start(source, name="second")
        sentinel = second["state"] / "memory.json"
        sentinel.write_text("only-the-second-workspace", encoding="utf-8")
        first_result = json.loads(first["channel"].exchange(self.frame({"path": str(sentinel)}), timeout=3))
        second_result = json.loads(second["channel"].exchange(self.frame({"path": str(sentinel)}), timeout=3))
        self.assertEqual("allowed", first_result["outcome"])
        self.assertEqual("allowed", second_result["outcome"])
        first["channel"].close()
        self.assertEqual(0, job_active_processes(first["job"]))
        self.assertGreaterEqual(job_active_processes(second["job"]), 1)
        self.assertIsNone(second["process"].poll(), "Closing the first Job must preserve the exact second process.")
        self.assertEqual("allowed", json.loads(second["channel"].exchange(self.frame({"path": str(sentinel)}), timeout=2))["outcome"])

    def test_close_waits_for_original_job_accounting_after_root_exit(self):
        item = self.start("process.stdin.resume();setInterval(()=>{},1000);")
        with patch('py_laravel_supervisor.duplex.job_active_processes', side_effect=[1, 1, 0]) as accounting:
            item['channel'].close()
        self.assertEqual(3, accounting.call_count)
        self.assertTrue(item['channel']._closed)
        self.assertEqual(0, job_active_processes(item['job']))

    def test_failed_cleanup_is_not_marked_successful_on_the_next_close(self):
        item = self.start("process.stdin.resume();setInterval(()=>{},1000);")
        original = ManagedWindowsProcess.terminate_tree
        attempted = threading.Event()
        def fail_once(process):
            if process is item["process"] and not attempted.is_set():
                attempted.set()
                raise WindowsProcessError("synthetic OS close failure")
            return original(process)
        with patch.object(ManagedWindowsProcess, "terminate_tree", fail_once):
            with self.assertRaisesRegex(WindowsProcessError, "synthetic OS close failure"):
                item["channel"].close()
            self.assertGreaterEqual(job_active_processes(item["job"]), 1)
            self.assertIsNone(item["process"].poll(), "The original child is demonstrably still alive.")
            item["channel"].close()
        self.assertEqual(0, job_active_processes(item["job"]), "A failed cleanup cannot become a silent no-op: retry must close the same owned child.")
        self.assertTrue(all(not thread.is_alive() for thread in item["channel"]._threads))

    def test_incomplete_io_cleanup_preserves_handles_for_an_exact_owner_retry(self):
        item = self.start("process.stdin.resume();setInterval(()=>{},1000);")
        reader = item["channel"]._threads[0]
        with patch.object(reader, "is_alive", return_value=True):
            with self.assertRaisesRegex(WindowsProcessError, "cleanup is incomplete"):
                item["channel"].close()
        self.assertFalse(item["channel"]._closed)
        self.assertNotEqual(0, item["process"].process_handle, "The original process handle must survive an unproven I/O close.")
        item["channel"].close()
        self.assertTrue(item["channel"]._closed)
        self.assertEqual(0, item["process"].process_handle)
        self.assertEqual(0, job_active_processes(item["job"]))

    def test_each_caller_deadline_includes_waiting_for_the_original_channel(self):
        item = self.start("""
require('readline').createInterface({input:process.stdin}).on('line', line=>{
 const input=JSON.parse(line);setTimeout(()=>process.stdout.write(JSON.stringify(input)+'\\n'),input.delay||0);
});
""")
        enqueued = threading.Event()
        original_put = item["channel"]._writes.put_nowait
        first_result = []
        def observed_put(value):
            result = original_put(value)
            if value is not None:
                enqueued.set()
            return result
        def first_exchange():
            try:
                first_result.append(item["channel"].exchange(self.frame({"delay": 550}), timeout=2))
            except WindowsProcessError as error:
                first_result.append(error)
        with patch.object(item["channel"]._writes, "put_nowait", observed_put):
            first = threading.Thread(target=first_exchange)
            first.start()
            self.assertTrue(enqueued.wait(2), "The first caller is already in the owned exchange.")
            started = time.monotonic()
            try:
                try:
                    item["channel"].exchange(self.frame({"delay": 0}), timeout=0.05)
                except WindowsProcessError:
                    pass
                elapsed = time.monotonic() - started
            finally:
                first.join(timeout=3)
        self.assertFalse(first.is_alive())
        self.assertEqual([self.frame({"delay": 550})], first_result, "Rejecting a competing caller must not cancel or replay the original exchange.")
        self.assertLess(elapsed, 0.3, "A 50ms call cannot acquire a fresh deadline after silently waiting 550ms for another caller.")


if __name__ == "__main__":
    unittest.main()
