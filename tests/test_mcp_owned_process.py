import json
import os
import shutil
import socket
import tempfile
import unittest
import uuid
from pathlib import Path

from py_laravel_supervisor.duplex import DuplexChannel
from py_laravel_supervisor.windows import (
    WindowsProcessError, close_handle, create_job, job_active_processes, spawn_process,
)


@unittest.skipUnless(os.name == "nt", "Windows isolation fixture")
class McpOwnedProcessTest(unittest.TestCase):
    def setUp(self):
        self.node = shutil.which("node.exe")
        self.assertIsNotNone(self.node, "the P0 Node fixture requires an installed Node executable")
        self.temp = tempfile.TemporaryDirectory(prefix="mcp-p0-")
        self.root = Path(self.temp.name)
        self.code = self.root / "code"
        self.state = self.root / "state"
        self.code.mkdir()
        self.state.mkdir()
        self.denied = self.root / "other-owner.txt"
        self.denied.write_text("must-not-be-readable", encoding="utf8")
        self.job = create_job("Local\\McpP0-" + uuid.uuid4().hex)
        self.channel = None
        self.process = None

    def tearDown(self):
        try:
            if self.channel is not None:
                self.channel.close()
            elif self.process is not None:
                self.process.terminate_tree()
                self.process.wait(2)
                self.process.close()
            self.assertEqual(0, job_active_processes(self.job))
        finally:
            close_handle(self.job)
            self.temp.cleanup()

    def start(self, source, **limits):
        fixture = self.code / "fixture.js"
        fixture.write_text(source, encoding="utf8")
        environment = {
            "SystemRoot": os.environ["SystemRoot"], "WINDIR": os.environ["SystemRoot"],
            "TEMP": str(self.state), "TMP": str(self.state), "HOME": str(self.state),
            "USERPROFILE": str(self.state), "LOCALAPPDATA": str(self.state), "APPDATA": str(self.state),
        }
        self.process = spawn_process(
            [self.node, '--preserve-symlinks', '--preserve-symlinks-main', str(fixture)], cwd=self.state, environment=environment,
            job_handles=[self.job], exact_job_handle=self.job, cleanup_job_handle=self.job,
            stdin_pipe=True, exact_environment=True,
        )
        self.channel = DuplexChannel(self.process, **limits)
        return self.channel

    def test_trusted_node_access_and_no_ambient_environment_leak(self):
        os.environ['MCP_P0_SECRET'] = 'not-inherited'
        self.addCleanup(os.environ.pop, 'MCP_P0_SECRET', None)
        channel = self.start("""
const fs = require('fs');
require('readline').createInterface({input: process.stdin}).on('line', line => {
 const request=JSON.parse(line); let read='allowed'; let write='allowed';
 try { fs.readFileSync(request.denied); } catch(e) { read=e.code; }
 try { fs.writeFileSync(request.codeFile, 'changed'); } catch(e) { write=e.code; }
 fs.writeFileSync('owned.txt', request.text);
 process.stdout.write(JSON.stringify({echo:request.text,read,write,secret:process.env.MCP_P0_SECRET||null})+'\\n');
});
""")
        os.environ['MCP_P0_SECRET'] = 'not-inherited'
        try:
            result = json.loads(channel.exchange(json.dumps({"text": "hello ü", "denied": str(self.denied), "codeFile": str(self.code / "fixture.js")}).encode() + b"\n", timeout=5))
        finally:
            del os.environ['MCP_P0_SECRET']
        self.assertEqual("hello ü", result["echo"])
        self.assertEqual("allowed", result["read"])
        self.assertEqual("allowed", result["write"])
        self.assertIsNone(result["secret"])
        self.assertEqual("hello ü", (self.state / "owned.txt").read_text(encoding="utf8"))

    def test_trusted_local_process_can_connect_to_a_local_listener(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        # A successful non-sandboxed control proves the fixture is reachable.
        with socket.create_connection(listener.getsockname(), timeout=1):
            accepted, _ = listener.accept()
            accepted.close()
        listener.settimeout(0.2)
        try:
            channel = self.start("""
require('readline').createInterface({input:process.stdin}).on('line', line => {
 const port=JSON.parse(line).port;
 const socket=require('net').connect({host:'127.0.0.1',port});
 socket.on('connect',()=>{process.stdout.write('"allowed"\\n'); socket.destroy();});
 socket.on('error',e=>process.stdout.write(JSON.stringify(e.code)+'\\n'));
});
""")
            result = json.loads(channel.exchange(json.dumps({"port": listener.getsockname()[1]}).encode() + b"\n", timeout=5))
            self.assertEqual("allowed", result)
            accepted, _ = listener.accept()
            accepted.close()
        finally:
            listener.close()

    def test_deadline_closes_original_child_without_replay(self):
        channel = self.start("process.stdin.resume(); setInterval(()=>{},1000);")
        with self.assertRaisesRegex(WindowsProcessError, "deadline"):
            channel.exchange(b"{}\n", timeout=0.15)
        self.assertEqual(0, job_active_processes(self.job))

    def test_stderr_flood_is_bounded_and_not_logged(self):
        channel = self.start("process.stdin.on('data',()=>process.stderr.write('x'.repeat(131072)));", stderr_bytes=1024)
        with self.assertRaises(WindowsProcessError):
            channel.exchange(b"{}\n", timeout=3)
