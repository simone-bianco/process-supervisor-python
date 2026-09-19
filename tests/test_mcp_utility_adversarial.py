"""Finite isolated-installer utility checks; no package download or managed runtime start."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from py_laravel_supervisor import mcp_utility
from py_laravel_supervisor.appcontainer import AppContainerProfile
from py_laravel_supervisor.mcp_utility import run_isolated_utility
from py_laravel_supervisor.windows import WindowsProcessError


@unittest.skipUnless(os.name == "nt", "Windows isolated utility regression")
class McpUtilityAdversarialTest(unittest.TestCase):
    def setUp(self):
        self.node = shutil.which("node.exe")
        self.assertIsNotNone(self.node)
        self.temporary = tempfile.TemporaryDirectory(prefix="mcp-utility-qa417-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.code, self.state = self.root / "code", self.root / "state"
        self.code.mkdir()
        self.state.mkdir()
        self.environment = {"SystemRoot": os.environ["SystemRoot"], "WINDIR": os.environ["SystemRoot"],
                            **{name: str(self.state) for name in ("HOME", "USERPROFILE", "TEMP", "TMP", "APPDATA", "LOCALAPPDATA")}}

    def run_source(self, source, *, environment=None, **limits):
        script = self.code / "fixture.cjs"
        script.write_text(source, encoding="utf-8")
        return run_isolated_utility([self.node, "--preserve-symlinks", "--preserve-symlinks-main", str(script)],
                                    cwd=self.state, read_directories=[self.code], write_directories=[self.state],
                                    environment=self.environment if environment is None else environment,
                                    owner_id=uuid.uuid4().hex, **limits)

    def test_disallowed_preload_and_secret_env_are_rejected_before_os_profile_creation(self):
        for name in ["NODE_OPTIONS", "NODE_PATH", "NPM_CONFIG_USERCONFIG", "MCP_API_TOKEN"]:
            with self.subTest(name=name), patch.object(mcp_utility, "AppContainerProfile") as constructor:
                with self.assertRaisesRegex(WindowsProcessError, "explicit environment"):
                    self.run_source("require('fs').writeFileSync('side-effect','bad');", environment={**self.environment, name: "forbidden"})
                constructor.assert_not_called()
                self.assertFalse((self.state / "side-effect").exists())

    def test_output_at_exact_limit_succeeds_and_one_byte_over_never_returns_partial_success(self):
        result = self.run_source("process.stdout.write('a'.repeat(1024));", stdout_limit=1024)
        self.assertEqual(0, result.exit_code)
        self.assertEqual(b"a" * 1024, result.stdout)
        with self.assertRaises(WindowsProcessError):
            self.run_source("process.stdout.write('b'.repeat(1025));", stdout_limit=1024)

    def test_nonzero_exit_is_returned_without_leaking_stderr_body(self):
        result = self.run_source("process.stderr.write('synthetic-sensitive-stderr');process.stdout.write('result');process.exitCode=7;")
        self.assertEqual(7, result.exit_code)
        self.assertEqual(b"result", result.stdout)
        self.assertEqual(len(b"synthetic-sensitive-stderr"), result.stderr_bytes)
        self.assertNotIn("synthetic-sensitive-stderr", repr(result))

    def test_ambient_node_preload_cannot_execute_even_when_it_is_readable_by_the_child(self):
        marker = self.state / "preload-executed"
        preload = self.code / "preload.cjs"
        preload.write_text("require('fs').writeFileSync(" + json.dumps(str(marker)) + ", 'executed');", encoding="utf-8")
        raw_env = os.environ.copy()
        raw_env["NODE_OPTIONS"] = "--require=" + str(preload)
        subprocess.run([self.node, "-e", "process.stdout.write('control');"], env=raw_env,
                       check=True, timeout=3, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(marker.exists(), "The positive control proves the ambient preload is executable.")
        marker.unlink()
        with patch.dict(os.environ, {"NODE_OPTIONS": raw_env["NODE_OPTIONS"]}):
            result = self.run_source("process.stdout.write('isolated');")
        self.assertEqual(b"isolated", result.stdout)
        self.assertFalse(marker.exists())

    def test_directory_junction_is_rejected_without_changing_target_file_access(self):
        target = self.root / "other-owner"
        target.mkdir()
        sentinel = target / "protected.txt"
        sentinel.write_text("outside-owner", encoding="utf-8")
        junction = self.root / "redirect"
        executable = str(Path(os.environ["SystemRoot"]) / "System32" / "cmd.exe")
        # This explicit test-owned junction checks OS reparse semantics; it is never a user-input shell surface.
        created = subprocess.run([executable, "/d", "/c", "mklink", "/J", str(junction), str(target)],
                                 timeout=5, capture_output=True, text=True)
        self.assertEqual(0, created.returncode, "The Windows fixture requires directory junction support.")
        profile = AppContainerProfile("localgpt." + uuid.uuid4().hex)
        try:
            with self.assertRaisesRegex(WindowsProcessError, "reparse"):
                profile.grant_directory(junction, writable=True)
            self.assertEqual("outside-owner", sentinel.read_text(encoding="utf-8"))
        finally:
            profile.close()
            if junction.exists():
                junction.rmdir()


if __name__ == "__main__":
    unittest.main()
