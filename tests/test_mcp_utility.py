import os
from pathlib import Path
import shutil
import tempfile
import unittest
import uuid

from py_laravel_supervisor.mcp_utility import run_isolated_utility
from py_laravel_supervisor.windows import WindowsProcessError


@unittest.skipUnless(os.name == 'nt', 'Windows isolated utility fixture')
class IsolatedUtilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='mcp-utility-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.code = self.root / 'code'
        self.state = self.root / 'state'
        self.code.mkdir()
        self.state.mkdir()
        self.node = shutil.which('node.exe')
        self.assertIsNotNone(self.node)
        self.env = {'SystemRoot': os.environ['SystemRoot'], 'WINDIR': os.environ['SystemRoot'],
                    'TEMP': str(self.state), 'TMP': str(self.state), 'HOME': str(self.state),
                    'USERPROFILE': str(self.state), 'APPDATA': str(self.state), 'LOCALAPPDATA': str(self.state)}

    def run_code(self, source, **limits):
        fixture = self.code / 'main.cjs'
        fixture.write_text(source)
        return run_isolated_utility([self.node, '--preserve-symlinks', '--preserve-symlinks-main', str(fixture)],
                                    cwd=self.state, read_directories=[self.code], write_directories=[self.state],
                                    environment=self.env, owner_id=uuid.uuid4().hex, **limits)

    def test_captures_finite_output_without_leaking_ambient_environment(self):
        os.environ['MCP_FIXTURE_SECRET'] = 'never-inherited'
        self.addCleanup(os.environ.pop, 'MCP_FIXTURE_SECRET', None)
        result = self.run_code("process.stdout.write(JSON.stringify({secret:process.env.MCP_FIXTURE_SECRET||null}));")
        self.assertEqual(0, result.exit_code)
        self.assertEqual(b'{"secret":null}', result.stdout)

    def test_stops_an_output_flood_and_enforces_deadline(self):
        with self.assertRaises(WindowsProcessError):
            self.run_code("process.stdout.write('x'.repeat(200000));", stdout_limit=1024)
        with self.assertRaises(WindowsProcessError):
            self.run_code("setInterval(()=>{},100);", timeout=0.2)

    def test_package_child_process_is_denied_by_job_limit(self):
        # Positive control: this Node child can write the sentinel outside the sandbox.
        import subprocess
        child = self.code / 'child.cjs'
        child.write_text("require('fs').writeFileSync(process.argv[2], 'executed');")
        sentinel = self.state / 'child-executed'
        subprocess.run([self.node, str(child), str(sentinel)], check=True, timeout=3)
        self.assertTrue(sentinel.is_file())
        sentinel.unlink()
        source = ("const cp=require('child_process');"
                  "const c=cp.spawn(process.execPath,[" + repr(str(child)).replace('\\\\', '/') + "," + repr(str(sentinel)).replace('\\\\', '/') + "]);"
                  "c.on('error',()=>process.stdout.write('denied'));"
                  "setTimeout(()=>process.exit(0),400);")
        try:
            result = self.run_code(source, timeout=2)
            self.assertNotIn(b'allowed', result.stdout)
        except WindowsProcessError as error:
            # Some Windows policies block spawn until the owning utility deadline.
            # The runner still proves its original Job empty before returning.
            self.assertIn('deadline', str(error))
        self.assertFalse(sentinel.exists(), 'No package child can execute its first filesystem effect')


if __name__ == '__main__':
    unittest.main()
