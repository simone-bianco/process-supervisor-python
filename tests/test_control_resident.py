import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from py_laravel_supervisor.control import SupervisorControl
from py_laravel_supervisor.lifecycle import LifecycleLedger
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.windows import close_handle, open_job, terminate_job, wait_for_job_absent


@unittest.skipUnless(os.name == "nt", "Windows resident control coverage")
class SupervisorControlResidentTest(unittest.TestCase):
    def test_bootstrap_is_idempotent_and_disable_shuts_down_exact_anchor(self) -> None:
        installation = "c" * 32
        package_root = Path(__file__).resolve().parents[1]
        fixture = package_root / "tests" / "fixtures" / "resident_entry.py"
        with tempfile.TemporaryDirectory() as temporary:
            runtime_root = Path(temporary) / "runtime"
            store = RuntimeStore(runtime_root)
            store.initialize(installation)
            self._write_desired(store, installation, enabled=True, revision=1, package_root=package_root)
            store.enable_gate(installation, 1)

            def resident_argv(runtime, install, incarnation, attempt, ready_nonce, anchor_name):
                return (
                    sys.executable,
                    str(fixture),
                    "--runtime-root",
                    str(runtime),
                    "--installation-id",
                    install,
                    "--incarnation",
                    incarnation,
                    "--attempt-id",
                    attempt,
                    "--ready-nonce",
                    ready_nonce,
                    "--anchor-job-name",
                    anchor_name,
                )

            control = SupervisorControl(
                runtime_root,
                installation,
                resident_argv_builder=resident_argv,
            )
            anchor_name = None
            try:
                started = control.ensure_running(ready_timeout_seconds=3.0)
                self.assertEqual("started", started["status"])
                again = control.ensure_running(ready_timeout_seconds=1.0)
                self.assertEqual("already_running", again["status"])
                ledger = LifecycleLedger(
                    store,
                    store.paths.supervisor_ledger,
                    installation_id=installation,
                    role="resident",
                )
                state = ledger.read()
                self.assertEqual("resident_active", state["state"])
                anchor_name = state["ownership_id"]
                self.assertIsInstance(anchor_name, str)

                self._write_desired(store, installation, enabled=False, revision=2, package_root=package_root)
                store.disable_gate(installation, 2)
                stopped = control.shutdown(
                    graceful_timeout_seconds=3.0,
                    hard_timeout_seconds=2.0,
                )
                self.assertIn(stopped["status"], {"already_clean", "recovered"})
                self.assertEqual("clean", ledger.read()["state"])
                self.assertFalse(store.paths.ready.exists())
                self.assertTrue(wait_for_job_absent(anchor_name, 2.0))
            finally:
                self._write_desired(store, installation, enabled=False, revision=3, package_root=package_root)
                store.disable_gate(installation, 3)
                try:
                    control.shutdown(graceful_timeout_seconds=0.2, hard_timeout_seconds=1.0)
                except Exception:
                    pass
                if isinstance(anchor_name, str):
                    handle = open_job(anchor_name, terminate=True)
                    if handle is not None:
                        try:
                            terminate_job(handle)
                        finally:
                            close_handle(handle)
                    self.assertTrue(wait_for_job_absent(anchor_name, 2.0))

    @staticmethod
    def _write_desired(
        store: RuntimeStore,
        installation: str,
        *,
        enabled: bool,
        revision: int,
        package_root: Path,
    ) -> None:
        store.write_json(
            store.paths.desired,
            {
                "schema_version": 1,
                "installation_id": installation,
                "revision": revision,
                "enabled": enabled,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "runtime": {
                    "project_root": str(package_root.resolve()),
                    "php_executable": str(Path(sys.executable).resolve()),
                    "child_environment": {},
                },
                "groups": [],
            },
        )


if __name__ == "__main__":
    unittest.main()