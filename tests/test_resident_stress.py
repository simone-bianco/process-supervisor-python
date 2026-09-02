import ctypes
import os
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from py_laravel_supervisor.control import SupervisorControl
from py_laravel_supervisor.locks import WindowsMutex, mutex_name
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.windows import close_handle, open_job, terminate_job, wait_for_job_absent


@unittest.skipUnless(os.name == "nt", "Windows resident stress coverage")
class ResidentStressTest(unittest.TestCase):
    def setUp(self) -> None:
        self.package_root = Path(__file__).resolve().parents[1]
        self.resident_fixture = self.package_root / "tests" / "fixtures" / "resident_entry.py"
        self.queue_resident_fixture = self.package_root / "tests" / "fixtures" / "resident_queue_entry.py"

    def test_concurrent_ensure_running_never_spawns_two_residents(self) -> None:
        installation = "1" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            self._apply_desired(store, installation, enabled=True, revision=1, groups=[])

            def builder(runtime, install, incarnation, attempt, nonce, anchor_name):
                direct_python = getattr(sys, "_base_executable", sys.executable)
                return (
                    direct_python,
                    str(self.resident_fixture),
                    "--runtime-root",
                    str(runtime),
                    "--installation-id",
                    install,
                    "--incarnation",
                    incarnation,
                    "--attempt-id",
                    attempt,
                    "--ready-nonce",
                    nonce,
                    "--anchor-job-name",
                    anchor_name,
                    "--startup-delay",
                    "0.20",
                )

            controls = [
                SupervisorControl(store.paths.root, installation, resident_argv_builder=builder),
                SupervisorControl(store.paths.root, installation, resident_argv_builder=builder),
            ]
            anchor_name = None
            try:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = list(executor.map(lambda control: control.ensure_running(ready_timeout_seconds=3.0), controls))
                statuses = sorted(str(result["status"]) for result in results)
                self.assertEqual(["already_running", "started"], statuses)
                state = controls[0].ledger.read()
                self.assertEqual("resident_active", state["state"])
                anchor_name = state["ownership_id"]
                self._apply_desired(store, installation, enabled=False, revision=2, groups=[])
                controls[0].shutdown(graceful_timeout_seconds=2.0, hard_timeout_seconds=2.0)
                self.assertEqual("clean", controls[0].ledger.read()["state"])
            finally:
                self._apply_desired(store, installation, enabled=False, revision=3, groups=[])
                self._cleanup(controls[0], anchor_name)

    def test_resident_survives_status_readers_that_deny_delete_sharing(self) -> None:
        installation = "3" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            self._apply_desired(store, installation, enabled=True, revision=1, groups=[])
            control = SupervisorControl(store.paths.root, installation)
            anchor_name = None
            try:
                self.assertEqual("started", control.ensure_running(ready_timeout_seconds=3.0)["status"])
                anchor_name = control.ledger.read()["ownership_id"]
                deadline = time.monotonic() + 3.0
                opened = 0
                while time.monotonic() < deadline:
                    if store.paths.status.exists():
                        handle = self._open_without_delete_share(store.paths.status)
                        if handle is not None:
                            opened += 1
                            time.sleep(0.08)
                            ctypes.windll.kernel32.CloseHandle(handle)
                    time.sleep(0.01)

                self.assertGreater(opened, 10)
                self.assertEqual("enabled", store.gate(installation)["state"])
                self.assertEqual("resident_active", control.ledger.read()["state"])
                self.assertEqual("ready", store.read_json(store.paths.status)["summary"])
            finally:
                self._apply_desired(store, installation, enabled=False, revision=2, groups=[])
                self._cleanup(control, anchor_name)

    def test_resident_recycles_fake_queue_and_applies_generation_change(self) -> None:
        installation = "2" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            self._apply_desired(store, installation, enabled=True, revision=1, groups=[self._queue_group(generation=1)])

            def builder(runtime, install, incarnation, attempt, nonce, anchor_name):
                direct_python = getattr(sys, "_base_executable", sys.executable)
                return (
                    direct_python,
                    str(self.queue_resident_fixture),
                    "--runtime-root",
                    str(runtime),
                    "--installation-id",
                    install,
                    "--incarnation",
                    incarnation,
                    "--attempt-id",
                    attempt,
                    "--ready-nonce",
                    nonce,
                    "--anchor-job-name",
                    anchor_name,
                )

            control = SupervisorControl(store.paths.root, installation, resident_argv_builder=builder)
            anchor_name = None
            try:
                self.assertEqual("started", control.ensure_running(ready_timeout_seconds=3.0)["status"])
                anchor_name = control.ledger.read()["ownership_id"]
                first = self._wait_status(
                    store,
                    lambda status: self._instance_state(status, "queue-default") == "idle",
                    timeout=4.0,
                )
                self.assertEqual("ready", first["summary"])
                self.assertEqual(0, self._restart_count(first, "queue-default"))
                self._apply_desired(store, installation, enabled=True, revision=2, groups=[self._queue_group(generation=2)])
                changed = self._wait_status(
                    store,
                    lambda status: self._group_generation(status, "queue-default") == 2,
                    timeout=4.0,
                )
                self.assertEqual(2, self._group_generation(changed, "queue-default"))
                self._apply_desired(store, installation, enabled=False, revision=3, groups=[])
                control.shutdown(graceful_timeout_seconds=2.0, hard_timeout_seconds=2.0)
                self.assertEqual("clean", control.ledger.read()["state"])
            finally:
                self._apply_desired(store, installation, enabled=False, revision=4, groups=[])
                self._cleanup(control, anchor_name)

    def _apply_desired(self, store: RuntimeStore, installation: str, *, enabled: bool, revision: int, groups: list[dict]) -> None:
        store.initialize(installation)
        with WindowsMutex(mutex_name(installation, "transition"), timeout_ms=2000):
            store.write_json(
                store.paths.desired,
                {
                    "schema_version": 1,
                    "installation_id": installation,
                    "revision": revision,
                    "enabled": enabled,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "runtime": {
                        "project_root": str(self.package_root.resolve()),
                        "php_executable": str(Path(sys.executable).resolve()),
                        "child_environment": {},
                    },
                    "groups": groups,
                },
            )
            if enabled:
                store.enable_gate(installation, revision)
            else:
                store.disable_gate(installation, revision)

    @staticmethod
    def _queue_group(*, generation: int) -> dict:
        return {
            "id": "queue-default",
            "kind": "queue_once",
            "generation": generation,
            "desired_processes": 1,
            "stop_grace_seconds": 0.5,
            "restart_policy": {
                "enabled": True,
                "base_delay_seconds": 0.05,
                "max_delay_seconds": 0.2,
                "crash_window_seconds": 5,
                "max_crashes": 3,
            },
            "queue": {
                "connection": "database",
                "queues": ["default"],
                "backoff": [0],
                "tries": 1,
                "sleep_seconds": 0,
                "watchdog_seconds": 0,
            },
        }

    @staticmethod
    def _open_without_delete_share(path: Path) -> int | None:
        kernel32 = ctypes.windll.kernel32
        kernel32.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_void_p,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_void_p,
        ]
        kernel32.CreateFileW.restype = ctypes.c_void_p
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel32.CreateFileW(
            str(path),
            0x80000000,
            0x00000001 | 0x00000002,
            None,
            3,
            0x80,
            None,
        )
        invalid = ctypes.c_void_p(-1).value
        return None if handle in (None, 0, invalid) else int(handle)

    @staticmethod
    def _wait_status(store: RuntimeStore, predicate, *, timeout: float):
        deadline = time.monotonic() + timeout
        latest = None
        while time.monotonic() < deadline:
            latest = store.read_json(store.paths.status, required=False)
            if isinstance(latest, dict) and predicate(latest):
                return latest
            time.sleep(0.03)
        raise AssertionError(f"status condition was not reached; latest={latest!r}")

    @staticmethod
    def _instance_state(status: dict, group_id: str) -> str | None:
        for group in status.get("groups", []):
            if group.get("id") == group_id:
                instances = group.get("instances") or []
                return str(instances[0].get("state")) if instances else None
        return None

    @staticmethod
    def _restart_count(status: dict, group_id: str) -> int:
        for group in status.get("groups", []):
            if group.get("id") == group_id:
                instances = group.get("instances") or []
                return max((int(instance.get("restart_count", 0)) for instance in instances), default=0)
        return 0

    @staticmethod
    def _group_generation(status: dict, group_id: str) -> int | None:
        for group in status.get("groups", []):
            if group.get("id") == group_id:
                return int(group.get("generation"))
        return None

    @staticmethod
    def _cleanup(control: SupervisorControl, anchor_name) -> None:
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
            assert wait_for_job_absent(anchor_name, 2.0)


if __name__ == "__main__":
    unittest.main()