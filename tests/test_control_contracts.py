import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from py_laravel_supervisor.control import ControlError, SupervisorControl
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.windows import (
    close_handle,
    create_job,
    job_exists,
    process_exists,
    spawn_process as windows_spawn_process,
    terminate_job,
)


@unittest.skipUnless(os.name == "nt", "Windows control contract coverage")
class SupervisorControlContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.installation = "7" * 32
        self.package_root = Path(__file__).resolve().parents[1]
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temporary.name) / "runtime"
        self.control = SupervisorControl(self.runtime_root, self.installation)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_apply_desired_is_monotonic_idempotent_and_fail_closed(self) -> None:
        enabled = self._desired(enabled=True, revision=1)
        self.assertEqual(
            {
                "status": "applied",
                "enabled": True,
                "revision": 1,
                "spawn_gate": "enabled",
                "recovery_reduction": False,
            },
            self.control.apply_desired(enabled),
        )
        self.assertEqual(enabled, self.control.store.read_json(self.control.store.paths.desired))
        self.assertEqual("enabled", self.control.store.gate(self.installation)["state"])

        self.assertEqual("applied", self.control.apply_desired(enabled)["status"])
        changed_same_revision = {**enabled, "groups": [self._queue_group()]}
        with self.assertRaises(ControlError):
            self.control.apply_desired(changed_same_revision)
        with self.assertRaises(ControlError):
            self.control.apply_desired(self._desired(enabled=True, revision=0))

        disabled = self._desired(enabled=False, revision=2)
        self.control.apply_desired(disabled)
        self.assertEqual("disabled", self.control.store.gate(self.installation)["state"])
        self.assertFalse(self.control.store.desired_manifest().enabled)

    def test_disable_gate_preserves_corrupt_desired_and_never_spawns(self) -> None:
        enabled = self._desired(enabled=True, revision=7)
        self.control.apply_desired(enabled)
        desired_path = self.control.store.paths.desired
        desired_path.write_bytes(b"")

        result = self.control.disable_gate()

        self.assertEqual(
            {"status": "disabled", "spawn_gate": "disabled", "revision": 7},
            result,
        )
        self.assertEqual(b"", desired_path.read_bytes())
        self.assertEqual("disabled", self.control.store.gate(self.installation)["state"])
        self.assertFalse(self.control.store.paths.ready.exists())
        self.assertFalse(self.control.store.paths.supervisor_ledger.exists())

    def test_recovery_gate_allows_only_idempotent_or_monotonic_desired_reduction(self) -> None:
        enabled = self._desired(enabled=True, revision=1)
        enabled["groups"] = [self._queue_group(), self._reverb_group()]
        self.control.apply_desired(enabled)
        self.control.store.mark_recovery_required(
            self.installation,
            1,
            "synthetic_recovery_gate",
        )

        reduced = {
            **enabled,
            "revision": 2,
            "generated_at": "2026-09-01T12:00:00+00:00",
            "groups": [
                dict(self._queue_group(), desired_processes=1),
                dict(self._reverb_group(), desired_processes=0),
            ],
        }
        applied = self.control.apply_desired(reduced)
        self.assertEqual("recovery_required", applied["spawn_gate"])
        self.assertTrue(applied["recovery_reduction"])
        self.assertEqual(
            0,
            next(
                group.desired_processes
                for group in self.control.store.desired_manifest().groups
                if group.id == "reverb"
            ),
        )
        self.assertEqual(
            "recovery_required",
            self.control.store.gate(self.installation)["state"],
        )

        retry = self.control.apply_desired(reduced)
        self.assertEqual("recovery_required", retry["spawn_gate"])
        self.assertFalse(retry["recovery_reduction"])

        increased = {
            **reduced,
            "revision": 3,
            "groups": [self._queue_group(), self._reverb_group()],
        }
        with self.assertRaises(ControlError):
            self.control.apply_desired(increased)

        changed_generation = {
            **reduced,
            "revision": 3,
            "groups": [
                dict(self._queue_group(), desired_processes=1),
                dict(self._reverb_group(), desired_processes=0, generation=2),
            ],
        }
        with self.assertRaises(ControlError):
            self.control.apply_desired(changed_generation)

        self.assertEqual(2, self.control.store.desired_manifest().revision)
        self.assertEqual(
            "recovery_required",
            self.control.store.gate(self.installation)["state"],
        )

    def test_resident_outlives_short_lived_control_process(self) -> None:
        self.control.apply_desired(self._desired(enabled=True, revision=1))
        package_src = str((self.package_root / "src").resolve())
        script = (
            "import sys\n"
            f"sys.path.insert(0, {package_src!r})\n"
            "from py_laravel_supervisor.control import SupervisorControl\n"
            f"control=SupervisorControl({str(self.runtime_root)!r}, {self.installation!r})\n"
            "result=control.ensure_running(ready_timeout_seconds=3.0)\n"
            "raise SystemExit(0 if result.get('status') in {'started','already_running'} else 2)\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=self.package_root,
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        self.assertEqual(0, completed.returncode, completed.stderr or completed.stdout)

        # The resident must remain alive after the short-lived control process has
        # fully exited and Windows has had time to close all parent-owned handles.
        time.sleep(1.5)

        try:
            deadline = time.monotonic() + 2.0
            ready = None
            while time.monotonic() < deadline:
                ready = self.control.store.read_json(self.control.store.paths.ready, required=False)
                if isinstance(ready, dict) and isinstance(ready.get("pid"), int) and process_exists(ready["pid"]):
                    break
                time.sleep(0.05)
            self.assertIsInstance(ready, dict)
            resident_pid = ready.get("pid") if isinstance(ready, dict) else None
            self.assertIsInstance(resident_pid, int)
            self.assertTrue(process_exists(resident_pid))

            observed = self.control.ensure_running(ready_timeout_seconds=2.0)
            self.assertEqual("already_running", observed.get("status"))
            self.assertEqual(resident_pid, observed.get("pid"))
        finally:
            try:
                self.control.apply_desired(self._desired(enabled=False, revision=2))
            finally:
                stopped = self.control.shutdown(
                    graceful_timeout_seconds=2.0,
                    hard_timeout_seconds=2.0,
                )
                self.assertIn(stopped.get("status"), {"stopped", "recovered"})

    def test_resident_breaks_away_from_a_terminating_caller_job(self) -> None:
        self.control.apply_desired(self._desired(enabled=True, revision=1))
        package_src = str((self.package_root / "src").resolve())
        script = (
            "import sys\n"
            f"sys.path.insert(0, {package_src!r})\n"
            "from py_laravel_supervisor.control import SupervisorControl\n"
            f"control=SupervisorControl({str(self.runtime_root)!r}, {self.installation!r})\n"
            "result=control.ensure_running(ready_timeout_seconds=3.0)\n"
            "raise SystemExit(0 if result.get('status') in {'started','already_running'} else 2)\n"
        )
        caller_job = create_job(
            f"Local\\PyLaravelSupervisorTest-caller-{time.time_ns()}",
            allow_breakaway=True,
        )
        caller_process = None
        try:
            caller_process = windows_spawn_process(
                [getattr(sys, "_base_executable", sys.executable), "-c", script],
                cwd=self.package_root,
                environment={
                    "PYTHONPATH": package_src,
                    "PYTHONUTF8": "1",
                    "PYTHONIOENCODING": "utf-8",
                },
                job_handles=[caller_job],
                exact_job_handle=caller_job,
                cleanup_job_handle=caller_job,
                capture_output=False,
            )
            self.assertEqual(0, caller_process.wait(10.0))

            ready = self.control.store.read_json(self.control.store.paths.ready, required=False)
            self.assertIsInstance(ready, dict)
            resident_pid = ready.get("pid") if isinstance(ready, dict) else None
            self.assertIsInstance(resident_pid, int)
            self.assertTrue(process_exists(resident_pid))

            terminate_job(caller_job)
            time.sleep(0.75)
            self.assertTrue(process_exists(resident_pid))
            observed = self.control.ensure_running(ready_timeout_seconds=2.0)
            self.assertEqual("already_running", observed.get("status"))
            self.assertEqual(resident_pid, observed.get("pid"))
        finally:
            if caller_process is not None:
                caller_process.close()
            close_handle(caller_job)
            try:
                self.control.apply_desired(self._desired(enabled=False, revision=2))
            finally:
                stopped = self.control.shutdown(
                    graceful_timeout_seconds=2.0,
                    hard_timeout_seconds=2.0,
                )
                self.assertIn(stopped.get("status"), {"stopped", "recovered"})

    def test_default_resident_bootstrap_survives_control_exit_with_long_running_reverb_slot(self) -> None:
        project = Path(self.temporary.name) / "fake-laravel"
        project.mkdir(parents=True, exist_ok=True)
        artisan = project / "artisan"
        artisan.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
        enabled = self._desired(enabled=True, revision=1)
        enabled["runtime"] = {
            "project_root": str(project.resolve()),
            "php_executable": str(Path(sys.executable).resolve()),
            "child_environment": {},
        }
        enabled["groups"] = [self._reverb_group()]
        self.control.apply_desired(enabled)

        package_src = str((self.package_root / "src").resolve())
        script = (
            "import sys\n"
            f"sys.path.insert(0, {package_src!r})\n"
            "from py_laravel_supervisor.control import SupervisorControl\n"
            f"control=SupervisorControl({str(self.runtime_root)!r}, {self.installation!r})\n"
            "result=control.ensure_running(ready_timeout_seconds=3.0)\n"
            "raise SystemExit(0 if result.get('status') in {'started','already_running'} else 2)\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=self.package_root,
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        self.assertEqual(0, completed.returncode, completed.stderr or completed.stdout)

        try:
            time.sleep(3.0)
            ready = self.control.store.read_json(self.control.store.paths.ready, required=False)
            self.assertIsInstance(ready, dict)
            resident_pid = ready.get("pid") if isinstance(ready, dict) else None
            self.assertIsInstance(resident_pid, int)
            self.assertTrue(process_exists(resident_pid))
            observed = self.control.status()
            groups = observed.get("groups")
            self.assertIsInstance(groups, list)
            self.assertEqual("reverb", groups[0]["id"])
            self.assertEqual("running", groups[0]["instances"][0]["state"])
        finally:
            disabled = dict(enabled)
            disabled["revision"] = 2
            disabled["enabled"] = False
            disabled["groups"] = [dict(enabled["groups"][0], desired_processes=0)]
            try:
                self.control.apply_desired(disabled)
            finally:
                stopped = self.control.shutdown(
                    graceful_timeout_seconds=2.0,
                    hard_timeout_seconds=2.0,
                )
                self.assertIn(stopped.get("status"), {"stopped", "recovered"})

    def test_disable_remains_available_during_delayed_resident_spawn(self) -> None:
        self.control.apply_desired(self._desired(enabled=True, revision=1))
        entered = threading.Event()
        release = threading.Event()
        result: dict[str, object] = {}
        failure: list[BaseException] = []

        def resident_argv(_runtime, _install, _incarnation, _attempt, _nonce, _anchor):
            return (sys.executable, "-c", "import time; time.sleep(10)")

        control = SupervisorControl(
            self.runtime_root,
            self.installation,
            resident_argv_builder=resident_argv,
        )

        def delayed_spawn(*args, **kwargs):
            self.assertIs(kwargs.get("breakaway_from_parent_job"), True)
            entered.set()
            if not release.wait(timeout=2.0):
                raise RuntimeError("test did not release delayed spawn")
            return windows_spawn_process(*args, **kwargs)

        def ensure() -> None:
            try:
                result.update(control.ensure_running(ready_timeout_seconds=1.0))
            except BaseException as error:
                failure.append(error)

        thread = threading.Thread(target=ensure)
        anchor_name: str | None = None
        try:
            with patch("py_laravel_supervisor.control.spawn_process", side_effect=delayed_spawn):
                thread.start()
                self.assertTrue(entered.wait(timeout=2.0))
                state = control.ledger.read()
                anchor_name = str(state["ownership_id"])
                self.assertEqual("spawn_armed", state["state"])

                disabled = self._desired(enabled=False, revision=2)
                self.assertEqual("applied", self.control.apply_desired(disabled)["status"])
                self.assertEqual("disabled", self.control.store.gate(self.installation)["state"])

                release.set()
                thread.join(timeout=4.0)
                self.assertFalse(thread.is_alive())
        finally:
            release.set()
            thread.join(timeout=1.0)

        self.assertEqual([], failure)
        self.assertEqual("disabled", result.get("status"))
        self.assertEqual("clean", control.ledger.read()["state"])
        if anchor_name is not None:
            self.assertFalse(job_exists(anchor_name))

    def _desired(self, *, enabled: bool, revision: int) -> dict[str, object]:
        return {
            "schema_version": 1,
            "installation_id": self.installation,
            "revision": revision,
            "enabled": enabled,
            "generated_at": "2026-08-31T23:00:00+00:00",
            "runtime": {
                "project_root": str(self.package_root.resolve()),
                "php_executable": str(Path(sys.executable).resolve()),
                "child_environment": {},
            },
            "groups": [],
        }

    @staticmethod
    def _reverb_group() -> dict[str, object]:
        return {
            "id": "reverb",
            "kind": "reverb",
            "generation": 1,
            "desired_processes": 1,
            "stop_grace_seconds": 1,
            "restart_policy": {
                "enabled": True,
                "base_delay_seconds": 0.1,
                "max_delay_seconds": 1,
                "crash_window_seconds": 10,
                "max_crashes": 3,
            },
            "queue": None,
        }

    @staticmethod
    def _queue_group() -> dict[str, object]:
        return {
            "id": "queue-default",
            "kind": "queue_once",
            "generation": 1,
            "desired_processes": 1,
            "stop_grace_seconds": 1,
            "restart_policy": {
                "enabled": True,
                "base_delay_seconds": 0.1,
                "max_delay_seconds": 1,
                "crash_window_seconds": 10,
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


if __name__ == "__main__":
    unittest.main()