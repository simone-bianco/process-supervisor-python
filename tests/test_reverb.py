import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from py_laravel_supervisor.backend import WindowsProcessBackend
from py_laravel_supervisor.contracts import DesiredManifest
from py_laravel_supervisor.events import EventStore
from py_laravel_supervisor.reverb import ReverbGracefulSignaler
from py_laravel_supervisor.resident import SupervisorResident
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.slot import ManagedSlot
from py_laravel_supervisor.windows import (
    close_handle,
    create_job,
    job_active_processes,
    spawn_process,
    terminate_job,
)


@unittest.skipUnless(os.name == "nt", "Windows Reverb signaling coverage")
class ReverbGracefulSignalerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.package_root = Path(__file__).resolve().parents[1]
        self.fixture = self.package_root / "tests" / "fixtures" / "reverb_signal_fake.py"

    def test_successful_signal_does_not_count_as_target_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "signal-env.txt"
            manifest = self._manifest(stop_grace=1.0)
            group = manifest.groups[0]
            anchor = create_job(f"Local\\PyLaravelSupervisor-reverb-anchor-{time.time_ns()}")
            target_job = create_job(f"Local\\PyLaravelSupervisor-reverb-target-{time.time_ns()}")
            target = spawn_process(
                (sys.executable, "-c", "import time; time.sleep(30)"),
                cwd=self.package_root,
                environment={},
                job_handles=[anchor, target_job],
                exact_job_handle=target_job,
                cleanup_job_handle=target_job,
                capture_output=False,
            )
            try:
                signaler = ReverbGracefulSignaler(
                    installation_id=manifest.installation_id,
                    supervisor_incarnation="a" * 32,
                    anchor_job_handle=anchor,
                    command_builder=lambda _manifest: (
                        sys.executable,
                        str(self.fixture),
                        "--mode",
                        "success",
                        "--marker",
                        str(marker),
                    ),
                )
                with patch.dict(os.environ, {"OPENAI_API_KEY": "parent-secret-canary"}, clear=False):
                    self.assertTrue(
                        signaler.signal(
                            manifest,
                            group,
                            target_job,
                            manifest.runtime.child_environment_mapping(),
                        )
                    )
                self.assertIsNone(target.poll())
                self.assertGreater(job_active_processes(target_job), 0)
                text = marker.read_text(encoding="utf-8")
                self.assertIn("APP_ENV=testing", text)
                self.assertIn("OPENAI_API_KEY=None", text)
                self.assertNotIn("parent-secret-canary", text)
            finally:
                terminate_job(target_job)
                target.wait(2.0)
                target.close()
                close_handle(target_job)
                close_handle(anchor)

    def test_hung_signal_is_bounded_and_leaves_target_for_exact_grace_policy(self) -> None:
        manifest = self._manifest(stop_grace=0.2)
        group = manifest.groups[0]
        anchor = create_job(f"Local\\PyLaravelSupervisor-reverb-anchor-{time.time_ns()}")
        target_job = create_job(f"Local\\PyLaravelSupervisor-reverb-target-{time.time_ns()}")
        target = spawn_process(
            (sys.executable, "-c", "import time; time.sleep(30)"),
            cwd=self.package_root,
            environment={},
            job_handles=[anchor, target_job],
            exact_job_handle=target_job,
            cleanup_job_handle=target_job,
            capture_output=False,
        )
        started = time.monotonic()
        try:
            signaler = ReverbGracefulSignaler(
                installation_id=manifest.installation_id,
                supervisor_incarnation="b" * 32,
                anchor_job_handle=anchor,
                command_builder=lambda _manifest: (
                    sys.executable,
                    str(self.fixture),
                    "--mode",
                    "hang",
                ),
            )
            self.assertFalse(
                signaler.signal(
                    manifest,
                    group,
                    target_job,
                    manifest.runtime.child_environment_mapping(),
                )
            )
            self.assertLess(time.monotonic() - started, 2.0)
            self.assertIsNone(target.poll())
            self.assertGreater(job_active_processes(target_job), 0)
        finally:
            terminate_job(target_job)
            target.wait(2.0)
            target.close()
            close_handle(target_job)
            close_handle(anchor)

    def test_managed_slot_graceful_signal_reaches_exact_target_exit_without_hard_kill(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "graceful.marker"
            manifest = self._manifest(stop_grace=0.8)
            backend = WindowsProcessBackend()
            anchor = backend.create_job(f"Local\\PyLaravelSupervisor-reverb-slot-anchor-{time.time_ns()}")
            signaler = ReverbGracefulSignaler(
                installation_id=manifest.installation_id,
                supervisor_incarnation="c" * 32,
                anchor_job_handle=anchor,
                backend=backend,
                command_builder=lambda _manifest: (
                    sys.executable,
                    str(self.fixture),
                    "--mode",
                    "success",
                    "--marker",
                    str(marker),
                ),
            )
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(manifest.installation_id)
            target_script = (
                "import pathlib,time,sys; p=pathlib.Path(sys.argv[1]); "
                "deadline=time.monotonic()+3; "
                "exec('while time.monotonic() < deadline and not p.exists():\\n time.sleep(0.02)'); "
                "raise SystemExit(0 if p.exists() else 9)"
            )
            slot = ManagedSlot(
                store=store,
                events=EventStore(store, manifest.installation_id),
                installation_id=manifest.installation_id,
                supervisor_incarnation="c" * 32,
                anchor_job_handle=anchor,
                group=manifest.groups[0],
                slot=0,
                command_builder=lambda _manifest, _group: (
                    sys.executable,
                    "-c",
                    target_script,
                    str(marker),
                ),
                backend=backend,
            )
            resident = SupervisorResident(
                runtime_root=store.paths.root,
                installation_id=manifest.installation_id,
                incarnation="c" * 32,
                attempt_id="1" * 32,
                ready_nonce="2" * 32,
                anchor_handle=anchor,
                reverb_signaler=signaler,
            )
            try:
                slot.spawn(manifest)
                target_handle = slot.job_handle
                self.assertIsNotNone(target_handle)
                with patch.object(backend, "terminate_job", wraps=backend.terminate_job) as terminate:
                    resident._request_slot_stop(manifest, slot)
                    result = self._until_slot_stopped(slot)
                self.assertTrue(result.healthy_completion)
                terminated_handles = [call.args[0] for call in terminate.call_args_list]
                self.assertNotIn(target_handle, terminated_handles)
                self.assertEqual("clean", slot.ledger.read()["state"])
            finally:
                slot.close()
                backend.close_handle(anchor)

    def test_managed_slot_hard_stops_exact_target_after_failed_graceful_signal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self._manifest(stop_grace=0.35)
            backend = WindowsProcessBackend()
            anchor = backend.create_job(f"Local\\PyLaravelSupervisor-reverb-slot-anchor-{time.time_ns()}")
            signaler = ReverbGracefulSignaler(
                installation_id=manifest.installation_id,
                supervisor_incarnation="d" * 32,
                anchor_job_handle=anchor,
                backend=backend,
                command_builder=lambda _manifest: (
                    sys.executable,
                    str(self.fixture),
                    "--mode",
                    "hang",
                ),
            )
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(manifest.installation_id)
            slot = ManagedSlot(
                store=store,
                events=EventStore(store, manifest.installation_id),
                installation_id=manifest.installation_id,
                supervisor_incarnation="d" * 32,
                anchor_job_handle=anchor,
                group=manifest.groups[0],
                slot=0,
                command_builder=lambda _manifest, _group: (
                    sys.executable,
                    "-c",
                    "import time; time.sleep(30)",
                ),
                backend=backend,
            )
            resident = SupervisorResident(
                runtime_root=store.paths.root,
                installation_id=manifest.installation_id,
                incarnation="d" * 32,
                attempt_id="3" * 32,
                ready_nonce="4" * 32,
                anchor_handle=anchor,
                reverb_signaler=signaler,
            )
            try:
                slot.spawn(manifest)
                target_handle = slot.job_handle
                self.assertIsNotNone(target_handle)
                with patch.object(backend, "terminate_job", wraps=backend.terminate_job) as terminate:
                    resident._request_slot_stop(manifest, slot)
                    self.assertTrue(slot.active)
                    result = self._until_slot_stopped(slot)
                self.assertTrue(result.healthy_completion)
                terminated_handles = [call.args[0] for call in terminate.call_args_list]
                self.assertIn(target_handle, terminated_handles)
                self.assertEqual("clean", slot.ledger.read()["state"])
            finally:
                slot.close()
                backend.close_handle(anchor)

    @staticmethod
    def _until_slot_stopped(slot: ManagedSlot):
        deadline = time.monotonic() + 4.0
        result = None
        while time.monotonic() < deadline:
            result = slot.tick()
            if not slot.active:
                return result
            time.sleep(0.02)
        raise AssertionError("fake Reverb slot did not stop within the bounded deadline")

    def _manifest(self, *, stop_grace: float) -> DesiredManifest:
        return DesiredManifest.from_mapping(
            {
                "schema_version": 1,
                "installation_id": "r" * 32,
                "revision": 1,
                "enabled": True,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "runtime": {
                    "project_root": str(self.package_root.resolve()),
                    "php_executable": str(Path(sys.executable).resolve()),
                    "child_environment": {"APP_ENV": "testing"},
                },
                "groups": [
                    {
                        "id": "reverb",
                        "kind": "reverb",
                        "generation": 1,
                        "desired_processes": 1,
                        "stop_grace_seconds": stop_grace,
                        "restart_policy": None,
                        "queue": None,
                    }
                ],
            }
        )


if __name__ == "__main__":
    unittest.main()