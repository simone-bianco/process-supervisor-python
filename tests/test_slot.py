import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from pathlib import Path

from py_laravel_supervisor.contracts import DesiredManifest
from py_laravel_supervisor.events import EventStore
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.slot import ManagedSlot
from py_laravel_supervisor.windows import close_handle, create_job


@unittest.skipUnless(os.name == "nt", "Windows slot coverage")
class ManagedSlotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.package_root = Path(__file__).resolve().parents[1]
        self.fixture = self.package_root / "tests" / "fixtures" / "queue_once_fake.py"

    def test_success_failed_and_empty_are_healthy_cycles(self) -> None:
        for mode, expected in [("success", "success"), ("failed", "failed"), ("empty", "empty")]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                slot, anchor, store, manifest = self._slot(temporary, mode=mode, watchdog=0)
                try:
                    slot.spawn(manifest)
                    result = self._until_finished(slot)
                    self.assertTrue(result.healthy_completion)
                    self.assertFalse(result.process_failure)
                    self.assertEqual(expected, slot.last_outcome)
                    self.assertEqual(0, len(slot.crashes))
                    self.assertEqual(0, slot.status()["restart_count"])
                    self.assertEqual("idle", slot.status()["state"])
                    self.assertEqual("clean", slot.ledger.read()["state"])
                    persisted = "\n".join(
                        path.read_text(encoding="utf-8")
                        for path in store.paths.root.rglob("*.json")
                    )
                    self.assertNotIn("fixture-secret", persisted)
                finally:
                    slot.close()
                    close_handle(anchor)

    def test_healthy_recycle_deadline_is_based_on_post_cleanup_clock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            slot, anchor, _, manifest = self._slot(
                temporary,
                mode="empty",
                watchdog=0,
            )
            original = slot._ensure_job_quiescent

            def slow_quiescent(job_handle: int, *, timeout: float) -> bool:
                time.sleep(0.20)
                return original(job_handle, timeout=timeout)

            try:
                slot._ensure_job_quiescent = slow_quiescent
                slot.spawn(manifest)
                result = self._until_finished(slot)
                completed_at = time.monotonic()
                self.assertTrue(result.healthy_completion)
                self.assertEqual("idle", slot.status()["state"])
                self.assertGreater(slot.next_spawn_at - completed_at, 0.15)
                self.assertEqual(0, slot.status()["restart_count"])
            finally:
                slot.close()
                close_handle(anchor)

    def test_child_environment_is_allowlisted_and_does_not_inherit_parent_secret(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "child-env.txt"

            def builder(_manifest, _group):
                script = (
                    "import os, pathlib; "
                    f"pathlib.Path({str(marker)!r}).write_text("
                    "'APP_ENV='+str(os.getenv('APP_ENV'))+'\\nUSERPROFILE='+str(os.getenv('USERPROFILE'))+'\\nOPENAI_API_KEY='+str(os.getenv('OPENAI_API_KEY'))+'\\n', "
                    "encoding='utf-8')"
                )
                return (sys.executable, "-c", script)

            with patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "parent-secret-canary",
                    "USERPROFILE": r"C:\\Users\\supervisor-test",
                },
                clear=False,
            ):
                slot, anchor, store, manifest = self._slot(
                    temporary,
                    mode="empty",
                    watchdog=0,
                    child_environment={"APP_ENV": "testing"},
                    builder_override=builder,
                )
                try:
                    slot.spawn(manifest)
                    result = self._until_finished(slot)
                    self.assertTrue(result.healthy_completion)
                    observed = marker.read_text(encoding="utf-8")
                    self.assertIn("APP_ENV=testing", observed)
                    self.assertIn(r"USERPROFILE=C:\\Users\\supervisor-test", observed)
                    self.assertIn("OPENAI_API_KEY=None", observed)
                    self.assertNotIn("parent-secret-canary", observed)
                    persisted = "\n".join(
                        path.read_text(encoding="utf-8")
                        for path in store.paths.root.rglob("*.json")
                    )
                    self.assertNotIn("parent-secret-canary", persisted)
                finally:
                    slot.close()
                    close_handle(anchor)

    def test_nonzero_process_exit_counts_as_crash_not_job_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            slot, anchor, _, manifest = self._slot(temporary, mode="crash", watchdog=0)
            try:
                slot.spawn(manifest)
                result = self._until_finished(slot)
                self.assertTrue(result.process_failure)
                self.assertEqual(1, len(slot.crashes))
                self.assertFalse(slot.fatal)
            finally:
                slot.close()
                close_handle(anchor)

    def test_watchdog_terminates_exact_hung_slot_and_records_process_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            slot, anchor, _, manifest = self._slot(temporary, mode="hang", watchdog=1)
            try:
                slot.spawn(manifest)
                deadline = time.monotonic() + 4.0
                result = None
                while time.monotonic() < deadline:
                    result = slot.tick()
                    if not slot.active:
                        break
                    time.sleep(0.03)
                self.assertFalse(slot.active)
                self.assertIsNotNone(result)
                self.assertTrue(result.process_failure)
                self.assertEqual(1, len(slot.crashes))
                self.assertEqual("clean", slot.ledger.read()["state"])
            finally:
                slot.close()
                close_handle(anchor)

    def _slot(
        self,
        temporary: str,
        *,
        mode: str,
        watchdog: int,
        child_environment: dict[str, str] | None = None,
        builder_override=None,
    ):
        installation = "f" * 32
        store = RuntimeStore(Path(temporary) / "runtime")
        store.initialize(installation)
        manifest = DesiredManifest.from_mapping(
            {
                "schema_version": 1,
                "installation_id": installation,
                "revision": 1,
                "enabled": True,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "runtime": {
                    "project_root": str(self.package_root.resolve()),
                    "php_executable": str(Path(sys.executable).resolve()),
                    "child_environment": child_environment or {},
                },
                "groups": [
                    {
                        "id": "queue-default",
                        "kind": "queue_once",
                        "generation": 1,
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
                            "watchdog_seconds": watchdog,
                        },
                    }
                ],
            }
        )
        group = manifest.groups[0]
        anchor = create_job(f"Local\\PyLaravelSupervisor-slot-test-{time.time_ns()}")
        events = EventStore(store, installation)

        def default_builder(_manifest, _group):
            return (sys.executable, str(self.fixture), "--mode", mode, "--sleep", "0.03")

        slot = ManagedSlot(
            store=store,
            events=events,
            installation_id=installation,
            supervisor_incarnation="a" * 32,
            anchor_job_handle=anchor,
            group=group,
            slot=0,
            command_builder=builder_override or default_builder,
        )
        return slot, anchor, store, manifest

    @staticmethod
    def _until_finished(slot: ManagedSlot):
        deadline = time.monotonic() + 3.0
        last = None
        while time.monotonic() < deadline:
            last = slot.tick()
            if not slot.active:
                return last
            time.sleep(0.02)
        raise AssertionError("managed slot did not finish within the bounded test deadline")


if __name__ == "__main__":
    unittest.main()