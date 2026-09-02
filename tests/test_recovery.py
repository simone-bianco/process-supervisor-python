import os
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from py_laravel_supervisor.contracts import ProcessGroupSpec
from py_laravel_supervisor.lifecycle import LifecycleLedger
from py_laravel_supervisor.locks import WindowsMutex, mutex_name
from py_laravel_supervisor.recovery import RecoveryError, RuntimeRecovery
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.scheduler import SchedulerTrigger
from py_laravel_supervisor.windows import close_handle, create_job


@unittest.skipUnless(os.name == "nt", "Windows recovery coverage")
class RuntimeRecoveryTest(unittest.TestCase):
    def test_recovery_terminates_recorded_job_but_cleans_only_after_exact_absence(self) -> None:
        installation = "d" * 32
        attempt = "1" * 32
        incarnation = "2" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 1)
            ledger = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            ledger.initialize_clean()
            job_name = f"Local\\PyLaravelSupervisor-recovery-live-{uuid.uuid4().hex}"
            ledger.arm(
                attempt_id=attempt,
                desired_revision=1,
                ownership_id=job_name,
                supervisor_incarnation=incarnation,
                ready_nonce="3" * 32,
            )
            ledger.mark_uncertain(attempt)
            handle = create_job(job_name)
            try:
                with self.assertRaises(RecoveryError):
                    RuntimeRecovery(store.paths.root, installation).recover()
                self.assertEqual("uncertain", ledger.read()["state"])
            finally:
                close_handle(handle)
            self.assertEqual("recovered", RuntimeRecovery(store.paths.root, installation).recover())
            self.assertEqual("clean", ledger.read()["state"])

    def test_foreign_runtime_owner_recovery_is_side_effect_free(self) -> None:
        installation = "1" * 32
        foreign = "2" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 1)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            before = self._runtime_bytes(store.paths.root)

            with self.assertRaisesRegex(RecoveryError, "runtime owner identity mismatch"):
                RuntimeRecovery(store.paths.root, foreign).recover()

            self.assertEqual(before, self._runtime_bytes(store.paths.root))

    def test_recovery_refuses_while_resident_singleton_lock_is_held(self) -> None:
        installation = "9" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 1)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            acquired = threading.Event()
            release = threading.Event()

            def hold_resident_mutex() -> None:
                lock = WindowsMutex(mutex_name(installation, "resident"), timeout_ms=0).acquire()
                try:
                    acquired.set()
                    release.wait(timeout=3.0)
                finally:
                    lock.close()

            holder = threading.Thread(target=hold_resident_mutex, daemon=True)
            holder.start()
            self.assertTrue(acquired.wait(timeout=2.0))
            try:
                with self.assertRaises(RecoveryError):
                    RuntimeRecovery(store.paths.root, installation).recover()
            finally:
                release.set()
                holder.join(timeout=2.0)
            self.assertFalse(holder.is_alive())
            self.assertEqual("already_clean", RuntimeRecovery(store.paths.root, installation).recover())

    def test_recovery_restores_last_known_good_authority_before_cleaning(self) -> None:
        installation = "7" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 4)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            store.write_json(
                store.paths.status,
                {
                    "schema_version": 1,
                    "installation_id": installation,
                    "state": "stopped",
                },
            )
            store.paths.manifest.write_bytes(b"")
            store.paths.spawn_gate.write_bytes(b"")
            store.paths.supervisor_ledger.write_bytes(b"")

            self.assertEqual("recovered", RuntimeRecovery(store.paths.root, installation).recover())
            self.assertEqual(installation, store.read_json(store.paths.manifest)["installation_id"])
            self.assertEqual("disabled", store.gate(installation)["state"])
            self.assertEqual("clean", resident.read()["state"])

    def test_fully_corrupt_lifecycle_authority_without_valid_backup_fails_closed(self) -> None:
        installation = "8" * 32
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "runtime"
            store = RuntimeStore(root)
            store.initialize(installation)
            for name in [
                "manifest.json",
                "spawn-gate.json",
                "desired.json",
                "supervisor-ledger.json",
                "status.json",
                "events.json",
            ]:
                (root / name).write_bytes(b"")
            queue_ledger = root / "instances" / "queue-default" / "0.json"
            queue_ledger.parent.mkdir(parents=True)
            queue_ledger.write_bytes(b"")
            before = self._runtime_bytes(root)

            with self.assertRaisesRegex(
                RecoveryError,
                "runtime lifecycle ownership is corrupt without a valid backup",
            ):
                RuntimeRecovery(root, installation).recover()

            self.assertEqual(before, self._runtime_bytes(root))
            self.assertFalse((root / "corrupt").exists())

    def test_live_resident_rejection_preserves_corrupt_primary_and_lkg_bytes(self) -> None:
        installation = "a" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 2)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            store.write_json(
                store.paths.desired,
                self._desired(installation, revision=2, enabled=False),
            )
            store.paths.desired.write_bytes(b"")
            before = self._runtime_bytes(store.paths.root)
            acquired = threading.Event()
            release = threading.Event()

            def hold_resident() -> None:
                lock = WindowsMutex(
                    mutex_name(installation, "resident"), timeout_ms=0
                ).acquire()
                try:
                    acquired.set()
                    release.wait(timeout=3.0)
                finally:
                    lock.close()

            holder = threading.Thread(target=hold_resident, daemon=True)
            holder.start()
            self.assertTrue(acquired.wait(timeout=2.0))
            try:
                with self.assertRaises(RecoveryError):
                    RuntimeRecovery(store.paths.root, installation).recover()
            finally:
                release.set()
                holder.join(timeout=2.0)

            self.assertFalse(holder.is_alive())
            self.assertEqual(before, self._runtime_bytes(store.paths.root))

    def test_busy_transition_rejection_is_byte_for_byte_side_effect_free(self) -> None:
        installation = "b" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 1)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            store.write_json(
                store.paths.desired,
                self._desired(installation, revision=1, enabled=False),
            )
            store.paths.desired.write_bytes(b"")
            before = self._runtime_bytes(store.paths.root)
            acquired = threading.Event()
            release = threading.Event()

            def hold_transition() -> None:
                lock = WindowsMutex(
                    mutex_name(installation, "transition"), timeout_ms=0
                ).acquire()
                try:
                    acquired.set()
                    release.wait(timeout=3.0)
                finally:
                    lock.close()

            holder = threading.Thread(target=hold_transition, daemon=True)
            holder.start()
            self.assertTrue(acquired.wait(timeout=2.0))
            try:
                with self.assertRaises(RecoveryError):
                    RuntimeRecovery(
                        store.paths.root,
                        installation,
                        transition_timeout_ms=0,
                    ).recover()
            finally:
                release.set()
                holder.join(timeout=2.0)

            self.assertFalse(holder.is_alive())
            self.assertEqual(before, self._runtime_bytes(store.paths.root))

    def test_corrupt_desired_without_backup_is_salvaged_and_recovery_is_idempotent(self) -> None:
        installation = "c" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 3)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            store.paths.desired.write_bytes(b"")

            self.assertEqual(
                "recovered",
                RuntimeRecovery(store.paths.root, installation).recover(),
            )
            self.assertFalse(store.paths.desired.exists())
            archives = list((store.paths.root / "corrupt").iterdir())
            self.assertEqual(1, len(archives))
            self.assertEqual(b"", (archives[0] / "desired.json").read_bytes())
            self.assertEqual(
                "already_clean",
                RuntimeRecovery(store.paths.root, installation).recover(),
            )
            self.assertFalse(store.paths.desired.exists())

    def test_corrupt_current_instance_without_backup_fails_closed(self) -> None:
        installation = "f" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 4)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            instance = store.paths.instance_ledger("queue-default", 0)
            instance.parent.mkdir(parents=True, exist_ok=True)
            instance.write_bytes(b"")
            before = self._runtime_bytes(store.paths.root)

            with self.assertRaisesRegex(
                RecoveryError,
                "runtime lifecycle ownership is corrupt without a valid backup",
            ):
                RuntimeRecovery(store.paths.root, installation).recover()

            self.assertEqual(before, self._runtime_bytes(store.paths.root))
            self.assertFalse((store.paths.root / "corrupt").exists())

    def test_salvage_retires_old_backup_lineage_and_concurrent_retry_cannot_resurrect_it(self) -> None:
        installation = "0" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 7)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            desired = self._desired(installation, revision=7, enabled=True)
            desired["groups"][0]["desired_processes"] = 1
            store.write_json(store.paths.desired, desired)
            child = LifecycleLedger(
                store,
                store.paths.instance_ledger("queue-default", 0),
                installation_id=installation,
                role="child",
            )
            child.initialize_clean()
            store.paths.manifest.write_bytes(b"")
            store.paths.backup(store.paths.manifest).write_bytes(b"")
            barrier = threading.Barrier(2)

            def recover() -> str:
                barrier.wait(timeout=2.0)
                return RuntimeRecovery(
                    store.paths.root,
                    installation,
                    recovery_timeout_ms=10_000,
                ).recover()

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = sorted(executor.map(lambda _index: recover(), range(2)))

            self.assertEqual(["already_clean", "recovered"], outcomes)
            self.assertFalse(store.paths.desired.exists())
            self.assertFalse(store.paths.instance_ledger("queue-default", 0).exists())
            self.assertFalse(store.paths.backup(store.paths.desired).exists())
            self.assertFalse((store.paths.backups / "instances").exists())
            self.assertEqual("disabled", store.gate(installation)["state"])
            self.assertEqual(
                "already_clean",
                RuntimeRecovery(store.paths.root, installation).recover(),
            )

    def test_corrupt_scheduler_salvage_preserves_uncertain_lifecycle_until_exact_jobs_are_absent(self) -> None:
        installation = "4" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 4)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            child = LifecycleLedger(
                store,
                store.paths.instance_ledger("queue-default", 0),
                installation_id=installation,
                role="child",
            )
            child.initialize_clean()
            resident_attempt = "a" * 32
            child_attempt = "b" * 32
            resident.arm(
                attempt_id=resident_attempt,
                desired_revision=4,
                ownership_id="Local\\PyLaravelSupervisor-missing-resident-job",
                supervisor_incarnation="c" * 32,
                ready_nonce="d" * 32,
            )
            resident.mark_uncertain(resident_attempt)
            child.arm(
                attempt_id=child_attempt,
                desired_revision=4,
                ownership_id="Local\\PyLaravelSupervisor-missing-child-job",
                supervisor_incarnation="c" * 32,
                group_id="queue-default",
                slot=0,
            )
            child.mark_uncertain(child_attempt)
            claim = store.paths.scheduler_state("scheduler")
            claim.parent.mkdir(parents=True, exist_ok=True)
            claim.write_bytes(b"")

            self.assertEqual("recovered", RuntimeRecovery(store.paths.root, installation).recover())
            self.assertFalse(claim.exists())
            self.assertEqual("clean", resident.read()["state"])
            self.assertFalse(store.paths.instance_ledger("queue-default", 0).exists())
            archives = list((store.paths.root / "corrupt").iterdir())
            self.assertEqual(1, len(archives))
            archived_resident = RuntimeStore(archives[0]).read_json(archives[0] / "supervisor-ledger.json")
            archived_child = RuntimeStore(archives[0]).read_json(
                archives[0] / "instances" / "queue-default" / "0.json"
            )
            self.assertEqual("uncertain", archived_resident["state"])
            self.assertEqual("uncertain", archived_child["state"])

    def test_corrupt_scheduler_claim_is_archived_and_salvaged(self) -> None:
        installation = "6" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 1)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            claim = store.paths.scheduler_state("scheduler")
            claim.parent.mkdir(parents=True, exist_ok=True)
            claim.write_bytes(b"")

            self.assertEqual("recovered", RuntimeRecovery(store.paths.root, installation).recover())
            self.assertFalse(claim.exists())
            archives = list((store.paths.root / "corrupt").iterdir())
            self.assertEqual(1, len(archives))
            self.assertEqual(
                b"",
                (archives[0] / "schedulers" / "scheduler.json").read_bytes(),
            )
            self.assertEqual(
                "already_clean",
                RuntimeRecovery(store.paths.root, installation).recover(),
            )

    def test_valid_scheduler_claim_survives_recovery_and_blocks_same_minute_duplicate(self) -> None:
        installation = "5" * 32
        current = datetime(2026, 9, 1, 20, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 3)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            claim = store.paths.scheduler_state("scheduler")
            payload = {
                "schema_version": 1,
                "group_id": "scheduler",
                "generation": 4,
                "minute": current.isoformat(),
            }
            store.write_json(claim, payload)
            before = claim.read_bytes()

            self.assertEqual(
                "already_clean",
                RuntimeRecovery(store.paths.root, installation).recover(),
            )
            self.assertEqual(before, claim.read_bytes())

            group = ProcessGroupSpec.from_mapping(
                {
                    "id": "scheduler",
                    "kind": "scheduler",
                    "generation": 4,
                    "desired_processes": 1,
                    "stop_grace_seconds": 5,
                    "restart_policy": {
                        "enabled": True,
                        "base_delay_seconds": 1,
                        "max_delay_seconds": 5,
                        "crash_window_seconds": 60,
                        "max_crashes": 5,
                    },
                    "queue": None,
                    "scheduler": {
                        "cron": "* * * * *",
                        "timezone": "UTC",
                        "watchdog_seconds": 90,
                    },
                }
            )
            self.assertFalse(
                SchedulerTrigger(store, now=lambda: current).claim_if_due(group)
            )

    def test_recovery_cleans_quiescent_child_ledger_without_pid_authority(self) -> None:
        installation = "e" * 32
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize(installation)
            store.disable_gate(installation, 3)
            resident = LifecycleLedger(
                store,
                store.paths.supervisor_ledger,
                installation_id=installation,
                role="resident",
            )
            resident.initialize_clean()
            child = LifecycleLedger(
                store,
                store.paths.instance_ledger("queue-default", 0),
                installation_id=installation,
                role="child",
            )
            child.initialize_clean()
            attempt = "3" * 32
            child.arm(
                attempt_id=attempt,
                desired_revision=3,
                ownership_id="Local\\PyLaravelSupervisor-missing-child-job",
                supervisor_incarnation="4" * 32,
                group_id="queue-default",
                slot=0,
            )
            child.mark_uncertain(attempt)
            self.assertEqual("recovered", RuntimeRecovery(store.paths.root, installation).recover())
            self.assertEqual("clean", child.read()["state"])


    @staticmethod
    def _desired(
        installation: str,
        *,
        revision: int,
        enabled: bool,
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "installation_id": installation,
            "revision": revision,
            "enabled": enabled,
            "generated_at": "2026-09-01T00:00:00+00:00",
            "runtime": {
                "project_root": "C:\\runtime",
                "php_executable": "C:\\php\\php.exe",
                "child_environment": {},
            },
            "groups": [
                {
                    "id": "queue-default",
                    "kind": "queue_once",
                    "generation": 0,
                    "desired_processes": 0,
                    "stop_grace_seconds": 10,
                    "restart_policy": {
                        "enabled": True,
                        "base_delay_seconds": 1,
                        "max_delay_seconds": 30,
                        "crash_window_seconds": 60,
                        "max_crashes": 5,
                    },
                    "queue": {
                        "connection": "database",
                        "queues": ["default"],
                        "backoff": [0],
                        "tries": 1,
                        "sleep_seconds": 1,
                        "watchdog_seconds": 120,
                    },
                }
            ],
        }

    @staticmethod
    def _runtime_bytes(root: Path) -> dict[str, bytes]:
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }


if __name__ == "__main__":
    unittest.main()