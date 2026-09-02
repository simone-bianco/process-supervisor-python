import tempfile
import unittest
from pathlib import Path

from py_laravel_supervisor.contracts import ContractError
from py_laravel_supervisor.lifecycle import LifecycleLedger, LifecycleState
from py_laravel_supervisor.runtime_files import RuntimeStore


class LifecycleLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = RuntimeStore(Path(self.temporary.name) / "runtime")
        self.store.initialize("a" * 32)
        self.resident = LifecycleLedger(
            self.store,
            self.store.paths.supervisor_ledger,
            installation_id="a" * 32,
            role="resident",
        )
        self.resident.initialize_clean()
        self.child = LifecycleLedger(
            self.store,
            self.store.paths.instance_ledger("queue-default", 0),
            installation_id="a" * 32,
            role="child",
        )
        self.child.initialize_clean()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_write_ahead_arm_precedes_child_active(self) -> None:
        armed = self.child.arm(
            attempt_id="1" * 32,
            desired_revision=7,
            ownership_id="Local\\LaravelSupervisor-child-1",
            supervisor_incarnation="2" * 32,
            group_id="queue-default",
            slot=0,
        )
        self.assertEqual(LifecycleState.SPAWN_ARMED.value, armed["state"])
        self.assertIsNone(armed["pid"])
        self.assertTrue(self.child.requires_recovery())

        active = self.child.mark_active("1" * 32, pid=1234)
        self.assertEqual(LifecycleState.CHILD_ACTIVE.value, active["state"])
        self.assertEqual(1234, active["pid"])

    def test_uncertain_attempt_cannot_be_rearmed_or_cleaned_by_another_attempt(self) -> None:
        self.child.arm(
            attempt_id="3" * 32,
            desired_revision=8,
            ownership_id="Local\\LaravelSupervisor-child-3",
            supervisor_incarnation="4" * 32,
            group_id="queue-default",
            slot=0,
        )
        self.child.mark_uncertain("3" * 32)

        with self.assertRaises(ContractError):
            self.child.arm(
                attempt_id="5" * 32,
                desired_revision=8,
                ownership_id="Local\\LaravelSupervisor-child-5",
                supervisor_incarnation="4" * 32,
                group_id="queue-default",
                slot=0,
            )
        with self.assertRaises(ContractError):
            self.child.mark_clean("5" * 32)
        self.assertEqual(LifecycleState.UNCERTAIN.value, self.child.read()["state"])

    def test_exact_attempt_can_transition_to_clean_and_clear_ownership(self) -> None:
        self.resident.arm(
            attempt_id="6" * 32,
            desired_revision=9,
            ownership_id="Local\\LaravelSupervisor-anchor-6",
            supervisor_incarnation="7" * 32,
            ready_nonce="8" * 32,
        )
        self.resident.mark_active("6" * 32, pid=4321)
        self.resident.mark_stopping("6" * 32)
        clean = self.resident.mark_clean("6" * 32)

        self.assertEqual(LifecycleState.CLEAN.value, clean["state"])
        self.assertFalse(self.resident.requires_recovery())
        self.assertIsNone(clean["ownership_id"])
        self.assertIsNone(clean["attempt_id"])


if __name__ == "__main__":
    unittest.main()