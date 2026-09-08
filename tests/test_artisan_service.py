import os
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from py_laravel_supervisor.commands import build_group_command
from py_laravel_supervisor.contracts import ContractError, DesiredManifest
from py_laravel_supervisor.control import ResidentUpgradeRequired, SupervisorControl
from py_laravel_supervisor.events import EventStore
from py_laravel_supervisor.slot import ManagedSlot
from py_laravel_supervisor.windows import close_handle, create_job, job_exists


def payload(revision=1):
    return {
        "schema_version": 1, "installation_id": "artisan-service-test-01",
        "revision": revision, "enabled": True, "generated_at": "2026-09-08T10:00:00Z",
        "runtime": {"project_root": str(Path.cwd()), "php_executable": str(Path(sys.executable)), "child_environment": {}},
        "groups": [{
            "id": "browser-service", "kind": "artisan_service", "generation": 0,
            "desired_processes": 1, "stop_grace_seconds": 0.1, "restart_policy": None,
            "queue": None, "service": {"command": "app:realtime"},
        }],
    }


class ArtisanServiceContractTest(unittest.TestCase):
    def test_builds_fixed_artisan_command_without_shell_or_client_arguments(self):
        manifest = DesiredManifest.from_mapping(payload())
        self.assertEqual((str(manifest.runtime.php_executable), str(manifest.runtime.project_root / "artisan"),
                          "app:realtime", "--no-interaction"), build_group_command(manifest, manifest.groups[0]))

    def test_rejects_scaling_arguments_shell_and_non_service_fields(self):
        for command in ["app:realtime --port=9002", "app:realtime;whoami", "../artisan", "--help", "app:x\n"]:
            data = payload()
            data["groups"][0]["service"]["command"] = command
            with self.subTest(command=command), self.assertRaises(ContractError):
                DesiredManifest.from_mapping(data)
        for field, value in [("desired_processes", 2), ("queue", {}), ("argv", ["whoami"]), ("scheduler", {})]:
            data = payload()
            data["groups"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ContractError):
                DesiredManifest.from_mapping(data)

    def test_recovery_reduction_cannot_change_service_command(self):
        current = DesiredManifest.from_mapping(payload())
        data = payload(2)
        data["groups"][0]["desired_processes"] = 0
        self.assertTrue(SupervisorControl._is_recovery_safe_reduction(current, DesiredManifest.from_mapping(data)))
        data["groups"][0]["service"]["command"] = "app:another"
        self.assertFalse(SupervisorControl._is_recovery_safe_reduction(current, DesiredManifest.from_mapping(data)))

    @unittest.skipUnless(os.name == "nt", "Windows ownership contract")
    def test_resident_capability_mismatch_never_publishes_unreadable_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            control = SupervisorControl(temporary, "artisan-service-test-01")
            initial = payload()
            initial["groups"] = []
            control.apply_desired(initial)
            with patch.object(control, "_resident_lock_free", return_value=False):
                with self.assertRaises(ResidentUpgradeRequired):
                    control.apply_desired(payload(2))
            self.assertEqual(initial, control.store.read_json(control.store.paths.desired))
            self.assertEqual(1, control.store.gate(control.installation_id)["revision"])
            control.store.write_json(control.store.paths.ready, {"process_kinds": ["artisan_service"]})
            with patch.object(control, "_resident_lock_free", return_value=False):
                self.assertEqual("applied", control.apply_desired(payload(2))["status"])

    @unittest.skipUnless(os.name == "nt", "Windows ownership contract")
    def test_restart_cannot_replace_active_slot_and_closes_exact_job_before_respawn(self):
        with tempfile.TemporaryDirectory() as temporary:
            control = SupervisorControl(temporary, "artisan-service-test-01")
            control.store.initialize(control.installation_id)
            manifest = DesiredManifest.from_mapping(payload())
            anchor = create_job(f"Local\\ArtisanServiceTest-{time.time_ns()}")
            slot = ManagedSlot(
                store=control.store, events=EventStore(control.store, control.installation_id),
                installation_id=control.installation_id, supervisor_incarnation="a" * 32,
                anchor_job_handle=anchor, group=manifest.groups[0], slot=0,
                command_builder=lambda *_: (sys.executable, "-c", "import time; time.sleep(10)"),
            )
            try:
                slot.spawn(manifest)
                first_job, first_pid = slot.job_name, slot.process.pid
                replacement = replace(slot.group, generation=1)
                with self.assertRaises(RuntimeError):
                    slot.replace_group(replacement)
                slot.spawn(manifest)
                self.assertEqual(first_pid, slot.process.pid)
                slot.request_stop()
                deadline = time.monotonic() + 4
                while slot.active and time.monotonic() < deadline:
                    slot.tick()
                    time.sleep(0.02)
                self.assertFalse(slot.active)
                self.assertFalse(job_exists(first_job))
                self.assertEqual("clean", slot.ledger.read()["state"])
                slot.replace_group(replacement)
                slot.spawn(manifest)
                self.assertNotEqual(first_job, slot.job_name)
                self.assertEqual(1, slot.group.generation)
            finally:
                slot.close()
                close_handle(anchor)
