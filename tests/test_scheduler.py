import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from py_laravel_supervisor.commands import build_group_command
from py_laravel_supervisor.contracts import ContractError, DesiredManifest
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.scheduler import CronSchedule, SchedulerTrigger, validate_cron


class SchedulerContractTest(unittest.TestCase):
    def test_numeric_five_field_cron_matches_expected_application_timezone(self) -> None:
        schedule = CronSchedule.parse("*/5 12 * * 1-5")
        self.assertTrue(
            schedule.matches(
                datetime(2026, 9, 1, 10, 15, tzinfo=timezone.utc),
                "Europe/Rome",
            )
        )
        self.assertFalse(
            schedule.matches(
                datetime(2026, 9, 1, 10, 16, tzinfo=timezone.utc),
                "Europe/Rome",
            )
        )
        self.assertFalse(
            schedule.matches(
                datetime(2026, 9, 6, 10, 15, tzinfo=timezone.utc),
                "Europe/Rome",
            )
        )

    def test_rejects_non_numeric_or_non_five_field_cron(self) -> None:
        for expression in ["@hourly", "* * * *", "MON * * * *", "0.5 * * * *"]:
            with self.subTest(expression=expression), self.assertRaises(ContractError):
                validate_cron(expression)

    def test_claim_is_persistent_per_generation_and_minute(self) -> None:
        fixed = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            group = self._manifest(generation=1, cron="* * * * *").groups[0]
            trigger = SchedulerTrigger(store, now=lambda: fixed)

            self.assertTrue(trigger.claim_if_due(group))
            self.assertFalse(trigger.claim_if_due(group))

            changed = self._manifest(generation=2, cron="* * * * *").groups[0]
            self.assertTrue(trigger.claim_if_due(changed))

    def test_not_due_does_not_create_a_claim(self) -> None:
        fixed = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            group = self._manifest(generation=1, cron="1 * * * *").groups[0]
            trigger = SchedulerTrigger(store, now=lambda: fixed)

            self.assertFalse(trigger.claim_if_due(group))
            self.assertFalse(store.paths.scheduler_state(group.id).exists())

    def test_scheduler_command_is_closed_schedule_run(self) -> None:
        manifest = self._manifest(generation=1, cron="* * * * *")
        command = build_group_command(manifest, manifest.groups[0])
        self.assertEqual(
            (
                str(manifest.runtime.php_executable),
                str(manifest.runtime.project_root / "artisan"),
                "schedule:run",
                "--no-interaction",
            ),
            command,
        )

    @staticmethod
    def _manifest(*, generation: int, cron: str) -> DesiredManifest:
        return DesiredManifest.from_mapping(
            {
                "schema_version": 1,
                "installation_id": "local-gpt-installation-01",
                "revision": 1,
                "enabled": True,
                "generated_at": "2026-09-01T10:00:00Z",
                "runtime": {
                    "project_root": "C:/app",
                    "php_executable": "C:/php/php.exe",
                    "child_environment": {"APP_ENV": "testing"},
                },
                "groups": [
                    {
                        "id": "scheduler",
                        "kind": "scheduler",
                        "generation": generation,
                        "desired_processes": 1,
                        "stop_grace_seconds": 65,
                        "restart_policy": {
                            "enabled": True,
                            "base_delay_seconds": 1,
                            "max_delay_seconds": 15,
                            "crash_window_seconds": 120,
                            "max_crashes": 5,
                        },
                        "queue": None,
                        "scheduler": {
                            "cron": cron,
                            "timezone": "Europe/Rome",
                            "watchdog_seconds": 90,
                        },
                    }
                ],
            }
        )


if __name__ == "__main__":
    unittest.main()