import unittest

from py_laravel_supervisor.contracts import ContractError, DesiredManifest


class DesiredManifestTest(unittest.TestCase):
    def test_parses_bounded_server_owned_group(self) -> None:
        manifest = DesiredManifest.from_mapping(
            {
                "schema_version": 1,
                "installation_id": "local-gpt-installation-01",
                "revision": 3,
                "enabled": True,
                "generated_at": "2026-08-31T20:00:00Z",
                "runtime": {
                    "project_root": "C:/app",
                    "php_executable": "C:/php/php.exe",
                    "child_environment": {"APP_ENV": "testing", "QUEUE_CONNECTION": "database"},
                },
                "groups": [
                    {
                        "id": "queue-default",
                        "kind": "queue_once",
                        "generation": 0,
                        "desired_processes": 2,
                        "stop_grace_seconds": 10,
                        "restart_policy": {
                            "enabled": True,
                            "base_delay_seconds": 0.25,
                            "max_delay_seconds": 5,
                            "crash_window_seconds": 30,
                            "max_crashes": 5,
                        },
                        "queue": {
                            "connection": "database",
                            "queues": ["default"],
                            "backoff": [0],
                            "tries": 3,
                            "sleep_seconds": 1,
                            "watchdog_seconds": 30,
                        },
                    }
                ],
            }
        )
        self.assertEqual("queue-default", manifest.groups[0].id)
        self.assertEqual(2, manifest.groups[0].desired_processes)
        self.assertEqual(30, manifest.groups[0].queue.watchdog_seconds)
        self.assertEqual(
            {"APP_ENV": "testing", "QUEUE_CONNECTION": "database"},
            manifest.runtime.child_environment_mapping(),
        )

    def test_rejects_privileged_child_environment_keys(self) -> None:
        for key in ["OPENAI_API_KEY", "LOCAL_GPT_API_TOKEN", "DB_PASSWORD", "REVERB_APP_SECRET"]:
            with self.subTest(key=key), self.assertRaises(ContractError):
                DesiredManifest.from_mapping(
                    {
                        "schema_version": 1,
                        "installation_id": "local-gpt-installation-01",
                        "revision": 0,
                        "enabled": True,
                        "generated_at": "now",
                        "runtime": {
                            "project_root": "C:/app",
                            "php_executable": "C:/php/php.exe",
                            "child_environment": {key: "secret-canary"},
                        },
                        "groups": [],
                    }
                )

    def test_rejects_unknown_process_kind_and_duplicate_ids(self) -> None:
        with self.assertRaises(ContractError):
            DesiredManifest.from_mapping(
                {
                    "schema_version": 1,
                    "installation_id": "local-gpt-installation-01",
                    "revision": 0,
                    "enabled": True,
                    "generated_at": "now",
                    "runtime": {"project_root": "C:/app", "php_executable": "C:/php/php.exe", "child_environment": {}},
                    "groups": [
                        {
                            "id": "bad",
                            "kind": "shell",
                            "generation": 0,
                            "desired_processes": 1,
                            "stop_grace_seconds": 1,
                            "restart_policy": None,
                            "queue": None,
                        }
                    ],
                }
            )
        group = {
            "id": "same",
            "kind": "reverb",
            "generation": 0,
            "desired_processes": 1,
            "stop_grace_seconds": 1,
            "restart_policy": None,
            "queue": None,
        }
        with self.assertRaises(ContractError):
            DesiredManifest.from_mapping(
                {
                    "schema_version": 1,
                    "installation_id": "local-gpt-installation-01",
                    "revision": 0,
                    "enabled": True,
                    "generated_at": "now",
                    "runtime": {"project_root": "C:/app", "php_executable": "C:/php/php.exe", "child_environment": {}},
                    "groups": [group, group],
                }
            )


if __name__ == "__main__":
    unittest.main()