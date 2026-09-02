import ctypes
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from py_laravel_supervisor.contracts import ContractError
from py_laravel_supervisor.runtime_files import RuntimeStore


class RuntimeStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "runtime"
        self.store = RuntimeStore(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_initialize_is_disabled_by_default_and_preserves_identity(self) -> None:
        self.store.initialize("a" * 32)
        self.assertEqual("disabled", self.store.read_json(self.store.paths.spawn_gate)["state"])

        with self.assertRaises(ContractError):
            self.store.initialize("b" * 32)

        self.assertEqual("a" * 32, self.store.read_json(self.store.paths.manifest)["installation_id"])

    def test_atomic_write_leaves_no_temporary_file_and_keeps_last_known_good_authority_backup(self) -> None:
        payload = {
            "schema_version": 1,
            "installation_id": "a" * 32,
            "state": "disabled",
            "revision": 4,
        }
        self.store.write_json(self.store.paths.spawn_gate, payload)
        self.assertEqual("disabled", self.store.read_json(self.store.paths.spawn_gate)["state"])
        self.assertEqual(payload, self.store.read_json(self.store.paths.backup(self.store.paths.spawn_gate)))
        self.assertEqual([], list(self.root.glob(".*.tmp")))

    def test_exclusive_create_never_exposes_partial_json_to_concurrent_reader(self) -> None:
        target = self.root / "exclusive.json"
        payload = {"schema_version": 1, "value": "x" * 10000}

        def create() -> str:
            try:
                self.store.create_json_exclusive(target, payload)
                return "created"
            except FileExistsError:
                return "exists"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = sorted(executor.map(lambda _index: create(), range(2)))
        self.assertEqual(["created", "exists"], results)
        self.assertEqual(payload, self.store.read_json(target))
        self.assertEqual([], list(self.root.glob(".*.tmp")))

    def test_corrupt_runtime_json_fails_closed(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.store.paths.status.write_text("not json", encoding="utf-8")
        with self.assertRaises(ContractError):
            self.store.read_json(self.store.paths.status)

    def test_last_known_good_restores_a_truncated_authority_file_without_overwriting_the_backup(self) -> None:
        installation = "c" * 32
        self.store.initialize(installation)
        payload = {
            "schema_version": 1,
            "installation_id": installation,
            "state": "enabled",
            "revision": 7,
        }
        self.store.write_json(self.store.paths.spawn_gate, payload)
        backup = self.store.paths.backup(self.store.paths.spawn_gate)
        before = backup.read_bytes()

        self.store.paths.spawn_gate.write_bytes(b"")
        restored = self.store.restore_backed_files(installation)

        self.assertIn("spawn-gate.json", restored)
        self.assertEqual(payload, self.store.read_json(self.store.paths.spawn_gate))
        self.assertEqual(before, backup.read_bytes())

    @unittest.skipUnless(os.name == "nt", "Windows sharing collision coverage")
    def test_atomic_replace_retries_until_a_reader_releases_delete_sharing(self) -> None:
        installation = "e" * 32
        initial = {
            "schema_version": 1,
            "installation_id": installation,
            "summary": "starting",
        }
        updated = {**initial, "summary": "ready"}
        self.store.write_json(self.store.paths.status, initial)

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
        generic_read = 0x80000000
        share_read = 0x00000001
        share_write = 0x00000002
        open_existing = 3
        normal = 0x80
        invalid = ctypes.c_void_p(-1).value
        handle = kernel32.CreateFileW(
            str(self.store.paths.status),
            generic_read,
            share_read | share_write,
            None,
            open_existing,
            normal,
            None,
        )
        self.assertNotIn(handle, (None, 0, invalid))

        failures: list[BaseException] = []

        def write() -> None:
            try:
                self.store.write_json(self.store.paths.status, updated)
            except BaseException as error:
                failures.append(error)

        writer = threading.Thread(target=write)
        writer.start()
        try:
            time.sleep(0.1)
            self.assertTrue(writer.is_alive())
        finally:
            kernel32.CloseHandle(handle)
        writer.join(timeout=3.0)

        self.assertFalse(writer.is_alive())
        self.assertEqual([], failures)
        self.assertEqual("ready", self.store.read_json(self.store.paths.status)["summary"])

    def test_high_churn_status_and_events_are_not_backed_up(self) -> None:
        installation = "e" * 32
        status = {
            "schema_version": 1,
            "installation_id": installation,
            "summary": "ready",
        }
        events = {
            "schema_version": 1,
            "installation_id": installation,
            "events": [],
        }
        self.store.write_json(self.store.paths.status, status)
        self.store.write_json(self.store.paths.events, events)

        self.assertFalse(self.store.paths.backup(self.store.paths.status).exists())
        self.assertFalse(self.store.paths.backup(self.store.paths.events).exists())

    def test_instance_ledger_is_backed_and_restored(self) -> None:
        installation = "d" * 32
        path = self.store.paths.instance_ledger("queue-default", 0)
        payload = {
            "schema_version": 1,
            "installation_id": installation,
            "role": "child",
            "state": "clean",
        }
        self.store.write_json(path, payload)
        path.write_bytes(b"")

        restored = self.store.restore_backed_files(installation)

        self.assertIn("instances/queue-default/0.json", restored)
        self.assertEqual(payload, self.store.read_json(path))


if __name__ == "__main__":
    unittest.main()