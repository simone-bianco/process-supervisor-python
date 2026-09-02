import json
import tempfile
import unittest
from pathlib import Path

from py_laravel_supervisor.queue_protocol import QueueProtocolClassifier
from py_laravel_supervisor.redaction import RedactedLineAccumulator
from py_laravel_supervisor.runtime_files import RuntimeStore


class OutputSecrecyTest(unittest.TestCase):
    def test_protocol_observer_can_parse_raw_frame_but_runtime_persists_only_sanitized_data(self) -> None:
        secret = "super-secret-canary-value"
        with tempfile.TemporaryDirectory() as temporary:
            store = RuntimeStore(Path(temporary) / "runtime")
            store.initialize("a" * 32)
            classifier = QueueProtocolClassifier()
            collector = RedactedLineAccumulator(protocol_line_observer=classifier.consume_line)
            redacted_lines: list[str] = []
            raw = json.dumps(
                {
                    "job": "App\\Jobs\\Example",
                    "queue": "default",
                    "connection": "database",
                    "attempts": 1,
                    "status": "starting",
                    "message": f"token={secret}",
                },
                separators=(",", ":"),
            ).encode("utf-8") + b"\n"

            redacted_lines.extend(collector.feed(raw[:17]))
            redacted_lines.extend(collector.feed(raw[17:]))
            redacted_lines.extend(collector.finish())
            store.write_json(
                store.paths.status,
                {
                    "schema_version": 1,
                    "state": "running",
                    "last_job": classifier.sanitized_projection(),
                },
            )

            persisted = "\n".join(
                path.read_text(encoding="utf-8")
                for path in store.paths.root.rglob("*.json")
            )
            self.assertNotIn(secret, persisted)
            self.assertNotIn("message", persisted)
            self.assertTrue(any("[REDACTED]" in line for line in redacted_lines))
            self.assertFalse((store.paths.root / "tails").exists())
            self.assertEqual("starting", classifier.sanitized_projection()["status"])


if __name__ == "__main__":
    unittest.main()