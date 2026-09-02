import os
import tempfile
import unittest

from py_laravel_supervisor.events import EventStore
from py_laravel_supervisor.queue_protocol import QueueProtocolClassifier
from py_laravel_supervisor.runtime_files import RuntimeStore
from py_laravel_supervisor.windows import start_pipe_reader


class OutputBoundaryTest(unittest.TestCase):
    def test_raw_queue_protocol_secret_never_reaches_persisted_runtime_state(self) -> None:
        secret = "raw-secret-canary-8472"
        classifier = QueueProtocolClassifier()
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            store = RuntimeStore(directory)
            events = EventStore(store, "a" * 32)
            read_fd, write_fd = os.pipe()
            def observe_protocol(raw_line: str) -> None:
                if classifier.consume_line(raw_line):
                    events.publish_queue(group_id="queue-default", slot=0, frame=classifier.frames[-1])

            reader = start_pipe_reader(
                read_fd,
                lambda _line: None,
                errors.append,
                protocol_line_observer=observe_protocol,
            )
            starting = (
                '{"job":"App\\\\Jobs\\\\Example","queue":"default",'
                '"connection":"database","attempts":1,"status":"starting",'
                f'"message":"Authorization: Bearer {secret}"}}\n'
            )
            success = (
                '{"job":"App\\\\Jobs\\\\Example","queue":"default",'
                '"connection":"database","attempts":1,"status":"success",'
                '"result":"deleted"}\n'
            )
            os.write(write_fd, (starting + success).encode("utf-8"))
            os.close(write_fd)
            reader.join(timeout=2.0)

            self.assertFalse(reader.is_alive())
            self.assertEqual([], errors)
            healthy, outcome = classifier.completion(0)
            store.write_json(
                store.paths.status,
                {
                    "schema_version": 1,
                    "process_health": "healthy" if healthy else "failed",
                    "job_outcome": outcome,
                    "job": classifier.sanitized_projection(),
                },
            )

            persisted = "\n".join(
                path.read_text(encoding="utf-8")
                for path in (store.paths.status, store.paths.events)
            )
            self.assertNotIn(secret, persisted)
            self.assertNotIn("Bearer raw-secret", persisted)
            self.assertNotIn("message", persisted)
            self.assertEqual("success", outcome)
            self.assertFalse((store.paths.root / "tails").exists())


if __name__ == "__main__":
    unittest.main()