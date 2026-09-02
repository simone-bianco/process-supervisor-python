import unittest

from py_laravel_supervisor.queue_protocol import QueueProtocolClassifier, QueueProtocolError


def frame(status: str, *, result: str | None = None, job: str = "App\\Jobs\\Example") -> str:
    result_field = "" if result is None else f',"result":"{result}"'
    return (
        '{"job":"'
        + job.replace("\\", "\\\\")
        + '","queue":"default","connection":"database","attempts":1,"status":"'
        + status
        + '"'
        + result_field
        + "}"
    )


class QueueProtocolTest(unittest.TestCase):
    def test_exit_zero_without_frames_is_healthy_empty(self) -> None:
        classifier = QueueProtocolClassifier()
        self.assertEqual((True, "empty"), classifier.completion(0))

    def test_valid_success_is_healthy(self) -> None:
        classifier = QueueProtocolClassifier()
        self.assertTrue(classifier.consume_line(frame("starting")))
        self.assertTrue(classifier.consume_line(frame("success", result="deleted")))
        self.assertEqual((True, "success"), classifier.completion(0))

    def test_failed_and_released_jobs_do_not_make_process_unhealthy(self) -> None:
        failed = QueueProtocolClassifier()
        failed.consume_line(frame("starting"))
        failed.consume_line(frame("failed", result="failed"))
        self.assertEqual((True, "failed"), failed.completion(0))

        released = QueueProtocolClassifier()
        released.consume_line(frame("starting"))
        released.consume_line(frame("released_after_exception", result="released"))
        self.assertEqual((True, "released"), released.completion(0))

    def test_nonzero_exit_runtime_error_or_incomplete_started_job_is_unhealthy(self) -> None:
        classifier = QueueProtocolClassifier()
        self.assertEqual((False, "unknown"), classifier.completion(1))
        self.assertEqual((False, "unknown"), classifier.completion(0, runtime_error=True))
        classifier = QueueProtocolClassifier()
        classifier.consume_line(frame("starting"))
        self.assertEqual((False, "unknown"), classifier.completion(0))

    def test_malformed_unsupported_and_incomplete_frames_fail_closed(self) -> None:
        for value in (
            '{"status":',
            frame("unsupported"),
            '{"status":"starting","job":"Example"}',
        ):
            with self.subTest(value=value):
                classifier = QueueProtocolClassifier()
                with self.assertRaises(QueueProtocolError):
                    classifier.consume_line(value)

    def test_terminal_without_start_or_with_changed_identity_is_rejected(self) -> None:
        classifier = QueueProtocolClassifier()
        with self.assertRaises(QueueProtocolError):
            classifier.consume_line(frame("success"))

        classifier = QueueProtocolClassifier()
        classifier.consume_line(frame("starting"))
        with self.assertRaises(QueueProtocolError):
            classifier.consume_line(frame("success", job="App\\Jobs\\Other"))

    def test_non_protocol_text_and_json_are_ignored(self) -> None:
        classifier = QueueProtocolClassifier()
        self.assertFalse(classifier.consume_line("ordinary warning"))
        self.assertFalse(classifier.consume_line('{"event":"application-log"}'))
        self.assertEqual((True, "empty"), classifier.completion(0))


if __name__ == "__main__":
    unittest.main()