import unittest

from py_laravel_supervisor.redaction import RedactedLineAccumulator, redact_line


class RedactionTest(unittest.TestCase):
    def test_redacts_known_credentials(self) -> None:
        self.assertNotIn("secret-token", redact_line("Authorization: Bearer secret-token"))
        self.assertNotIn("hunter2", redact_line("password=hunter2"))
        self.assertNotIn("sk-abcdefghijklmnop", redact_line("key sk-abcdefghijklmnop"))

    def test_secret_split_across_chunks_is_redacted_before_line_emission(self) -> None:
        accumulator = RedactedLineAccumulator()
        self.assertEqual([], accumulator.feed(b"Authorization: Bearer sec"))
        emitted = accumulator.feed(b"ret-token\n")
        self.assertEqual(1, len(emitted))
        self.assertNotIn("secret-token", emitted[0])
        self.assertIn("[REDACTED]", emitted[0])

    def test_invalid_utf8_and_oversized_lines_are_bounded(self) -> None:
        accumulator = RedactedLineAccumulator(max_line_bytes=16, max_lines=4, max_bytes=128)
        accumulator.feed(b"ok-\xff\n")
        accumulator.feed(b"x" * 40 + b"\n")
        snapshot = accumulator.snapshot()
        self.assertTrue(any("\ufffd" in line for line in snapshot))
        self.assertIn("[oversized output suppressed]", snapshot)


if __name__ == "__main__":
    unittest.main()