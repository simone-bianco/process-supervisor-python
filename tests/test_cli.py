import contextlib
import io
import json
import os
import unittest

from py_laravel_supervisor.cli import main


class CliDoctorTest(unittest.TestCase):
    def test_doctor_projects_nested_job_capability(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["doctor"])
        payload = json.loads(stdout.getvalue())

        self.assertTrue(payload["windows_first_v1"])
        self.assertEqual(
            "Windows 10 / Windows Server 2016",
            payload["minimum_windows_contract"],
        )
        if os.name == "nt":
            self.assertEqual(0, exit_code)
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["nested_job_list_supported"])
        else:
            self.assertEqual(2, exit_code)
            self.assertFalse(payload["ok"])
            self.assertFalse(payload["nested_job_list_supported"])


if __name__ == "__main__":
    unittest.main()