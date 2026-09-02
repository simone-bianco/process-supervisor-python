import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from py_laravel_supervisor.windows import (
    WindowsProcessError,
    close_handle,
    create_job,
    job_active_processes,
    process_exists,
    spawn_process,
    start_pipe_reader,
    terminate_job,
    wait_for_job_absent,
)


@unittest.skipUnless(os.name == "nt", "Windows Job Object coverage")
class WindowsJobObjectTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Path(__file__).parent / "fixtures" / "child_tree.py"

    def test_exact_child_and_anchor_jobs_terminate_only_owned_trees(self) -> None:
        namespace = uuid.uuid4().hex
        anchor_name = f"Local\\PyLaravelSupervisorTest-anchor-{namespace}"
        child_name = f"Local\\PyLaravelSupervisorTest-child-{namespace}"
        anchor = create_job(anchor_name)
        child = create_job(child_name)
        unrelated = subprocess.Popen(
            [sys.executable, str(self.fixture), "--sleep", "10"],
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        process = None
        try:
            process = spawn_process(
                [sys.executable, str(self.fixture), "--sleep", "10", "--spawn-child"],
                cwd=Path(__file__).parent,
                environment={},
                job_handles=[anchor, child],
                exact_job_handle=child,
                cleanup_job_handle=child,
                capture_output=False,
            )
            self.assertIsNone(process.poll())
            terminate_job(child)
            self.assertIsNotNone(process.wait(3.0))
            self.assertIsNone(unrelated.poll())
        finally:
            if process is not None:
                process.close()
            close_handle(child)
            close_handle(anchor)
            if unrelated.poll() is None:
                unrelated.terminate()
            unrelated.wait(timeout=3)
        self.assertTrue(wait_for_job_absent(child_name, 2.0))
        self.assertTrue(wait_for_job_absent(anchor_name, 2.0))

    def test_anchor_termination_cascades_parent_and_descendant_only(self) -> None:
        namespace = uuid.uuid4().hex
        anchor_name = f"Local\\PyLaravelSupervisorTest-anchor-{namespace}"
        child_name = f"Local\\PyLaravelSupervisorTest-child-{namespace}"
        anchor = create_job(anchor_name)
        child = create_job(child_name)
        unrelated = subprocess.Popen(
            [sys.executable, str(self.fixture), "--sleep", "10"],
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        process = None
        with tempfile.TemporaryDirectory() as temporary:
            pid_file = Path(temporary) / "managed-pids.txt"
            try:
                direct_python = getattr(sys, "_base_executable", sys.executable)
                process = spawn_process(
                    [
                        direct_python,
                        str(self.fixture),
                        "--sleep",
                        "10",
                        "--spawn-child",
                        "--pid-file",
                        str(pid_file),
                    ],
                    cwd=Path(__file__).parent,
                    environment={},
                    job_handles=[anchor, child],
                    exact_job_handle=child,
                    cleanup_job_handle=child,
                    capture_output=False,
                )
                parent_pid, descendant_pid = _wait_for_pid_file(pid_file, 2.0)
                self.assertEqual(process.pid, parent_pid)
                self.assertTrue(process_exists(parent_pid))
                self.assertTrue(process_exists(descendant_pid))

                terminate_job(anchor)

                self.assertIsNotNone(process.wait(3.0))
                self.assertTrue(_wait_until(lambda: not process_exists(descendant_pid), 2.0))
                self.assertFalse(process_exists(parent_pid))
                self.assertTrue(process_exists(unrelated.pid))
                self.assertIsNone(unrelated.poll())
            finally:
                if process is not None:
                    process.close()
                close_handle(child)
                close_handle(anchor)
                if unrelated.poll() is None:
                    unrelated.terminate()
                unrelated.wait(timeout=3)

    def test_stdout_and_stderr_drain_to_eof_without_leaking_raw_secret(self) -> None:
        namespace = uuid.uuid4().hex
        anchor = create_job(f"Local\\PyLaravelSupervisorTest-anchor-{namespace}")
        child = create_job(f"Local\\PyLaravelSupervisorTest-child-{namespace}")
        process = None
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        errors: list[str] = []
        try:
            process = spawn_process(
                [sys.executable, str(self.fixture), "--sleep", "0.1", "--noise"],
                cwd=Path(__file__).parent,
                environment={},
                job_handles=[anchor, child],
                exact_job_handle=child,
                cleanup_job_handle=child,
                capture_output=True,
            )
            stdout_fd, stderr_fd = process.stdout_fd, process.stderr_fd
            process.stdout_fd = process.stderr_fd = None
            self.assertIsNotNone(stdout_fd)
            self.assertIsNotNone(stderr_fd)
            threads = [
                start_pipe_reader(stdout_fd, stdout_lines.append, errors.append),
                start_pipe_reader(stderr_fd, stderr_lines.append, errors.append),
            ]
            self.assertIsNotNone(process.wait(3.0))
            for thread in threads:
                thread.join(timeout=2.0)
                self.assertFalse(thread.is_alive())
            combined = "\n".join(stdout_lines + stderr_lines)
            self.assertNotIn("split-secret", combined)
            self.assertNotIn("hunter2", combined)
            self.assertEqual([], errors)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate_tree()
                    process.wait(2.0)
                process.close()
            close_handle(child)
            close_handle(anchor)

    def test_anchor_only_post_create_failure_terminates_spawned_process(self) -> None:
        namespace = uuid.uuid4().hex
        anchor = create_job(f"Local\\PyLaravelSupervisorTest-anchor-{namespace}")
        try:
            with patch(
                "py_laravel_supervisor.windows._resume_thread",
                side_effect=WindowsProcessError("fault injection after CreateProcessW"),
            ):
                with self.assertRaises(WindowsProcessError):
                    spawn_process(
                        [sys.executable, str(self.fixture), "--sleep", "10"],
                        cwd=Path(__file__).parent,
                        environment={},
                        job_handles=[anchor],
                        exact_job_handle=None,
                        cleanup_job_handle=anchor,
                        capture_output=False,
                    )
            self.assertTrue(_wait_until(lambda: job_active_processes(anchor) == 0, 2.0))
        finally:
            close_handle(anchor)


def _wait_for_pid_file(path: Path, timeout: float) -> tuple[int, int]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            lines = path.read_text(encoding="ascii").strip().splitlines()
            if len(lines) == 2:
                return int(lines[0]), int(lines[1])
        time.sleep(0.02)
    raise AssertionError("managed child pid file was not published")


def _wait_until(predicate, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


if __name__ == "__main__":
    unittest.main()