"""Real Windows IPC checks; no app service, Docker daemon or provider is started."""
import os
import secrets
import threading
import time
import unittest

from py_laravel_supervisor.mcp_pipe import PrivatePipe
from py_laravel_supervisor.windows import WindowsProcessError


@unittest.skipUnless(os.name == 'nt', 'Windows private IPC')
class PrivateMcpPipeTest(unittest.TestCase):
    def setUp(self):
        self.name = r'\\.\pipe\LocalGptMcp-' + secrets.token_hex(32)
        self.server = PrivatePipe(self.name, server=True)
        self.addCleanup(self.server.close)
        self.failures = []
        self.threads = []

    def tearDown(self):
        self.server.close()
        for thread in self.threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        if self.failures: raise self.failures[0]

    def work(self, callback):
        def run():
            try: callback()
            except BaseException as error: self.failures.append(error)
        thread = threading.Thread(target=run, daemon=True)
        thread.start(); self.threads.append(thread)

    def test_large_byte_frames_are_preserved_and_the_original_process_is_verified(self):
        message = ('fixture-non-ascii-ü'.encode() + b'\x00') * 16000
        def server():
            deadline = time.monotonic() + 5
            self.server.accept(deadline)
            self.assertEqual(message, self.server.receive(deadline))
            self.server.send(message[::-1], deadline)
            self.assertEqual(b'ack', self.server.receive(deadline))
            self.server.disconnect()
        self.work(server)
        client = PrivatePipe(self.name, server=False, server_pid=os.getpid())
        try:
            deadline = time.monotonic() + 5
            client.send(message, deadline)
            self.assertEqual(message[::-1], client.receive(deadline))
            client.send(b'ack', deadline)
        finally: client.close()

    def test_same_pipe_name_is_not_adopted_by_a_second_server(self):
        with self.assertRaises(WindowsProcessError):
            PrivatePipe(self.name, server=True)

    def test_wrong_server_pid_fails_before_any_payload_can_be_delivered(self):
        self.work(lambda: self.server.accept(time.monotonic() + 3))
        with self.assertRaisesRegex(WindowsProcessError, 'identity'):
            PrivatePipe(self.name, server=False, server_pid=os.getpid() + 100000)

    def test_read_timeout_cancels_original_overlapped_io_without_hanging(self):
        connected = threading.Event(); release = threading.Event()
        def server():
            self.server.accept(time.monotonic() + 3)
            connected.set()
            release.wait(2)
        self.work(server)
        client = PrivatePipe(self.name, server=False, server_pid=os.getpid())
        try:
            self.assertTrue(connected.wait(1))
            start = time.monotonic()
            with self.assertRaisesRegex(WindowsProcessError, 'deadline'):
                client.receive(start + 0.1)
            self.assertLess(time.monotonic() - start, 0.8)
        finally:
            client.close(); release.set()

    def test_oversized_local_packet_is_not_transmitted(self):
        self.work(lambda: self.server.accept(time.monotonic() + 3))
        client = PrivatePipe(self.name, server=False, server_pid=os.getpid())
        try:
            with self.assertRaises(WindowsProcessError):
                client.send(b'x' * (PrivatePipe.MAX_PACKET + 1), time.monotonic() + 1)
        finally: client.close()


if __name__ == '__main__': unittest.main()
