"""Real Windows private-pipe regression for reply lifetime, no app or MCP fixture needed."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
import unittest

from py_laravel_supervisor.mcp_pipe import PrivatePipe
from py_laravel_supervisor.mcp_exchange import send_reply, receive_reply
from py_laravel_supervisor.windows import WindowsProcessError


@unittest.skipUnless(os.name == 'nt', 'Windows overlapped private IPC required')
class PrivateExchangeTests(unittest.TestCase):
    def setUp(self):
        self.original = {'token': secrets.token_hex(32), 'binding': secrets.token_hex(32), 'sequence': 3}
        self.name = r'\\.\pipe\LocalGptMcp-' + secrets.token_hex(32)
        self.sent = threading.Event()
        self.completed = threading.Event()
        self.result = []
        ready = self.sent
        class ObservedPipe(PrivatePipe):
            def send(self, data: bytes, deadline: float) -> None:
                super().send(data, deadline)
                ready.set()
        self.server = ObservedPipe(self.name, server=True)
        self.client = None
        self.thread = None

    def tearDown(self):
        if self.client:
            self.client.close()
        self.server.close()
        if self.thread:
            self.thread.join(3)
            self.assertFalse(self.thread.is_alive(), 'Owned exchange thread must terminate within its bound')

    def owner(self, reply=b'{"synthetic":true}', timeout=2):
        def serve():
            try:
                self.server.accept(time.monotonic() + 3)
                self.server.receive(time.monotonic() + 3)
                send_reply(self.server, reply, self.original, time.monotonic() + timeout)
                self.result.append('consumed')
            except WindowsProcessError:
                self.result.append('rejected')
            finally:
                self.server.disconnect()
                self.completed.set()
        self.thread = threading.Thread(target=serve)
        self.thread.start()
        self.client = PrivatePipe(self.name, server=False, server_pid=os.getpid())
        self.client.send(b'{"command":"idle"}', time.monotonic() + 3)
        self.assertTrue(self.sent.wait(3))

    def test_reply_survives_after_write_completed_until_delayed_client_consumes_it(self):
        self.owner()
        self.assertFalse(self.completed.is_set(), 'Write completion cannot authorize disconnection')
        reply = receive_reply(self.client, self.original, time.monotonic() + 2)
        self.assertEqual(b'{"synthetic":true}', reply)
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['consumed'], self.result)

    def test_another_generation_or_binding_cannot_acknowledge_an_original_reply(self):
        self.owner()
        altered = {**self.original, 'sequence': self.original['sequence'] + 1}
        receive_reply(self.client, altered, time.monotonic() + 2)
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['rejected'], self.result)

    def test_no_acknowledgement_times_out_without_a_retry_or_success_receipt(self):
        self.owner(timeout=0.3)
        self.assertEqual(b'{"synthetic":true}', self.client.receive(time.monotonic() + 2))
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['rejected'], self.result)

    def test_plain_digest_without_the_original_secret_is_not_an_authenticated_receipt(self):
        self.owner()
        reply = self.client.receive(time.monotonic() + 2)
        fake = {'type': 'reply_received', 'binding': self.original['binding'], 'sequence': self.original['sequence'],
                'digest': hashlib.sha256(reply).hexdigest(), 'mac': '0' * 64}
        self.client.send(json.dumps(fake).encode(), time.monotonic() + 2)
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['rejected'], self.result)

    def test_duplicate_control_fields_and_unknown_keys_are_not_normalized_into_receipts(self):
        self.owner()
        self.client.receive(time.monotonic() + 2)
        self.client.send(b'{"type":"reply_received","type":"reply_received"}', time.monotonic() + 2)
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['rejected'], self.result)

    def test_non_ascii_receipt_is_a_closed_error_not_an_unhandled_comparison_exception(self):
        self.owner()
        reply = self.client.receive(time.monotonic() + 2)
        fake = {'type': 'reply_received', 'binding': self.original['binding'], 'sequence': self.original['sequence'],
                'digest': hashlib.sha256(reply).hexdigest(), 'mac': '\u00e9' * 64}
        self.client.send(json.dumps(fake, ensure_ascii=False).encode(), time.monotonic() + 2)
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['rejected'], self.result)

    def test_client_disconnect_after_sending_receipt_does_not_drop_the_receipt(self):
        self.owner()
        self.assertEqual(b'{"synthetic":true}', receive_reply(self.client, self.original, time.monotonic() + 2))
        self.client.close()
        self.client = None
        self.assertTrue(self.completed.wait(3))
        self.assertEqual(['consumed'], self.result)


if __name__ == '__main__':
    unittest.main()
