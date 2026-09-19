"""Bounded reply-consumption receipt on the original private IPC connection.

Windows DisconnectNamedPipe discards unread data even after WriteFile completes.
The owner must consume this acknowledgement before disconnecting. This is only
control-envelope transport, not MCP interpretation, retry or operation authority.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re

from .mcp_pipe import PrivatePipe
from .windows import WindowsProcessError

MAX_ACK_BYTES = 512


def _identity(original: dict) -> tuple[str, str, int]:
    if not isinstance(original, dict):
        raise WindowsProcessError('invalid original exchange identity')
    token, binding, sequence = original.get('token'), original.get('binding'), original.get('sequence')
    if (not isinstance(token, str) or re.fullmatch(r'[a-f0-9]{64}', token) is None
            or not isinstance(binding, str) or re.fullmatch(r'[a-f0-9]{64}', binding) is None
            or type(sequence) is not int or not 1 <= sequence <= 2147483647):
        raise WindowsProcessError('invalid original exchange identity')
    return token, binding, sequence


def _receipt(reply: bytes, original: dict) -> dict:
    token, binding, sequence = _identity(original)
    if not isinstance(reply, bytes) or not 1 <= len(reply) <= PrivatePipe.MAX_PACKET:
        raise WindowsProcessError('owned exchange frame budget exceeded')
    digest = hashlib.sha256(reply).hexdigest()
    signed = (binding + '\0' + str(sequence) + '\0' + digest).encode('ascii')
    return {'type': 'reply_received', 'binding': binding, 'sequence': sequence, 'digest': digest,
            'mac': hmac.new(bytes.fromhex(token), signed, hashlib.sha256).hexdigest()}


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise WindowsProcessError('ambiguous private receipt')
        value[key] = item
    return value


def send_reply(pipe: PrivatePipe, reply: bytes, original: dict, deadline: float) -> None:
    """Send once, then prove that the original client consumed exactly these bytes.

    The caller retains process cleanup responsibility on failure, and may only
    disconnect the pipe after success or abandoning this same original exchange.
    """
    expected = _receipt(reply, original)
    pipe.send(reply, deadline)
    raw = pipe.receive(deadline)
    if len(raw) > MAX_ACK_BYTES:
        raise WindowsProcessError('private receipt budget exceeded')
    try:
        acknowledgement = json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs)
    except (ValueError, UnicodeError, RecursionError):
        raise WindowsProcessError('invalid private receipt') from None
    if (not isinstance(acknowledgement, dict) or set(acknowledgement) != set(expected)
            or acknowledgement['type'] != expected['type'] or acknowledgement['binding'] != expected['binding']
            or type(acknowledgement['sequence']) is not int or acknowledgement['sequence'] != expected['sequence']
            or not isinstance(acknowledgement['digest'], str) or not isinstance(acknowledgement['mac'], str)
            or re.fullmatch(r'[a-f0-9]{64}', acknowledgement['digest']) is None
            or re.fullmatch(r'[a-f0-9]{64}', acknowledgement['mac']) is None
            or not hmac.compare_digest(acknowledgement['digest'], expected['digest'])
            or not hmac.compare_digest(acknowledgement['mac'], expected['mac'])):
        raise WindowsProcessError('original exchange receipt rejected')


def receive_reply(pipe: PrivatePipe, original: dict, deadline: float) -> bytes:
    """Consume once and acknowledge on the same pipe. Do not reconnect or retry."""
    _identity(original)
    reply = pipe.receive(deadline)
    receipt = _receipt(reply, original)
    pipe.send(json.dumps(receipt, separators=(',', ':'), ensure_ascii=True).encode('ascii'), deadline)
    return reply
