"""Versioned data for a separately approved, non-root Docker state verifier.

This module performs no Docker command, process spawn, state mutation or image
selection. It never upgrades labels/caller booleans to writable readiness. A caller
must own the exact originally empty volume and trusted verifier image, execute the
frozen restricted helper plan and validate the original runtime lifecycle before
accepting a receipt. The MCP server image is not implicitly a trusted helper.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
from typing import Any

SCHEMA = 'localgpt.mcp-state-proof.v1'
UID = GID = 65532
ROOT_MODE = 0o700
MAX_RECEIPT = 1024
_HEX = re.compile(r'[a-f0-9]{64}\Z')
_DIGEST = re.compile(r'sha256:[a-f0-9]{64}\Z')
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
_VOLUME = re.compile(r'localgpt-mcp-state-[a-f0-9]{48}\Z')
_FIELDS = frozenset({'installation_id', 'state_domain_id', 'owner_scope', 'owner_key', 'generation',
                     'engine_id', 'volume_name', 'helper_image_id', 'helper_config_sha256', 'verifier_sha256', 'nonce'})


class StateProofRejected(ValueError):
    pass


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    data = {}
    for key, value in pairs:
        if key in data:
            raise StateProofRejected('ambiguous state proof')
        data[key] = value
    return data


def challenge(context: dict[str, Any]) -> bytes:
    """Freeze a public owner/challenge marker. No URLs, paths, argv or secrets are accepted."""
    if type(context) is not dict or set(context) != _FIELDS:
        raise StateProofRejected('invalid state proof context')
    for field in ('installation_id', 'state_domain_id', 'owner_key', 'engine_id'):
        if not isinstance(context[field], str) or _IDENTIFIER.fullmatch(context[field]) is None:
            raise StateProofRejected('invalid state proof owner')
    if context['owner_scope'] not in ('global', 'workspace'):
        raise StateProofRejected('invalid state proof scope')
    if type(context['generation']) is not int or not 1 <= context['generation'] <= 2147483647:
        raise StateProofRejected('invalid state proof generation')
    for field in ('helper_config_sha256', 'verifier_sha256', 'nonce'):
        if not isinstance(context[field], str) or _HEX.fullmatch(context[field]) is None:
            raise StateProofRejected('invalid state proof digest')
    if not isinstance(context['helper_image_id'], str) or _DIGEST.fullmatch(context['helper_image_id']) is None:
        raise StateProofRejected('helper image must be immutable')
    if not isinstance(context['volume_name'], str) or _VOLUME.fullmatch(context['volume_name']) is None:
        raise StateProofRejected('unowned state volume')
    data = {'schema': SCHEMA, **context, 'uid': UID, 'gid': GID}
    marker = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')
    if len(marker) > 2048:
        raise StateProofRejected('state proof context budget exceeded')
    return marker


def stdin_request(context: dict[str, Any]) -> bytes:
    """The fixed helper command accepts no argv overrides; send only this bounded public request."""
    marker = challenge(context)
    return json.dumps({'schema': 'localgpt.mcp-state-proof.request.v1',
        'context_sha256': hashlib.sha256(marker).hexdigest()}, separators=(',', ':')).encode('ascii') + b'\n'


def initialization_archive(context: dict[str, Any]) -> tuple[bytes, str]:
    """Build a deterministic tar for extraction into the original, *new* /mcp-state.

    Exactly the root directory and one public marker: no traversal, links,
    application files, generic chown command or executable is included. Never
    apply this archive to an existing/active data domain merely to repair it.
    """
    marker = challenge(context)
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w', format=tarfile.USTAR_FORMAT) as output:
        directory = tarfile.TarInfo('.')
        directory.type = tarfile.DIRTYPE
        directory.mode = ROOT_MODE
        directory.uid = UID; directory.gid = GID; directory.mtime = 0
        output.addfile(directory)
        file = tarfile.TarInfo('./.localgpt-state-owner.json')
        file.mode = 0o400; file.uid = UID; file.gid = GID; file.mtime = 0; file.size = len(marker)
        output.addfile(file, io.BytesIO(marker))
    return archive.getvalue(), hashlib.sha256(marker).hexdigest()


def verify_receipt(raw: bytes, original_context: dict[str, Any]) -> dict[str, Any]:
    """Validate only a receipt's shape and original context, not the trust of its emitter.

    There is deliberately no 'writable_ready=True' return field. The control plane
    must also prove the approved helper image, UID/config, exit and original volume.
    """
    marker = challenge(original_context)
    if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_RECEIPT:
        raise StateProofRejected('state proof receipt budget exceeded')
    try:
        receipt = json.loads(raw.decode('utf8'), object_pairs_hook=_pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(StateProofRejected('non-finite proof value')))
    except (ValueError, UnicodeError, RecursionError):
        raise StateProofRejected('invalid state proof receipt') from None
    expected = {'schema': SCHEMA, 'nonce': original_context['nonce'], 'context_sha256': hashlib.sha256(marker).hexdigest(),
                'state_domain_id': original_context['state_domain_id'], 'generation': original_context['generation'],
                'uid': UID, 'gid': GID, 'directory_mode': '0700', 'write_read_delete': True}
    if type(receipt) is not dict or set(receipt) != set(expected):
        raise StateProofRejected('invalid state proof fields')
    if (type(receipt['uid']) is not int or type(receipt['gid']) is not int or type(receipt['generation']) is not int
            or receipt['write_read_delete'] is not True):
        raise StateProofRejected('invalid state proof types')
    if receipt != expected:
        raise StateProofRejected('state proof does not match original owner')
    return expected
