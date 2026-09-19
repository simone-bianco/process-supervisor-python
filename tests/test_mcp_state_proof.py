"""Closed recipe/receipt tests only: they do not certify a Docker engine or Linux ownership."""
from __future__ import annotations

import hashlib
import io
import json
import tarfile
import unittest

from py_laravel_supervisor.mcp_state_proof import challenge, initialization_archive, stdin_request, verify_receipt, StateProofRejected


def original():
    return {'installation_id': 'instance-original', 'state_domain_id': 'workspace-a-memory', 'owner_scope': 'workspace',
            'owner_key': 'workspace-a', 'generation': 4, 'engine_id': 'engine-original',
            'volume_name': 'localgpt-mcp-state-' + 'a' * 48, 'helper_image_id': 'sha256:' + 'b' * 64,
            'helper_config_sha256': 'c' * 64, 'verifier_sha256': 'd' * 64, 'nonce': 'e' * 64}


def good(context):
    return json.dumps({'schema': 'localgpt.mcp-state-proof.v1', 'nonce': context['nonce'],
        'context_sha256': hashlib.sha256(challenge(context)).hexdigest(), 'uid': 65532, 'gid': 65532,
        'state_domain_id': context['state_domain_id'], 'generation': context['generation'],
        'directory_mode': '0700', 'write_read_delete': True}, separators=(',', ':')).encode()


class StateProofRecipeTests(unittest.TestCase):
    def test_deterministic_archive_has_only_fresh_root_and_public_owner_marker(self):
        context = original()
        one, digest = initialization_archive(context)
        self.assertEqual((one, digest), initialization_archive(context))
        with tarfile.open(fileobj=io.BytesIO(one), mode='r:') as archive:
            members = archive.getmembers()
            self.assertEqual(['.', './.localgpt-state-owner.json'], [member.name for member in members])
            self.assertTrue(members[0].isdir()); self.assertTrue(members[1].isfile())
            for member in members:
                self.assertEqual(65532, member.uid); self.assertEqual(65532, member.gid)
                self.assertFalse(member.issym()); self.assertFalse(member.islnk())
                self.assertEqual(0, member.mtime)
            self.assertEqual(0o700, members[0].mode); self.assertEqual(0o400, members[1].mode)
            marker = archive.extractfile(members[1]).read()
            self.assertEqual(challenge(context), marker)
            self.assertEqual(hashlib.sha256(marker).hexdigest(), digest)
            self.assertNotIn(b'password', marker)
        self.assertLessEqual(len(one), 20480)

    def test_stdin_request_is_bounded_and_does_not_allow_a_different_command_or_executable(self):
        context = original()
        raw = stdin_request(context)
        self.assertLessEqual(len(raw), 256)
        self.assertEqual({'schema': 'localgpt.mcp-state-proof.request.v1',
            'context_sha256': hashlib.sha256(challenge(context)).hexdigest()}, json.loads(raw))
        self.assertNotIn(context['owner_key'].encode(), raw)

    def test_receipt_binds_all_original_owner_and_helper_context_and_no_ready_boolean_is_issued(self):
        context = original()
        receipt = verify_receipt(good(context), context)
        self.assertTrue(receipt['write_read_delete'])
        self.assertNotIn('writable_ready', receipt)
        for key, value in {'installation_id':'other', 'state_domain_id':'other', 'owner_scope':'global', 'owner_key':'other',
            'generation':5, 'engine_id':'other', 'volume_name':'localgpt-mcp-state-'+'f'*48,
            'helper_image_id':'sha256:'+'f'*64, 'helper_config_sha256':'f'*64, 'verifier_sha256':'f'*64, 'nonce':'f'*64}.items():
            with self.subTest(key=key), self.assertRaises(StateProofRejected):
                verify_receipt(good(context), {**context, key:value})

    def test_untrusted_context_never_becomes_a_tar_path_or_command(self):
        context = original()
        variants = [
            {**context, 'owner_key':'../../other'}, {**context, 'state_domain_id':'a\\b'},
            {**context, 'helper_image_id':'node:latest'}, {**context, 'uid':0}, {**context, 'command':'sh -c true'},
            {**context, 'generation':True}, {**context, 'generation':0}, {**context, 'generation':2**31},
            {**context, 'owner_scope':'automatic'}, {**context, 'volume_name':'other-volume'},
            {**context, 'nonce':'f'*64+'\n'}, {**context, 'helper_config_sha256':'é'*64},
        ]
        for value in variants:
            with self.subTest(value=value), self.assertRaises(StateProofRejected): initialization_archive(value)

    def test_root_uid_and_partial_read_or_delete_never_pass(self):
        context = original(); receipt = json.loads(good(context))
        for key, value in {'uid':0,'gid':0,'directory_mode':'0777','write_read_delete':False}.items():
            with self.subTest(key=key), self.assertRaises(StateProofRejected):
                verify_receipt(json.dumps({**receipt,key:value}).encode(),context)
        for key, value in {'uid':65532.0,'gid':'65532','write_read_delete':1}.items():
            with self.subTest(key=key), self.assertRaises(StateProofRejected):
                verify_receipt(json.dumps({**receipt,key:value}).encode(),context)

    def test_duplicate_or_unknown_receipt_fields_and_malformed_bytes_are_rejected(self):
        context=original(); receipt=good(context)
        bad=[receipt[:-1]+b',"uid":65532}', receipt[:-1]+b',"ready":true}', b'[]', b'null', b'NaN',
             b'\xff', b'x'*1025, receipt+b'{}', json.dumps({**json.loads(receipt),'write_read_delete':None}).encode()]
        for value in bad:
            with self.subTest(value=value[:80]), self.assertRaises(StateProofRejected): verify_receipt(value, context)


if __name__ == '__main__': unittest.main()
