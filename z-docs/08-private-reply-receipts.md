# Private IPC: bounded reply-consumption receipt

`py_laravel_supervisor.mcp_exchange` is a private control transport primitive. It does not parse MCP, authorize a tool or replace the PostgreSQL dispatch/release fences.

## Problem and ownership

A completed Windows pipe write does not prove that its peer has read the bytes. Disconnecting a named-pipe server immediately after `send()` can discard an unread response. This was reproduced with a real private pipe and a barrier delaying the reader until after the server's write completed and disconnected. The symptom in the actual PHP/Node bridge was a successful owned-process readiness receipt followed by failure of the very first `idle` exchange.

The owner keeps the original connection alive until a bounded receipt proves that the original client consumed that response. There is no blocking flush, reconnect, retry, public listener or alternate protocol.

## Caller contract

The original request supplies an in-memory dictionary with `token` and `binding` (64 lowercase hex each) and integer `sequence` in `[1, 2147483647]`. This is the already established private channel context, never an application credential.

Server success path:

```python
from py_laravel_supervisor.mcp_exchange import send_reply

# Run the already admitted original control operation first.
# reply_bytes is the serialized bounded control response.
send_reply(pipe, reply_bytes, original_request, deadline)
# Only now may the owner disconnect this pipe instance.
```

Client path:

```python
from py_laravel_supervisor.mcp_exchange import receive_reply

pipe.send(serialized_original_request, deadline)
reply_bytes = receive_reply(pipe, original_request, deadline)
# Then seal the payload for any Process adapter that spools stdout.
```

The same absolute monotonic deadline covers the response and its receipt. The wire receipt contains exact binding, sequence and SHA-256 of the response plus HMAC-SHA-256 using the original per-channel secret. It contains no raw token. Unknown/duplicate fields, wrong types, non-ASCII digests, invalid authentication or missing receipt fail closed. No result is normalized into another request, no old sequence is accepted as current, and failure never authorizes a re-dispatch.

`send_reply()` has no MCP/process side effects other than this original IPC exchange. On failure the caller retains its existing cleanup/recovery responsibility. The owner must not report successful delivery merely because it wrote bytes. A consumed receipt proves delivery to the immediate helper, **not** eventual release to a remote Agent; the gateway still rechecks release authority and the ledger retains any uncertain effect.

The helper does not log plaintext responses, credentials or the channel token. If a local Process transport writes stdout to files, encrypt/seal the response before output using the existing `control_seal` boundary. Error responses need the same delivery discipline when they are sent; malformed identities can be disconnected without inventing a new identity or retry.

## Verification

`tests/test_mcp_exchange.py` runs seven tests against actual Windows `PrivatePipe` and overlapped I/O: delayed reader, original-sequence mismatch, missing ACK deadline, forged MAC, duplicate fields, non-ASCII rejection, and client close after sending the receipt. All passed on the existing Windows test host. The independent reproducer recorded the previous send/disconnect data-loss behavior.

The Laravel host worker `scripts/mcp-runtime/worker.py` now uses both helpers atomically. Host `McpWorkspaceSharedMemoryGatewayTest` passed four real HTTP/PostgreSQL/AppContainer Node cases (84 assertions): A1/A2 shared state, B isolation, C missing-binding denial, global shared deployment, Stop at final release and re-enable with retained data/immutable old pins. The installed artifact in those tests is synthetic; no transport/authority/OS mocks. The separate existing physical-channel suite passed six tests (65 assertions). These results validate this Node path, not arbitrary third-party recipes or Docker writable-state readiness.
