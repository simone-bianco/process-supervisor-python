# Owned MCP byte frames — Windows host primitive

`py_laravel_supervisor.framed_channel.FramedStdioChannel` is a raw framed byte API over an **already owned** `ManagedWindowsProcess`. It reuses `DuplexChannel`'s bounded writer/stderr handling and exact Job cleanup. It does not launch a child, parse JSON-RPC, authenticate an Agent, select a deployment, reconnect or retry a request.

The control plane must own admission and supply a process with its original stdin/stdout/stderr handles, exact Job and sandbox. `StdioProtocolClient` in the gateway remains the MCP protocol owner; it determines whether a write is a notification, whether to read more than one frame and whether an incoming frame matches the original call.

## Contract

```python
channel = FramedStdioChannel(process, frame_bytes=262144, stderr_bytes=65536)
channel.assert_usable()
channel.assert_idle()
channel.write(frame, timeout=3.0)       # bytes, exactly one LF-terminated frame
reply = channel.read(timeout=2.0)      # bytes including its terminating LF
channel.close()                       # exact owned Job and I/O quiescence
```

Timeouts are **seconds**, strictly positive and at most 60 per I/O call. The caller must calculate remaining time from its own original request deadline, not give every frame a fresh whole-request budget. Input/output frame bounds are 1 KiB through 1 MiB; stderr is counted and discarded, never persisted or returned. Frame contents are opaque, including non-ASCII bytes. Invalid outbound frame shape/size is rejected before I/O without poisoning an otherwise usable channel.

`write` completes after the whole frame was written and does not wait for a response. This permits `notifications/initialized` and other notification writes. `read` returns one complete bounded frame; the gateway may read multiple notifications before its matching response. A partial write/read is never replayed. EOF before a complete frame, output overflow or an exceeded deadline closes the original channel and raises an error, not truncated success.

The host must serialize each protocol interaction across the separate methods. Concurrent method entry fails immediately and does not reset a waiting deadline or kill the already active call. `assert_idle` is a nonblocking readiness assertion: it checks queued complete frames, incomplete buffered bytes **and bytes still pending in the original Windows kernel pipe**. The only stdout reader peeks and consumes already available bytes under the same short lock, so scheduler lag cannot turn unread output into an apparent idle channel. It cannot promise that a malicious child will never emit a future frame after that observation; protocol correlation still belongs to the gateway.

`close` terminates only the exact original Job and verifies process, Job accounting and all I/O threads before marking success. If cleanup fails, handles remain available for cleanup of that same owner; the next `close` does not silently succeed. Close is not proof that unrelated remote jobs have stopped or that an application-level remote capacity debt can be released.

`buffered_bytes` exposes only the count of a partial in-memory frame for bounded diagnostics; it is not a public data API. No payload, credential, raw stderr or provider configuration is logged by this primitive.

## Verification

`tests/test_mcp_framed_channel.py` exercises the real Windows process owner and AppContainer with test-owned Node scripts/directories. It covers notification-only writes; multiple response frames; exact Unicode bytes; partial output and idle state including a deliberately paused reader; complete trailing frames; stdin backpressure; timeouts without replay; malformed input; EOF; competing readers; and cleanup failure followed by exact-owner retry.

At the verified 2026-09-17 snapshot, all ten new tests and all 37 `test_mcp_*.py` tests passed with no skips. This is source-level Windows evidence, not a deployment of the resident or proof that Node/Docker installation, workspace binding, secrets, HTTP/native gateway delivery or Tasks are fully integrated. The code did not change `duplex.py`, `windows.py` or `appcontainer.py`. Host integration must verify the executed module path and preserve the original binding; synchronize/restart a resident only when that resident actually consumes changed source under its own operational gate.
