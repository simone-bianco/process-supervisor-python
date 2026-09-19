# Owned Docker MCP runtime boundary

Status: verified implementation slice, 2026-09-17. This document covers only `py_laravel_supervisor.mcp_docker`; Laravel remains admission/deployment authority and `FramedStdioChannel` remains the byte transport.

## Public API

The module intentionally exposes two operational entrypoints:

- `spawn(plan: dict) -> DockerOwnedProcess`
- `prepare_state(plan: dict) -> DockerStatePreparation`

There is no public `launch_docker` compatibility alias. The worker must call `spawn` explicitly.

### Stateless `spawn` plan

Required keys are exactly:

- `docker_binary`: canonical absolute existing Docker CLI path;
- `engine_endpoint`: local Windows named pipe (`npipe:////./pipe/...`);
- `engine_id`: frozen local engine identity;
- `image_id`: `sha256:<64 lowercase hex>`;
- `defaults_hash`: SHA-256 of the raw Docker image `Config` represented as UTF-8 canonical JSON with recursive sorted keys, compact `,` / `:` separators, `ensure_ascii=false` and NaN forbidden;
- `container_name`: `localgpt-mcp-<48 lowercase hex>`;
- `installation_id`, `subject_id`, `pin_id`, `incarnation_id`: frozen server-owned opaque identities;
- `working_directory`: canonical absolute existing private CLI-home/scratch directory;
- `timeout_seconds`: finite `> 0` and `<= 60`.

Only two additional keys are recognized: `state_volume` and `state_env`. A stateless deployment should omit both (or send null). Unknown keys fail closed.

`spawn` accepts only a reachable local Linux Docker engine whose current ID equals `engine_id`. It never starts Docker Desktop/the daemon, pulls, uses `docker run`, executes arbitrary commands, prunes, deletes, adopts existing objects or reads ambient Docker context/config. Every CLI command passes the frozen `--host` and uses an exact private environment.

The image is inspected by immutable ID before create. The complete image `Config` must reproduce `defaults_hash`; image labels may not predeclare `localgpt.*`; persistent image volumes outside `/mcp-state` are rejected, and `/mcp-state` itself requires a state plan (currently blocked as described below).

Container creation is separate from start and enforces image ID directly, `--pull never`, network `none`, read-only root, UID/GID `65532:65532`, `cap-drop=ALL`, `no-new-privileges`, restart `no`, disabled healthcheck, stdin open without TTY, `log-driver=none`, no host binds/ports/devices and a bounded `/tmp` tmpfs. Managed installation/subject/pin/incarnation labels are exact. Effective inspect is verified while stopped and again immediately before the owned `docker container start --attach --interactive <original-id>` CLI is created inside its exact Windows Job Object.

`DockerOwnedProcess` exposes the original owned CLI `stdin_fd`, `stdout_fd`, `stderr_fd`, `process_handle`, `cleanup_job_handle`, `poll()`, `wait()`, `terminate_tree()` and `close()`, so it is directly compatible with `FramedStdioChannel`. Python does not parse MCP JSON-RPC. Every Docker lifecycle mutation revalidates the frozen engine and original container identity/config/labels. `terminate_tree()` and `close()` stop and prove the original container quiescent before reducing the CLI Job; killing the CLI is never treated as evidence that the container stopped. Timeout, lost identity or an ambiguous Docker effect produces non-retryable `DockerRecoveryRequired` and is never auto-replayed/adopted.

## Stateful volume identity — not runtime-ready

`prepare_state(plan)` is explicit identity provisioning, not proof that the volume is writable by the MCP runtime.

Its exact required keys are `docker_binary`, `engine_endpoint`, `engine_id`, `installation_id`, `volume_name`, `state_domain`, `scope`, `owner_key`, `working_directory`, `timeout_seconds`. `volume_name` is restricted to `localgpt-mcp-state-<48 lowercase hex>`, `scope` is `global|workspace`, and all values are server-owned.

The operation proves the name is absent, then performs exactly one `docker volume create --driver local` with no driver options and exact labels: `localgpt.managed=true`, installation ID, state domain, scope and owner key. Post-create inspect must prove the same local-volume identity. Ambiguous create is retained as non-retryable recovery: the backend never retries, adopts or deletes the object.

The result is intentionally:

- `writable_ready=false`
- `blocker=STATE_UID_GID_UNPROVEN`

Docker's CLI reference lists **mounts created by the user in the container** among copy corner cases and documents `docker exec ... tar` as the generic workaround. That is not sufficient to conclude that every daemon-managed named volume is unwritable through the archive API: current Moby `checkWritablePath` explicitly allows extraction when the destination resolves inside a mounted resource whose mount is RW. Therefore `docker cp -a`/archive extraction into our exact stopped-container named-volume shape is **[DA VERIFICARE] on the real approved engine**, not rejected solely from the CLI wording. Generic `docker exec ... tar` remains outside this backend's approved authority. Primary references: <https://docs.docker.com/reference/cli/docker/container/cp/#corner-cases> and Moby `daemon/archive_unix.go::checkWritablePath`.

Docker separately documents that an empty volume can be pre-populated from files already present at the image mount destination when the volume is mounted without `volume-nocopy`. See <https://docs.docker.com/engine/storage/volumes/#mounting-a-volume-over-existing-data>. This is also a viable *future mechanism*, but neither mechanism is a proof by itself: using an arbitrary MCP image would trust unreviewed filesystem metadata and executing that image to test writes would execute untrusted code.

The minimum evidence-backed extension therefore requires a **dedicated trusted state-proof OCI artifact**, approved/acquired by the existing digest-pinned installer authority, not invented by this Python backend. Its immutable image must freeze: image digest + Config hash; a `/mcp-state` directory whose layer metadata is UID/GID `65532:65532`; and one fixed non-shell verifier executable. A future proof step may then (1) create an owned helper container over the original empty managed volume **without** `volume-nocopy` so Docker performs documented copy-up, (2) re-inspect engine/image/container/volume/labels, (3) start only the fixed verifier as `65532:65532` under network-none/read-only/cap-drop-all/no-new-privileges/restart-no/log-none policy, and (4) accept only a bounded receipt binding nonce + state-domain + generation + effective UID/GID + write/read/delete success. Ambiguous create/start/receipt remains retained recovery with no replay/adoption. No such helper artifact or approval exists in the current repository/contract, so this extension is **not implemented** here.

Accordingly **persistent Docker runtime remains rejected before container creation even if the caller invents a `writable_ready=true` field**. That field is not accepted by `spawn`; caller assertions are not proof. `prepare_state` continues to emit only `volume ls/create/inspect` operations; regression coverage forbids `cp`, `exec`, container start/run, or hidden volume driver options on that path. Stateless Docker is the only currently runnable Docker mode.

## Future state environment contract

`state_env` is accepted syntactically only as a single environment-variable **name** matching `[A-Z][A-Z0-9_]{0,63}`. `NODE_*`, `NPM_*`, `LD_*`, `DYLD_*`, `PATH`, `HOME`, `USERPROFILE`, `TEMP`, `TMP`, `APPDATA`, `LOCALAPPDATA`, `SYSTEMROOT`, `WINDIR` and `MCP_STATE_DIR` are forbidden. The caller never supplies an environment value or path.

If an independently verified state-writability contract is introduced later, the only value this backend will inject is exactly:

`<state_env>=/mcp-state/memory.json`

The selected name may not collide with an image `Config.Env` entry. Current `spawn` rejects every non-null persistent `state_volume` with `STATE_UID_GID_UNPROVEN`, so this future environment contract cannot presently be used to claim stateful readiness.

## Verification and real-engine status

The deterministic suite never requires Docker. It covers strict stateless admission, explicit `spawn` / `prepare_state` API shape, local Linux engine identity, image/default drift, post-create security drift, non-retryable cleanup ambiguity, original-container stop ordering, no ambient Docker config, explicit state-volume identity provisioning, rejection of caller-supplied readiness/helper authority, an actual CLI-emission proof that `prepare_state` is volume-only, and the fixed future `state_env` contract. A Windows receipt test uses a real Job Object and owned stdio pipes with a Python echo process behind the fake Docker control plane, proving byte-for-byte `FramedStdioChannel` compatibility without starting Docker.

A bounded real availability check on 2026-09-17 used the installed `C:\Program Files\Docker\Docker\resources\bin\docker.exe` with explicit `--host npipe:////./pipe/dockerDesktopLinuxEngine`. The CLI was present (client 29.2.0), but the named pipe was absent and the command failed with daemon-unavailable/file-not-found. Docker Desktop/service was not started. Therefore no real container runtime or UID/GID state-writability proof is claimed by this batch.
