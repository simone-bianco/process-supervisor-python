"""Owned local-Docker stdio launcher for one frozen MCP runtime plan.

This module is intentionally not an MCP parser, Docker installer, daemon manager,
container adopter, or arbitrary command surface. Laravel freezes the plan; this
backend validates it, can explicitly provision only a named-volume identity, and
creates exactly one stopped runtime container before attaching one owned Docker
CLI process whose pipes can be consumed by :class:`FramedStdioChannel`.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .windows import (
    ManagedWindowsProcess,
    WindowsProcessError,
    close_handle,
    create_job,
    job_active_processes,
    spawn_process,
)

_IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")
_CONTAINER_NAME = re.compile(r"localgpt-mcp-[a-f0-9]{48}\Z")
_STATE_VOLUME_NAME = re.compile(r"localgpt-mcp-state-[a-f0-9]{48}\Z")
_STATE_ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_RESERVED_STATE_ENV = re.compile(
    r"(?:NODE_|NPM_|LD_|DYLD_|PATH\Z|HOME\Z|USERPROFILE\Z|TEMP\Z|TMP\Z|APPDATA\Z|LOCALAPPDATA\Z|SYSTEMROOT\Z|WINDIR\Z|MCP_STATE_DIR\Z)",
    re.IGNORECASE,
)
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_VOLUME_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_NPIPE_PREFIX = "npipe:////./pipe/"
_STATE_ENV_VALUE = "/mcp-state/memory.json"
_TMPFS_BYTES = 64 * 1024 * 1024
_TMPFS_OPTIONS = frozenset({"rw", "noexec", "nosuid", "nodev", f"size={_TMPFS_BYTES}"})
_CLI_OUTPUT_LIMIT = 1024 * 1024
_CLI_STDERR_LIMIT = 64 * 1024
_CLI_MEMORY_BYTES = 512 * 1024 * 1024
_MANAGED_CONTAINER_LABEL_KEYS = frozenset({
    "localgpt.managed",
    "localgpt.installation_id",
    "localgpt.subject_id",
    "localgpt.pin_id",
    "localgpt.incarnation_id",
})


class DockerBackendError(WindowsProcessError):
    """A deterministic Docker admission/runtime failure with no raw CLI output."""


class DockerRecoveryRequired(DockerBackendError):
    """An effect may exist but cannot be proven safe; caller must reconcile, not retry."""

    retryable = False


@dataclass(frozen=True, slots=True)
class _StateVolume:
    name: str
    state_domain: str
    scope: str
    owner_key: str


@dataclass(frozen=True, slots=True)
class _DockerStatePlan:
    docker_binary: Path
    engine_endpoint: str
    engine_id: str
    installation_id: str
    volume_name: str
    state_domain: str
    scope: str
    owner_key: str
    working_directory: Path
    timeout_seconds: float

    @property
    def labels(self) -> dict[str, str]:
        return {
            "localgpt.managed": "true",
            "localgpt.installation_id": self.installation_id,
            "localgpt.state_domain": self.state_domain,
            "localgpt.scope": self.scope,
            "localgpt.owner_key": self.owner_key,
        }


@dataclass(frozen=True, slots=True)
class DockerStatePreparation:
    volume_id: str
    engine_id: str
    state_domain: str
    owner_scope: str
    owner_key: str
    labels: tuple[tuple[str, str], ...]
    writable_ready: bool = False
    blocker: str = "STATE_UID_GID_UNPROVEN"


@dataclass(frozen=True, slots=True)
class _DockerPlan:
    docker_binary: Path
    engine_endpoint: str
    engine_id: str
    image_id: str
    defaults_hash: str
    container_name: str
    installation_id: str
    subject_id: str
    pin_id: str
    incarnation_id: str
    working_directory: Path
    timeout_seconds: float
    state_volume: _StateVolume | None
    state_env: str | None

    @property
    def managed_labels(self) -> dict[str, str]:
        return {
            "localgpt.managed": "true",
            "localgpt.installation_id": self.installation_id,
            "localgpt.subject_id": self.subject_id,
            "localgpt.pin_id": self.pin_id,
            "localgpt.incarnation_id": self.incarnation_id,
        }

    @property
    def state_labels(self) -> dict[str, str]:
        if self.state_volume is None:
            return {}
        return {
            "localgpt.managed": "true",
            "localgpt.installation_id": self.installation_id,
            "localgpt.state_domain": self.state_volume.state_domain,
            "localgpt.scope": self.state_volume.scope,
            "localgpt.owner_key": self.state_volume.owner_key,
        }


@dataclass(frozen=True, slots=True)
class _CliResult:
    exit_code: int
    stdout: bytes


class DockerOwnedProcess:
    """FramedStdioChannel-compatible owner for one original container + attach CLI.

    Docker container lifecycle and Windows CLI-process lifecycle are deliberately
    separate.  A CLI Job termination is never evidence that the container stopped.
    """

    def __init__(
        self,
        *,
        plan: _DockerPlan,
        client: _DockerCli,
        container_id: str,
        image_config: Mapping[str, Any],
        expected_labels: Mapping[str, str],
        process: ManagedWindowsProcess,
        cleanup_job_handle: int,
    ) -> None:
        self._plan = plan
        self._client = client
        self._container_id = container_id
        self._image_config = dict(image_config)
        self._expected_labels = dict(expected_labels)
        self._process = process
        self.cleanup_job_handle: int | None = cleanup_job_handle
        self._container_stopped = False
        self._local_closed = False
        self._recovery_error: DockerRecoveryRequired | None = None

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def process_handle(self) -> int:
        return self._process.process_handle

    @property
    def job_handle(self) -> int | None:
        return self.cleanup_job_handle

    @property
    def stdin_fd(self) -> int | None:
        return self._process.stdin_fd

    @property
    def stdout_fd(self) -> int | None:
        return self._process.stdout_fd

    @property
    def stderr_fd(self) -> int | None:
        return self._process.stderr_fd

    @property
    def container_id(self) -> str:
        return self._container_id

    def poll(self) -> int | None:
        if self._local_closed:
            return 1
        return self._process.poll()

    def wait(self, timeout: float) -> int | None:
        if self._local_closed:
            return 1
        return self._process.wait(timeout)

    def terminate_tree(self) -> None:
        if self._recovery_error is not None:
            raise self._recovery_error
        if self._local_closed:
            return
        try:
            if not self._container_stopped:
                record = self._owned_container()
                state = _container_state(record)
                if state["running"]:
                    self._client.stop_container(self._container_id)
                    record = self._owned_container()
                    state = _container_state(record)
                if state["running"] or state["status"] not in {"created", "exited"}:
                    raise DockerRecoveryRequired("owned Docker container quiescence is unproven")
                self._container_stopped = True
            if self._process.poll() is None:
                self._process.terminate_tree()
        except DockerRecoveryRequired as error:
            self._fail_recovery(error)
        except (DockerBackendError, WindowsProcessError) as error:
            self._fail_recovery(DockerRecoveryRequired("owned Docker container cleanup is uncertain"))

    def close(self) -> None:
        if self._recovery_error is not None:
            raise self._recovery_error
        if self._local_closed:
            return
        self.terminate_tree()
        try:
            if self._process.wait(2.0) is None:
                raise DockerRecoveryRequired("owned Docker CLI did not quiesce")
            job = self.cleanup_job_handle
            if job is None or job_active_processes(job) != 0:
                raise DockerRecoveryRequired("owned Docker CLI Job quiescence is unproven")
            self._process.close()
            close_handle(job)
            self.cleanup_job_handle = None
            self._local_closed = True
        except DockerRecoveryRequired as error:
            self._fail_recovery(error)

    def _owned_container(self) -> Mapping[str, Any]:
        try:
            _assert_engine(self._client, self._plan)
            record = self._client.inspect_container(self._container_id)
            _validate_container(
                record,
                self._plan,
                self._container_id,
                self._image_config,
                self._expected_labels,
                expected_status=None,
            )
            return record
        except DockerRecoveryRequired:
            raise
        except DockerBackendError as error:
            raise DockerRecoveryRequired("original Docker container identity is unproven") from error

    def _fail_recovery(self, error: DockerRecoveryRequired) -> None:
        self._recovery_error = error
        # Local Windows containment is still reduced even when Docker state became
        # uncertain.  This is not evidence that the container stopped.
        try:
            if self._process.process_handle and self._process.poll() is None:
                self._process.terminate_tree()
            self._process.wait(2.0)
        except WindowsProcessError:
            pass
        job = self.cleanup_job_handle
        try:
            quiescent = job is not None and job_active_processes(job) == 0
        except WindowsProcessError:
            quiescent = False
        if quiescent:
            self._process.close()
            close_handle(job)
            self.cleanup_job_handle = None
            self._local_closed = True
        raise error


class _DockerCli:
    def __init__(self, plan: _DockerPlan | _DockerStatePlan) -> None:
        self.plan = plan
        system_root = os.environ.get("SystemRoot") or os.environ.get("WINDIR")
        if not system_root:
            raise DockerBackendError("Windows system root is unavailable")
        private = str(plan.working_directory)
        self.environment = {
            "SystemRoot": system_root,
            "WINDIR": system_root,
            "TEMP": private,
            "TMP": private,
            "HOME": private,
            "USERPROFILE": private,
            "DOCKER_CONFIG": private,
        }

    def info(self) -> Mapping[str, Any]:
        value = self._json(self._run(("info", "--format", "{{json .}}")))
        if not isinstance(value, dict):
            raise DockerBackendError("Docker engine identity response is invalid")
        return value

    def inspect_image(self, image_id: str) -> Mapping[str, Any]:
        value = self._json(self._run(("image", "inspect", image_id)))
        return _one_object(value, "Docker image inspect")

    def inspect_volume(self, name: str) -> Mapping[str, Any]:
        value = self._json(self._run(("volume", "inspect", name)))
        return _one_object(value, "Docker volume inspect")

    def assert_name_absent(self, name: str) -> None:
        result = self._run((
            "container", "ls", "--all", "--no-trunc", "--filter", f"name=^{name}$",
            "--format", "{{.ID}}\t{{.Names}}",
        ))
        if result.stdout.strip():
            raise DockerBackendError("pre-existing Docker container name cannot be adopted")

    def assert_volume_name_absent(self, name: str) -> None:
        result = self._run(("volume", "ls", "--filter", f"name={name}", "--format", "{{.Name}}"))
        try:
            names = result.stdout.decode("utf-8", errors="strict").splitlines()
        except UnicodeDecodeError as error:
            raise DockerBackendError("Docker volume listing is invalid") from error
        if any(candidate == name for candidate in names):
            raise DockerBackendError("pre-existing Docker state volume cannot be adopted")

    def create_volume(self, name: str, labels: Mapping[str, str]) -> str:
        args: list[str] = ["volume", "create", "--driver", "local"]
        for key, value in sorted(labels.items()):
            args.extend(("--label", f"{key}={value}"))
        args.append(name)
        result = self._run(tuple(args), effect=True)
        try:
            created = result.stdout.decode("utf-8", errors="strict").strip()
        except UnicodeDecodeError as error:
            raise DockerRecoveryRequired("Docker volume create returned invalid identity") from error
        if created != name:
            raise DockerRecoveryRequired("Docker volume create returned unexpected identity")
        return created

    def create_container(self, expected_labels: Mapping[str, str]) -> str:
        args: list[str] = [
            "container", "create",
            "--name", self.plan.container_name,
            "--pull", "never",
            "--network", "none",
            "--read-only",
            "--user", "65532:65532",
            "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges=true",
            "--restart", "no",
            "--no-healthcheck",
            "--interactive",
            "--log-driver", "none",
            "--tmpfs", f"/tmp:rw,noexec,nosuid,nodev,size={_TMPFS_BYTES}",
        ]
        for key, value in sorted(expected_labels.items()):
            args.extend(("--label", f"{key}={value}"))
        if self.plan.state_volume is not None:
            args.extend((
                "--mount",
                f"type=volume,src={self.plan.state_volume.name},dst=/mcp-state,volume-nocopy",
            ))
        if self.plan.state_env is not None:
            args.extend(("--env", f"{self.plan.state_env}={_STATE_ENV_VALUE}"))
        args.append(self.plan.image_id)
        result = self._run(tuple(args), effect=True)
        container_id = result.stdout.decode("ascii", errors="strict").strip()
        if re.fullmatch(r"[a-f0-9]{64}", container_id) is None:
            raise DockerRecoveryRequired("Docker create returned an invalid container identity")
        return container_id

    def inspect_container(self, container_id: str) -> Mapping[str, Any]:
        value = self._json(self._run(("container", "inspect", container_id)))
        return _one_object(value, "Docker container inspect")

    def stop_container(self, container_id: str) -> None:
        stop_seconds = max(1, min(60, math.ceil(self.plan.timeout_seconds)))
        self._run(("container", "stop", "--time", str(stop_seconds), container_id), effect=True)

    def spawn_attach(self, container_id: str) -> tuple[ManagedWindowsProcess, int]:
        job: int | None = None
        try:
            job = create_job(
                "Local\\McpDockerAttach-" + uuid.uuid4().hex,
                process_limit=8,
                memory_bytes=_CLI_MEMORY_BYTES,
            )
            process = spawn_process(
                [
                    str(self.plan.docker_binary),
                    "--host", self.plan.engine_endpoint,
                    "container", "start", "--attach", "--interactive", container_id,
                ],
                cwd=self.plan.working_directory,
                environment=self.environment,
                job_handles=[job],
                exact_job_handle=job,
                cleanup_job_handle=job,
                capture_output=True,
                stdin_pipe=True,
                exact_environment=True,
            )
            return process, job
        except (OSError, WindowsProcessError) as error:
            close_handle(job)
            raise DockerRecoveryRequired("Docker attach/start ownership is uncertain") from error

    def _run(self, arguments: tuple[str, ...], *, effect: bool = False) -> _CliResult:
        return _run_owned_cli(
            self.plan.docker_binary,
            ("--host", self.plan.engine_endpoint, *arguments),
            cwd=self.plan.working_directory,
            environment=self.environment,
            timeout=self.plan.timeout_seconds,
            effect=effect,
        )

    @staticmethod
    def _json(result: _CliResult) -> Any:
        try:
            return json.loads(result.stdout.decode("utf-8", errors="strict"), object_pairs_hook=_no_duplicate_object)
        except (UnicodeDecodeError, json.JSONDecodeError, DockerBackendError) as error:
            raise DockerBackendError("Docker JSON response is invalid") from error


def prepare_state(plan: dict[str, Any]) -> DockerStatePreparation:
    """Create and prove one managed named-volume identity without starting an MCP.

    This primitive deliberately does not claim UID/GID write readiness. Docker
    CLI documentation treats user-created mounts conservatively, while current
    Moby writable-path handling admits writable mount points; the exact named-
    volume copy behavior still requires real-engine proof plus a separate trusted
    non-root write/read/delete verifier. Until then the retained volume is only a
    control-plane identity.
    """
    frozen = _validate_state_plan(plan)
    client = _DockerCli(frozen)
    _assert_engine(client, frozen)
    client.assert_volume_name_absent(frozen.volume_name)
    _assert_engine(client, frozen)
    created = client.create_volume(frozen.volume_name, frozen.labels)
    try:
        _assert_engine(client, frozen)
        volume = client.inspect_volume(created)
        _validate_volume_identity(volume, name=created, labels=frozen.labels)
    except DockerRecoveryRequired:
        raise
    except DockerBackendError as error:
        raise DockerRecoveryRequired("created Docker state volume cannot be proven owned") from error
    return DockerStatePreparation(
        volume_id=created,
        engine_id=frozen.engine_id,
        state_domain=frozen.state_domain,
        owner_scope=frozen.scope,
        owner_key=frozen.owner_key,
        labels=tuple(sorted(frozen.labels.items())),
    )


def spawn(plan: dict[str, Any]) -> DockerOwnedProcess:
    """Create, verify and attach one server-owned local Docker MCP container.

    The function never starts Docker Desktop/the daemon, pulls an image, runs an
    arbitrary command, creates a volume, parses MCP JSON-RPC, or adopts an
    existing Docker object.
    """
    frozen = _validate_plan(plan)
    client = _DockerCli(frozen)
    _assert_engine(client, frozen)
    image = client.inspect_image(frozen.image_id)
    image_config = _validate_image(image, frozen)
    expected_labels = _expected_container_labels(image_config, frozen)
    if frozen.state_volume is not None:
        # Current public validation rejects persistent plans before this point.
        # Keep the runtime verifier in place for a future evidence-backed proof
        # contract; do not make a caller boolean equivalent to readiness.
        _assert_engine(client, frozen)
        _validate_volume(client.inspect_volume(frozen.state_volume.name), frozen)
    client.assert_name_absent(frozen.container_name)

    _assert_engine(client, frozen)
    container_id = client.create_container(expected_labels)
    try:
        _assert_engine(client, frozen)
        created = client.inspect_container(container_id)
        _validate_container(
            created,
            frozen,
            container_id,
            image_config,
            expected_labels,
            expected_status="created",
        )
    except DockerRecoveryRequired:
        raise
    except DockerBackendError as error:
        raise DockerRecoveryRequired("created Docker container cannot be proven owned") from error

    process: ManagedWindowsProcess | None = None
    job: int | None = None
    try:
        try:
            _assert_engine(client, frozen)
            _validate_container(
                client.inspect_container(container_id),
                frozen,
                container_id,
                image_config,
                expected_labels,
                expected_status="created",
            )
        except DockerRecoveryRequired:
            raise
        except DockerBackendError as error:
            raise DockerRecoveryRequired("created Docker container changed before start") from error
        process, job = client.spawn_attach(container_id)
        try:
            _await_running(client, frozen, container_id, image_config, expected_labels, process)
        except DockerRecoveryRequired:
            raise
        except DockerBackendError as error:
            raise DockerRecoveryRequired("Docker start result cannot be proven safe") from error
        return DockerOwnedProcess(
            plan=frozen,
            client=client,
            container_id=container_id,
            image_config=image_config,
            expected_labels=expected_labels,
            process=process,
            cleanup_job_handle=job,
        )
    except BaseException:
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate_tree()
                process.wait(2.0)
            except WindowsProcessError:
                pass
            process.close()
        close_handle(job)
        raise


def _validate_plan(raw: dict[str, Any]) -> _DockerPlan:
    if type(raw) is not dict:
        raise DockerBackendError("Docker plan must be an exact object")
    required = {
        "docker_binary", "engine_endpoint", "engine_id", "image_id", "defaults_hash",
        "container_name", "installation_id", "subject_id", "pin_id", "incarnation_id",
        "working_directory", "timeout_seconds",
    }
    allowed = required | {"state_volume", "state_env"}
    if set(raw) - allowed or required - set(raw):
        raise DockerBackendError("Docker plan shape is invalid")

    binary = _owned_path(raw["docker_binary"], directory=False, label="Docker binary")
    working = _owned_path(raw["working_directory"], directory=True, label="Docker working directory")
    endpoint = _string(raw["engine_endpoint"], "engine endpoint")
    if not endpoint.startswith(_NPIPE_PREFIX) or _VOLUME_NAME.fullmatch(endpoint[len(_NPIPE_PREFIX):]) is None:
        raise DockerBackendError("Docker engine endpoint must be a local named pipe")
    engine_id = _safe_identifier(raw["engine_id"], "engine identity")
    image_id = _string(raw["image_id"], "image identity")
    if _IMAGE_ID.fullmatch(image_id) is None:
        raise DockerBackendError("Docker image identity must be a sha256 digest")
    defaults_hash = _string(raw["defaults_hash"], "image defaults hash")
    if _HASH.fullmatch(defaults_hash) is None:
        raise DockerBackendError("Docker image defaults hash is invalid")
    container_name = _string(raw["container_name"], "container name")
    if _CONTAINER_NAME.fullmatch(container_name) is None:
        raise DockerBackendError("Docker container name is outside the managed namespace")

    timeout = raw["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(float(timeout)) or not 0 < float(timeout) <= 60:
        raise DockerBackendError("Docker timeout is outside the bounded range")

    state: _StateVolume | None = None
    state_raw = raw.get("state_volume")
    if state_raw is not None:
        if type(state_raw) is not dict or set(state_raw) != {"name", "state_domain", "scope", "owner_key"}:
            raise DockerBackendError("Docker state volume shape is invalid")
        name = _string(state_raw["name"], "state volume name")
        if _STATE_VOLUME_NAME.fullmatch(name) is None:
            raise DockerBackendError("Docker state volume name is outside the managed namespace")
        scope = state_raw["scope"]
        if scope not in {"global", "workspace"}:
            raise DockerBackendError("Docker state volume scope is invalid")
        state = _StateVolume(
            name=name,
            state_domain=_safe_identifier(state_raw["state_domain"], "state domain"),
            scope=scope,
            owner_key=_safe_identifier(state_raw["owner_key"], "state owner key"),
        )

    state_env = _validate_state_env(raw.get("state_env"))
    if state is not None:
        raise DockerBackendError("Docker persistent state is not ready: STATE_UID_GID_UNPROVEN")
    if state_env is not None:
        raise DockerBackendError("Docker state environment requires proven writable state")

    return _DockerPlan(
        docker_binary=binary,
        engine_endpoint=endpoint,
        engine_id=engine_id,
        image_id=image_id,
        defaults_hash=defaults_hash,
        container_name=container_name,
        installation_id=_safe_identifier(raw["installation_id"], "installation identity"),
        subject_id=_safe_identifier(raw["subject_id"], "subject identity"),
        pin_id=_safe_identifier(raw["pin_id"], "pin identity"),
        incarnation_id=_safe_identifier(raw["incarnation_id"], "incarnation identity"),
        working_directory=working,
        timeout_seconds=float(timeout),
        state_volume=state,
        state_env=state_env,
    )


def _validate_state_plan(raw: dict[str, Any]) -> _DockerStatePlan:
    if type(raw) is not dict:
        raise DockerBackendError("Docker state plan must be an exact object")
    required = {
        "docker_binary", "engine_endpoint", "engine_id", "installation_id",
        "volume_name", "state_domain", "scope", "owner_key", "working_directory",
        "timeout_seconds",
    }
    if set(raw) != required:
        raise DockerBackendError("Docker state plan shape is invalid")
    binary = _owned_path(raw["docker_binary"], directory=False, label="Docker binary")
    working = _owned_path(raw["working_directory"], directory=True, label="Docker working directory")
    endpoint = _string(raw["engine_endpoint"], "engine endpoint")
    if not endpoint.startswith(_NPIPE_PREFIX) or _VOLUME_NAME.fullmatch(endpoint[len(_NPIPE_PREFIX):]) is None:
        raise DockerBackendError("Docker engine endpoint must be a local named pipe")
    volume_name = _string(raw["volume_name"], "state volume name")
    if _STATE_VOLUME_NAME.fullmatch(volume_name) is None:
        raise DockerBackendError("Docker state volume name is outside the managed namespace")
    scope = raw["scope"]
    if scope not in {"global", "workspace"}:
        raise DockerBackendError("Docker state volume scope is invalid")
    timeout = raw["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(float(timeout)) or not 0 < float(timeout) <= 60:
        raise DockerBackendError("Docker timeout is outside the bounded range")
    return _DockerStatePlan(
        docker_binary=binary,
        engine_endpoint=endpoint,
        engine_id=_safe_identifier(raw["engine_id"], "engine identity"),
        installation_id=_safe_identifier(raw["installation_id"], "installation identity"),
        volume_name=volume_name,
        state_domain=_safe_identifier(raw["state_domain"], "state domain"),
        scope=scope,
        owner_key=_safe_identifier(raw["owner_key"], "state owner key"),
        working_directory=working,
        timeout_seconds=float(timeout),
    )


def _owned_path(value: Any, *, directory: bool, label: str) -> Path:
    text = _string(value, label)
    candidate = Path(text)
    if not candidate.is_absolute():
        raise DockerBackendError(f"{label} must be absolute")
    normalized = os.path.abspath(os.path.normpath(text))
    if os.path.normcase(normalized) != os.path.normcase(text.rstrip("\\/")):
        raise DockerBackendError(f"{label} is not canonical")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise DockerBackendError(f"{label} is unavailable") from error
    if os.path.normcase(str(resolved)) != os.path.normcase(normalized):
        raise DockerBackendError(f"{label} cannot traverse a link or reparse target")
    if directory and not resolved.is_dir():
        raise DockerBackendError(f"{label} is not a directory")
    if not directory and not resolved.is_file():
        raise DockerBackendError(f"{label} is not a file")
    return resolved


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\r" in value or "\n" in value:
        raise DockerBackendError(f"{label} is invalid")
    return value


def _safe_identifier(value: Any, label: str) -> str:
    text = _string(value, label)
    if _SAFE_ID.fullmatch(text) is None:
        raise DockerBackendError(f"{label} is invalid")
    return text


def _validate_state_env(value: Any) -> str | None:
    if value is None:
        return None
    text = _string(value, "state environment name")
    if _STATE_ENV_NAME.fullmatch(text) is None or _RESERVED_STATE_ENV.match(text) is not None:
        raise DockerBackendError("Docker state environment name is not allowed")
    return text


def _environment_map(value: Any, *, label: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, list):
        raise DockerBackendError(f"{label} is invalid")
    result: dict[str, str] = {}
    for item in value:
        if not isinstance(item, str) or "=" not in item:
            raise DockerBackendError(f"{label} is invalid")
        name, content = item.split("=", 1)
        if not name or name in result:
            raise DockerBackendError(f"{label} contains duplicate or empty names")
        result[name] = content
    return result


def _expected_container_environment(image_config: Mapping[str, Any], plan: _DockerPlan) -> dict[str, str]:
    expected = _environment_map(image_config.get("Env"), label="Docker image environment")
    if plan.state_env is None:
        return expected
    if plan.state_env in expected:
        raise DockerBackendError("Docker state environment collides with image defaults")
    return {**expected, plan.state_env: _STATE_ENV_VALUE}


def _no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DockerBackendError("Docker JSON contains duplicate keys")
        result[key] = value
    return result


def _one_object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise DockerBackendError(f"{label} response is invalid")
    return value[0]


def _defaults_hash(config: Mapping[str, Any]) -> str:
    try:
        canonical = json.dumps(
            config,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise DockerBackendError("Docker image defaults are not canonical JSON") from error
    return hashlib.sha256(canonical).hexdigest()


def _assert_engine(client: _DockerCli, plan: _DockerPlan | _DockerStatePlan) -> None:
    info = client.info()
    if info.get("ID") != plan.engine_id or info.get("OSType") != "linux":
        raise DockerBackendError("Docker engine identity or operating system changed")


def _validate_image(image: Mapping[str, Any], plan: _DockerPlan) -> Mapping[str, Any]:
    if image.get("Id") != plan.image_id:
        raise DockerBackendError("Docker image identity changed")
    config = image.get("Config")
    if not isinstance(config, dict) or _defaults_hash(config) != plan.defaults_hash:
        raise DockerBackendError("Docker image defaults changed")
    labels = config.get("Labels")
    if labels is not None:
        if not isinstance(labels, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()):
            raise DockerBackendError("Docker image labels are invalid")
        if any(key.startswith("localgpt.") for key in labels):
            raise DockerBackendError("Docker image may not predeclare managed Local GPT labels")
    volumes = config.get("Volumes")
    declared = set()
    if volumes is not None:
        if not isinstance(volumes, dict) or any(not isinstance(path, str) for path in volumes):
            raise DockerBackendError("Docker image volume declarations are invalid")
        declared = set(volumes)
    if declared - {"/mcp-state"}:
        raise DockerBackendError("Docker image declares an unmapped persistent volume")
    if "/mcp-state" in declared and plan.state_volume is None:
        raise DockerBackendError("Docker image state volume has no approved state domain")
    _expected_container_environment(config, plan)
    return config


def _expected_container_labels(image_config: Mapping[str, Any], plan: _DockerPlan) -> dict[str, str]:
    labels = image_config.get("Labels") or {}
    if not isinstance(labels, dict):
        raise DockerBackendError("Docker image labels are invalid")
    expected = dict(labels)
    expected.update(plan.managed_labels)
    return expected


def _validate_volume(volume: Mapping[str, Any], plan: _DockerPlan) -> None:
    expected = plan.state_volume
    if expected is None:
        raise DockerBackendError("unexpected Docker state volume")
    _validate_volume_identity(volume, name=expected.name, labels=plan.state_labels)


def _validate_volume_identity(volume: Mapping[str, Any], *, name: str, labels: Mapping[str, str]) -> None:
    if volume.get("Name") != name or volume.get("Driver") != "local" or volume.get("Scope") != "local":
        raise DockerBackendError("Docker state volume identity changed")
    options = volume.get("Options")
    if options not in (None, {}):
        raise DockerBackendError("Docker state volume driver options are not allowed")
    observed = volume.get("Labels") or {}
    if observed != dict(labels):
        raise DockerBackendError("Docker state volume ownership labels changed")


def _validate_container(
    container: Mapping[str, Any],
    plan: _DockerPlan,
    container_id: str,
    image_config: Mapping[str, Any],
    expected_labels: Mapping[str, str],
    *,
    expected_status: str | None,
) -> None:
    if container.get("Id") != container_id or re.fullmatch(r"[a-f0-9]{64}", container_id) is None:
        raise DockerBackendError("Docker container identity changed")
    if container.get("Image") != plan.image_id:
        raise DockerBackendError("Docker container image changed")
    config = container.get("Config")
    host = container.get("HostConfig")
    state = container.get("State")
    if not isinstance(config, dict) or not isinstance(host, dict) or not isinstance(state, dict):
        raise DockerBackendError("Docker container inspect is incomplete")
    if config.get("Image") != plan.image_id or config.get("Labels") != dict(expected_labels):
        raise DockerBackendError("Docker container configuration identity changed")
    for key in ("Cmd", "Entrypoint", "WorkingDir"):
        if config.get(key) != image_config.get(key):
            raise DockerBackendError("Docker container command defaults changed")
    if _environment_map(config.get("Env"), label="Docker container environment") != _expected_container_environment(image_config, plan):
        raise DockerBackendError("Docker container environment changed")
    if config.get("User") != "65532:65532" or config.get("Tty") is not False or config.get("OpenStdin") is not True:
        raise DockerBackendError("Docker container stdio/user policy changed")
    health = config.get("Healthcheck")
    if not isinstance(health, dict) or health.get("Test") != ["NONE"]:
        raise DockerBackendError("Docker container healthcheck was not disabled")
    if host.get("NetworkMode") != "none" or host.get("ReadonlyRootfs") is not True:
        raise DockerBackendError("Docker container network/filesystem policy changed")
    if host.get("Privileged") is not False or host.get("AutoRemove") is not False:
        raise DockerBackendError("Docker container privilege/lifecycle policy changed")
    restart = host.get("RestartPolicy")
    if not isinstance(restart, dict) or restart.get("Name") != "no" or restart.get("MaximumRetryCount", 0) != 0:
        raise DockerBackendError("Docker container restart policy changed")
    if set(host.get("CapDrop") or []) != {"ALL"} or host.get("CapAdd") not in (None, []):
        raise DockerBackendError("Docker container capability policy changed")
    security = {_normalize_security(value) for value in (host.get("SecurityOpt") or []) if isinstance(value, str)}
    if security != {"no-new-privileges:true"}:
        raise DockerBackendError("Docker no-new-privileges policy changed")
    log = host.get("LogConfig")
    if not isinstance(log, dict) or log.get("Type") != "none":
        raise DockerBackendError("Docker protocol logging is not disabled")
    tmpfs = host.get("Tmpfs")
    if not isinstance(tmpfs, dict) or set(tmpfs) != {"/tmp"} or _option_set(tmpfs["/tmp"]) != _TMPFS_OPTIONS:
        raise DockerBackendError("Docker tmpfs policy changed")
    if host.get("Binds") not in (None, []) or host.get("PortBindings") not in (None, {}) or host.get("PublishAllPorts") is not False:
        raise DockerBackendError("Docker host exposure policy changed")
    if host.get("Devices") not in (None, []):
        raise DockerBackendError("Docker device policy changed")
    _validate_mounts(container.get("Mounts"), host.get("Mounts"), plan)
    running, status = _container_state(container).values()
    if state.get("Paused") is True or state.get("Restarting") is True:
        raise DockerBackendError("Docker container entered an unsupported lifecycle state")
    if expected_status == "created" and (running or status != "created"):
        raise DockerBackendError("Docker container was not inspected before start")
    if expected_status == "running" and (not running or status != "running"):
        raise DockerBackendError("Docker container did not reach running state")


def _validate_mounts(actual: Any, host_mounts: Any, plan: _DockerPlan) -> None:
    mounts = actual or []
    configured = host_mounts or []
    if not isinstance(mounts, list) or not isinstance(configured, list):
        raise DockerBackendError("Docker mount projection is invalid")
    if plan.state_volume is None:
        if mounts or configured:
            raise DockerBackendError("Docker container acquired an unapproved mount")
        return
    if len(mounts) != 1 or len(configured) != 1:
        raise DockerBackendError("Docker state mount count changed")
    observed = mounts[0]
    desired = configured[0]
    if not isinstance(observed, dict) or not isinstance(desired, dict):
        raise DockerBackendError("Docker state mount is invalid")
    if (
        observed.get("Type") != "volume"
        or observed.get("Name") != plan.state_volume.name
        or observed.get("Destination") != "/mcp-state"
        or observed.get("RW") is not True
    ):
        raise DockerBackendError("Docker state mount identity changed")
    volume_options = desired.get("VolumeOptions")
    if (
        desired.get("Type") != "volume"
        or desired.get("Source") != plan.state_volume.name
        or desired.get("Target") != "/mcp-state"
        or desired.get("ReadOnly") is not False
        or not isinstance(volume_options, dict)
        or volume_options.get("NoCopy") is not True
    ):
        raise DockerBackendError("Docker state mount configuration changed")


def _container_state(container: Mapping[str, Any]) -> dict[str, Any]:
    state = container.get("State")
    if not isinstance(state, dict) or not isinstance(state.get("Running"), bool) or not isinstance(state.get("Status"), str):
        raise DockerBackendError("Docker container state is invalid")
    return {"running": state["Running"], "status": state["Status"]}


def _normalize_security(value: str) -> str:
    return value.replace("=true", ":true")


def _option_set(value: Any) -> frozenset[str]:
    if not isinstance(value, str):
        raise DockerBackendError("Docker option set is invalid")
    return frozenset(item for item in value.split(",") if item)


def _await_running(
    client: _DockerCli,
    plan: _DockerPlan,
    container_id: str,
    image_config: Mapping[str, Any],
    expected_labels: Mapping[str, str],
    process: ManagedWindowsProcess,
) -> None:
    deadline = time.monotonic() + plan.timeout_seconds
    while True:
        if time.monotonic() >= deadline:
            raise DockerRecoveryRequired("Docker start completion is uncertain")
        _assert_engine(client, plan)
        container = client.inspect_container(container_id)
        _validate_container(container, plan, container_id, image_config, expected_labels, expected_status=None)
        state = _container_state(container)
        if state["running"] and state["status"] == "running":
            _validate_container(container, plan, container_id, image_config, expected_labels, expected_status="running")
            return
        if state["status"] in {"exited", "dead"} or process.poll() is not None:
            raise DockerBackendError("Docker MCP process exited before readiness")
        time.sleep(0.02)


def _run_owned_cli(
    binary: Path,
    arguments: tuple[str, ...],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout: float,
    effect: bool,
) -> _CliResult:
    job: int | None = None
    process: ManagedWindowsProcess | None = None
    readers: list[threading.Thread] = []
    stdout = bytearray()
    stderr_bytes = 0
    overflow = threading.Event()

    def drain(fd: int, *, output: bool) -> None:
        nonlocal stderr_bytes
        try:
            while True:
                chunk = os.read(fd, 4096)
                if not chunk:
                    return
                if output:
                    if len(stdout) + len(chunk) > _CLI_OUTPUT_LIMIT:
                        overflow.set()
                        return
                    stdout.extend(chunk)
                else:
                    stderr_bytes += len(chunk)
                    if stderr_bytes > _CLI_STDERR_LIMIT:
                        overflow.set()
                        return
        except OSError:
            overflow.set()

    failure_type = DockerRecoveryRequired if effect else DockerBackendError
    main_error: BaseException | None = None
    try:
        job = create_job(
            "Local\\McpDockerCli-" + uuid.uuid4().hex,
            process_limit=8,
            memory_bytes=_CLI_MEMORY_BYTES,
        )
        process = spawn_process(
            [str(binary), *arguments],
            cwd=cwd,
            environment=environment,
            job_handles=[job],
            exact_job_handle=job,
            cleanup_job_handle=job,
            capture_output=True,
            exact_environment=True,
        )
        if process.stdout_fd is None or process.stderr_fd is None:
            raise failure_type("Docker CLI pipes are unavailable")
        readers = [
            threading.Thread(target=drain, kwargs={"fd": process.stdout_fd, "output": True}, daemon=True),
            threading.Thread(target=drain, kwargs={"fd": process.stderr_fd, "output": False}, daemon=True),
        ]
        for reader in readers:
            reader.start()
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            if overflow.wait(0.01):
                raise failure_type("Docker CLI exceeded bounded output")
            if time.monotonic() >= deadline:
                raise failure_type("Docker CLI deadline exceeded")
        for reader in readers:
            reader.join(timeout=1.0)
        if overflow.is_set() or any(reader.is_alive() for reader in readers):
            raise failure_type("Docker CLI output/quiescence is unproven")
        exit_code = process.poll()
        if exit_code is None:
            raise failure_type("Docker CLI exit state is unknown")
        if exit_code != 0:
            raise failure_type("Docker CLI command failed")
        if job_active_processes(job) != 0:
            raise failure_type("Docker CLI Job quiescence is unproven")
        return _CliResult(exit_code=exit_code, stdout=bytes(stdout))
    except BaseException as error:
        main_error = error
        if isinstance(error, (DockerBackendError, DockerRecoveryRequired)):
            raise
        raise failure_type("Docker CLI ownership failed") from error
    finally:
        cleanup_error: BaseException | None = None
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate_tree()
                if process.wait(2.0) is None:
                    cleanup_error = failure_type("Docker CLI local process remains active")
            except WindowsProcessError:
                cleanup_error = failure_type("Docker CLI local cleanup failed")
            for reader in readers:
                reader.join(timeout=1.0)
            process.close()
        if job is not None:
            try:
                active = job_active_processes(job)
            except WindowsProcessError:
                active = -1
            close_handle(job)
            if active != 0:
                cleanup_error = failure_type("Docker CLI local tree remains active")
        if cleanup_error is not None and main_error is None:
            raise cleanup_error
