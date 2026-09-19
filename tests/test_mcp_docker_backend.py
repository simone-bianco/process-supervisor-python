from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from py_laravel_supervisor.framed_channel import FramedStdioChannel
from py_laravel_supervisor import mcp_docker
from py_laravel_supervisor.mcp_docker import (
    DockerBackendError,
    DockerRecoveryRequired,
    prepare_state,
    spawn as launch_docker,
)
from py_laravel_supervisor.windows import create_job, spawn_process


IMAGE_ID = "sha256:" + "a" * 64
CONTAINER_ID = "b" * 64
ENGINE_ID = "fixture-engine-01"
CONTAINER_NAME = "localgpt-mcp-" + "c" * 48
ENDPOINT = "npipe:////./pipe/dockerDesktopLinuxEngine"


def image_config(*, volumes=None):
    return {
        "Cmd": ["node", "server.js"],
        "Entrypoint": None,
        "WorkingDir": "/app",
        "Env": ["NODE_ENV=production"],
        "Labels": {"vendor.fixture": "true"},
        "Volumes": volumes,
    }


def managed_labels(config, plan):
    labels = dict(config.get("Labels") or {})
    labels.update({
        "localgpt.managed": "true",
        "localgpt.installation_id": plan["installation_id"],
        "localgpt.subject_id": plan["subject_id"],
        "localgpt.pin_id": plan["pin_id"],
        "localgpt.incarnation_id": plan["incarnation_id"],
    })
    return labels


def container_record(config, plan, *, running: bool, status: str, drift: dict | None = None):
    labels = managed_labels(config, plan)
    environment = list(config.get("Env") or [])
    if plan.get("state_env"):
        environment.append(f"{plan['state_env']}=/mcp-state/memory.json")
    host_mounts = []
    mounts = []
    if plan.get("state_volume") is not None:
        name = plan["state_volume"]["name"]
        host_mounts = [{
            "Type": "volume",
            "Source": name,
            "Target": "/mcp-state",
            "ReadOnly": False,
            "VolumeOptions": {"NoCopy": True},
        }]
        mounts = [{
            "Type": "volume",
            "Name": name,
            "Destination": "/mcp-state",
            "RW": True,
        }]
    value = {
        "Id": CONTAINER_ID,
        "Image": IMAGE_ID,
        "Config": {
            "Image": IMAGE_ID,
            "Labels": labels,
            "Cmd": config["Cmd"],
            "Entrypoint": config["Entrypoint"],
            "WorkingDir": config["WorkingDir"],
            "Env": environment,
            "User": "65532:65532",
            "Tty": False,
            "OpenStdin": True,
            "Healthcheck": {"Test": ["NONE"]},
        },
        "HostConfig": {
            "NetworkMode": "none",
            "ReadonlyRootfs": True,
            "Privileged": False,
            "AutoRemove": False,
            "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "CapDrop": ["ALL"],
            "CapAdd": None,
            "SecurityOpt": ["no-new-privileges:true"],
            "LogConfig": {"Type": "none", "Config": {}},
            "Tmpfs": {"/tmp": "rw,noexec,nosuid,nodev,size=67108864"},
            "Binds": None,
            "PortBindings": {},
            "PublishAllPorts": False,
            "Devices": [],
            "Mounts": host_mounts,
        },
        "Mounts": mounts,
        "State": {
            "Running": running,
            "Status": status,
            "Paused": False,
            "Restarting": False,
        },
    }
    if drift:
        for section, updates in drift.items():
            value[section].update(updates)
    return value


class FakeProcess:
    def __init__(self, events: list[str], job: int = 7001):
        self.pid = 101
        self.process_handle = 202
        self.job_handle = job
        self.stdin_fd = 301
        self.stdout_fd = 302
        self.stderr_fd = 303
        self._exit = None
        self.events = events
        self.closed = False

    def poll(self):
        return self._exit

    def wait(self, timeout):
        return self._exit

    def terminate_tree(self):
        self.events.append("cli_terminate")
        self._exit = 143

    def close(self):
        self.events.append("cli_close")
        self.process_handle = 0
        self.stdin_fd = self.stdout_fd = self.stderr_fd = None
        self.closed = True


class FakeDockerCli:
    def __init__(self, config, raw_plan, *, process=None, volume=None, stop_error=None, drift=None):
        self.config = config
        self.raw_plan = raw_plan
        self.plan = None
        self.events: list[str] = []
        self.process = process or FakeProcess(self.events)
        self.volume = volume
        self.stop_error = stop_error
        self.drift = drift
        self.running = False
        self.status = "created"
        self.created_labels = None
        self.stop_calls = 0
        self.created_volume = None

    def bind(self, frozen):
        self.plan = frozen
        return self

    def info(self):
        self.events.append("info")
        return {"ID": ENGINE_ID, "OSType": "linux"}

    def inspect_image(self, image_id):
        self.events.append("image_inspect")
        return {"Id": IMAGE_ID, "Config": self.config}

    def inspect_volume(self, name):
        self.events.append("volume_inspect")
        if self.volume is None:
            raise DockerBackendError("missing volume")
        return self.volume

    def assert_name_absent(self, name):
        self.events.append("name_absent")

    def assert_volume_name_absent(self, name):
        self.events.append("volume_name_absent")

    def create_volume(self, name, labels):
        self.events.append("volume_create")
        self.created_volume = {"name": name, "labels": dict(labels)}
        self.volume = {
            "Name": name,
            "Driver": "local",
            "Scope": "local",
            "Options": None,
            "Labels": dict(labels),
        }
        return name

    def create_container(self, labels):
        self.events.append("create")
        self.created_labels = dict(labels)
        return CONTAINER_ID

    def inspect_container(self, container_id):
        self.events.append("container_inspect")
        return container_record(
            self.config,
            self.raw_plan,
            running=self.running,
            status=self.status,
            drift=self.drift,
        )

    def spawn_attach(self, container_id):
        self.events.append("spawn_attach")
        self.running = True
        self.status = "running"
        return self.process, self.process.job_handle

    def stop_container(self, container_id):
        self.events.append("stop")
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error
        self.running = False
        self.status = "exited"


class DockerBackendTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mcp-docker-")
        self.root = Path(self.temporary.name).resolve()
        self.binary = self.root / "docker.exe"
        self.binary.write_bytes(b"fixture")
        self.work = self.root / "private"
        self.work.mkdir()
        self.config = image_config()
        self.plan = {
            "docker_binary": str(self.binary),
            "engine_endpoint": ENDPOINT,
            "engine_id": ENGINE_ID,
            "image_id": IMAGE_ID,
            "defaults_hash": mcp_docker._defaults_hash(self.config),
            "container_name": CONTAINER_NAME,
            "installation_id": "installation-01",
            "subject_id": "01M2R8SUBJECT0000000000000",
            "pin_id": "01M2R8PIN000000000000000000",
            "incarnation_id": "01M2R8INCARNATION0000000000",
            "working_directory": str(self.work),
            "timeout_seconds": 5,
        }

    def tearDown(self):
        self.temporary.cleanup()

    def launch(self, fake: FakeDockerCli):
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)), \
             patch.object(mcp_docker, "job_active_processes", return_value=0), \
             patch.object(mcp_docker, "close_handle"):
            return launch_docker(dict(self.plan))

    def test_public_api_is_explicit_spawn_and_prepare_state_only(self):
        self.assertTrue(callable(mcp_docker.spawn))
        self.assertTrue(callable(mcp_docker.prepare_state))
        self.assertFalse(hasattr(mcp_docker, "launch_docker"))

    def test_strict_plan_rejects_unknown_remote_ambiguous_and_unbounded_inputs(self):
        invalid = []
        invalid.append({**self.plan, "unknown": True})
        invalid.append({**self.plan, "docker_binary": "docker.exe"})
        invalid.append({**self.plan, "engine_endpoint": "tcp://127.0.0.1:2375"})
        invalid.append({**self.plan, "container_name": "external-container"})
        invalid.append({**self.plan, "image_id": "latest"})
        invalid.append({**self.plan, "timeout_seconds": True})
        invalid.append({**self.plan, "timeout_seconds": 61})
        invalid.append({**self.plan, "state_volume": {"name": "state", "state_domain": "domain", "extra": "x"}})
        for candidate in invalid:
            with self.subTest(candidate=candidate):
                with self.assertRaises(DockerBackendError):
                    mcp_docker._validate_plan(candidate)

    def test_unreachable_or_changed_engine_never_reaches_create(self):
        fake = FakeDockerCli(self.config, self.plan)
        def unavailable():
            fake.events.append("info")
            raise DockerBackendError("engine unavailable")
        fake.info = unavailable
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaises(DockerBackendError):
                launch_docker(dict(self.plan))
        self.assertEqual(["info"], fake.events)
        self.assertNotIn("create", fake.events)

        fake = FakeDockerCli(self.config, self.plan)
        fake.info = lambda: {"ID": "other-engine", "OSType": "linux"}
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaises(DockerBackendError):
                launch_docker(dict(self.plan))
        self.assertNotIn("create", fake.events)

    def test_windows_container_engine_is_rejected_before_any_effect(self):
        fake = FakeDockerCli(self.config, self.plan)
        fake.info = lambda: {"ID": ENGINE_ID, "OSType": "windows"}
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaises(DockerBackendError):
                launch_docker(dict(self.plan))
        self.assertNotIn("create", fake.events)

    def test_image_defaults_and_declared_volumes_are_frozen_before_create(self):
        changed = FakeDockerCli({**self.config, "Cmd": ["evil"]}, self.plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: changed.bind(frozen)):
            with self.assertRaisesRegex(DockerBackendError, "defaults"):
                launch_docker(dict(self.plan))
        self.assertNotIn("create", changed.events)

        config = image_config(volumes={"/unmanaged": {}})
        plan = {**self.plan, "defaults_hash": mcp_docker._defaults_hash(config)}
        fake = FakeDockerCli(config, plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaisesRegex(DockerBackendError, "unmapped"):
                launch_docker(plan)
        self.assertNotIn("create", fake.events)

    def test_persistent_state_plan_is_blocked_before_any_docker_effect_even_with_caller_ready_flag(self):
        config = image_config(volumes={"/mcp-state": {}})
        plan = {
            **self.plan,
            "defaults_hash": mcp_docker._defaults_hash(config),
            "state_volume": {
                "name": "localgpt-mcp-state-" + "d" * 48,
                "state_domain": "workspace-state-01",
                "scope": "workspace",
                "owner_key": "workspace-01",
            },
        }
        with self.assertRaisesRegex(DockerBackendError, "STATE_UID_GID_UNPROVEN"):
            mcp_docker._validate_plan(plan)

        invented = {
            **plan,
            "state_volume": {**plan["state_volume"], "writable_ready": True},
        }
        with self.assertRaisesRegex(DockerBackendError, "shape"):
            mcp_docker._validate_plan(invented)

    def test_effective_container_security_drift_after_create_is_recovery_not_start(self):
        fake = FakeDockerCli(
            self.config,
            self.plan,
            drift={"HostConfig": {"NetworkMode": "bridge"}},
        )
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaises(DockerRecoveryRequired) as captured:
                launch_docker(dict(self.plan))
        self.assertFalse(captured.exception.retryable)
        self.assertIn("create", fake.events)
        self.assertNotIn("spawn_attach", fake.events)

    def test_success_returns_owned_process_and_stop_precedes_cli_job_termination(self):
        fake = FakeDockerCli(self.config, self.plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)), \
             patch.object(mcp_docker, "job_active_processes", return_value=0), \
             patch.object(mcp_docker, "close_handle"):
            owned = launch_docker(dict(self.plan))
            self.assertEqual(CONTAINER_ID, owned.container_id)
            self.assertEqual(301, owned.stdin_fd)
            self.assertEqual(302, owned.stdout_fd)
            self.assertEqual(303, owned.stderr_fd)
            self.assertEqual(202, owned.process_handle)
            self.assertEqual(7001, owned.cleanup_job_handle)
            owned.terminate_tree()
            self.assertLess(fake.events.index("stop"), fake.events.index("cli_terminate"))
            self.assertGreaterEqual(fake.events.count("info"), 4)
            owned.close()
            self.assertTrue(fake.process.closed)
            self.assertIsNone(owned.cleanup_job_handle)

    def test_stop_timeout_is_nonretryable_and_never_reissues_container_stop(self):
        fake = FakeDockerCli(
            self.config,
            self.plan,
            stop_error=DockerRecoveryRequired("synthetic stop timeout"),
        )
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)), \
             patch.object(mcp_docker, "job_active_processes", return_value=0), \
             patch.object(mcp_docker, "close_handle"):
            owned = launch_docker(dict(self.plan))
            with self.assertRaises(DockerRecoveryRequired) as first:
                owned.terminate_tree()
            self.assertFalse(first.exception.retryable)
            self.assertEqual(1, fake.stop_calls)
            self.assertIn("cli_terminate", fake.events)
            with self.assertRaises(DockerRecoveryRequired):
                owned.terminate_tree()
            self.assertEqual(1, fake.stop_calls)

    def test_cleanup_identity_drift_refuses_container_mutation_but_reduces_local_cli_once(self):
        fake = FakeDockerCli(self.config, self.plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)), \
             patch.object(mcp_docker, "job_active_processes", return_value=0), \
             patch.object(mcp_docker, "close_handle"):
            owned = launch_docker(dict(self.plan))
            fake.drift = {"Config": {"Labels": {"localgpt.managed": "false"}}}
            with self.assertRaises(DockerRecoveryRequired) as captured:
                owned.terminate_tree()
            self.assertFalse(captured.exception.retryable)
            self.assertEqual(0, fake.stop_calls)
            self.assertEqual(1, fake.events.count("cli_terminate"))
            with self.assertRaises(DockerRecoveryRequired):
                owned.close()
            self.assertEqual(0, fake.stop_calls)
            self.assertEqual(1, fake.events.count("cli_terminate"))

    def test_create_cli_is_host_pinned_offline_nonlogging_and_uses_no_ambient_docker_environment(self):
        frozen = mcp_docker._validate_plan(dict(self.plan))
        client = mcp_docker._DockerCli(frozen)
        captures = []
        def run(binary, arguments, **kwargs):
            captures.append((binary, arguments, kwargs))
            return mcp_docker._CliResult(0, (CONTAINER_ID + "\n").encode("ascii"))
        labels = managed_labels(self.config, self.plan)
        with patch.object(mcp_docker, "_run_owned_cli", side_effect=run):
            self.assertEqual(CONTAINER_ID, client.create_container(labels))
        binary, args, kwargs = captures[0]
        self.assertEqual(self.binary, binary)
        self.assertEqual(("--host", ENDPOINT, "container", "create"), args[:4])
        self.assertNotIn("run", args)
        self.assertEqual("never", args[args.index("--pull") + 1])
        for token in ("--network", "--read-only", "--cap-drop", "--security-opt", "--no-healthcheck", "--log-driver", "--tmpfs"):
            self.assertIn(token, args)
        self.assertEqual("none", args[args.index("--network") + 1])
        self.assertEqual("none", args[args.index("--log-driver") + 1])
        self.assertEqual("65532:65532", args[args.index("--user") + 1])
        self.assertNotIn("--mount", args)
        self.assertNotIn("--env", args)
        environment = kwargs["environment"]
        self.assertEqual(str(self.work), environment["DOCKER_CONFIG"])
        self.assertNotIn("DOCKER_HOST", environment)
        self.assertNotIn("DOCKER_CONTEXT", environment)
        self.assertNotIn("PATH", environment)
        self.assertTrue(kwargs["effect"])

    def test_state_env_is_fixed_path_future_contract_and_rejects_reserved_names_or_image_collision(self):
        with self.assertRaisesRegex(DockerBackendError, "requires proven writable state"):
            mcp_docker._validate_plan({**self.plan, "state_env": "MEMORY_FILE"})

        for reserved in ("NODE_OPTIONS", "PATH", "MCP_STATE_DIR"):
            with self.subTest(reserved=reserved):
                with self.assertRaisesRegex(DockerBackendError, "not allowed"):
                    mcp_docker._validate_state_env(reserved)

        state = mcp_docker._StateVolume(
            name="localgpt-mcp-state-" + "9" * 48,
            state_domain="workspace-state-01",
            scope="workspace",
            owner_key="workspace-01",
        )
        config = image_config(volumes={"/mcp-state": {}})
        config["Env"] = [*config["Env"], "MEMORY_FILE=image-default"]
        future_plan = mcp_docker._DockerPlan(
            docker_binary=self.binary,
            engine_endpoint=ENDPOINT,
            engine_id=ENGINE_ID,
            image_id=IMAGE_ID,
            defaults_hash=mcp_docker._defaults_hash(config),
            container_name=CONTAINER_NAME,
            installation_id="installation-01",
            subject_id="subject-01",
            pin_id="pin-01",
            incarnation_id="incarnation-01",
            working_directory=self.work,
            timeout_seconds=5,
            state_volume=state,
            state_env="MEMORY_FILE",
        )
        with self.assertRaisesRegex(DockerBackendError, "collides"):
            mcp_docker._expected_container_environment(config, future_plan)

        clean = image_config(volumes={"/mcp-state": {}})
        future_plan = mcp_docker._DockerPlan(
            **{
                **{field: getattr(future_plan, field) for field in future_plan.__dataclass_fields__},
                "defaults_hash": mcp_docker._defaults_hash(clean),
            }
        )
        self.assertEqual(
            "/mcp-state/memory.json",
            mcp_docker._expected_container_environment(clean, future_plan)["MEMORY_FILE"],
        )

    def test_prepare_state_real_cli_surface_is_volume_only_and_never_uses_copy_exec_or_container_start(self):
        volume_name = "localgpt-mcp-state-" + "7" * 48
        state_plan = {
            "docker_binary": str(self.binary),
            "engine_endpoint": ENDPOINT,
            "engine_id": ENGINE_ID,
            "installation_id": "installation-01",
            "volume_name": volume_name,
            "state_domain": "workspace-state-01",
            "scope": "workspace",
            "owner_key": "workspace-01",
            "working_directory": str(self.work),
            "timeout_seconds": 5,
        }
        labels = {
            "localgpt.managed": "true",
            "localgpt.installation_id": "installation-01",
            "localgpt.state_domain": "workspace-state-01",
            "localgpt.scope": "workspace",
            "localgpt.owner_key": "workspace-01",
        }
        engine = json.dumps({"ID": ENGINE_ID, "OSType": "linux"}).encode("utf-8")
        volume = json.dumps([{
            "Name": volume_name,
            "Driver": "local",
            "Scope": "local",
            "Options": None,
            "Labels": labels,
        }]).encode("utf-8")
        responses = iter([
            mcp_docker._CliResult(0, engine),
            mcp_docker._CliResult(0, b""),
            mcp_docker._CliResult(0, engine),
            mcp_docker._CliResult(0, (volume_name + "\n").encode("utf-8")),
            mcp_docker._CliResult(0, engine),
            mcp_docker._CliResult(0, volume),
        ])
        calls = []
        def run(binary, arguments, **kwargs):
            calls.append((binary, arguments, kwargs))
            return next(responses)

        with patch.object(mcp_docker, "_run_owned_cli", side_effect=run):
            result = prepare_state(state_plan)

        self.assertEqual(volume_name, result.volume_id)
        self.assertEqual("STATE_UID_GID_UNPROVEN", result.blocker)
        tokens = [token for _, arguments, _ in calls for token in arguments]
        for forbidden in ("cp", "exec", "start", "run", "container"):
            self.assertNotIn(forbidden, tokens)
        effect_calls = [arguments for _, arguments, kwargs in calls if kwargs["effect"]]
        self.assertEqual(1, len(effect_calls))
        self.assertEqual(("--host", ENDPOINT, "volume", "create"), effect_calls[0][:4])
        self.assertNotIn("--opt", effect_calls[0])

    def test_prepare_state_rejects_unapproved_helper_authority_fields(self):
        state_plan = {
            "docker_binary": str(self.binary),
            "engine_endpoint": ENDPOINT,
            "engine_id": ENGINE_ID,
            "installation_id": "installation-01",
            "volume_name": "localgpt-mcp-state-" + "6" * 48,
            "state_domain": "workspace-state-01",
            "scope": "workspace",
            "owner_key": "workspace-01",
            "working_directory": str(self.work),
            "timeout_seconds": 5,
            "proof_image_id": IMAGE_ID,
        }
        with self.assertRaisesRegex(DockerBackendError, "shape"):
            mcp_docker._validate_state_plan(state_plan)

    def test_prepare_state_creates_only_managed_volume_identity_and_returns_uid_blocker(self):
        state_plan = {
            "docker_binary": str(self.binary),
            "engine_endpoint": ENDPOINT,
            "engine_id": ENGINE_ID,
            "installation_id": "installation-01",
            "volume_name": "localgpt-mcp-state-" + "f" * 48,
            "state_domain": "workspace-state-01",
            "scope": "workspace",
            "owner_key": "workspace-01",
            "working_directory": str(self.work),
            "timeout_seconds": 5,
        }
        fake = FakeDockerCli(self.config, state_plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            result = prepare_state(state_plan)
        self.assertEqual(state_plan["volume_name"], result.volume_id)
        self.assertFalse(result.writable_ready)
        self.assertEqual("STATE_UID_GID_UNPROVEN", result.blocker)
        self.assertEqual("workspace", result.owner_scope)
        self.assertEqual("workspace-01", result.owner_key)
        self.assertEqual(["info", "volume_name_absent", "info", "volume_create", "info", "volume_inspect"], fake.events)
        self.assertEqual({
            "localgpt.managed": "true",
            "localgpt.installation_id": "installation-01",
            "localgpt.state_domain": "workspace-state-01",
            "localgpt.scope": "workspace",
            "localgpt.owner_key": "workspace-01",
        }, fake.created_volume["labels"])

    def test_prepare_state_ambiguous_create_is_nonretryable_and_never_adopts(self):
        state_plan = {
            "docker_binary": str(self.binary),
            "engine_endpoint": ENDPOINT,
            "engine_id": ENGINE_ID,
            "installation_id": "installation-01",
            "volume_name": "localgpt-mcp-state-" + "1" * 48,
            "state_domain": "global-state-01",
            "scope": "global",
            "owner_key": "global-01",
            "working_directory": str(self.work),
            "timeout_seconds": 5,
        }
        fake = FakeDockerCli(self.config, state_plan)
        def ambiguous(name, labels):
            fake.events.append("volume_create")
            raise DockerRecoveryRequired("ambiguous volume create")
        fake.create_volume = ambiguous
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaises(DockerRecoveryRequired) as captured:
                prepare_state(state_plan)
        self.assertFalse(captured.exception.retryable)
        self.assertEqual(1, fake.events.count("volume_create"))
        self.assertNotIn("volume_inspect", fake.events)

    def test_launch_rejects_prepare_state_result_until_uid_gid_readiness_is_proven(self):
        config = image_config(volumes={"/mcp-state": {}})
        plan = {
            **self.plan,
            "defaults_hash": mcp_docker._defaults_hash(config),
            "state_volume": {
                "name": "localgpt-mcp-state-" + "2" * 48,
                "state_domain": "workspace-state-01",
                "scope": "workspace",
                "owner_key": "workspace-01",
            },
            "state_env": "MEMORY_FILE",
        }
        fake = FakeDockerCli(config, plan)
        with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
            with self.assertRaisesRegex(DockerBackendError, "STATE_UID_GID_UNPROVEN"):
                launch_docker(plan)
        self.assertNotIn("create", fake.events)

    def test_attach_cli_uses_exact_job_owned_pipes_and_original_container_id(self):
        frozen = mcp_docker._validate_plan(dict(self.plan))
        client = mcp_docker._DockerCli(frozen)
        fake_process = FakeProcess([])
        with patch.object(mcp_docker, "create_job", return_value=7001) as create, \
             patch.object(mcp_docker, "spawn_process", return_value=fake_process) as spawn:
            process, job = client.spawn_attach(CONTAINER_ID)
        self.assertIs(fake_process, process)
        self.assertEqual(7001, job)
        create.assert_called_once()
        argv = spawn.call_args.args[0]
        self.assertEqual([
            str(self.binary), "--host", ENDPOINT,
            "container", "start", "--attach", "--interactive", CONTAINER_ID,
        ], argv)
        self.assertTrue(spawn.call_args.kwargs["stdin_pipe"])
        self.assertTrue(spawn.call_args.kwargs["exact_environment"])
        self.assertEqual([7001], spawn.call_args.kwargs["job_handles"])
        self.assertEqual(7001, spawn.call_args.kwargs["exact_job_handle"])
        self.assertEqual(7001, spawn.call_args.kwargs["cleanup_job_handle"])


@unittest.skipUnless(os.name == "nt", "Owned Windows pipe proof without Docker engine")
class DockerFramedReceiptWindowsTest(unittest.TestCase):
    def test_owned_wrapper_is_framed_channel_compatible_without_starting_docker(self):
        with tempfile.TemporaryDirectory(prefix="mcp-docker-frame-") as temporary:
            root = Path(temporary).resolve()
            binary = root / "docker.exe"
            binary.write_bytes(b"fixture")
            work = root / "private"
            work.mkdir()
            config = image_config()
            plan = {
                "docker_binary": str(binary),
                "engine_endpoint": ENDPOINT,
                "engine_id": ENGINE_ID,
                "image_id": IMAGE_ID,
                "defaults_hash": mcp_docker._defaults_hash(config),
                "container_name": CONTAINER_NAME,
                "installation_id": "installation-01",
                "subject_id": "01M2R8SUBJECT0000000000000",
                "pin_id": "01M2R8PIN000000000000000000",
                "incarnation_id": "01M2R8INCARNATION0000000000",
                "working_directory": str(work),
                "timeout_seconds": 5,
            }
            fake = FakeDockerCli(config, plan)
            real_job = None
            real_process = None
            def spawn_attach(container_id):
                nonlocal real_job, real_process
                fake.events.append("spawn_attach")
                fake.running = True
                fake.status = "running"
                real_job = create_job("Local\\McpDockerFrame-" + os.urandom(8).hex())
                python = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
                source = (
                    "import sys\n"
                    "for line in sys.stdin.buffer:\n"
                    " sys.stdout.buffer.write(line);sys.stdout.buffer.flush()\n"
                )
                real_process = spawn_process(
                    [str(python), "-u", "-c", source],
                    cwd=work,
                    environment={},
                    job_handles=[real_job],
                    exact_job_handle=real_job,
                    cleanup_job_handle=real_job,
                    capture_output=True,
                    stdin_pipe=True,
                )
                return real_process, real_job
            fake.spawn_attach = spawn_attach
            with patch.object(mcp_docker, "_DockerCli", side_effect=lambda frozen: fake.bind(frozen)):
                owned = launch_docker(plan)
                channel = FramedStdioChannel(owned)
                channel.write(b'{"jsonrpc":"2.0","id":"proof"}\n', timeout=2)
                self.assertEqual(b'{"jsonrpc":"2.0","id":"proof"}\n', channel.read(timeout=2))
                channel.assert_idle()
                channel.close()
            self.assertIn("stop", fake.events)
            self.assertEqual(1, fake.stop_calls)
            self.assertFalse(fake.running)
            self.assertIsNone(owned.cleanup_job_handle)


if __name__ == "__main__":
    unittest.main()
