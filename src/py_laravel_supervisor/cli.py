from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from typing import Any

from .control import ControlError, SupervisorControl
from .recovery import RecoveryError
from .resident import RecoveryRequired, SupervisorResident


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="process-supervisor-python")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor")
    _runtime_args(doctor, installation_required=False)

    for name in ("apply-desired", "disable-gate", "ensure-running", "shutdown", "recover", "status"):
        subparser = subparsers.add_parser(name)
        _runtime_args(subparser, installation_required=True)

    resident = subparsers.add_parser("resident")
    _runtime_args(resident, installation_required=True)
    resident.add_argument("--incarnation", required=True)
    resident.add_argument("--attempt-id", required=True)
    resident.add_argument("--ready-nonce", required=True)
    resident.add_argument("--anchor-job-name", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "doctor":
            supported = _windows_runtime_supported()
            return _emit({
                "ok": supported,
                "platform": platform.system(),
                "platform_release": platform.release(),
                "architecture": platform.machine(),
                "python": platform.python_version(),
                "windows_first_v1": True,
                "nested_job_list_supported": supported,
                "minimum_windows_contract": "Windows 10 / Windows Server 2016",
            }, 0 if supported else 2)
        if args.command == "resident":
            resident_runtime = SupervisorResident(
                runtime_root=args.runtime_root,
                installation_id=args.installation_id,
                incarnation=args.incarnation,
                attempt_id=args.attempt_id,
                ready_nonce=args.ready_nonce,
                anchor_name=args.anchor_job_name,
            )
            return resident_runtime.run()
        control = SupervisorControl(args.runtime_root, args.installation_id)
        if args.command == "apply-desired":
            return _emit({"ok": True, **control.apply_desired(_read_json_stdin())})
        if args.command == "disable-gate":
            return _emit({"ok": True, **control.disable_gate()})
        if args.command == "ensure-running":
            return _emit({"ok": True, **control.ensure_running()})
        if args.command == "shutdown":
            return _emit({"ok": True, **control.shutdown()})
        if args.command == "recover":
            return _emit({"ok": True, **control.recover()})
        if args.command == "status":
            return _emit({"ok": True, "status": control.status()})
        raise AssertionError("unsupported command")
    except (ControlError, RecoveryError, RecoveryRequired, ValueError, OSError) as error:
        return _emit(
            {
                "ok": False,
                "error": {
                    "code": _error_code(error),
                    "message": str(error)[:240],
                },
            },
            2,
        )
    except Exception:
        return _emit(
            {
                "ok": False,
                "error": {
                    "code": "SUPERVISOR_INTERNAL_ERROR",
                    "message": "The supervisor failed unexpectedly.",
                },
            },
            1,
        )


def _windows_runtime_supported() -> bool:
    if os.name != "nt" or sys.maxsize <= 2**32 or not hasattr(sys, "getwindowsversion"):
        return False
    return sys.getwindowsversion().major >= 10


def _runtime_args(parser: argparse.ArgumentParser, *, installation_required: bool) -> None:
    parser.add_argument("--runtime-root", default=".")
    parser.add_argument("--installation-id", required=installation_required)


def _read_json_stdin() -> dict[str, object]:
    payload = sys.stdin.buffer.read(1_000_001)
    if len(payload) > 1_000_000:
        raise ValueError("desired state input exceeds one megabyte")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("desired state input must be valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ValueError("desired state input must be a JSON object")
    return value


def _error_code(error: BaseException) -> str:
    if isinstance(error, RecoveryRequired):
        return "RECOVERY_REQUIRED"
    if isinstance(error, RecoveryError):
        return "RECOVERY_REJECTED"
    if isinstance(error, ControlError):
        return "CONTROL_REJECTED"
    if isinstance(error, ValueError):
        return "INVALID_RUNTIME_STATE"
    return "OS_ERROR"


def _emit(payload: dict[str, Any], exit_code: int = 0) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.write("\n")
    sys.stdout.flush()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())