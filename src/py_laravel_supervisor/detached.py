from __future__ import annotations

import base64
import json
import os
import subprocess
from pathlib import Path
from typing import Iterable


class DetachedLaunchError(RuntimeError):
    pass


def launch_via_windows_management(
    argv: Iterable[str],
    *,
    cwd: str | Path,
    timeout_seconds: float = 8.0,
) -> int:
    """Ask Win32_Process to create a process outside the short-lived caller tree."""
    command = [str(value) for value in argv]
    if not command or not Path(command[0]).is_absolute():
        raise DetachedLaunchError("detached process requires an absolute executable")

    working_directory = str(Path(cwd).resolve())
    command_line = subprocess.list2cmdline(command)
    if len(command_line) > 30_000:
        raise DetachedLaunchError("detached process command line is too long")

    command_payload = base64.b64encode(command_line.encode("utf-8")).decode("ascii")
    cwd_payload = base64.b64encode(working_directory.encode("utf-8")).decode("ascii")
    script = (
        "$ErrorActionPreference='Stop';"
        f"$cmd=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{command_payload}'));"
        f"$cwd=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{cwd_payload}'));"
        "$result=Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
        "-Arguments @{CommandLine=$cmd;CurrentDirectory=$cwd};"
        "$payload=@{return_value=[int]$result.ReturnValue;process_id=[int]$result.ProcessId};"
        "[Console]::Out.Write(($payload|ConvertTo-Json -Compress));"
        "exit [int]$result.ReturnValue"
    )
    encoded_script = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    system_root = os.environ.get("SystemRoot") or os.environ.get("WINDIR") or r"C:\Windows"
    powershell = Path(system_root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    if not powershell.is_file():
        raise DetachedLaunchError("Windows PowerShell is unavailable for detached launch")

    try:
        completed = subprocess.run(
            [
                str(powershell),
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-EncodedCommand",
                encoded_script,
            ],
            cwd=working_directory,
            capture_output=True,
            text=True,
            timeout=max(1.0, min(timeout_seconds, 30.0)),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise DetachedLaunchError("Windows process broker did not complete") from error

    try:
        response = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise DetachedLaunchError("Windows process broker returned an invalid response") from error
    if (
        completed.returncode != 0
        or not isinstance(response, dict)
        or response.get("return_value") != 0
        or isinstance(response.get("process_id"), bool)
        or not isinstance(response.get("process_id"), int)
        or response["process_id"] <= 0
    ):
        raise DetachedLaunchError("Windows process broker rejected the detached launch")

    return response["process_id"]