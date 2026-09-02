from __future__ import annotations

import re

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def anchor_job_name(installation_id: str, incarnation: str, attempt_id: str) -> str:
    return _name("anchor", installation_id, incarnation[:12], attempt_id[:12])


def child_job_name(
    installation_id: str,
    incarnation: str,
    group_id: str,
    slot: int,
    attempt_id: str,
) -> str:
    return _name("child", installation_id, incarnation[:12], group_id[:36], str(slot), attempt_id[:12])


def signal_job_name(
    installation_id: str,
    incarnation: str,
    group_id: str,
    attempt_id: str,
) -> str:
    return _name("signal", installation_id, incarnation[:12], group_id[:36], attempt_id[:12])


def _name(kind: str, *parts: str) -> str:
    clean = [_SAFE.sub("-", part).strip("-")[:64] for part in parts]
    return "Local\\PyLaravelSupervisor-" + kind + "-" + "-".join(clean)