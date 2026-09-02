from __future__ import annotations

from .contracts import DesiredManifest, ProcessGroupSpec


def build_reverb_restart_command(manifest: DesiredManifest) -> tuple[str, ...]:
    return (
        str(manifest.runtime.php_executable),
        str(manifest.runtime.project_root / "artisan"),
        "reverb:restart",
        "--no-interaction",
    )


def build_group_command(manifest: DesiredManifest, group: ProcessGroupSpec) -> tuple[str, ...]:
    php = str(manifest.runtime.php_executable)
    artisan = str(manifest.runtime.project_root / "artisan")
    if group.kind == "reverb":
        return php, artisan, "reverb:start", "--no-interaction"
    if group.kind == "scheduler":
        return php, artisan, "schedule:run", "--no-interaction"

    if group.queue is None:
        raise ValueError("queue_once group is missing queue options")
    queue = group.queue
    return (
        php,
        artisan,
        "queue:work",
        queue.connection,
        "--once",
        "--json",
        "--no-interaction",
        f"--queue={','.join(queue.queues)}",
        f"--backoff={','.join(str(value) for value in queue.backoff)}",
        f"--tries={queue.tries}",
        f"--sleep={queue.sleep_seconds:g}",
    )