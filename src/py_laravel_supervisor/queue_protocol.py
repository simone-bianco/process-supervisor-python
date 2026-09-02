from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

_ALLOWED_STATUS = {"starting", "success", "released_after_exception", "failed"}
JobOutcome = Literal["success", "failed", "released", "empty", "unknown"]


class QueueProtocolError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class QueueFrame:
    status: str
    result: str | None
    job: str
    queue: str
    connection: str
    attempts: int


class QueueProtocolClassifier:
    def __init__(self) -> None:
        self.frames: list[QueueFrame] = []

    def consume_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped.startswith("{"):
            return False
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as error:
            raise QueueProtocolError("Malformed queue JSON protocol frame.") from error
        if not isinstance(payload, dict) or "status" not in payload:
            return False
        status = payload.get("status")
        if status not in _ALLOWED_STATUS:
            raise QueueProtocolError("Unsupported queue JSON status.")
        required = ("job", "queue", "connection", "attempts")
        if any(key not in payload for key in required):
            raise QueueProtocolError("Incomplete queue JSON protocol frame.")
        job, queue, connection, attempts = (payload[key] for key in required)
        result = payload.get("result")
        if not all(isinstance(value, str) and value for value in (job, queue, connection)):
            raise QueueProtocolError("Invalid queue JSON identity fields.")
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            raise QueueProtocolError("Invalid queue JSON attempts field.")
        if result is not None and not isinstance(result, str):
            raise QueueProtocolError("Invalid queue JSON result field.")
        frame = QueueFrame(status, result, job, queue, connection, attempts)
        if status == "starting":
            if self.frames:
                raise QueueProtocolError("Queue one-shot emitted more than one starting frame.")
        else:
            if len(self.frames) != 1 or self.frames[0].status != "starting":
                raise QueueProtocolError("Queue terminal frame has no matching starting frame.")
            started = self.frames[0]
            if (started.job, started.queue, started.connection) != (frame.job, frame.queue, frame.connection):
                raise QueueProtocolError("Queue terminal frame identity changed within one-shot execution.")
        self.frames.append(frame)
        return True

    def sanitized_projection(self) -> dict[str, str | int | None] | None:
        if not self.frames:
            return None
        frame = self.frames[-1]
        return {
            "status": frame.status,
            "result": frame.result,
            "job": frame.job,
            "queue": frame.queue,
            "connection": frame.connection,
            "attempts": frame.attempts,
        }

    def completion(self, exit_code: int, *, runtime_error: bool = False) -> tuple[bool, JobOutcome]:
        if runtime_error or exit_code != 0:
            return False, "unknown"
        terminal = [frame for frame in self.frames if frame.status != "starting"]
        if not self.frames:
            return True, "empty"
        if not terminal:
            return False, "unknown"
        last = terminal[-1]
        if last.status == "success":
            return True, "success"
        if last.status == "released_after_exception" or last.result == "released":
            return True, "released"
        if last.status == "failed" or last.result == "failed":
            return True, "failed"
        return True, "unknown"