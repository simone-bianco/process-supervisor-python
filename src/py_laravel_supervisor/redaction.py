from __future__ import annotations

import codecs
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)((?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
)


def redact_line(value: str) -> str:
    redacted = value
    for pattern in _SECRET_PATTERNS:
        if pattern.groups:
            redacted = pattern.sub(lambda match: f"{match.group(1)}[REDACTED]", redacted)
        else:
            redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


@dataclass(slots=True)
class RedactedLineAccumulator:
    max_line_bytes: int = 16_384
    max_lines: int = 200
    max_bytes: int = 64_000
    protocol_line_observer: Callable[[str], None] | None = None
    _decoder: codecs.IncrementalDecoder = field(init=False, repr=False)
    _pending: str = field(default="", init=False, repr=False)
    _suppressed: bool = field(default=False, init=False, repr=False)
    _lines: deque[str] = field(default_factory=deque, init=False, repr=False)
    _bytes: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    def feed(self, chunk: bytes) -> list[str]:
        return self._consume(self._decoder.decode(chunk, final=False))

    def finish(self) -> list[str]:
        completed = self._consume(self._decoder.decode(b"", final=True))
        if self._pending and not self._suppressed:
            completed.extend(self._emit_line(self._pending))
        elif self._suppressed:
            completed.extend(self._append("[oversized output suppressed]"))
        self._pending = ""
        self._suppressed = False
        return completed

    def snapshot(self) -> list[str]:
        return list(self._lines)

    def _consume(self, text: str) -> list[str]:
        emitted: list[str] = []
        for character in text:
            if character == "\n":
                if self._suppressed:
                    emitted.extend(self._append("[oversized output suppressed]"))
                else:
                    emitted.extend(self._emit_line(self._pending.rstrip("\r")))
                self._pending = ""
                self._suppressed = False
                continue
            if self._suppressed:
                continue
            self._pending += character
            if len(self._pending.encode("utf-8", errors="replace")) > self.max_line_bytes:
                self._pending = ""
                self._suppressed = True
        return emitted

    def _emit_line(self, raw_line: str) -> list[str]:
        if raw_line == "":
            return []
        if self.protocol_line_observer is not None:
            # Internal memory-only hook for protocol parsing. Persistence must
            # consume the redacted line returned below, never this raw value.
            self.protocol_line_observer(raw_line)
        return self._append(redact_line(raw_line))

    def _append(self, line: str) -> list[str]:
        if line == "":
            return []
        encoded = len(line.encode("utf-8"))
        self._lines.append(line)
        self._bytes += encoded
        while self._lines and (len(self._lines) > self.max_lines or self._bytes > self.max_bytes):
            removed = self._lines.popleft()
            self._bytes -= len(removed.encode("utf-8"))
        return [line]