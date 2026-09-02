from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .contracts import ContractError, ProcessGroupSpec, SCHEMA_VERSION
from .runtime_files import RuntimeStore


@dataclass(frozen=True, slots=True)
class CronField:
    values: frozenset[int]
    wildcard: bool


@dataclass(frozen=True, slots=True)
class CronSchedule:
    minute: CronField
    hour: CronField
    day_of_month: CronField
    month: CronField
    day_of_week: CronField

    @classmethod
    def parse(cls, expression: str) -> "CronSchedule":
        parts = expression.split()
        if len(parts) != 5:
            raise ContractError("scheduler.cron must contain exactly five fields")
        return cls(
            minute=_parse_field(parts[0], 0, 59),
            hour=_parse_field(parts[1], 0, 23),
            day_of_month=_parse_field(parts[2], 1, 31),
            month=_parse_field(parts[3], 1, 12),
            day_of_week=_parse_field(parts[4], 0, 7, sunday_alias=True),
        )

    def matches(self, value: datetime, timezone_name: str = "UTC") -> bool:
        current = value.astimezone(ZoneInfo(validate_timezone(timezone_name)))
        if current.minute not in self.minute.values or current.hour not in self.hour.values:
            return False
        if current.month not in self.month.values:
            return False

        dom_match = current.day in self.day_of_month.values
        cron_dow = (current.weekday() + 1) % 7
        dow_match = cron_dow in self.day_of_week.values
        if self.day_of_month.wildcard and self.day_of_week.wildcard:
            return dom_match and dow_match
        if self.day_of_month.wildcard:
            return dow_match
        if self.day_of_week.wildcard:
            return dom_match
        return dom_match or dow_match


def validate_cron(expression: str) -> str:
    normalized = " ".join(expression.strip().split())
    if not normalized or len(normalized) > 128:
        raise ContractError("scheduler.cron must be a bounded non-empty expression")
    CronSchedule.parse(normalized)
    return normalized


def validate_timezone(timezone_name: str) -> str:
    if not timezone_name or len(timezone_name) > 64:
        raise ContractError("scheduler.timezone must be a bounded IANA timezone")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ContractError("scheduler.timezone is unavailable") from error
    return timezone_name


class SchedulerTrigger:
    """Persistently claims a scheduler minute before spawn to prevent duplicate runs."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.now = now or (lambda: datetime.now(timezone.utc))

    def claim_if_due(self, group: ProcessGroupSpec) -> bool:
        if group.kind != "scheduler":
            return True
        if group.scheduler is None:
            raise ContractError("scheduler group is missing scheduler options")

        current_utc = self.now().astimezone(timezone.utc).replace(second=0, microsecond=0)
        if not CronSchedule.parse(group.scheduler.cron).matches(
            current_utc,
            group.scheduler.timezone,
        ):
            return False

        minute_key = current_utc.isoformat()
        path = self.store.paths.scheduler_state(group.id)
        state = self.store.read_json(path, required=False)
        if (
            isinstance(state, dict)
            and state.get("schema_version") == SCHEMA_VERSION
            and state.get("generation") == group.generation
            and state.get("minute") == minute_key
        ):
            return False

        self.store.write_json(
            path,
            {
                "schema_version": SCHEMA_VERSION,
                "group_id": group.id,
                "generation": group.generation,
                "minute": minute_key,
            },
        )
        return True


def _parse_field(raw: str, minimum: int, maximum: int, *, sunday_alias: bool = False) -> CronField:
    if not raw or len(raw) > 64:
        raise ContractError("scheduler.cron field is invalid")
    values: set[int] = set()
    wildcard = raw.startswith("*")
    for segment in raw.split(","):
        if not segment:
            raise ContractError("scheduler.cron field is invalid")
        base, step = _split_step(segment)
        start: int
        end: int
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            pieces = base.split("-", 1)
            if len(pieces) != 2:
                raise ContractError("scheduler.cron range is invalid")
            start = _cron_int(pieces[0], minimum, maximum)
            end = _cron_int(pieces[1], minimum, maximum)
            if start > end:
                raise ContractError("scheduler.cron range is invalid")
        else:
            start = _cron_int(base, minimum, maximum)
            end = maximum if "/" in segment else start
        for candidate in range(start, end + 1, step):
            values.add(0 if sunday_alias and candidate == 7 else candidate)
    if not values:
        raise ContractError("scheduler.cron field has no values")
    return CronField(frozenset(values), wildcard)


def _split_step(segment: str) -> tuple[str, int]:
    if "/" not in segment:
        return segment, 1
    pieces = segment.split("/", 1)
    if len(pieces) != 2 or not pieces[1].isdigit():
        raise ContractError("scheduler.cron step is invalid")
    step = int(pieces[1])
    if step < 1:
        raise ContractError("scheduler.cron step is invalid")
    return pieces[0], step


def _cron_int(raw: str, minimum: int, maximum: int) -> int:
    if not raw.isdigit():
        raise ContractError("scheduler.cron value is invalid")
    value = int(raw)
    if value < minimum or value > maximum:
        raise ContractError("scheduler.cron value is outside the supported range")
    return value