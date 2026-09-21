"""`Target`, `FastLaneState`/`SlowLaneState` (`research.md` §5, `data-model.md`).

`Target` holds coordination state only — never a live session, connector, or
token (that's `TargetRefreshContext`'s job). Lane publication is always a
reference replace, never in-place mutation, so a concurrent reader sees
either the fully-old or fully-new object with no lock needed.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


def compact_json_size(data: Any) -> int:
    """`research.md` Group 5 (item 5): a byte-size ESTIMATE for storage
    bounds only — this must never become a second full-fidelity copy of
    the snapshot purely to measure it. `json.dumps`'s default
    `ensure_ascii=True` already guarantees the output is pure ASCII (any
    non-ASCII character is escaped as `\\uXXXX`), which makes `len()` on
    the resulting STRING itself exactly equal to its UTF-8-encoded byte
    length — the extra `.encode("utf-8")` call the previous version made
    materialized a SECOND full copy purely to call `len()` on it, for a
    result that was always identical to the string's own length.
    `sort_keys=True` is dropped too: key ordering never changes the
    resulting length, only the cost of achieving it."""
    return len(json.dumps(data, separators=(",", ":")))


@dataclass(frozen=True)
class LaneState:
    """Shared shape for `FastLaneState`/`SlowLaneState` — both immutable,
    published by reference-replace only."""

    snapshot: Optional[dict[str, Any]] = None
    serialized_size: int = 0
    generation: int = 0
    last_success_at: Optional[float] = None
    last_error: Optional[str] = None
    deadline_seconds: float = 15.0
    ttl_seconds: float = 30.0

    def is_usable(self, *, now: Optional[float] = None) -> bool:
        if self.snapshot is None or self.last_success_at is None:
            return False
        current = time.time() if now is None else now
        return (current - self.last_success_at) <= self.ttl_seconds

    def published(self, snapshot: dict[str, Any], *, now: Optional[float] = None) -> "LaneState":
        """Returns a NEW `LaneState` — publication is always reference
        replacement, never in-place mutation of `self`."""
        return LaneState(
            snapshot=snapshot,
            serialized_size=compact_json_size(snapshot),
            generation=self.generation + 1,
            last_success_at=time.time() if now is None else now,
            last_error=None,
            deadline_seconds=self.deadline_seconds,
            ttl_seconds=self.ttl_seconds,
        )

    def failed(self, error_summary: str) -> "LaneState":
        """A failed refresh leaves the snapshot/generation untouched (no
        stale-on-error *change*, resolved decision #4/#5) — only records the
        redacted error for observability."""
        return LaneState(
            snapshot=self.snapshot,
            serialized_size=self.serialized_size,
            generation=self.generation,
            last_success_at=self.last_success_at,
            last_error=error_summary,
            deadline_seconds=self.deadline_seconds,
            ttl_seconds=self.ttl_seconds,
        )


def FastLaneState(*, deadline_seconds: float = 15.0, ttl_seconds: float = 30.0) -> LaneState:
    return LaneState(deadline_seconds=deadline_seconds, ttl_seconds=ttl_seconds)


def SlowLaneState(*, deadline_seconds: float = 90.0, ttl_seconds: float = 900.0) -> LaneState:
    return LaneState(deadline_seconds=deadline_seconds, ttl_seconds=ttl_seconds)


class TargetLifecycleState(str, Enum):
    NEW = "NEW"
    FAST_WARMING = "FAST_WARMING"
    FAST_READY = "FAST_READY"
    WARM = "WARM"
    EVICTING = "EVICTING"



def merge_canonical_snapshot(
    fast: LaneState, slow: LaneState, *, now: Optional[float] = None
) -> dict[str, Any]:
    """Read-time merge into the same shape `dataReconstructor` produces
    today — a plain dict union, since Fast/Slow-owned keys never overlap.

    Only a lane's CURRENTLY usable (unexpired, per its own TTL) snapshot is
    merged in (`research.md` §3: "failed/expired Fast cannot emit
    `PhysicalServer_Query=1`"; "expired failed Slow is not emitted") — a
    lane that succeeded once and then went stale/unreachable must stop
    being served once past its own freshness window, not indefinitely. An
    ordinary transient failure (`LaneState.failed()`) does NOT itself
    expire anything — the prior snapshot/generation and its
    `last_success_at` are untouched, so it remains usable exactly until its
    own TTL elapses, same as any other successful publication would."""
    current = time.time() if now is None else now
    merged: dict[str, Any] = {}
    if fast.is_usable(now=current):
        merged.update(fast.snapshot)  # type: ignore[arg-type]
    if slow.is_usable(now=current):
        merged.update(slow.snapshot)  # type: ignore[arg-type]
    return merged


@dataclass
class Target:
    """Persistent coordination state for one `(serverAddress, config)`
    identity. NEVER holds a live session/connector/token — those belong
    only to the open `TargetRefreshContext`, if any."""

    key: tuple[str, str]
    cycle_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_cycle: Any = None
    active_cycle_task: Optional["asyncio.Task[Any]"] = None
    fast: LaneState = field(default_factory=FastLaneState)
    slow: LaneState = field(default_factory=SlowLaneState)
    last_activity: float = field(default_factory=time.time)
    state: TargetLifecycleState = TargetLifecycleState.NEW
    # Set once this target's selected schema is discovered to declare
    # `Slow.Resources: []` — permanently disables Slow for this Target's
    # lifetime (never re-added to a future `lanes_due`), rather than
    # merely resolving until the next TTL elapses and becoming due again
    # forever (`research.md` §3: "does not remain perpetually due").
    slow_disabled: bool = False

    def touch(self, *, now: Optional[float] = None) -> None:
        self.last_activity = time.time() if now is None else now

    def canonical_snapshot(self, *, now: Optional[float] = None) -> dict[str, Any]:
        return merge_canonical_snapshot(self.fast, self.slow, now=now)

    def is_idle(self, *, idle_ttl_seconds: float, now: Optional[float] = None) -> bool:
        if self.active_cycle_task is not None:
            return False
        current = time.time() if now is None else now
        return (current - self.last_activity) > idle_ttl_seconds

    def advance_state_on_fast_success(self) -> None:
        if self.state in (TargetLifecycleState.NEW, TargetLifecycleState.FAST_WARMING):
            self.state = TargetLifecycleState.FAST_READY

    def advance_state_on_fast_failure(self) -> None:
        if self.state == TargetLifecycleState.FAST_WARMING:
            self.state = TargetLifecycleState.NEW

    def advance_state_on_slow_success(self) -> None:
        if self.state in (TargetLifecycleState.FAST_READY, TargetLifecycleState.WARM):
            self.state = TargetLifecycleState.WARM

    def begin_fast_warming(self) -> None:
        if self.state == TargetLifecycleState.NEW:
            self.state = TargetLifecycleState.FAST_WARMING
