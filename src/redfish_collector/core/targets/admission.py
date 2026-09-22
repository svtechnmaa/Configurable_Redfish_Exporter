"""`AdmissionLimiter` (scrape overload -> HTTP 503) and `GlobalRefreshAdmission`
(separate ceiling on concurrently-open `TargetRefreshContext`s) — two
intentionally distinct gates (`research.md` §2, §14; `data-model.md`).
"""

from __future__ import annotations


class AdmissionLimiter:
    """Gates incoming HTTP scrapes only — never cycle creation."""

    def __init__(self, *, max_parallel_targets: int) -> None:
        self.max_parallel_targets = max_parallel_targets
        self.active_and_pending = 0

    def try_admit(self) -> bool:
        if self.active_and_pending >= self.max_parallel_targets:
            return False
        self.active_and_pending += 1
        return True

    def release(self) -> None:
        if self.active_and_pending <= 0:
            raise RuntimeError("AdmissionLimiter.release called with nothing admitted")
        self.active_and_pending -= 1


class GlobalRefreshAdmission:
    """Bounds how many `TargetRefreshContext`s may be open across the whole
    registry at once — never queues."""

    def __init__(self, *, max_concurrent_refreshes: int) -> None:
        self.max_concurrent_refreshes = max_concurrent_refreshes
        self.active_cycles = 0

    def try_acquire(self) -> bool:
        if self.active_cycles >= self.max_concurrent_refreshes:
            return False
        self.active_cycles += 1
        return True

    def release(self) -> None:
        if self.active_cycles <= 0:
            raise RuntimeError("GlobalRefreshAdmission.release called with nothing acquired")
        self.active_cycles -= 1
