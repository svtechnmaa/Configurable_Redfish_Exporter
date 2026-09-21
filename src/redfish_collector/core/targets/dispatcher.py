"""Canonical-IP `BmcCoordinator` + `PriorityRequestDispatcher` (`research.md` §4).

Not a bare `asyncio.Semaphore` — that has no priority between waiters and
cannot give Fast a real precedence guarantee over Slow. Every acquisition,
cancellation, and cleanup operation is tagged to its owning refresh-cycle ID
so `cancel_cycle` never touches another cycle's (or another config's) work.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Optional


class DispatcherQueueFullError(Exception):
    """A lane's queue is at `MaxQueuedRequestsPerLane` — a terminal internal
    lane error for this attempt, never retried by the network retry policy."""


class CycleLeaseDeniedError(Exception):
    """`MaxConcurrentCyclesPerBmc` reached — non-blocking, caller maps this
    to overload 503 (cold identity) or a skipped due Slow-only attempt."""


class _Waiter:
    __slots__ = ("cycle_id", "request_id", "future")

    def __init__(self, cycle_id: str, request_id: str, future: "asyncio.Future[None]") -> None:
        self.cycle_id = cycle_id
        self.request_id = request_id
        self.future = future


class _Lease:
    """`async with dispatcher.lease(priority, cycle_id, request_id):` wraps
    one bounded Redfish request with guaranteed release, including on
    cancellation/exception."""

    def __init__(self, dispatcher: "PriorityRequestDispatcher", priority: str, cycle_id: str, request_id: str) -> None:
        self._dispatcher = dispatcher
        self._priority = priority
        self._cycle_id = cycle_id
        self._request_id = request_id
        self._acquired = False

    async def __aenter__(self) -> None:
        await self._dispatcher.acquire(self._priority, self._cycle_id, self._request_id)
        self._acquired = True

    async def __aexit__(self, *exc: object) -> None:
        if self._acquired:
            self._acquired = False
            self._dispatcher.release(self._cycle_id, self._request_id)


class PriorityRequestDispatcher:
    """One dispatcher per canonical BMC IP, shared by every open
    `TargetRefreshContext` for that IP across all `config` identities."""

    def __init__(self, *, capacity: int, max_queued_per_lane: int, fast_batch_limit: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self.max_queued_per_lane = max_queued_per_lane
        self.fast_batch_limit = fast_batch_limit
        self.in_flight = 0
        self.fast_queue: deque[_Waiter] = deque()
        self.slow_queue: deque[_Waiter] = deque()
        self.consecutive_fast_without_slow = 0
        # Every currently-admitted (cycle_id, request_id) pair. `release()`
        # only decrements `in_flight` for a pair actually present here — a
        # wrong, duplicate, or cross-cycle release call is a no-op rather
        # than silently freeing a capacity slot no request actually held.
        self._admitted: set[tuple[str, str]] = set()
        # Round 5A.2: pairs `abandon_cycle()` forcibly stripped out of
        # `_admitted` at shutdown, before the owning request task itself ever
        # got to call `release()`. A LATE `release()` call for one of these
        # (the request task's own cancellation unwind finally reaching its
        # `_Lease.__aexit__`, possibly long after `abandon_cycle()` already
        # ran) is an entirely EXPECTED outcome of emergency abandonment, not
        # a wrong/duplicate/cross-cycle release — `release()` below
        # recognizes it here and returns silently rather than logging a
        # warning per reclaimed request (which would violate the "exactly
        # one safe aggregate abandonment warning" contract). Each entry is
        # consumed (discarded) the first time its matching late `release()`
        # arrives, so this set only ever holds pairs genuinely still
        # awaiting their one expected late release.
        self._abandoned_pairs: set[tuple[str, str]] = set()

    def lease(self, priority: str, cycle_id: str, request_id: str) -> _Lease:
        return _Lease(self, priority, cycle_id, request_id)

    async def acquire(self, priority: str, cycle_id: str, request_id: str) -> None:
        if priority not in ("fast", "slow"):
            raise ValueError(f"priority must be 'fast' or 'slow', got {priority!r}")
        if self.in_flight < self.capacity:
            self.in_flight += 1
            self._admitted.add((cycle_id, request_id))
            return

        queue = self.fast_queue if priority == "fast" else self.slow_queue
        if len(queue) >= self.max_queued_per_lane:
            raise DispatcherQueueFullError(
                f"{priority} queue full ({len(queue)}/{self.max_queued_per_lane}) for cycle {cycle_id!r}"
            )
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[None]" = loop.create_future()
        waiter = _Waiter(cycle_id, request_id, future)
        queue.append(waiter)
        try:
            await future
        except asyncio.CancelledError:
            if waiter in queue:
                queue.remove(waiter)
            elif (cycle_id, request_id) in self._admitted:
                # Already admitted (moved from queue to `_admitted` by
                # `_admit_next`) between being selected and this
                # cancellation reaching us before `__aenter__` returned to
                # the caller — the lease was granted but the caller never
                # got to use or release it. Release it now so capacity
                # isn't silently lost.
                self.release(cycle_id, request_id)
            # else: this waiter is in neither `queue` nor `_admitted` — it
            # was already reclaimed out from under it by `cancel_cycle()` /
            # `abandon_cycle()` (round 5A.2) while still queued. That is an
            # expected, already-accounted-for outcome (queue membership was
            # the ONLY thing this waiter ever held), never a call to
            # `release()` for a pair that was never admitted in the first
            # place — the prior version always fell into the `else` branch
            # here whenever the waiter wasn't in `queue`, which incorrectly
            # called `release()` for a merely-dequeued, never-admitted
            # waiter and produced a false "not currently admitted" warning.
            raise

    def release(self, cycle_id: str, request_id: str) -> None:
        pair = (cycle_id, request_id)
        if pair not in self._admitted:
            if pair in self._abandoned_pairs:
                # Expected: `abandon_cycle()` already forcibly reclaimed this
                # pair's capacity at shutdown; this is that request's own
                # (possibly much later) lease-exit finally catching up. Not a
                # wrong/duplicate/cross-cycle release — no warning.
                self._abandoned_pairs.discard(pair)
                return
            logging.warning(
                "dispatcher.release() called for a lease that is not currently admitted "
                "(cycle_id=%r, request_id=%r) — ignored, no capacity was freed", cycle_id, request_id,
            )
            return
        self._admitted.discard(pair)
        self.in_flight -= 1
        self._admit_next()

    def _admit_next(self) -> None:
        waiter: Optional[_Waiter] = None
        if self.fast_queue and self.consecutive_fast_without_slow < self.fast_batch_limit:
            waiter = self.fast_queue.popleft()
            self.consecutive_fast_without_slow += 1
        elif self.slow_queue:
            waiter = self.slow_queue.popleft()
            self.consecutive_fast_without_slow = 0
        elif self.fast_queue:
            waiter = self.fast_queue.popleft()
            self.consecutive_fast_without_slow += 1

        if waiter is None:
            return
        self.in_flight += 1
        self._admitted.add((waiter.cycle_id, waiter.request_id))
        if not waiter.future.done():
            waiter.future.set_result(None)
        else:
            # Already cancelled between being queued and admitted here —
            # undo the increment and try the next waiter instead.
            self._admitted.discard((waiter.cycle_id, waiter.request_id))
            self.in_flight -= 1
            self._admit_next()

    def cancel_cycle(self, cycle_id: str) -> None:
        """Removes only `cycle_id`'s queued waiters; never touches another
        cycle's (or config's) queued or in-flight work. Admitted (in-flight)
        requests are the caller's own tasks to cancel — the dispatcher only
        owns queue membership, not task lifecycles."""
        for queue in (self.fast_queue, self.slow_queue):
            remaining: deque[_Waiter] = deque()
            for waiter in queue:
                if waiter.cycle_id == cycle_id:
                    if not waiter.future.done():
                        waiter.future.cancel()
                else:
                    remaining.append(waiter)
            queue.clear()
            queue.extend(remaining)

    def abandon_cycle(self, cycle_id: str) -> None:
        """Cycle-scoped EMERGENCY reclamation (round 5A.1): unlike
        `cancel_cycle` (which only ever removes queue membership — admitted
        in-flight requests are normally the owning task's own responsibility
        to cancel, which then calls `release()` itself), this additionally
        repairs `in_flight`/`_admitted` for every one of `cycle_id`'s
        currently-admitted requests directly, for the case where the owning
        cycle is being abandoned at shutdown and its request tasks may never
        reach their own `release()` call. Membership-gated per pair (exactly
        like `release()`) and idempotent: calling this twice, or calling it
        once and then having a request's own normal `release()` arrive late
        for a pair already reclaimed here, is a safe no-op in both orders —
        `release()`'s own `pair not in self._admitted` guard already handles
        the second case. Never touches another cycle's admitted or queued
        work."""
        self.cancel_cycle(cycle_id)
        stale = [pair for pair in self._admitted if pair[0] == cycle_id]
        for pair in stale:
            self._admitted.discard(pair)
            self.in_flight -= 1
            # Record so the owning request task's own eventual (possibly
            # much later) `release()` call for this exact pair is recognized
            # as an expected late lease-exit, not a wrong/duplicate release —
            # see `_abandoned_pairs`' own docstring above.
            self._abandoned_pairs.add(pair)
        for _ in stale:
            self._admit_next()

    @property
    def queue_depth(self) -> int:
        return len(self.fast_queue) + len(self.slow_queue)


class BmcCoordinator:
    """Owns one `PriorityRequestDispatcher` and a non-blocking cycle-lease
    ceiling (`Tuning.Refresh.MaxConcurrentCyclesPerBmc`) for one canonical
    BMC IP, shared across every `config` identity that targets it."""

    def __init__(
        self, *, capacity: int, max_queued_per_lane: int, fast_batch_limit: int, max_concurrent_cycles: int
    ) -> None:
        self.dispatcher = PriorityRequestDispatcher(
            capacity=capacity, max_queued_per_lane=max_queued_per_lane, fast_batch_limit=fast_batch_limit
        )
        self.max_concurrent_cycles = max_concurrent_cycles
        self.lease_count = 0

    def try_acquire_cycle_lease(self) -> bool:
        """Non-blocking. `False` means the caller must not open a new cycle
        this scrape (cold identity -> overload 503; a target with usable
        Fast data skips a due Slow-only attempt and retries later)."""
        if self.lease_count >= self.max_concurrent_cycles:
            return False
        self.lease_count += 1
        return True

    def release_cycle_lease(self) -> None:
        if self.lease_count <= 0:
            raise RuntimeError("release_cycle_lease called with no held lease")
        self.lease_count -= 1

    def is_idle(self) -> bool:
        return (
            self.lease_count == 0
            and self.dispatcher.in_flight == 0
            and self.dispatcher.queue_depth == 0
        )
