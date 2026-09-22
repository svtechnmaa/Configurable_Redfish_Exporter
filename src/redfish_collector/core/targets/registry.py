"""`TargetRegistry` (`data-model.md`): the single process-local map owning
every known target, plus the shared per-canonical-IP `BmcCoordinator`s.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from ..security.redaction import safe_exception_summary
from .admission import AdmissionLimiter, GlobalRefreshAdmission
from .dispatcher import BmcCoordinator
from .refresh_context import apply_shutdown_deadline, emergency_abandon
from .request_execution import InFlightByteBudget
from .response_cache import ResponseCache
from .target_state import FastLaneState, SlowLaneState, Target


class RegistryFull(Exception):
    """Every eviction candidate has an open/opening cycle — bounded capacity
    outcome, never exceeds `max_targets`, never evicts active state. The
    router converts this to overload HTTP 503 and starts no BMC work."""


class TargetRegistry:
    def __init__(
        self,
        *,
        max_targets: int,
        idle_ttl_seconds: float,
        max_parallel_targets: int,
        max_concurrent_refreshes: int,
        response_cache: Optional[ResponseCache] = None,
        # `research.md` §3: every newly-created Target's lane state must
        # reflect the process-wide configured profile Tuning, not
        # `FastLaneState`/`SlowLaneState`'s own hard-coded constructor
        # defaults. Defaults here match those constructors' own defaults
        # purely so every existing caller that doesn't pass these stays
        # correct without change.
        fast_ttl_seconds: float = 30.0,
        fast_deadline_seconds: float = 15.0,
        slow_ttl_seconds: float = 900.0,
        slow_deadline_seconds: float = 90.0,
        # `research.md` Group 2 (item 1): ONE process-wide in-flight-
        # response-byte budget, sized from `Tuning.IO.MaxInFlightResponseBytes`
        # — never a separate per-login, per-logout, or hard-coded budget.
        # Every cycle's login/GET/logout shares THIS single instance via
        # `TargetRefreshContext.io_byte_budget` (set at `ensure_cycle()`
        # time from `target_registry.io_byte_budget`).
        max_in_flight_response_bytes: int = 67108864,
        # `research.md` Group 5 (item 3): the process-wide sum of every
        # currently-held Fast/Slow lane snapshot's own `serialized_size`
        # (`Tuning.Cache.MaxTotalSnapshotBytes`) — a target's own
        # `Cache.MaxSnapshotBytesPerLane` bound (enforced at publish time
        # in `cycle_callbacks.py`) only ever caps ONE lane; this is the
        # separate, genuinely process-wide total across every target.
        max_total_snapshot_bytes: int = 104857600,
        # `Tuning.Cache.MaxInFlightCandidateBytes` — the process-wide budget
        # for a BUILT-BUT-NOT-YET-PUBLISHED lane candidate (`cycle_callbacks.
        # py`'s `new_lane_state` between `run_lane_job()` returning and the
        # publish/reject decision). Distinct from `max_total_snapshot_bytes`
        # (already-published snapshots) — see `try_reserve_candidate_bytes`.
        max_in_flight_candidate_bytes: int = 104857600,
    ) -> None:
        self.max_targets = max_targets
        self.idle_ttl_seconds = idle_ttl_seconds
        self.fast_ttl_seconds = fast_ttl_seconds
        self.fast_deadline_seconds = fast_deadline_seconds
        self.slow_ttl_seconds = slow_ttl_seconds
        self.slow_deadline_seconds = slow_deadline_seconds
        self.targets: dict[tuple[str, str], Target] = {}
        self.bmc_coordinators: dict[str, BmcCoordinator] = {}
        self.admission = AdmissionLimiter(max_parallel_targets=max_parallel_targets)
        self.refresh_admission = GlobalRefreshAdmission(max_concurrent_refreshes=max_concurrent_refreshes)
        self.response_cache = response_cache or ResponseCache(max_entries=500, max_total_bytes=52_428_800)
        self.io_byte_budget = InFlightByteBudget(capacity_bytes=max_in_flight_response_bytes)
        self.max_total_snapshot_bytes = max_total_snapshot_bytes
        self.total_snapshot_bytes_used = 0
        self.max_in_flight_candidate_bytes = max_in_flight_candidate_bytes
        self.in_flight_candidate_bytes_used = 0
        self.registry_lock = asyncio.Lock()

    def try_reserve_candidate_bytes(self, amount: int) -> bool:
        """`Tuning.Cache.MaxInFlightCandidateBytes`: reserves `amount` bytes
        for a lane candidate that has finished collection and is about to
        go through the publish/reject decision, but is not yet published —
        several concurrent target cycles' own in-flight candidates must
        never together exceed this configured total, independent of (and
        checked BEFORE) `max_total_snapshot_bytes`, which only ever tracks
        already-published state. Non-blocking, same reasoning as
        `try_reserve_snapshot_bytes_delta`: no `await` inside, so this is
        atomic under asyncio's single-threaded cooperative scheduling.
        `amount <= 0` always succeeds without reserving (mirrors that
        function's non-positive-delta shortcut)."""
        if amount <= 0:
            return True
        if self.in_flight_candidate_bytes_used + amount > self.max_in_flight_candidate_bytes:
            return False
        self.in_flight_candidate_bytes_used += amount
        return True

    def release_candidate_bytes(self, amount: int) -> None:
        """Releases a candidate reservation exactly once — the caller
        (`cycle_callbacks.py`) MUST call this on every exit path following a
        successful `try_reserve_candidate_bytes` (publish, reject, or an
        exception raised while deciding) via `try`/`finally`, never
        conditionally on success alone."""
        if amount:
            self.in_flight_candidate_bytes_used = max(0, self.in_flight_candidate_bytes_used - amount)

    def try_reserve_snapshot_bytes_delta(self, delta: int) -> bool:
        """Non-blocking: a lane publish never WAITS for space — it either
        fits under `max_total_snapshot_bytes` right now or is rejected
        outright (the caller keeps the prior lane state, per
        `cycle_callbacks.py`'s enforcement). `delta` is the NET change this
        publish would make to the process-wide total (new snapshot's
        `serialized_size` minus the lane's own previous one) — always
        applied when non-positive (a smaller replacement, or a lane being
        cleared, can never itself push the total over budget). Contains no
        `await`, so this is atomic under asyncio's single-threaded
        cooperative scheduling — no lock needed, same reasoning as every
        other lock-free synchronous state transition in this package."""
        if delta > 0 and self.total_snapshot_bytes_used + delta > self.max_total_snapshot_bytes:
            return False
        self.total_snapshot_bytes_used = max(0, self.total_snapshot_bytes_used + delta)
        return True

    def release_snapshot_bytes(self, amount: int) -> None:
        """Releases `amount` bytes back to the process-wide total —
        used when a target holding that many reserved bytes is evicted
        (`_remove_locked`), so eviction always releases every reservation
        it held, never leaking capacity."""
        if amount:
            self.total_snapshot_bytes_used = max(0, self.total_snapshot_bytes_used - amount)

    async def get_or_create(self, key: tuple[str, str]) -> Target:
        async with self.registry_lock:
            existing = self.targets.get(key)
            if existing is not None:
                return existing

            if len(self.targets) >= self.max_targets:
                evicted = self._evict_one_idle_locked()
                if not evicted:
                    raise RegistryFull(f"registry full at {self.max_targets} and no idle candidate to evict")

            target = Target(
                key=key,
                fast=FastLaneState(deadline_seconds=self.fast_deadline_seconds, ttl_seconds=self.fast_ttl_seconds),
                slow=SlowLaneState(deadline_seconds=self.slow_deadline_seconds, ttl_seconds=self.slow_ttl_seconds),
            )
            self.targets[key] = target
            return target

    def _evict_one_idle_locked(self) -> bool:
        candidates = [t for t in self.targets.values() if t.active_cycle_task is None]
        if not candidates:
            return False
        oldest = min(candidates, key=lambda t: t.last_activity)
        self._remove_locked(oldest.key)
        return True

    def _remove_locked(self, key: tuple[str, str]) -> None:
        removed = self.targets.pop(key, None)
        if removed is not None:
            # `research.md` Group 5 (item 3): release this target's own
            # held share of the process-wide snapshot-byte total — an
            # evicted target's Fast/Slow snapshots are dropped along with
            # it, and the total must reflect that immediately, not stay
            # inflated by capacity nothing still holds.
            self.release_snapshot_bytes(removed.fast.serialized_size + removed.slow.serialized_size)
        self.response_cache.invalidate(key)
        canonical_ip = key[0]
        coordinator = self.bmc_coordinators.get(canonical_ip)
        if coordinator is not None and coordinator.is_idle():
            # Only remove the coordinator once truly idle; other configs for
            # the same canonical IP may still be using it.
            still_used = any(k[0] == canonical_ip for k in self.targets)
            if not still_used:
                del self.bmc_coordinators[canonical_ip]

    def get_or_create_coordinator(
        self, canonical_ip: str, *, capacity: int, max_queued_per_lane: int, fast_batch_limit: int, max_concurrent_cycles: int
    ) -> BmcCoordinator:
        coordinator = self.bmc_coordinators.get(canonical_ip)
        if coordinator is None:
            coordinator = BmcCoordinator(
                capacity=capacity, max_queued_per_lane=max_queued_per_lane,
                fast_batch_limit=fast_batch_limit, max_concurrent_cycles=max_concurrent_cycles,
            )
            self.bmc_coordinators[canonical_ip] = coordinator
        return coordinator

    async def sweep_idle(self, *, now: Optional[float] = None) -> list[tuple[str, str]]:
        """Evicts every idle-with-no-open-cycle target past `idle_ttl_seconds`.
        Returns the evicted keys."""
        async with self.registry_lock:
            evicted: list[tuple[str, str]] = []
            for key, target in list(self.targets.items()):
                if target.is_idle(idle_ttl_seconds=self.idle_ttl_seconds, now=now):
                    self._remove_locked(key)
                    evicted.append(key)
            return evicted

    async def shutdown_drain(self, *, grace_seconds: float = 30.0) -> None:
        """Cancels every target's open cycle task and drains it, and its
        cycle's own shared cleanup task if one exists or gets created during
        cancellation, CONCURRENTLY within one shared `grace_seconds`
        wall-clock window (`Tuning.Shutdown.GraceSeconds` is one
        process-wide budget for the whole shutdown, not `grace_seconds` per
        target).

        Round 5A.3 (defect #1): `close_cycle()`/`_do_cleanup()` run cleanup
        in a SEPARATE task (`context.cleanup_task`), shielded via
        `asyncio.shield` so a caller's own cancellation of its `close_cycle`
        await can never accidentally cancel shared cleanup. That means
        cancelling a cycle's outer `active_cycle_task` while it is already
        suspended awaiting that shield only cancels the OUTER wait — the
        outer task becomes done almost immediately while `cleanup_task`
        keeps running independently. Tracking only `active_cycle_task` (the
        round 5A.2 version of this method) could therefore return believing
        a cycle was fully drained while its actual cleanup — session/
        connector close, logout, permit release, target-reference removal —
        was still genuinely in flight and entirely untracked. This version
        snapshots `(target, context, cycle_task)` together up front and
        treats a cycle as cooperatively drained only once BOTH its cycle
        task AND its context's `cleanup_task` (if any — created lazily by
        `close_cycle`, possibly only after cancellation lands) have reached
        a terminal state, re-polling for a newly-created `cleanup_task` on
        every iteration since it need not exist yet at snapshot time.

        Emergency abandonment for whatever remains non-terminal at the
        shared deadline always uses the exact snapshotted `(target,
        context)` pair — never `target.active_cycle` re-read late, which
        could by then already reference a newer, unrelated cycle."""
        async with self.registry_lock:
            snapshot = [
                (t, t.active_cycle, t.active_cycle_task)
                for t in self.targets.values()
                if t.active_cycle_task is not None
            ]
        if not snapshot:
            return

        # Round 9 (Group A item 2): the whole shutdown's own (typically
        # tighter) grace window is propagated into each context's cleanup
        # budget BEFORE cancellation lands, so `_do_cleanup`'s own bounded
        # waits already measure against whichever deadline is smaller —
        # not left to rely solely on this method's own separate emergency-
        # abandonment fallback below as the only enforcement.
        shutdown_deadline = time.monotonic() + grace_seconds
        for _target, _context, cycle_task in snapshot:
            if _context is not None:
                apply_shutdown_deadline(_context, shutdown_deadline)
            cycle_task.cancel()

        loop = asyncio.get_event_loop()
        deadline = loop.time() + grace_seconds

        def _owned_pending(context) -> set:
            """Round 9 (Group A item 3): `shutdown_drain` must observe every
            task a context still owns — not only its outer cycle task and
            `cleanup_task` — so it never declares a cycle drained while its
            lane/request tasks, per-request registrations, or finalizer
            helpers (e.g. `_commit_to_closing`'s/`_clear_target_reference`'s
            own locked-path tasks, or `emergency_abandon`'s bounded
            best-effort session/connector close) are still genuinely
            in flight. Re-read live on every poll since `finalizer_tasks` in
            particular can gain new entries lazily, during cleanup itself."""
            if context is None:
                return set()
            pending: set = set()
            for task in context.owned_tasks:
                if not task.done():
                    pending.add(task)
            for task in context.active_requests.values():
                if not task.done():
                    pending.add(task)
            for task in context.finalizer_tasks:
                if not task.done():
                    pending.add(task)
            return pending

        def _still_pending_tasks() -> set:
            pending: set = set()
            for _target, context, cycle_task in snapshot:
                if not cycle_task.done():
                    pending.add(cycle_task)
                cleanup_task = getattr(context, "cleanup_task", None) if context is not None else None
                if cleanup_task is not None and not cleanup_task.done():
                    pending.add(cleanup_task)
                pending |= _owned_pending(context)
            return pending

        # Deliberately NOT `asyncio.wait_for(asyncio.gather(*tasks), ...)`:
        # when its own timeout fires, `wait_for` cancels the wrapped
        # awaitable and then AWAITS that cancellation actually completing
        # before raising `TimeoutError` — if a child task swallows
        # cancellation and keeps running (exactly the non-cooperative case
        # this method must survive), that cleanup await never returns, and
        # `wait_for` hangs past `grace_seconds` instead of bounding it.
        # `asyncio.wait` has no such cleanup phase: it returns at the
        # deadline without touching pending tasks itself. Looping on
        # `FIRST_COMPLETED` (rather than one single `ALL_COMPLETED` wait)
        # lets this re-discover a `cleanup_task` created only AFTER the
        # initial cancellation lands, still within the SAME shared deadline.
        while True:
            remaining = deadline - loop.time()
            wait_set = _still_pending_tasks()
            if not wait_set or remaining <= 0:
                break
            await asyncio.wait(wait_set, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)

        def _retrieve_exception(task) -> None:
            if not task.done() or task.cancelled():
                return
            exc = task.exception()  # retrieve: never leave it unretrieved
            if exc is not None:
                # A genuinely unexpected failure in an otherwise-cooperative
                # cycle/cleanup task must not be silently invisible
                # (code-review finding, round 5A.3) — logged via the same
                # value-safe helper the rest of this codebase uses, never
                # the raw exception text; names no target/config/request.
                logging.warning("shutdown_drain: cycle task failed: %s", safe_exception_summary(exc))

        abandoned_count = 0
        for target, context, cycle_task in snapshot:
            cleanup_task = getattr(context, "cleanup_task", None) if context is not None else None
            fully_drained = (
                cycle_task.done()
                and (cleanup_task is None or cleanup_task.done())
                and not _owned_pending(context)
            )
            if fully_drained:
                _retrieve_exception(cycle_task)
                if cleanup_task is not None:
                    _retrieve_exception(cleanup_task)
                continue

            # Best-effort emergency mitigation: asyncio cannot force-kill a
            # coroutine that swallows CancelledError — the only recourse
            # within the async model is to keep re-issuing cancel() (some
            # tasks only honor it on a later await point) and to make the
            # abandonment observable rather than silent. Beyond that, clear
            # every piece of registry/dispatcher/permit ownership this
            # process can still safely determine is held for this cycle,
            # using the exact SNAPSHOTTED context/target — never a later,
            # possibly-rotated `target.active_cycle` — so neither the
            # registry nor the dispatcher is left permanently believing an
            # abandoned cycle is still active (the underlying Python
            # task(s) are NOT force-killed — not possible in asyncio — only
            # this process's own exporter-owned resource ownership is
            # reclaimed).
            abandoned_count += 1
            if not cycle_task.done():
                cycle_task.cancel()
            else:
                _retrieve_exception(cycle_task)
            if cleanup_task is not None and not cleanup_task.done():
                cleanup_task.cancel()
            if target is not None and context is not None:
                emergency_abandon(context, target)

        if abandoned_count:
            logging.warning(
                "shutdown_drain: %d cycle(s) did not complete cooperative cleanup within the %.1fs "
                "grace window and were re-cancelled but abandoned (non-cooperative cleanup code)",
                abandoned_count, grace_seconds,
            )
