"""`TargetRefreshContext` (`data-model.md`, `research.md` §1): the single
active refresh cycle for one `Target`. Owns the one `ClientSession`/
connector/local semaphore/token/logout_uri/dispatcher lease for this open
cycle — `Target` itself never holds any of these.

Accepting-cycle state machine (round 5A): a cycle is not simply "open" or
"closed" — while `RUNNING` it is either `accepting` new lane attachments or
it has committed to closing. `ensure_cycle()` may only attach a newly-due
lane to an existing cycle while `context.accepting` is True; that check and
the runner's own "no more work, stop accepting" decision are both made
inside `Target.cycle_lock`, so they can never race each other (no
lost-wakeup window).

Fast/Slow scheduling (redesigned per `research.md` §B): Fast and Slow run as
INDEPENDENT concurrent tasks (`context.fast_task`/`context.slow_task`) once
both are active — a newly-due Fast generation never cancels, discards, or
restarts an in-progress Slow attempt; only the absolute cycle deadline can
still cut either off mid-flight. The only ordering constraint is at cold
start: if Fast and Slow become due together and Slow has never started yet
this cycle, Slow waits for that Fast attempt to resolve; once Slow has
started even once, later Fast generations are fully independent of it. See
`_run_lane_scheduler`'s own docstring for the exact rules.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from aiohttp import ClientSession, ClientTimeout, TCPConnector

from .. import rawCollector
from ..security.containment import canonicalize_address
from .admission import GlobalRefreshAdmission
from .dispatcher import BmcCoordinator
from .request_execution import InFlightByteBudget, bounded_fetch
from .target_state import Target

# Process-wide byte budget for best-effort logout DELETE calls only —
# logout responses are always small (empty/204 or a tiny status body), so
# one small shared reservation pool is sufficient; it is never shared with
# the much larger GET/login budget.
_LOGOUT_BYTE_BUDGET = InFlightByteBudget(capacity_bytes=8_388_608)

logger = logging.getLogger(__name__)

# Referenced via the `rawCollector` module (not imported directly) so that
# monkeypatching `rawCollector.ClientSession`/`rawCollector.TCPConnector` —
# the one mechanism every fixture-driven test in this repository already
# uses — transparently intercepts session/connector creation here too, for
# both the legacy path and this redesigned refresh-cycle path.


class Lane(str, Enum):
    FAST = "FAST"
    SLOW = "SLOW"


class CycleState(str, Enum):
    OPENING = "OPENING"
    RUNNING = "RUNNING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    # Round 5A.2: a distinct terminal state for a cycle whose task never
    # cooperatively finished within `shutdown_drain`'s shared grace window
    # and was forcibly reclaimed by `emergency_abandon()`. Deliberately NOT
    # `CLOSED` — that value means this cycle's own normal `_do_cleanup` ran
    # to completion; an abandoned cycle's underlying coroutine may still be
    # running (asyncio cannot force-kill it), so describing it as `CLOSED`
    # would misrepresent an abandoned coroutine as successfully completed.
    ABANDONED = "ABANDONED"


# Cleanup (logout, session/connector close, task drain) must not be able to
# make the complete cycle run meaningfully past its absolute deadline, but
# zero tolerance would make cleanup un-attemptable the instant the deadline
# is reached (a scheduler cannot execute a 0.0s-budget coroutine at all) —
# this is the small, deterministic allowance research.md accepts for cleanup.
_CLEANUP_TOLERANCE_SECONDS = 0.25

# Round 5A.3: there is deliberately no fixed/fresh timeout constant here
# (the round 5A.2 `_TARGET_LOCK_ACQUIRE_TIMEOUT_SECONDS = 2.0` was exactly
# that — an unrelated fresh budget granted to one finalizer step regardless
# of how much of the cycle's own absolute deadline actually remained).
# Every bounded wait inside this module's cleanup path now derives its
# timeout from `_remaining_cleanup_budget()` below: the cycle's own
# remaining deadline (which may already be zero or negative) plus the same
# small, fixed `_CLEANUP_TOLERANCE_SECONDS` allowance every other cleanup
# step already uses. The one process-wide OUTER bound on how long shutdown
# as a whole may wait for any of this to finish is `TargetRegistry.
# shutdown_drain`'s own `grace_seconds` window (`registry.py`) — this
# module's own per-step budgets only ever need to be "reasonable", never a
# second independent absolute ceiling, since shutdown_drain's emergency
# abandonment already provides that ceiling.


def _remaining_cleanup_budget(context: "TargetRefreshContext") -> float:
    """The bound every cleanup wait in this module derives its own timeout
    from — never a fresh, cleanup-step-specific constant. The `+
    _CLEANUP_TOLERANCE_SECONDS` allowance is applied to the cycle's own
    absolute deadline EXACTLY ONCE, memoized as `context._cleanup_deadline`
    on the first call from anywhere in this module (request drain, logout,
    session/connector close, or a finalizer) — every subsequent call, from
    any stage, measures its remaining time against that SAME fixed point.
    Without this, each of the ~6 independent call sites recomputed
    `max(remaining_seconds(), 0.0) + _CLEANUP_TOLERANCE_SECONDS` fresh,
    silently re-granting a brand-new 0.25s allowance to every later stage
    even after the deadline (plus tolerance) was already exhausted by an
    earlier one — letting a multi-stage cleanup run up to
    `stage_count * _CLEANUP_TOLERANCE_SECONDS` past its true budget in the
    worst case."""
    if context._cleanup_deadline is None:
        context._cleanup_deadline = context.cycle_deadline + _CLEANUP_TOLERANCE_SECONDS
    return max(context._cleanup_deadline - time.monotonic(), 0.0)


def apply_shutdown_deadline(context: "TargetRefreshContext", shutdown_deadline_monotonic: float) -> None:
    """`research.md` Group A item 2: propagate the SMALLER of the cycle's
    own cleanup deadline and process-wide shutdown's own (typically
    tighter) deadline through every cleanup stage — called by
    `registry.shutdown_drain()` for every context it is about to cancel,
    before cancellation lands, so this context's own `_do_cleanup` (however
    it gets triggered) already measures its remaining budget against
    whichever deadline is tighter, rather than only being stopped by
    `shutdown_drain`'s own separate emergency-abandonment fallback after
    the fact. Both values are on the same `time.monotonic()` clock."""
    current = context._cleanup_deadline
    if current is None:
        current = context.cycle_deadline + _CLEANUP_TOLERANCE_SECONDS
    context._cleanup_deadline = min(current, shutdown_deadline_monotonic)


def _track_finalizer_task(context: "TargetRefreshContext", task: "asyncio.Task[Any]") -> "asyncio.Task[Any]":
    """Every helper task this module fires off for cleanup/emergency-
    abandonment finalization (the lock-acquire race in
    `_clear_target_reference`/`_commit_to_closing`, the detached best-effort
    session/connector close in `emergency_abandon`) must be owned by an
    explicit, inspectable, bounded set — never a bare discarded
    `asyncio.ensure_future(...)` call, which a raising helper would make
    asyncio log as an "exception was never retrieved" warning at GC time,
    and which a test would otherwise have no way to assert is empty once
    cleanup genuinely finishes (round 5A.3, defect C).

    Round 9 (Group A item 4): the done-callback below closes ONLY over
    `finalizer_tasks` (the set object itself) and `cycle_id` (a stable,
    already-opaque string) — never over `context` itself. `context` also
    holds the live `session`/`connector`/`token`/`logout_uri` and every
    other cycle-owned field; a callback registered on a task can live on
    the event loop's internal callback list for as long as that task object
    itself is reachable (e.g. another finalizer task still awaiting it, or
    a test holding a reference for assertions), so closing over the whole
    context would keep all of that reachable for exactly as long, even
    once this context has otherwise fully torn down. Extracting only the
    two fields this callback actually needs bounds that retention to the
    minimal state required."""
    finalizer_tasks = context.finalizer_tasks
    cycle_id = context.cycle_id
    finalizer_tasks.add(task)

    def _on_done(finished: "asyncio.Task[Any]") -> None:
        finalizer_tasks.discard(finished)
        if finished.cancelled():
            return
        exc = finished.exception()  # retrieve: never leave it unretrieved
        if exc is not None:
            # A genuinely unexpected finalizer failure must not be silently
            # invisible (code-review finding, round 5A.3) — logged with only
            # a stable event name, the opaque `cycle_id`, and the
            # exception's CLASS name. `research.md` Group 1 requires this
            # path be stricter than most other call sites' `safe_exception_
            # summary` usage: that helper only strips KNOWN credential-
            # shaped patterns from the message text and explicitly does not
            # guarantee catching everything an underlying library's own
            # exception message might embed (a bare hostname, a URL, a
            # request ID) — for this specific hard-to-review finalizer/
            # helper path, never include ANY message text at all, not even
            # redacted.
            logger.warning(
                "cycle %s: finalizer task failed: %s", cycle_id, type(exc).__name__,
            )

    task.add_done_callback(_on_done)
    return task

LoginFunc = Callable[["TargetRefreshContext"], Awaitable[None]]
LogoutFunc = Callable[["TargetRefreshContext"], Awaitable[None]]
LaneJobFunc = Callable[["TargetRefreshContext", Lane], Awaitable[None]]


class FastFailedBeforeSlowError(Exception):
    """A cold Fast terminal failure closed the cycle before Slow ever ran —
    the reason recorded on Slow's lane future so no follower hangs waiting
    for a lane that was never going to start."""


@dataclass
class TargetRefreshContext:
    target_key: tuple[str, str]
    coordinator: BmcCoordinator
    cycle_deadline_seconds: float
    target_concurrency: int
    logout_timeout_seconds: float = 60.0
    # `research.md` Group 2 (items 1/3): the ONE shared, process-wide
    # in-flight-response-byte budget (`Tuning.IO.MaxInFlightResponseBytes`,
    # `TargetRegistry.io_byte_budget`) and the configured per-request
    # timeout (`Tuning.Request.TimeoutSeconds`)/response-byte cap
    # (`Tuning.MaxResponseBytes`) this cycle's login, GET, and logout must
    # all share — never a fresh per-call budget or a hard-coded constant.
    io_byte_budget: Optional[InFlightByteBudget] = None
    max_response_bytes: int = 10485760
    request_timeout_seconds: float = 30.0
    cycle_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session: Optional[ClientSession] = None
    connector: Optional[TCPConnector] = None
    local_semaphore: Optional[asyncio.Semaphore] = None
    token: Optional[str] = None
    logout_uri: Optional[str] = None
    # --- Shared bootstrap/model-selection (research.md §C) ---
    # Bootstrap discovery (Service Root + Systems collection + selected
    # System GET) and model selection happen at most ONCE per authenticated
    # cycle. `bootstrap_lock` guards the one-time computation (Fast and a
    # later-attached Slow could otherwise race to bootstrap independently);
    # every lane call after the first reuses `bootstrap_ctx`/
    # `selected_schema` instead of repeating Service Root/Systems-
    # collection/System discovery. `bootstrap_system_obj` is the raw System
    # body already fetched during discovery — consumed at most once (by the
    # very first Fast collection, via `bootstrap_system_reused`) so Common
    # is never fetched twice for a cold cycle; every later Fast generation
    # must genuinely refetch the System URI for freshness.
    bootstrap_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    bootstrap_ctx: Optional[dict[str, Any]] = None
    selected_schema: Any = None
    bootstrap_system_obj: Any = None
    bootstrap_system_reused: bool = False
    # `time.monotonic()`, NOT `time.time()`: `cycle_deadline` below is
    # compared against `time.monotonic()` everywhere it's consumed
    # (`remaining_seconds()`, `request_execution.py`'s deadline checks) —
    # an epoch `opened_at` produced a deadline on a completely different
    # clock/epoch than what it was compared against, making the "absolute"
    # cycle deadline effectively meaningless (monotonic() is typically far
    # smaller than a Unix epoch value, so `remaining_seconds()` returned a
    # huge, never-expiring number).
    opened_at: float = field(default_factory=time.monotonic)
    cycle_deadline: float = 0.0
    cycle_state: CycleState = CycleState.OPENING
    refresh_admission: Optional[GlobalRefreshAdmission] = None

    # --- Accepting-cycle / dynamic lane state (round 5A) ---
    # `accepting`: True while `ensure_cycle()` may still attach a newly-due
    # lane to this cycle. Flipped to False ONLY under `Target.cycle_lock`,
    # atomically with the runner's own "no more work" check, so a lane
    # attachment can never be lost to a concurrently-closing runner.
    accepting: bool = True
    # Lanes requested but not yet started by the runner's current pass.
    requested_lanes: set[Lane] = field(default_factory=set)
    # Every lane ever accepted into this cycle (used to validate a follower
    # is joining a future this cycle actually committed to running — the
    # mere existence of a pre-created future is never sufficient proof).
    accepted_lanes: set[Lane] = field(default_factory=set)
    # Fast's and Slow's current attempt, when running — Fast and Slow run
    # as INDEPENDENT concurrent tasks once both are active; a later Fast
    # generation never cancels or restarts Slow (`research.md` §B).
    # `.done()` on either doubles as that lane's idle/busy signal for the
    # scheduler's re-trigger decision (`_run_lane_scheduler`).
    fast_task: Optional["asyncio.Task[bool]"] = None
    slow_task: Optional["asyncio.Task[bool]"] = None
    # Set once a Fast attempt fails while Slow has never started this cycle
    # (a genuinely cold terminal failure) — permanently prevents Slow from
    # starting in THIS cycle, per `research.md` §B. Never set once Slow has
    # already started (a later Fast failure must not retroactively affect
    # an already-running/-completed Slow job).
    cold_fast_failed: bool = False
    # Every lane-attempt task this cycle currently owns — a set of the task
    # objects themselves (tasks are hashable, so no separate key is needed)
    # — cancelled and awaited
    # (concurrently, within the remaining monotonic budget) on close.
    # Individual HTTP fetches within a lane are not tracked separately here:
    # cancelling the owning lane task already cascades cancellation to
    # whatever fetch is in flight (the dispatcher lease's `async with`
    # `__aexit__` still runs during that unwind, so its capacity is still
    # released) — tracking every fetch as its own task would duplicate that
    # guarantee without changing the actual cancellation semantics, so this
    # cycle's task-ownership granularity is the lane attempt, not the
    # individual request.
    owned_tasks: set["asyncio.Task[Any]"] = field(default_factory=set)
    # Signaled by `ensure_cycle()` whenever a new Fast attachment might need
    # to preempt an in-progress Slow attempt; the runner races this against
    # the current Slow task so it can react without polling.
    wake_event: asyncio.Event = field(default_factory=asyncio.Event)
    lane_futures: dict[Lane, "asyncio.Future[None]"] = field(default_factory=dict)

    # --- Cycle-owned per-request task accounting (round 5A.1) ---
    # True while new bounded-fetch request work may still register itself
    # with this cycle. Flipped to False ONLY under `Target.cycle_lock`, at
    # the very start of cleanup, atomically with the snapshot of tasks
    # cleanup is about to cancel/drain — this closes the same kind of race
    # `accepting`/lane-attachment closes: a fetch that loses this race is
    # cancelled before it ever runs a real request, rather than being
    # silently left unregistered and outside cleanup's snapshot.
    accepting_requests: bool = True
    # Every currently in-flight redesigned bounded-fetch request this cycle
    # owns, keyed by its own unique `request_id` (never the URL — two
    # concurrent fetches of the same URL within one cycle must occupy two
    # distinct accounting slots, exactly like the dispatcher's own
    # `_admitted` set). Populated/removed only via
    # `run_as_cycle_owned_request()` below; inspectable for tests without
    # ever needing to log a URL, target, token, or credential.
    active_requests: dict[str, "asyncio.Task[Any]"] = field(default_factory=dict)

    # --- Cancellation-safe, idempotent cleanup (round 5A) ---
    # The single shared cleanup coroutine, wrapped in one Task the first
    # caller creates; every caller (including a second, concurrent close
    # request) joins the SAME task via `asyncio.shield` so cleanup is never
    # accidentally cancelled by a caller's own cancellation, and so cleanup
    # genuinely runs exactly once regardless of how many exit paths reach
    # `close_cycle()`.
    cleanup_task: Optional["asyncio.Task[None]"] = None
    _permits_released: bool = False
    # Round 9 (Group A item 2): ONE absolute deadline for the ENTIRE cleanup
    # sequence, computed once on first use (`_remaining_cleanup_budget()`)
    # and reused by every later stage — never a fresh `+_CLEANUP_TOLERANCE_
    # SECONDS` allowance re-granted per stage, which could let a
    # multi-stage cleanup run tolerance-seconds-times-stage-count past the
    # cycle's own deadline in the worst case.
    _cleanup_deadline: Optional[float] = None

    # --- Deterministic cleanup-phase observability (round 5A.2) ---
    # Set immediately before `_do_cleanup` starts the corresponding step —
    # lets a test await a genuine phase transition (`await
    # ctx.cleanup_entered_logout.wait()`) instead of a `sleep(0)`-loop guess
    # at how many scheduler turns cleanup needs to reach that point, which a
    # prior round's tests relied on and an independent audit flagged as
    # non-deterministic.
    cleanup_entered_logout: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_entered_session_close: asyncio.Event = field(default_factory=asyncio.Event)

    # --- Tracked cleanup/emergency-abandonment helper tasks (round 5A.3) ---
    # Every helper task `_track_finalizer_task()` fires off on this context's
    # behalf — never a bare discarded `ensure_future()`. Empty once every
    # such helper has completed and had its exception (if any) retrieved;
    # inspectable by tests without needing a private asyncio API.
    finalizer_tasks: set["asyncio.Task[Any]"] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.cycle_deadline = self.opened_at + self.cycle_deadline_seconds

    def remaining_seconds(self) -> float:
        return self.cycle_deadline - time.monotonic()

    def is_lane_accepted(self, lane: Lane) -> bool:
        """A follower may only `join_lane()` a lane this cycle actually
        committed to running — never merely because a future object exists
        for every `Lane` enum value."""
        return lane in self.accepted_lanes

    def is_live(self) -> bool:
        """True only while this cycle may still validly gain new
        state — `OPENING` (login in flight) or `RUNNING`. Once `CLOSING`,
        `CLOSED`, or `ABANDONED`, a cancellation-resistant login or lane job
        that finishes late must never be allowed to resurrect token/logout
        state or publish lane work onto this (or, since `Target.fast`/
        `Target.slow` are shared mutable fields, a NEWER cycle's) state —
        `research.md` Group 1: "an `ABANDONED`, `CLOSING`, or `CLOSED`
        context cannot regain token/logout state or start lane work." A
        plain state check with no `await` in between it and the caller's
        subsequent write is atomic under asyncio's single-threaded
        cooperative scheduling — no lock is needed.

        Deadline-aware (round 9): also False once this cycle's own absolute
        deadline has passed, even if `cycle_state` technically still reads
        `RUNNING` — `_run_cycle`'s own `asyncio.wait_for(...)` cancellation
        of a cancellation-resistant login/scheduler is not instantaneous;
        a late-completing write must not slip through during that narrow
        window just because the state transition to `CLOSING`/`CLOSED`
        hasn't landed yet."""
        return self.cycle_state in (CycleState.OPENING, CycleState.RUNNING) and self.remaining_seconds() > 0

    def try_set_login_result(self, token: Optional[str], logout_uri: Optional[str]) -> bool:
        """Sets `token`/`logout_uri` only while `is_live()` — returns
        whether the write happened. A cancellation-resistant login that
        completes after this cycle has already committed to closing must
        never resurrect credential/session state on it."""
        if not self.is_live():
            return False
        self.token = token
        self.logout_uri = logout_uri
        return True


def _new_lane_future() -> "asyncio.Future[None]":
    """A lane future may end up failed (e.g. `_fail_all_outstanding_lanes`)
    with NOTHING ever calling `join_lane()` to retrieve it — the router only
    joins `Lane.FAST`, never `Lane.SLOW` — which would otherwise make
    asyncio log a real "exception was never retrieved" warning at garbage
    collection time for every such Slow failure. The done-callback below
    marks the exception retrieved unconditionally; a genuine caller that DID
    `await join_lane(...)` still sees the real exception raised normally —
    `add_done_callback` never suppresses that, it only prevents the
    otherwise-inevitable GC-time warning for the (very common) case where no
    one ever joins this particular lane."""
    future: "asyncio.Future[None]" = asyncio.get_event_loop().create_future()
    future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
    return future


async def ensure_cycle(
    target: Target,
    *,
    coordinator: BmcCoordinator,
    cycle_deadline_seconds: float,
    target_concurrency: int,
    lanes_due: set[Lane],
    login: LoginFunc,
    run_lane: LaneJobFunc,
    on_close: Callable[[TargetRefreshContext], Awaitable[None]],
    refresh_admission: Optional[GlobalRefreshAdmission] = None,
    logout_timeout_seconds: float = 60.0,
    io_byte_budget: Optional[InFlightByteBudget] = None,
    max_response_bytes: int = 10485760,
    request_timeout_seconds: float = 30.0,
) -> tuple[Optional[TargetRefreshContext], Optional["asyncio.Task[None]"], frozenset]:
    """Attaches every lane in `lanes_due` to an accepting existing cycle, or
    atomically creates and publishes a new one, or — if a cycle exists but
    is no longer accepting (already committed to closing) — attaches
    nothing and returns the existing context/task with an EMPTY accepted set
    so the caller falls back to whatever snapshot is already usable /
    Query=0, rather than opening a second overlapping cycle for the same
    target while the first is still tearing down.

    Returns `(context, task, accepted_lanes)`: `accepted_lanes` is the exact
    subset of `lanes_due` this call actually got committed to running (or
    already running/queued) THIS cycle — the caller must only `join_lane()`
    lanes in this set (FR-016/SC-003: never wait on a lane that was never
    scheduled)."""
    async with target.cycle_lock:
        existing = target.active_cycle
        if existing is not None and target.active_cycle_task is not None:
            if not existing.accepting:
                # Closing already committed under this same lock — do not
                # attach, do not open a second overlapping cycle. The
                # caller uses whatever snapshot is already usable.
                return existing, target.active_cycle_task, frozenset()

            def _lane_idle(lane: Lane) -> bool:
                """True when `lane` has no queued request and no task
                currently running — i.e. its existing future already
                reflects a terminal (resolved) result, so a fresh due
                request for it is a genuinely NEW generation, not a joiner
                of in-flight work. Re-accepting an idle lane replaces its
                future (below) rather than being folded into
                `already_accepted`, so a follower calling `join_lane()`
                waits for THIS generation instead of replaying a stale
                already-resolved result (`research.md` §B: multiple Fast
                generations may occur within one Slow-spanning cycle)."""
                if lane in existing.requested_lanes:
                    return False
                running_task = existing.fast_task if lane == Lane.FAST else existing.slow_task
                return running_task is None or running_task.done()

            newly_due = {
                lane for lane in lanes_due
                if lane not in existing.accepted_lanes or _lane_idle(lane)
            }
            if newly_due:
                existing.accepted_lanes |= newly_due
                existing.requested_lanes |= newly_due
                for lane in newly_due:
                    existing.lane_futures[lane] = _new_lane_future()
                existing.wake_event.set()
            already_accepted = lanes_due & existing.accepted_lanes
            return existing, target.active_cycle_task, frozenset(newly_due | already_accepted)

        if refresh_admission is not None:
            if not refresh_admission.try_acquire():
                raise GlobalRefreshAdmissionDeniedForTarget(target.key)

        if not coordinator.try_acquire_cycle_lease():
            if refresh_admission is not None:
                refresh_admission.release()
            raise CycleLeaseDeniedForTarget(target.key)

        context = TargetRefreshContext(
            target_key=target.key, coordinator=coordinator,
            cycle_deadline_seconds=cycle_deadline_seconds, target_concurrency=target_concurrency,
            refresh_admission=refresh_admission, logout_timeout_seconds=logout_timeout_seconds,
            io_byte_budget=io_byte_budget, max_response_bytes=max_response_bytes,
            request_timeout_seconds=request_timeout_seconds,
        )
        context.accepted_lanes = set(lanes_due)
        context.requested_lanes = set(lanes_due)
        for lane in lanes_due:
            context.lane_futures[lane] = _new_lane_future()
        task = asyncio.create_task(_run_cycle(context, target, login=login, run_lane=run_lane, on_close=on_close))
        task.add_done_callback(lambda finished: _finalize_if_never_started(context, target, finished))
        target.active_cycle = context
        target.active_cycle_task = task
        return context, task, frozenset(lanes_due)


def _finalize_if_never_started(context: TargetRefreshContext, target: Target, task: "asyncio.Task[None]") -> None:
    """Safety net for `research.md` Group 1: "shutdown_drain() must own/
    drain cycle... tasks, including cancellation before `_run_cycle()`
    starts." If `task` is cancelled before the event loop ever gives its
    coroutine a single resumption, `_run_cycle`'s own `try/finally` never
    executes AT ALL — a coroutine thrown `CancelledError` before its first
    `send()` runs none of its body, `finally` included (well-documented
    CPython coroutine/generator behavior). Without this, `target.
    active_cycle`/`active_cycle_task` would stay pointing at this
    permanently-dead context/task forever, and the held cycle lease/
    refresh-admission permit would never be released — a silent permanent
    hang for every future scrape of this target.

    `context.cycle_state` reliably distinguishes the two cases: if
    `_run_cycle`'s `try:` block was entered even once, its `finally`
    unconditionally runs `close_cycle()`, which always moves `cycle_state`
    away from its `OPENING` default (to `CLOSED` via normal cleanup, or
    `ABANDONED` via emergency reclamation) before this done-callback can
    ever observe it — a plain callback (task done-callbacks cannot
    `await`), safe without `Target.cycle_lock` because it never runs
    interleaved with another coroutine's own non-awaiting critical section
    (asyncio's single-threaded cooperative scheduling)."""
    if context.cycle_state != CycleState.OPENING:
        return  # `_run_cycle` genuinely started; its own try/finally already handled real cleanup.

    if target.active_cycle is context:
        target.active_cycle = None
        target.active_cycle_task = None
    # Routed through the same idempotent helper every other release path
    # uses (`_do_cleanup`'s `finally`, `emergency_abandon`) — code review
    # finding: a direct release here left `context._permits_released`
    # unset, so a LATER `emergency_abandon()` call on this same
    # already-finalized context (not reachable via today's only call site,
    # `shutdown_drain`, but not structurally prevented either) would
    # silently double-release the cycle lease and refresh-admission permit.
    _release_permits_once(context)
    context.cycle_state = CycleState.ABANDONED if task.cancelled() else CycleState.CLOSED
    _fail_all_outstanding_lanes(context, asyncio.CancelledError("cycle task never started"))


class CycleLeaseDeniedForTarget(Exception):
    pass


class GlobalRefreshAdmissionDeniedForTarget(Exception):
    pass


async def _run_cycle(
    context: TargetRefreshContext, target: Target, *, login: LoginFunc, run_lane: LaneJobFunc,
    on_close: Callable[[TargetRefreshContext], Awaitable[None]],
) -> None:
    try:
        context.connector = rawCollector.TCPConnector(
            limit=context.target_concurrency, limit_per_host=context.target_concurrency, ssl=False
        )
        context.session = rawCollector.ClientSession(
            connector=context.connector, timeout=ClientTimeout(total=context.cycle_deadline_seconds),
            # `research.md` Group 2 (item 5): disable aiohttp's own implicit,
            # unbounded auto-decompression — `same_target._read_json_body_
            # bounded` performs bounded, incremental decompression itself
            # (independent wire/decoded byte limits, allow-listed encodings
            # only), which requires `response.content` to yield the raw,
            # still-compressed wire bytes rather than already-decompressed
            # ones.
            auto_decompress=False,
        )
        context.local_semaphore = asyncio.Semaphore(context.target_concurrency)
        context.cycle_state = CycleState.RUNNING

        remaining = context.remaining_seconds()
        try:
            await asyncio.wait_for(login(context), timeout=max(remaining, 0.001))
        except asyncio.TimeoutError:
            _fail_all_outstanding_lanes(context, TimeoutError("cycle deadline exceeded during login"))
            return
        except Exception as exc:  # noqa: BLE001 - login failure closes the cycle; every accepted lane fails
            _fail_all_outstanding_lanes(context, exc)
            return

        # Post-login scheduler fence (round 9): `login()` returning WITHOUT
        # raising does not by itself prove the cycle is still within its own
        # deadline — a cancellation-resistant login that ignores/outlives
        # `wait_for`'s cancellation attempt simply keeps `wait_for` blocked
        # until it genuinely finishes, however late, then returns normally
        # with no `TimeoutError` at all. Never start the lane scheduler on a
        # cycle that has already run out its own budget.
        if not context.is_live():
            _fail_all_outstanding_lanes(context, TimeoutError("cycle deadline exceeded during login"))
            return

        remaining = context.remaining_seconds()
        try:
            await asyncio.wait_for(_run_lane_scheduler(context, target, run_lane), timeout=max(remaining, 0.001))
        except asyncio.TimeoutError:
            _fail_all_outstanding_lanes(context, TimeoutError("cycle deadline exceeded"))
    finally:
        await close_cycle(context, target, on_close=on_close)


def _fail_all_outstanding_lanes(context: TargetRefreshContext, exc: BaseException) -> None:
    """Every accepted lane future that hasn't reached a terminal result yet
    is failed now — reached from login failure and cycle-deadline exhaustion
    alike, so no follower can hang waiting on a lane this cycle will never
    run."""
    for lane, future in context.lane_futures.items():
        if lane in context.accepted_lanes and not future.done():
            future.set_exception(exc)


async def _run_lane_scheduler(context: TargetRefreshContext, target: Target, run_lane: LaneJobFunc) -> None:
    """The accepting-cycle runner loop: starts newly-requested lanes as
    their own concurrent tasks, and stops accepting new work (atomically,
    under `Target.cycle_lock`) only once nothing remains requested or
    running.

    Fast and Slow run CONCURRENTLY once both are active — a newly-due Fast
    generation NEVER cancels or restarts an in-progress Slow attempt
    (`research.md` §B, controlling decision #9): Slow keeps its progress
    across every later Fast generation within the same authenticated cycle.
    The only ordering constraint is at COLD start: if Fast and Slow become
    due together and Slow has never yet started in this cycle, Slow waits
    for that specific Fast attempt to resolve (`gate_slow_for_first_run`
    below) — bootstrap/model selection, which Slow also depends on, is
    genuinely settled by Fast's first attempt (`schema/pipeline.py`'s
    `ensure_bootstrap`). Once Slow has started even once, or Fast wasn't
    requested alongside it, no further gating applies: a later Fast
    generation's own success or failure never affects an already-running or
    already-completed Slow job. A cold Fast terminal failure (no prior
    success, ever) permanently prevents Slow from starting in THIS cycle
    (`context.cold_fast_failed`) — Slow's own request is failed immediately
    with `FastFailedBeforeSlowError` rather than left to time out."""
    while True:
        async with target.cycle_lock:
            fast_idle = context.fast_task is None or context.fast_task.done()
            slow_idle = context.slow_task is None or context.slow_task.done()

            fast_pending = Lane.FAST in context.requested_lanes and fast_idle
            if fast_pending:
                context.requested_lanes.discard(Lane.FAST)

            if context.cold_fast_failed and Lane.SLOW in context.requested_lanes:
                context.requested_lanes.discard(Lane.SLOW)
                _fail_lane_future(context, Lane.SLOW, FastFailedBeforeSlowError())

            slow_requested_now = Lane.SLOW in context.requested_lanes and slow_idle
            slow_never_started = context.slow_task is None
            gate_slow_for_first_run = (
                slow_requested_now and slow_never_started and (fast_pending or not fast_idle)
            )
            slow_pending = slow_requested_now and not gate_slow_for_first_run
            if slow_pending:
                context.requested_lanes.discard(Lane.SLOW)

            if (
                not fast_pending and not slow_pending
                and fast_idle and slow_idle
                and not context.requested_lanes
            ):
                # Atomic with any concurrent `ensure_cycle()` attachment
                # under the SAME lock — closes the lost-wakeup window where
                # a lane could be attached in the gap between "nothing to
                # do" and "stop accepting".
                context.accepting = False
                return

        # FRAGILE INVARIANT: everything from the `async with target.cycle_lock`
        # block above down through these two assignments must contain NO
        # `await` — `ensure_cycle()` only ever reads `fast_task`/`slow_task`
        # under that same lock, so a plain (non-awaiting) attribute write here
        # is atomic with respect to it purely because asyncio's single-
        # threaded cooperative scheduling never interleaves two coroutines
        # except at an actual await point. Inserting an `await` anywhere in
        # this stretch (e.g. a logging call that awaits, or a new admission
        # check) would reopen a window where `ensure_cycle()` could observe
        # a lane as still-idle and double-start it.
        if fast_pending and _lane_start_deadline_permits(context):
            context.fast_task = asyncio.ensure_future(_run_one_lane(context, target, Lane.FAST, run_lane))
            context.owned_tasks.add(context.fast_task)
        elif fast_pending:
            _fail_lane_future(context, Lane.FAST, TimeoutError("insufficient remaining cycle deadline to start Fast"))

        if slow_pending and _lane_start_deadline_permits(context):
            context.slow_task = asyncio.ensure_future(_run_one_lane(context, target, Lane.SLOW, run_lane))
            context.owned_tasks.add(context.slow_task)
        elif slow_pending:
            _fail_lane_future(context, Lane.SLOW, TimeoutError("insufficient remaining cycle deadline to start Slow"))

        wake_wait = asyncio.ensure_future(context.wake_event.wait())
        wait_set: set[Any] = {wake_wait}
        if context.fast_task is not None and not context.fast_task.done():
            wait_set.add(context.fast_task)
        if context.slow_task is not None and not context.slow_task.done():
            wait_set.add(context.slow_task)

        await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)

        if not wake_wait.done():
            wake_wait.cancel()
            try:
                await wake_wait
            except asyncio.CancelledError:
                pass
        else:
            context.wake_event.clear()

        if context.fast_task is not None and context.fast_task.done():
            context.owned_tasks.discard(context.fast_task)
            if not context.fast_task.cancelled() and not context.fast_task.result() and context.slow_task is None:
                # This Fast attempt failed and Slow has never started this
                # cycle — a genuinely cold terminal failure. Any Slow
                # request queued now or later in this cycle must never
                # start (`research.md` §B: "Cold Fast terminal failure does
                # not begin new Slow work").
                context.cold_fast_failed = True

        if context.slow_task is not None and context.slow_task.done():
            context.owned_tasks.discard(context.slow_task)


def _lane_start_deadline_permits(context: TargetRefreshContext) -> bool:
    return context.remaining_seconds() > _CLEANUP_TOLERANCE_SECONDS


def _fail_lane_future(context: TargetRefreshContext, lane: Lane, exc: BaseException) -> None:
    future = context.lane_futures.get(lane)
    if future is not None and not future.done():
        future.set_exception(exc)


async def _run_one_lane(context: TargetRefreshContext, target: Target, lane: Lane, run_lane: LaneJobFunc) -> bool:
    """Returns `True` on success, `False` on a recorded lane failure — the
    caller uses this to decide whether Slow may start after Fast, since a
    lane failure is recorded on the lane's future (never re-raised, so
    callers awaiting it don't see the exception unless they explicitly
    check) rather than propagated as a raised exception here."""
    future = context.lane_futures.get(lane)
    try:
        await run_lane(context, lane)
        if future is not None and not future.done():
            future.set_result(None)
        return True
    except asyncio.CancelledError:
        # A preempted Slow attempt: its future is deliberately left
        # untouched here (not cancelled, not resolved) — it represents the
        # RESTARTED attempt's eventual result, not this discarded one.
        raise
    except Exception as exc:  # noqa: BLE001 - lane failures are recorded, not propagated to the cycle
        if future is not None and not future.done():
            future.set_exception(exc)
        return False


async def join_lane(context: TargetRefreshContext, lane: Lane, *, deadline_seconds: float) -> None:
    """A follower waits on the shared lane future within ITS OWN deadline;
    it never cancels the shared task on its own timeout (FR-016). Only
    valid for a lane this cycle actually accepted — callers must check
    `context.is_lane_accepted(lane)` (or use the `accepted_lanes` returned
    by `ensure_cycle()`) before calling this; a lane that was never accepted
    has no meaningful future to wait on."""
    future = context.lane_futures.get(lane)
    if future is None:
        raise LaneNotAcceptedError(lane)
    await asyncio.wait_for(asyncio.shield(future), timeout=deadline_seconds)


class LaneNotAcceptedError(Exception):
    pass


class RequestRegistrationClosedError(Exception):
    """The cycle has already committed to closing (`accepting_requests` is
    False) — no new bounded-fetch request may register with it. The caller
    must not retry on this cycle; the fetch attempt is over."""


class DuplicateRequestRegistrationError(Exception):
    """`request_id` is already registered with this cycle — always a caller
    bug (every logical fetch call must mint its own unique `request_id`,
    never reuse or default to the URL)."""


async def run_as_cycle_owned_request(
    context: TargetRefreshContext, target: Target, request_id: str, coro: Awaitable[Any],
) -> Any:
    """Runs `coro` as a cycle-owned request, uniquely identified by
    `request_id`, so cleanup can cancel and drain every active request — not
    only lane-attempt tasks. Every redesigned bounded fetch must go through
    this (`cycle_callbacks.py`'s `fetch()` closure).

    Registers the AMBIENT task actually executing `coro`
    (`asyncio.current_task()`) — it deliberately does NOT wrap `coro` in a
    brand-new task of its own. The redesigned v2 schema executor
    (`core/schema/executor.py`) fetches collection members strictly
    sequentially within one lane task (`await fetch(member)` in a loop, no
    `gather`); wrapping every such fetch in an additional task was tried
    first and reverted — it introduced a second, unnecessary layer of event-
    loop scheduling for what was otherwise a fully linear await chain,
    measurably shifting execution order between concurrently-scheduled work
    (this target's own Slow lane; other targets' cycles) closely enough to
    trigger CLAUDE.md's documented `rawDataCollector` keyDict-mutation
    ordering hazard ("concurrent component crawls receive independent
    mutable key dictionaries... discovery intentionally hands `serverid` to
    later model crawls before fan-out") and silently drop component data
    from certain crawls with no exception raised anywhere — confirmed via a
    golden-fixture regression before this was caught and fixed. Tracking
    the ambient task instead preserves the exact prior linear execution
    semantics for today's sequential executor, while still giving cleanup a
    real, cancellable, inspectable handle per `request_id` — and remains
    correct for any FUTURE concurrent fan-out (e.g. `gather`-based), where
    `asyncio.current_task()` inside each concurrently-running fetch would
    correctly resolve to gather's own distinct per-member task.

    The admission check and registration happen together under
    `Target.cycle_lock`, atomically with the snapshot cleanup takes of
    `active_requests` — this closes the registration-vs-cleanup race. A
    losing request (cycle closing, or a duplicate `request_id`) never runs
    `coro` at all — it is closed, un-started, entirely synchronously under
    the lock, so there is no window in which it could start real work
    before being rejected. The v1 legacy bridge does not go through this
    path yet (explicitly out of scope until round 5B's transport/adapter
    unification)."""
    current_task = asyncio.current_task()
    if current_task is None:
        coro.close()
        raise RuntimeError("run_as_cycle_owned_request() must run inside an asyncio task")

    async with target.cycle_lock:
        if not context.accepting_requests:
            coro.close()
            raise RequestRegistrationClosedError(request_id)
        if request_id in context.active_requests:
            coro.close()
            raise DuplicateRequestRegistrationError(request_id)
        context.active_requests[request_id] = current_task

    try:
        return await coro
    finally:
        context.active_requests.pop(request_id, None)


async def close_cycle(
    context: TargetRefreshContext, target: Target, *, on_close: Optional[Callable[[TargetRefreshContext], Awaitable[None]]] = None
) -> None:
    """The one cleanup path, reached from every exit reason. Idempotent and
    cancellation-safe: the actual cleanup work runs in ONE shared task every
    caller joins via `asyncio.shield`, so a caller's own cancellation (e.g.
    a second `shutdown_drain` cancel landing while `_run_cycle`'s `finally`
    is already awaiting this) can never interrupt cleanup partway and can
    never cause cleanup to run twice."""
    if context.cleanup_task is None:
        context.cleanup_task = asyncio.create_task(_do_cleanup(context, target, on_close))
    await asyncio.shield(context.cleanup_task)


async def _commit_to_closing_under_lock(context: TargetRefreshContext, target: Target) -> list:
    async with target.cycle_lock:
        return _commit_to_closing_sync(context)


def _commit_to_closing_sync(context: TargetRefreshContext) -> list:
    """The actual state-transition-and-snapshot body, factored out so both
    the locked path and the lock-free fallback below run the EXACT same
    steps: flip both `accepting` (lanes) and `accepting_requests` (per-fetch
    registration) False, and snapshot every task cleanup is about to
    cancel/drain. Contains no `await` — safe to run either under
    `Target.cycle_lock` or, when that can't be obtained in time, lock-free
    (single-threaded cooperative asyncio guarantees no other coroutine can
    interleave between these synchronous statements either way)."""
    context.accepting = False
    context.accepting_requests = False
    # Never reverse a terminal ABANDONED marker back to CLOSING: this is
    # this cycle's own FIRST (and only) `_do_cleanup` run even when it
    # starts AFTER `emergency_abandon()` already recorded ABANDONED for
    # this same context (round 5A.2) — a cancellation-resistant cycle's
    # own cleanup never even starts until its coroutine cooperates, by
    # which point emergency abandonment may already have run at
    # shutdown. ABANDONED must stay the permanent record that this
    # cycle's coroutine was forcibly reclaimed.
    if context.cycle_state != CycleState.ABANDONED:
        context.cycle_state = CycleState.CLOSING
    return list(context.owned_tasks) + list(context.active_requests.values())


async def _commit_to_closing(context: TargetRefreshContext, target: Target) -> list:
    """`_do_cleanup`'s own FIRST instruction (round 5A.3, defect B):
    previously an unguarded `async with target.cycle_lock:` sat BEFORE
    `_do_cleanup`'s `try/finally` even began, so a cancellation delivered
    while awaiting that initial lock acquisition (e.g. `shutdown_drain`
    directly cancelling an already-created `cleanup_task` it is abandoning)
    skipped every mandatory finalization step entirely. This step now lives
    INSIDE `_do_cleanup`'s own outer `try`, so its `finally` always runs
    regardless — but it is additionally bounded/shielded and falls back to
    the same safe, lock-free synchronous compare-and-flip
    `_clear_target_reference` already uses, following the identical
    reasoning: this fallback only ever mutates THIS context's own fields, so
    it can never corrupt or race a different cycle's state, and the lock's
    only real job (staying atomic with `ensure_cycle()`'s / `run_as_cycle_
    owned_request()`'s own locked sequences) is still honored whenever the
    lock is actually obtainable within budget."""
    task = _track_finalizer_task(context, asyncio.ensure_future(_commit_to_closing_under_lock(context, target)))
    budget = _remaining_cleanup_budget(context)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=budget)
    except asyncio.TimeoutError:
        return _commit_to_closing_sync(context)
    except asyncio.CancelledError:
        # Absorbed here for the same reason `_clear_target_reference`
        # absorbs a cancellation landing on its own final step: this is
        # `_do_cleanup`'s mandatory first instruction, and re-raising would
        # only be caught by `_do_cleanup`'s own `finally` anyway — running
        # the safe synchronous fallback immediately, rather than letting a
        # second cancellation short-circuit the rest of cleanup's best-
        # effort steps, keeps cleanup's behavior uniform regardless of
        # exactly when a cancellation lands.
        return _commit_to_closing_sync(context)


async def _do_cleanup(
    context: TargetRefreshContext, target: Target, on_close: Optional[Callable[[TargetRefreshContext], Awaitable[None]]],
) -> None:
    # From here on, ALL mandatory finalization (terminal lane futures,
    # credential/token clearing, exactly-once permit release, target-
    # reference removal, closed state) lives in the `finally` clause below
    # — a real one, not merely sequential code after best-effort steps — so
    # a cancellation or unexpected exception during the initial state
    # transition, request drain, logout, or session/connector close still
    # leaves the cycle in a fully reclaimed, non-leaking state rather than
    # skipping whatever came after the interruption point (round 5A.3: the
    # initial state-transition-and-snapshot step now lives INSIDE this same
    # `try`, not before it — see `_commit_to_closing`'s own docstring).
    owned: list = []
    try:
        owned = await _commit_to_closing(context, target)

        # 1. Cancel and await every lane-attempt AND registered request task
        # this cycle still owns (concurrently, within the remaining
        # monotonic budget) — never another cycle's tasks.
        for task in owned:
            if not task.done():
                task.cancel()
        if owned:
            done, pending = await asyncio.wait(owned, timeout=_remaining_cleanup_budget(context))
            for task in pending:
                task.cancel()

        # 2. Best-effort logout, bounded by whichever is smaller: the
        # configured `Logout.TimeoutSeconds`, or whatever's left of the
        # cycle's own absolute deadline (plus the small fixed cleanup
        # tolerance) — exactly once, only when a token exists. Never
        # allowed to run unbounded past the configured budget even though
        # the cycle deadline itself may already be exhausted.
        if context.token and context.logout_uri and context.session is not None:
            # `research.md` Group 2 (item 3): logout's own request/attempt
            # timeout is the SMALLEST of the configured per-request timeout
            # (`Tuning.Request.TimeoutSeconds`), the configured logout
            # timeout, and whatever's actually left of this cycle's shared
            # cleanup deadline — never a fixed logout-only constant on its
            # own. Computed BEFORE `_do_logout` is defined so the inner
            # `bounded_fetch` call's own `deadline` matches the SAME budget
            # the outer `wait_for` below enforces, rather than a separately
            # (and potentially more generous) computed one.
            logout_budget = min(
                context.logout_timeout_seconds, context.request_timeout_seconds,
                _remaining_cleanup_budget(context),
            )

            async def _do_logout() -> None:
                # `research.md` Group 2: logout uses the SAME dispatcher-
                # integrated, same-target-re-validated, size-bounded engine
                # every GET and login use — not a bare direct
                # `session.delete()`. Still fully best-effort: the whole
                # call is wrapped in the unconditional `except Exception:
                # pass` below, so any failure here (containment
                # re-validation, dispatcher admission, HTTP failure) never
                # claims or denies BMC-side revocation either way, exactly
                # as before.
                #
                # `research.md` Group 2 (item 2): the cycle-local semaphore
                # is acquired BEFORE the shared per-BMC dispatcher lease
                # (acquired inside `bounded_fetch` itself), and released in
                # reverse order via this `async with` block's own
                # `__aexit__` — the same acquisition order every GET
                # (`rawCollector.fetch`'s dispatcher branch) already uses.
                # `research.md` Group 2 (item 1): the ONE shared, process-
                # wide budget — never the separate `_LOGOUT_BYTE_BUDGET`
                # global. That global remains only as a back-compat
                # fallback for a context constructed without one (e.g. an
                # older direct `TargetRefreshContext(...)` test fixture);
                # the reserved size is capped to whichever budget's own
                # capacity is smaller so a reservation can never wait
                # forever for more capacity than the budget could ever
                # grant, even when using that smaller fallback.
                effective_byte_budget = context.io_byte_budget or _LOGOUT_BYTE_BUDGET
                effective_max_response_bytes = min(context.max_response_bytes, effective_byte_budget.capacity_bytes)
                async with context.local_semaphore:
                    await bounded_fetch(
                        context.session, context.logout_uri,
                        canonical_host=canonicalize_address(context.target_key[0]),
                        dispatcher=context.coordinator.dispatcher, priority="fast", cycle_id=context.cycle_id,
                        request_id=str(uuid.uuid4()), byte_budget=effective_byte_budget,
                        max_response_bytes=effective_max_response_bytes, max_attempts=1,
                        timeout_seconds=logout_budget,
                        backoff_base_seconds=0.0, backoff_cap_seconds=0.0,
                        deadline=time.monotonic() + logout_budget,
                        method="DELETE", headers={"X-Auth-Token": context.token},
                        return_headers=False, allow_empty_body=True,
                    )

            context.cleanup_entered_logout.set()
            try:
                await asyncio.wait_for(_do_logout(), timeout=logout_budget)
            except Exception:
                pass  # never let a failed/timed-out logout block the rest of cleanup

        # 3. Remove this cycle's queued dispatcher waiters (never another
        # cycle's).
        context.coordinator.dispatcher.cancel_cycle(context.cycle_id)

        # 4. Close session and connector — each bounded by the same
        # remaining-cleanup budget (round 5A.3: no longer an unbounded
        # `await ...close()` that could extend a detached cleanup invisibly
        # past this cycle's own remaining deadline). Accepted, deliberate
        # tradeoff (code-review finding, round 5A.3): once the cycle's own
        # absolute deadline has already elapsed, this budget collapses to
        # `_CLEANUP_TOLERANCE_SECONDS` (0.25s) — a `close()` that needs
        # longer than that is cut short and, per this function's own
        # never-retry policy below, simply left unclosed rather than
        # retried, which can leak that connector's underlying sockets in a
        # way an unbounded `await` would eventually have avoided. This is
        # accepted because the alternative — an unbounded close — is
        # exactly the "detached cleanup can continue invisibly forever"
        # hazard this round's own required fix (never let cleanup run
        # unbounded past its own deadline) exists to close, and because
        # `shutdown_drain`'s own shared `grace_seconds` is the real,
        # process-wide backstop for the shutdown path specifically (a
        # non-shutdown cycle overrunning its own deadline this way is
        # already the documented best-effort/asyncio-cannot-force-kill
        # limitation this module accepts elsewhere).
        context.cleanup_entered_session_close.set()
        if context.session is not None:
            try:
                await asyncio.wait_for(context.session.close(), timeout=_remaining_cleanup_budget(context))
            except Exception:
                pass
        if context.connector is not None:
            try:
                await asyncio.wait_for(context.connector.close(), timeout=_remaining_cleanup_budget(context))
            except Exception:
                pass
    finally:
        # Guaranteed finalizer: reached even if step 1-4 above raised or
        # was cancelled (a cancellation delivered while this coroutine
        # awaits inside the `try` is caught here like any other Python
        # `finally`, and this section itself still contains only bounded,
        # already-budgeted awaits — never a fresh unbounded one).
        owned_still_pending = [task for task in owned if not task.done()]
        for task in owned_still_pending:
            task.cancel()
        context.owned_tasks.clear()
        context.active_requests.clear()
        _fail_all_outstanding_lanes(context, RuntimeError("cycle closed before this lane ran"))
        context.token = None
        context.logout_uri = None
        _release_permits_once(context)
        # Detach session/connector/local-semaphore references — mandatory
        # (round 5A.2) even when a cancellation prevented step 4 above from
        # ever attempting its own `close()` calls (e.g. a cancellation
        # landing during step 1's task drain or step 2's logout, before step
        # 4 is ever reached). This is a pure reference detach, deliberately
        # NOT a repeated `close()` attempt: step 4 above is already the
        # cleanup's one bounded, best-effort close attempt (and, being
        # inside the `try` body, has already run to completion — or been
        # skipped by an earlier cancellation — by the time this `finally`
        # executes); retrying `close()` again here would re-invoke a
        # test/production fixture's own close() a second time with no
        # further cancellation ever coming to unblock a paused/blocking
        # implementation, which is a genuine hang hazard for no added
        # correctness (a session never closed by step 4 due to an earlier
        # cancellation is simply left unclosed, exactly as today's
        # documented best-effort/asyncio-cannot-force-kill limitation
        # already accepts elsewhere in this module).
        context.session = None
        context.connector = None
        context.local_semaphore = None
        # A cancellation delivered while THIS finalizer is itself suspended
        # awaiting `target.cycle_lock` must never be allowed to skip target-
        # reference removal or the terminal CLOSED state below — both are
        # mandatory. `_clear_target_reference` is bounded and shielded from
        # exactly that hazard (round 5A.2); see its own docstring.
        await _clear_target_reference(context, target)
        # Never reverse a terminal ABANDONED marker back to CLOSED: this
        # finalizer may run to completion AFTER `emergency_abandon()` already
        # recorded ABANDONED for this same cycle (a late-finishing normal
        # cleanup racing the emergency path) — ABANDONED must stay the
        # permanent record that this cycle's coroutine was forcibly
        # reclaimed rather than genuinely, cooperatively completing.
        if context.cycle_state != CycleState.ABANDONED:
            context.cycle_state = CycleState.CLOSED

    if on_close is not None:
        await on_close(context)


async def _clear_target_reference_under_lock(context: TargetRefreshContext, target: Target) -> None:
    async with target.cycle_lock:
        if target.active_cycle is context:
            target.active_cycle = None
            target.active_cycle_task = None


async def _clear_target_reference(context: TargetRefreshContext, target: Target) -> None:
    """Clears `target.active_cycle`/`active_cycle_task` back to `None` iff
    they still identify THIS context — never a newer cycle's. Synchronized
    with `Target.cycle_lock` on the normal path (so it can never race
    `ensure_cycle()`'s own locked check-then-open sequence), but bounded and
    shielded against a cancellation (or pathological contention) delivered
    while THIS mandatory finalizer step is suspended waiting for that lock —
    this step must be un-skippable.

    The actual lock-holding work runs in its own task (`asyncio.ensure_future`)
    so that even if this coroutine stops awaiting it (timeout, or a second
    cancellation), that task keeps running independently on the event loop
    and still correctly acquires, checks, clears (if still applicable), and
    releases the lock — it is never orphaned mid-acquisition holding the lock
    forever.

    If the bound is exceeded (or this awaiting coroutine is itself cancelled
    again while waiting), falls back to a synchronous, lock-free identity
    check-and-clear. This fallback is still safe: asyncio is single-threaded
    and cooperative, and this compare-and-clear contains no `await` between
    the check and the assignment, so no other coroutine can run in between
    regardless of whether the lock is held. The lock's actual job is only to
    make this step atomic WITH `ensure_cycle()`'s own multi-step
    check-then-open-a-new-cycle sequence; the background task above still
    provides that guarantee whenever the lock is obtainable, and the fallback
    only ever narrows what it clears to "still identifies this exact
    context" — it can never clear a newer cycle's reference."""
    task = _track_finalizer_task(context, asyncio.ensure_future(_clear_target_reference_under_lock(context, target)))
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=_remaining_cleanup_budget(context))
    except asyncio.TimeoutError:
        if target.active_cycle is context:
            target.active_cycle = None
            target.active_cycle_task = None
    except asyncio.CancelledError:
        # A cancellation landing exactly HERE — the mandatory finalizer's
        # own last step — is deliberately absorbed rather than re-raised:
        # this is the one narrow point where re-propagating it would skip
        # the terminal CLOSED state this same `finally` clause sets
        # immediately afterward, which is just as mandatory as this clear
        # itself. The synchronous fallback below still runs first, so the
        # clear itself is never skipped either way. `shutdown_drain` (the
        # only realistic source of a cancellation landing this late) only
        # ever inspects done/pending status afterward, never whether the
        # task ended up specifically `cancelled()` vs. completed normally,
        # so absorbing it here changes no externally-observable contract.
        if target.active_cycle is context:
            target.active_cycle = None
            target.active_cycle_task = None


def _release_permits_once(context: TargetRefreshContext) -> None:
    """Guards against a double-release: `shutdown_drain`'s emergency
    finalization for an abandoned (non-cooperative) cycle task may already
    have released these permits before this cycle's own cleanup ever got a
    chance to run (or finish) — asyncio cannot force-kill a coroutine that
    ignores cancellation, so both paths must be safe to call independently
    without double-releasing capacity that was already freed."""
    if context._permits_released:
        return
    context._permits_released = True
    context.coordinator.release_cycle_lease()
    if context.refresh_admission is not None:
        context.refresh_admission.release()


def emergency_abandon(context: TargetRefreshContext, target: Target) -> None:
    """Best-effort, synchronous, non-awaiting emergency finalization for a
    cycle whose task did not finish within a shared shutdown grace window
    and is being abandoned (`registry.shutdown_drain`). asyncio cannot
    force-kill a coroutine that ignores cancellation forever — this clears
    every piece of registry/dispatcher/permit/task/credential ownership this
    process can still safely determine is held, so the abandoned task (if it
    ever does finish) finds nothing left to release twice, and the
    registry/dispatcher are not left permanently believing this cycle is
    still active. Idempotent and safe to race against a late-finishing
    normal `_do_cleanup` for the SAME cycle in either order (every step here
    reuses the same membership/idempotency guards `_do_cleanup` uses:
    `_release_permits_once`, `dispatcher.abandon_cycle`'s per-pair
    membership gate, the `target.active_cycle is context` check before
    clearing target references) — never clobbers a newer cycle's state.

    Session/connector closure is fired off in the background using LOCALS
    captured before the context's own references are cleared (round 5A.2) —
    never the context itself: this function must stay synchronous so it
    cannot itself extend `shutdown_drain`'s already-elapsed shared grace
    window, and the background close task must not retain the context (which
    would let it observe/mutate state a subsequent late-finishing normal
    `_do_cleanup` is concurrently touching, and would keep the whole context
    object alive for longer than necessary). If the underlying connector is
    held open by a deliberately non-cooperative coroutine, that close may
    never actually complete — this is the same documented asyncio limitation
    as the abandoned task itself not being force-killable: asyncio cannot
    force-kill a coroutine, and this function never claims the network
    connection or remote BMC-side logout was actually, forcibly torn down —
    only that this process's own references to it were released."""
    context.accepting = False
    context.accepting_requests = False

    # Cycle-scoped dispatcher reclamation: queued waiters AND admitted
    # (in-flight) entries, membership-gated so a late normal `release()`
    # call for a request already reclaimed here safely no-ops.
    context.coordinator.dispatcher.abandon_cycle(context.cycle_id)

    # Best-effort cancellation (never awaited — this function is
    # synchronous) of every task this cycle still owns/registered. A
    # late-finishing one still safely no-ops when it eventually tries its
    # own cleanup, via the same idempotency guards used above/below.
    for task in list(context.owned_tasks) + list(context.active_requests.values()):
        if not task.done():
            task.cancel()
    context.owned_tasks.clear()
    context.active_requests.clear()

    context.token = None
    context.logout_uri = None
    _release_permits_once(context)
    if target.active_cycle is context:
        target.active_cycle = None
        target.active_cycle_task = None
    _fail_all_outstanding_lanes(context, RuntimeError("cycle abandoned during shutdown"))

    # Capture session/connector/local-semaphore LOCALS before detaching the
    # context's own references to them — the background close below (if any)
    # uses only these locals, never `context.session`/`context.connector`
    # again, so the context can be fully detached immediately and does not
    # need to stay alive or mutable for the background task's sake.
    session = context.session
    connector = context.connector
    context.session = None
    context.connector = None
    context.local_semaphore = None

    context.cycle_state = CycleState.ABANDONED

    if session is not None or connector is not None:
        # Round 5A.3 (defect C): tracked, not a bare discarded
        # `ensure_future()` — `_track_finalizer_task` retrieves this task's
        # exception (if any) once it finishes and removes it from
        # `context.finalizer_tasks`, so a raising close() never produces an
        # "exception was never retrieved" warning and a test can assert
        # `finalizer_tasks` is empty once every helper has genuinely
        # finished.
        #
        # Round 9 (Group A item 3): this task is created AFTER `shutdown_
        # drain`'s own shared grace window has already elapsed (this
        # function only runs once that window is exhausted) — it must
        # therefore never be genuinely unbounded itself, or it becomes an
        # invisible, ever-growing helper outliving every deadline this
        # module otherwise enforces. Bounded to the same small, fixed
        # `_CLEANUP_TOLERANCE_SECONDS` allowance every other cleanup step
        # uses — never a fresh unrelated timeout.
        _track_finalizer_task(
            context,
            asyncio.ensure_future(
                _bounded_best_effort_close(session, connector, _CLEANUP_TOLERANCE_SECONDS)
            ),
        )


async def _best_effort_close_session(session: Optional[ClientSession], connector: Optional[TCPConnector]) -> None:
    """Fired off (never awaited) by `emergency_abandon` to reclaim the
    detached session/connector's underlying network resources
    opportunistically, without extending `shutdown_drain`'s own bounded
    return and without retaining the `TargetRefreshContext` itself (round
    5A.2: only these two already-detached locals are closed over). Best-
    effort only — never claims the underlying connection was forcibly closed
    if `close()` itself never returns."""
    if session is not None:
        try:
            await session.close()
        except Exception:
            pass
    if connector is not None:
        try:
            await connector.close()
        except Exception:
            pass


async def _bounded_best_effort_close(
    session: Optional[ClientSession], connector: Optional[TCPConnector], timeout_seconds: float,
) -> None:
    """Round 9 (Group A item 3): wraps `_best_effort_close_session` in a
    fixed, small timeout — a `close()` implementation that itself never
    returns (the same non-cooperative-code hazard this whole module
    otherwise defends against) must not turn this already-past-deadline
    helper into an unbounded task with no ceiling at all. Absorbing the
    resulting `TimeoutError` is safe and consistent with this helper's
    existing best-effort contract: it never claimed the underlying
    connection was forcibly closed in the first place."""
    try:
        await asyncio.wait_for(_best_effort_close_session(session, connector), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        pass
