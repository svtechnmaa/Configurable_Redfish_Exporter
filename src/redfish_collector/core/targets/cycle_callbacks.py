"""Builds the `login`/`run_lane` callables `ensure_cycle` needs, wiring
together login, the v2/legacy schema pipeline, dispatcher-integrated fetch,
and lane-state publication for one `(serverAddress, config)` target.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Callable

logger = logging.getLogger(__name__)

from ..config.profile import DeploymentProfile
from ..lanes.fast import FastLaneError, run_fast_lane
from ..lanes.slow import SlowLaneError, run_slow_lane
from ..rawCollector import fetch as raw_fetch
from ..schema.legacy import collect_legacy_lane
from ..schema.models import CommonSchemaV2, SafetyConfig
from ..schema.pipeline import TargetSelectionError, bootstrap_and_select
from ..security.containment import canonicalize_address
from ..security.redaction import safe_exception_summary
from ..security.same_target import format_host_for_url
from .login import LoginFailedError, login_and_get_token
from .refresh_context import Lane, TargetRefreshContext, run_as_cycle_owned_request
from .target_state import Target


def make_login(profile: DeploymentProfile, common_schema: CommonSchemaV2) -> Callable[[TargetRefreshContext], Any]:
    # `common_schema` is always the top-level v2 CommonSchema (the mixed-version
    # bridge: its ModelSchemas rules may each point to a v1 or v2 vendor-model
    # file, but Common.yml itself is v2 — contract §7). Its declared
    # Bootstrap.Authentication.FallbackURI equals today's hardcoded v1
    # $tokenuri ("/redfish/v1/SessionService/Sessions") by construction.
    token_uri = common_schema.bootstrap.session_fallback_uri

    # Extract only what login needs — never close over the complete
    # `DeploymentProfile` (which also carries `Metrics` and every other
    # config's Tuning-equivalent surface area). `username`/`password` are
    # captured as local variables here (not `profile.auth.*` reads inside
    # the closure) so the returned `login` callable's cell variables are
    # exactly the minimal secret-bearing values it needs, not a reference to
    # the whole profile object.
    username = profile.auth.username
    password = profile.auth.password
    max_attempts = profile.tuning.login.max_attempts
    timeout_seconds = profile.tuning.login.timeout_seconds
    # `login_and_get_token`'s own defaults (10 MiB cap, 0.5s/30s backoff)
    # exist only for callers with no profile to consult — production login
    # must use the SAME configured response cap and retry/backoff policy as
    # every GET (`make_run_lane` above), never silently fall back to those
    # hardcoded defaults regardless of a smaller/larger configured value.
    max_response_bytes = profile.tuning.max_response_bytes
    backoff_base_seconds = profile.tuning.request.backoff_base_seconds
    backoff_cap_seconds = profile.tuning.request.backoff_cap_seconds
    # `Safety.FollowRedirects` from the schema (`common_schema`, not
    # `profile`) — never the transport's own hardcoded "enabled, 3 hops"
    # default regardless of a stricter/looser configured value.
    # `getattr(..., "safety", None)` falls back to `SafetyConfig()`'s
    # defaults (identical to the previous hardcoded values) for a caller
    # that supplies a `common_schema` stand-in with no `.safety` at all —
    # never a hard requirement change for an existing test double.
    _login_safety = getattr(common_schema, "safety", None) or SafetyConfig()
    max_redirects = _login_safety.max_redirects
    follow_redirects = _login_safety.follow_redirects_enabled

    async def login(context: TargetRefreshContext) -> None:
        nonlocal username, password
        canonical = canonicalize_address(context.target_key[0])
        # IPv6 must be bracketed in the URL authority component
        # (`https://[::1]/...`) — `canonical` itself is the unbracketed
        # form `canonicalize_address` returns for comparison/storage.
        url = f"https://{format_host_for_url(canonical)}{token_uri}"
        try:
            token, logout_uri = await login_and_get_token(
                context.session, url, username=username, password=password,
                canonical_address=canonical,
                max_attempts=max_attempts, timeout_seconds=timeout_seconds,
                # `research.md` Group 2: login routes through the SAME
                # dispatcher-integrated transport engine every GET (v1 and
                # v2 alike) uses, instead of its own separate direct call.
                dispatcher=context.coordinator.dispatcher, cycle_id=context.cycle_id,
                deadline=context.cycle_deadline,
                # `research.md` Group 2 (items 1/2): the ONE shared,
                # process-wide I/O budget and the cycle-local semaphore —
                # acquired before the dispatcher lease, same as every GET.
                byte_budget=context.io_byte_budget, semaphore=context.local_semaphore,
                # Configured response cap and retry/backoff policy — see the
                # comment above `max_response_bytes` at closure-build time.
                max_response_bytes=max_response_bytes,
                backoff_base_seconds=backoff_base_seconds,
                backoff_cap_seconds=backoff_cap_seconds,
                max_redirects=max_redirects, follow_redirects=follow_redirects,
            )
        except LoginFailedError as exc:
            raise LoginFailedError(safe_exception_summary(exc)) from exc
        finally:
            # Drop this closure's own references to BOTH credential values
            # after the single login attempt completes, fails, OR is
            # cancelled (a `finally` runs on `asyncio.CancelledError` too) —
            # as far as Python object ownership permits (the values may
            # still be referenced briefly by the aiohttp request body/
            # history in flight, but this closure retains neither longer
            # than one login call). Username is not itself a high-value
            # secret, but CLAUDE.md treats it the same as the password —
            # never retained longer than necessary either.
            username = None
            password = None
        if not context.try_set_login_result(token, logout_uri):
            # This login attempt outlived its own cycle deadline/cancellation
            # (a cancellation-resistant completion) — the cycle has already
            # committed to closing. Never resurrect token/logout state onto
            # it (`research.md` Group 1). Stable event name only, never the
            # token/URL/target itself.
            logger.warning("cycle %s: late login result discarded (cycle no longer live)", context.cycle_id)

    return login


def make_run_lane(
    profile: DeploymentProfile, target: Target, common_schema: CommonSchemaV2,
    response_cache: Any = None, target_registry: Any = None,
) -> Callable[[TargetRefreshContext, Lane], Any]:
    # Capture only the immutable, non-secret `Tuning` sub-object — never the
    # complete `DeploymentProfile` (which also carries `profile.auth`,
    # including the plaintext password). `run_lane`'s closure below must
    # never be able to reach a credential even by accident.
    tuning = profile.tuning
    # `Safety.FollowRedirects` from the schema — see `make_login`'s
    # identical capture above for why this is read from `common_schema`,
    # not `profile.tuning` (and why the fallback exists).
    safety = getattr(common_schema, "safety", None) or SafetyConfig()

    async def ensure_bootstrap(context: TargetRefreshContext, fetch: Any) -> None:
        """Runs bootstrap discovery + model selection at most once per
        cycle, under `context.bootstrap_lock` (Fast and a later-attached
        Slow could otherwise race to bootstrap independently). Every lane
        call after the first reuses the stored result instead of repeating
        Service Root/Systems-collection/System discovery (`research.md`
        §C)."""
        async with context.bootstrap_lock:
            if context.selected_schema is not None:
                return
            bootstrap_ctx, system_obj, schema = await bootstrap_and_select(common_schema, fetch)
            context.bootstrap_ctx = bootstrap_ctx
            context.bootstrap_system_obj = system_obj
            context.selected_schema = schema

    async def run_lane(context: TargetRefreshContext, lane: Lane) -> None:
        canonical = canonicalize_address(context.target_key[0])
        lane_name = "fast" if lane == Lane.FAST else "slow"

        async def fetch(url: str) -> Any:
            # A unique ID per logical fetch call, NOT the URL — two
            # concurrent in-flight fetches to the same URL within one cycle
            # (e.g. duplicate member links, or a retried/re-fetched
            # resource) must occupy two distinct dispatcher admission slots.
            # Defaulting to the URL made `_admitted` collapse both under one
            # set key: the second admission was a silent set no-op, and
            # whichever call released first stripped the OTHER's still-active
            # admission out of `_admitted`, permanently leaking that
            # dispatcher capacity slot for the rest of the process. The same
            # `request_id` also identifies this fetch as a cycle-owned
            # request task below (round 5A.1) — every redesigned bounded
            # fetch must be uniquely registered so cleanup can cancel and
            # drain it, not just the owning lane-attempt task.
            request_id = str(uuid.uuid4())

            async def _do_fetch() -> Any:
                return await raw_fetch(
                    url, context.token, context.session, canonical, context.local_semaphore,
                    dispatcher=context.coordinator.dispatcher, cycle_id=context.cycle_id, priority=lane_name,
                    request_id=request_id,
                    max_response_bytes=tuning.max_response_bytes,
                    max_attempts=tuning.request.max_attempts,
                    backoff_base_seconds=tuning.request.backoff_base_seconds,
                    backoff_cap_seconds=tuning.request.backoff_cap_seconds,
                    deadline=context.cycle_deadline,
                    # `research.md` Group 2 (items 1/3): the ONE shared,
                    # process-wide I/O budget and the configured per-request
                    # timeout — never a hard-coded 30s or a fresh budget.
                    byte_budget=context.io_byte_budget,
                    timeout_seconds=tuning.request.timeout_seconds,
                    max_redirects=safety.max_redirects,
                    follow_redirects=safety.follow_redirects_enabled,
                )

            return await run_as_cycle_owned_request(context, target, request_id, _do_fetch())

        lane_state = target.fast if lane == Lane.FAST else target.slow
        run_lane_job = run_fast_lane if lane == Lane.FAST else run_slow_lane
        try:
            try:
                await ensure_bootstrap(context, fetch)
            except TargetSelectionError as exc:
                raise (FastLaneError if lane == Lane.FAST else SlowLaneError)(safe_exception_summary(exc)) from exc

            system_obj_for_reuse = None
            if lane == Lane.FAST:
                async with context.bootstrap_lock:
                    if not context.bootstrap_system_reused:
                        context.bootstrap_system_reused = True
                        system_obj_for_reuse = context.bootstrap_system_obj

            # The lane facade (`core/lanes/fast.py`/`slow.py`) dispatches to
            # the v2 executor or the permanent v1 bridge per the SELECTED
            # rule's schema kind — the top-level `common_schema` is always
            # v2 (the mixed-version bridge). It wraps failures in its own
            # FastLaneError/SlowLaneError; safe_exception_summary is applied
            # again below purely to normalize the published lane-state
            # message regardless of which lane failed. `bootstrap_ctx`/
            # `selected_schema` are always shared from `ensure_bootstrap()`
            # above — bootstrap/model selection run at most once per cycle
            # regardless of how many Fast/Slow lane calls follow.
            snapshot = await run_lane_job(
                common_schema, fetch=fetch,
                legacy_lane_collector=collect_legacy_lane, legacy_session=context.session,
                legacy_semaphore=context.local_semaphore, legacy_token=context.token or "",
                legacy_log_level="info", server_address=canonical,
                bootstrap_ctx=context.bootstrap_ctx, selected_schema=context.selected_schema,
                # `research.md` Group 2: the permanent v1-compatibility
                # bridge's own crawl must use the SAME dispatcher-integrated
                # transport engine the v2 path uses, never a direct
                # unguarded `session.get()` — see `schema/legacy.py`'s
                # `collect_legacy_lane` docstring.
                legacy_dispatcher=context.coordinator.dispatcher, legacy_cycle_id=context.cycle_id,
                legacy_deadline=context.cycle_deadline,
                # `research.md` Group 2 (items 1/3): the same ONE shared
                # I/O budget and configured per-request timeout the v2 GET
                # path uses — never a separate hard-coded budget/timeout
                # for the permanent v1-compatibility bridge.
                legacy_byte_budget=context.io_byte_budget, legacy_timeout_seconds=tuning.request.timeout_seconds,
                legacy_max_redirects=safety.max_redirects, legacy_follow_redirects=safety.follow_redirects_enabled,
                # `research.md` Group 5 (item 2): the configured
                # `Tuning.Crawl.BatchSize` bounds concurrent resource/
                # member/child/component fetches for both the v2 executor
                # and the permanent v1 bridge's own fan-out — never a
                # list-sized `asyncio.gather`.
                batch_size=tuning.crawl.batch_size,
                # `research.md` Group 5 (item 3): the v2 executor's
                # Children recursion depth bound — configured, never a
                # hard-coded literal.
                max_depth=tuning.crawl.max_depth,
                **({"system_obj_for_reuse": system_obj_for_reuse} if lane == Lane.FAST else {}),
            )
        except (FastLaneError, SlowLaneError) as exc:
            if context.is_live():
                new_lane_state = lane_state.failed(safe_exception_summary(exc))
                _publish(target, lane, new_lane_state)
            else:
                # A cancellation-resistant lane job that fails AFTER its
                # own cycle already committed to closing must never write
                # to `target.fast`/`target.slow` — those are SHARED mutable
                # fields a NEWER cycle may already be using
                # (`research.md` Group 1: "...cannot... start lane work").
                logger.warning(
                    "cycle %s: late lane failure discarded (cycle no longer live)", context.cycle_id,
                )
            raise

        if not context.is_live():
            logger.warning(
                "cycle %s: late lane publication discarded (cycle no longer live)", context.cycle_id,
            )
            return
        new_lane_state = lane_state.published(snapshot)

        # `Tuning.Cache.MaxInFlightCandidateBytes`: this fully-built-but-
        # not-yet-published candidate is reserved from the process-wide
        # in-flight-candidate budget BEFORE any publish/reject decision
        # runs — several concurrent target cycles' own candidates must
        # never together exceed this configured total. Held through the
        # ENTIRE decision below and released EXACTLY ONCE, on every exit
        # path (publish, reject, or an exception raised while deciding),
        # via `try`/`finally` — never conditionally on success alone.
        # `target_registry` is `None` only for a caller that never built
        # one (no current production path).
        candidate_bytes_reserved = 0
        if target_registry is not None:
            if not target_registry.try_reserve_candidate_bytes(new_lane_state.serialized_size):
                logger.warning(
                    "cycle %s: %s lane candidate (%d bytes) rejected by MaxInFlightCandidateBytes bound; "
                    "keeping prior snapshot",
                    context.cycle_id, lane_name, new_lane_state.serialized_size,
                )
                _publish(target, lane, lane_state.failed("snapshot rejected by MaxInFlightCandidateBytes bound"))
                return
            candidate_bytes_reserved = new_lane_state.serialized_size

        try:
            # `research.md` Group 5 (item 3): a snapshot too large for its own
            # lane, or one that would push the process-wide total over budget,
            # is REJECTED (never truncated/silently accepted) — the prior
            # snapshot is kept exactly as-is, and this publish is treated the
            # same as any other lane failure: no cache invalidation, no
            # success-state advance.
            rejection_reason = None
            if new_lane_state.serialized_size > tuning.cache.max_snapshot_bytes_per_lane:
                rejection_reason = "MaxSnapshotBytesPerLane"
            elif target_registry is not None:
                delta = new_lane_state.serialized_size - lane_state.serialized_size
                if not target_registry.try_reserve_snapshot_bytes_delta(delta):
                    rejection_reason = "MaxTotalSnapshotBytes"

            if rejection_reason is not None:
                logger.warning(
                    "cycle %s: %s lane snapshot (%d bytes) rejected by %s bound; keeping prior snapshot",
                    context.cycle_id, lane_name, new_lane_state.serialized_size, rejection_reason,
                )
                _publish(target, lane, lane_state.failed(f"snapshot rejected by {rejection_reason} bound"))
            else:
                # Atomic transfer: the candidate reservation is released in
                # THIS `finally` block only after `try_reserve_snapshot_bytes_
                # delta` above has already moved the equivalent bytes onto
                # the published-snapshot total — no `await` runs between
                # that reservation and this release, so the process-wide
                # accounting never observes neither/both budgets charged.
                _publish(target, lane, new_lane_state)
                if response_cache is not None:
                    # A cached whole-response body reflects one specific
                    # (fast_generation, slow_generation) pair; once EITHER lane
                    # publishes a new generation, that cached body is stale
                    # relative to already-available newer data — remove it now
                    # rather than letting readers keep observing the old
                    # generation until its previously-recorded TTL/freshness
                    # deadline elapses on its own.
                    response_cache.invalidate(target.key)
                if lane == Lane.FAST:
                    target.advance_state_on_fast_success()
                else:
                    target.advance_state_on_slow_success()
        finally:
            if target_registry is not None:
                target_registry.release_candidate_bytes(candidate_bytes_reserved)

        if lane == Lane.SLOW:
            # `research.md` §3: `Slow.Resources: []` is valid and marks Slow
            # permanently disabled for this Target — never re-added to a
            # future `lanes_due` (the router checks `target.slow_disabled`),
            # so an empty Slow does not remain perpetually due every TTL
            # period forever. Only meaningful for a v2 `VendorModelSchemaV2`
            # selection; the permanent v1 legacy bridge has no equivalent
            # per-schema declared resource list to inspect here. Runs
            # regardless of the size-bound outcome above — it is about
            # whether Slow has any resources AT ALL, unrelated to size.
            slow_resources = getattr(context.selected_schema, "slow_resources", None)
            if slow_resources is not None and len(slow_resources) == 0:
                target.slow_disabled = True

    return run_lane


def _publish(target: Target, lane: Lane, new_state: Any) -> None:
    if lane == Lane.FAST:
        target.fast = new_state
    else:
        target.slow = new_state
