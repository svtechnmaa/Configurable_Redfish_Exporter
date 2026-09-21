import asyncio
import json
import logging
import re
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse
from pydantic import IPvAnyAddress
from starlette import status
from prometheus_client import generate_latest, Gauge, CollectorRegistry

from ..core.rawCollector import jsonpathCollector, readYAMLTemplate
from ..core.config.profile import (
    DeploymentProfile,
    ProfileValidationError,
    load_profile,
    validate_profiles_directory,
)
from ..core.schema.loader import load_schema_file
from ..core.schema.models import CommonSchemaV2
from ..core.security.containment import (
    ContainmentError,
    canonicalize_address,
    resolve_config_path,
    validate_config_stem,
)
from ..core.security.redaction import safe_exception_summary
from ..core.targets.admission import GlobalRefreshAdmission
from ..core.targets.cycle_callbacks import make_login, make_run_lane
from ..core.targets.refresh_context import (
    CycleLeaseDeniedForTarget,
    GlobalRefreshAdmissionDeniedForTarget,
    Lane,
    close_cycle,
    ensure_cycle,
    join_lane,
)
from ..core.targets.registry import RegistryFull, TargetRegistry
from ..core.targets.response_cache import ResponseCache
from ..core.debug.artifacts import DebugArtifactStore
from os import path

REDFISH_DATA = '/tmp/redfish-data/'

templateDir = config_path = path.join(path.dirname(__file__), '../core/templates/')

# Loaded once at import time — the migrated v2 CommonSchema (Version 2, the
# mixed-version bridge: HPE Gen11/Dell R650 selection rules point to their v2
# vendor-model files; every other currently-shipped model's rule still points
# to its unmodified v1 file, loaded through the permanent legacy bridge).
_COMMON_SCHEMA: CommonSchemaV2 = load_schema_file(Path(templateDir) / "schemas" / "Common.yml")

# Built once at startup via `startup_validate_and_build_registry()`
# (contracts/deployment-profile-contract.md "Process-wide consistency
# across profile files") — every config's profile is loaded and process-wide
# Tuning equality is enforced BEFORE the first request, so no single
# requested config can silently choose the process-wide limits every other
# target is also bound by.
_TARGET_REGISTRY: TargetRegistry | None = None

# `research.md` Group 5 (item 1): the immutable, fully-validated profile map
# built ONCE at startup — every request resolves its config from THIS map,
# never by reopening/reparsing the secret-bearing (Auth.Username/Password)
# YAML file again per scrape, cache hit or miss alike. `None` only when no
# startup validation has run yet (e.g. a test importing this module
# directly without the FastAPI lifespan) — production always goes through
# `startup_validate_and_build_registry()` first.
_VALIDATED_PROFILES: dict[str, DeploymentProfile] | None = None

# Built once at startup alongside `_TARGET_REGISTRY`, from the same
# process-wide-consistent `Tuning.Debug.*` — never per-request or
# per-config, matching every other shared, bounded, process-wide resource.
# `enabled()` is False (no writes ever attempted) unless an operator has
# explicitly turned on `WriteRawData`/`WriteNormalizedData` (FR-041:
# "disabled by default").
_DEBUG_ARTIFACT_STORE: DebugArtifactStore | None = None

# Round-of-repair (FR-041): the only request-level values `loglevel` may
# take — anything else is rejected as invalid input rather than silently
# accepted and forwarded verbatim (the prior behavior: an arbitrary,
# unvalidated string reached logging-adjacent code with no allow-list at
# all). This does not change process-wide log verbosity (logging is
# configured once at startup, per `main.py`/`logging/logging.yml` — a
# single Uvicorn worker cannot safely have per-request-mutable global log
# level without racing every OTHER concurrent request) — it is this
# request's own opt-in for whether bounded debug artifacts are written for
# ITS target/config, when the operator has also enabled that capability.
_VALID_LOGLEVELS = frozenset({"debug", "info", "warning", "error"})


def _validate_loglevel(loglevel: str) -> str:
    normalized = (loglevel or "info").strip().lower()
    if normalized not in _VALID_LOGLEVELS:
        logging.warning("rejected unrecognized loglevel query value (event=invalid_loglevel); using 'info'")
        return "info"
    return normalized


def startup_validate_and_build_registry(profile_metrics_dir: str | None = None) -> tuple[TargetRegistry, float]:
    """Loads and validates every `*.yml` profile under `configs/`, enforcing
    process-wide Tuning consistency across all of them, then builds the
    single process-wide `TargetRegistry` from that shared Tuning. Raises
    `ProfileValidationError` (caller/lifespan aborts startup) rather than
    silently deferring to whichever config a request happens to name
    first. Returns `(registry, shutdown_grace_seconds)`."""
    global _TARGET_REGISTRY, _VALIDATED_PROFILES, _DEBUG_ARTIFACT_STORE
    configs_dir = Path(profile_metrics_dir) if profile_metrics_dir else Path(templateDir) / "configs"
    profiles = validate_profiles_directory(configs_dir)
    if not profiles:
        raise ProfileValidationError(f"no profile files found under {configs_dir}")
    _VALIDATED_PROFILES = profiles
    shared_tuning = next(iter(profiles.values())).tuning
    _DEBUG_ARTIFACT_STORE = DebugArtifactStore(
        root_directory=shared_tuning.debug.root_directory,
        write_raw_data=shared_tuning.debug.write_raw_data,
        write_normalized_data=shared_tuning.debug.write_normalized_data,
        max_file_bytes=shared_tuning.debug.max_file_bytes,
        max_files_per_target=shared_tuning.debug.max_files_per_target,
        max_total_bytes=shared_tuning.debug.max_total_bytes,
        retention_seconds=shared_tuning.debug.retention_seconds,
    )
    _TARGET_REGISTRY = TargetRegistry(
        max_targets=shared_tuning.cache.max_targets,
        idle_ttl_seconds=shared_tuning.cache.idle_target_ttl_seconds,
        max_parallel_targets=shared_tuning.max_parallel_targets,
        max_concurrent_refreshes=shared_tuning.refresh.max_concurrent_refreshes,
        response_cache=ResponseCache(
            max_entries=shared_tuning.cache.max_entries,
            max_total_bytes=shared_tuning.cache.max_total_bytes,
            default_ttl_seconds=shared_tuning.cache.response_ttl_seconds,
        ),
        fast_ttl_seconds=shared_tuning.fast.ttl_seconds,
        fast_deadline_seconds=shared_tuning.fast.deadline_seconds,
        slow_ttl_seconds=shared_tuning.slow.ttl_seconds,
        slow_deadline_seconds=shared_tuning.slow.deadline_seconds,
        max_in_flight_response_bytes=shared_tuning.io.max_in_flight_response_bytes,
        max_total_snapshot_bytes=shared_tuning.cache.max_total_snapshot_bytes,
        max_in_flight_candidate_bytes=shared_tuning.cache.max_in_flight_candidate_bytes,
    )
    return _TARGET_REGISTRY, float(shared_tuning.shutdown.grace_seconds)


router = APIRouter(
    prefix='/metrics',
    tags=['Prometheus Metrics']
)


def _get_registry(profile: DeploymentProfile) -> tuple[TargetRegistry, GlobalRefreshAdmission]:
    global _TARGET_REGISTRY
    if _TARGET_REGISTRY is None:
        # Fallback for callers that never ran startup validation (e.g. a
        # test importing this module directly without the FastAPI
        # lifespan) — production always goes through
        # `startup_validate_and_build_registry()` via `main.py`'s lifespan.
        _TARGET_REGISTRY = TargetRegistry(
            max_targets=profile.tuning.cache.max_targets,
            idle_ttl_seconds=profile.tuning.cache.idle_target_ttl_seconds,
            max_parallel_targets=profile.tuning.max_parallel_targets,
            max_concurrent_refreshes=profile.tuning.refresh.max_concurrent_refreshes,
            response_cache=ResponseCache(
                max_entries=profile.tuning.cache.max_entries,
                max_total_bytes=profile.tuning.cache.max_total_bytes,
                default_ttl_seconds=profile.tuning.cache.response_ttl_seconds,
            ),
            max_in_flight_response_bytes=profile.tuning.io.max_in_flight_response_bytes,
            max_total_snapshot_bytes=profile.tuning.cache.max_total_snapshot_bytes,
            max_in_flight_candidate_bytes=profile.tuning.cache.max_in_flight_candidate_bytes,
        )
    return _TARGET_REGISTRY, _TARGET_REGISTRY.refresh_admission


def _get_debug_artifact_store(profile: DeploymentProfile) -> DebugArtifactStore:
    global _DEBUG_ARTIFACT_STORE
    if _DEBUG_ARTIFACT_STORE is None:
        # Fallback for callers that never ran startup validation — see
        # `_get_registry`'s identical fallback above.
        _DEBUG_ARTIFACT_STORE = DebugArtifactStore(
            root_directory=profile.tuning.debug.root_directory,
            write_raw_data=profile.tuning.debug.write_raw_data,
            write_normalized_data=profile.tuning.debug.write_normalized_data,
            max_file_bytes=profile.tuning.debug.max_file_bytes,
            max_files_per_target=profile.tuning.debug.max_files_per_target,
            max_total_bytes=profile.tuning.debug.max_total_bytes,
            retention_seconds=profile.tuning.debug.retention_seconds,
        )
    return _DEBUG_ARTIFACT_STORE


def _resolve_profile(config: str) -> DeploymentProfile:
    """`research.md` Group 5 (item 1): resolves `config` from the
    immutable, validated startup profile map — no filesystem access, no
    re-parsing the secret-bearing YAML, for every scrape (cache hit or
    miss alike). An unrecognized `config` (including any path-traversal-
    shaped value — a dict lookup cannot escape its own map, so this is
    strictly safer than the old per-request file-resolution path, not just
    faster) raises `ProfileValidationError`, handled by the caller exactly
    like the old "file not found"/"invalid YAML" failures were.

    Falls back to the original direct load-from-disk behavior only when no
    startup validation has run at all (`_VALIDATED_PROFILES is None` — a
    test importing this module directly without the FastAPI lifespan;
    production always calls `startup_validate_and_build_registry()`
    first)."""
    if _VALIDATED_PROFILES is not None:
        profile = _VALIDATED_PROFILES.get(config)
        if profile is None:
            raise ProfileValidationError(f"unknown config {config!r}")
        return profile
    config_file_path = resolve_config_path(Path(templateDir) / "configs", config)
    return load_profile(config_file_path)


def _overload_response(profile: DeploymentProfile) -> PlainTextResponse:
    return PlainTextResponse(
        json.dumps({"detail": "Exporter overloaded"}),
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        headers={"Retry-After": str(profile.tuning.overload.retry_after_seconds)},
    )


def _emit_metrics(registry, componentMetrics, collectedData, serverAddress, metricsConfig) -> None:
    """Unchanged from the pre-redesign emission logic — reads whatever shape
    `collectedData` has (today: `dataReconstructor`'s output; now: the
    read-time `CanonicalSnapshot` merge of Fast/Slow lane snapshots — both
    are the exact same `{componentName: [{Id:..., field:...}, ...]}` shape),
    so the public metric contract (FR-002/SC-001) is untouched."""
    hostName = collectedData['Common'][0].get('HostName', 'Unknown')
    # `research.md` Group 5 (item 4): `$..Id` is a full-tree JSONPath
    # traversal of the ENTIRE canonical snapshot — it does not depend on
    # which metric is currently being emitted, only on `collectedData`
    # itself, so computing it once per REQUEST (not once per metric,
    # `metricsConfig` may declare dozens) is behaviorally identical —
    # the exact same `idList` value is reused for every metric below,
    # never recomputed.
    idList = jsonpathCollector(collectedData, str("$..Id"), output='fullpath&value')
    for metric in metricsConfig:
        standard = ['Name', 'Description', 'Label', 'Datapoint', 'Result', 'Type']
        errorFlag = 0
        for key in standard:
            if key not in metric:
                logging.error("[%s] Can't find %s in metrics key, please check again!" % (serverAddress, key))
                errorFlag = 1
        if errorFlag == 1:
            continue
        else:
            if metric['Type'] == 'Gauge':
                componentMetrics[metric['Name']] = Gauge(metric['Name'], metric['Description'], metric['Label'], registry=registry)
                elements = metric['Datapoint'].split('.')
                logging.debug("[%s] Split datapoint %s to %s" % (serverAddress, metric['Datapoint'], elements))

                if not isinstance(collectedData.get(elements[0]), list):
                    logging.warning(f"[{serverAddress}] Data point root is not a list: {elements[0]}")
                    continue
                for memberID in idList:
                    if elements[-1] in memberID and elements[0] in memberID:
                        labelList = list()
                        for label in metric['Label']:
                            if label == 'ServerAddress':
                                labelList.append(serverAddress)
                            elif label == 'HostName':
                                labelList.append(hostName)
                            else:
                                newJSONPath = re.sub('Id', label, memberID)
                                result = jsonpathCollector(collectedData, newJSONPath)
                                if result is not False:
                                    labelList.append(result[0])
                                else:
                                    labelList.append('Unknown')
                                    continue
                        logging.debug("[%s] List Label: %s" % (serverAddress, labelList))
                        if 'State' in metric['Result'] or 'Health' in metric['Result']:
                            if 'StatusCode' not in metric:
                                logging.error("[%s] Can't find StatusCode, please check again!" % (serverAddress))
                                continue
                            state = 'Status.' + metric['Result']
                            newJSONPath = re.sub('Id', state, memberID)
                            logging.debug("[%s] newJSONPath: %s" % (serverAddress, newJSONPath))
                            value = jsonpathCollector(collectedData, str(newJSONPath))
                            if value is False:
                                logging.error("[%s] Value for %s isn't existed: %s" % (serverAddress, str(newJSONPath), value))
                                codeNumber = 999
                            elif value is None:
                                logging.warning("[%s] Value for %s is None: %s" % (serverAddress, str(newJSONPath), value))
                                codeNumber = 99
                            elif value[0] is None:
                                logging.warning("[%s] Value[0] for %s is None: %s" % (serverAddress, str(newJSONPath), value[0]))
                                codeNumber = 99
                            else:
                                if value[0].upper() in metric['StatusCode']:
                                    codeNumber = metric['StatusCode'][value[0].upper()]
                                    logging.debug("[%s] Value and CodeNumber: %s and %s" % (serverAddress, value, codeNumber))
                                else:
                                    logging.error("[%s] Maybe value isn't correct at %s: %s" % (serverAddress, metric['Name'], value))
                                    codeNumber = 999
                            componentMetrics[metric['Name']].labels(*labelList).set(float(codeNumber))
                        else:
                            newJSONPath = re.sub('Id', metric['Result'], memberID)
                            value = jsonpathCollector(collectedData, str(newJSONPath))
                            if value is False:
                                componentMetrics[metric['Name']].labels(*labelList).set(999)
                            else:
                                value = value[0]
                            logging.debug("Value type: %s" % type(value))
                            if isinstance(value, int) or isinstance(value, float):
                                componentMetrics[metric['Name']].labels(*labelList).set(float(value))
                            else:
                                logging.error("[%s] Value %s isn't float: %s" % (serverAddress, metric['Result'], value))
                                componentMetrics[metric['Name']].labels(*labelList).set(999)
                        logging.debug("[%s] ID List Collected with in tree %s to %s" % (serverAddress, metric['Datapoint'], labelList))
            else:
                logging.error("[%s] Not found Type %s, please call Admin" % (serverAddress, metric['Type']))


@router.get("", status_code=status.HTTP_200_OK)
async def read_all(serverAddress: IPvAnyAddress = Query(None), config: str = Query(None), loglevel: str = Query("info")) -> PlainTextResponse:
    registry = CollectorRegistry()
    componentMetrics = {'PhysicalServer_Query': Gauge('PhysicalServer_Query', 'physical server query status', ['ServerAddress'], registry=registry)}
    serverAddress_str = str(serverAddress) if serverAddress is not None else ""
    effective_loglevel = _validate_loglevel(loglevel)

    if (serverAddress is None) or (config is None):
        # User decision (round-of-repair): once a request reaches this
        # route (FastAPI's own query-param type coercion already ran, and
        # can still independently return 422 before this code executes —
        # e.g. a malformed IP literal), every subsequent invalid/
        # uncollectable condition — including missing required params —
        # returns HTTP 200 with `PhysicalServer_Query=0`, never HTTP 400.
        # This lets Prometheus distinguish "exporter is reachable" from
        # "target cannot be collected" using scrape success alone, and
        # keeps every failure branch below (unknown config, containment
        # failure, cold-Fast failure, ...) behaviorally consistent with
        # this one, rather than one param-shape special case returning a
        # different status code than every other invalid-request path.
        logging.error(f"[{serverAddress_str or '<missing>'}] missing required query param(s): "
                       f"serverAddress={serverAddress!r} config={config!r}")
        componentMetrics['PhysicalServer_Query'].labels(serverAddress_str).set(0)
        return PlainTextResponse(generate_latest(registry))

    try:
        canonical = canonicalize_address(serverAddress_str)
        validate_config_stem(config)
        profile = _resolve_profile(config)
    except (ContainmentError, ProfileValidationError) as err:
        logging.error(f"[{serverAddress_str}] config/profile validation failed: {err}")
        componentMetrics['PhysicalServer_Query'].labels(serverAddress_str).set(0)
        return PlainTextResponse(generate_latest(registry))

    target_registry, refresh_admission = _get_registry(profile)
    cache_key = (canonical, config)

    cached = target_registry.response_cache.get(cache_key)
    if cached is not None:
        existing = target_registry.targets.get(cache_key)
        if existing is None:
            # Invariant violation: a cache entry outlived its tracked
            # Target (e.g. evicted between publication and this read).
            # Never serve it independently of registry cardinality — treat
            # it as a miss and fall through to the normal collection path.
            target_registry.response_cache.invalidate(cache_key)
        else:
            existing.touch()
            return PlainTextResponse(cached)

    if not target_registry.admission.try_admit():
        return _overload_response(profile)

    try:
        try:
            target = await target_registry.get_or_create(cache_key)
        except RegistryFull:
            return _overload_response(profile)
        target.touch()

        lanes_due = set()
        if not target.fast.is_usable():
            lanes_due.add(Lane.FAST)
        if not target.slow.is_usable() and not target.slow_disabled:
            lanes_due.add(Lane.SLOW)

        accepted_lanes: frozenset = frozenset()
        cycle_context = None
        if lanes_due:
            # Always attempt attachment when any lane is due — including
            # while an active cycle already exists — so a newly-due Fast
            # lane can be dynamically attached to (and preempt Slow within)
            # an already-open Slow-only cycle, not just when starting a
            # brand-new cycle from cold.
            coordinator = target_registry.get_or_create_coordinator(
                canonical, capacity=profile.tuning.target_concurrency,
                max_queued_per_lane=profile.tuning.crawl.max_queued_requests_per_lane,
                fast_batch_limit=profile.tuning.crawl.fast_batch_limit,
                max_concurrent_cycles=profile.tuning.refresh.max_concurrent_cycles_per_bmc,
            )
            try:
                cycle_context, _, accepted_lanes = await ensure_cycle(
                    target, coordinator=coordinator,
                    cycle_deadline_seconds=profile.tuning.cycle.max_duration_seconds,
                    target_concurrency=profile.tuning.target_concurrency,
                    lanes_due=lanes_due,
                    login=make_login(profile, _COMMON_SCHEMA),
                    run_lane=make_run_lane(
                        profile, target, _COMMON_SCHEMA,
                        response_cache=target_registry.response_cache, target_registry=target_registry,
                    ),
                    on_close=None,
                    refresh_admission=refresh_admission,
                    logout_timeout_seconds=profile.tuning.logout.timeout_seconds,
                    # `research.md` Group 2 (items 1/3): the ONE shared,
                    # process-wide I/O budget and the configured
                    # per-request timeout/response-byte cap this cycle's
                    # login/GET/logout must all share.
                    io_byte_budget=target_registry.io_byte_budget,
                    max_response_bytes=profile.tuning.max_response_bytes,
                    request_timeout_seconds=profile.tuning.request.timeout_seconds,
                )
            except (CycleLeaseDeniedForTarget, GlobalRefreshAdmissionDeniedForTarget):
                # Non-blocking admission denial: a target with usable Fast
                # data skips this due refresh and retries on a later scrape;
                # a cold identity with no usable Fast data is overloaded.
                if not target.fast.is_usable():
                    return _overload_response(profile)

        # Only join a lane this cycle actually accepted (`ensure_cycle`'s
        # returned `accepted_lanes`), and only on the EXACT context object
        # `ensure_cycle()` returned for THIS request — never re-read the
        # mutable `target.active_cycle`, which may already have closed and
        # been replaced by a different cycle by the time this line runs
        # (e.g. a very fast cold cycle that opens, completes, and clears
        # `target.active_cycle` before this coroutine resumes). Re-reading
        # it would risk joining the WRONG cycle's Fast future.
        if Lane.FAST in accepted_lanes and cycle_context is not None:
            try:
                await join_lane(cycle_context, Lane.FAST, deadline_seconds=profile.tuning.fast.deadline_seconds)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass  # cold-start Fast-lane deadline exceeded; fall through to Query=0 below

        merged = target.canonical_snapshot()
        if not isinstance(merged, dict) or not merged.get('Common'):
            logging.error(f"[{serverAddress_str}] No usable Fast-lane snapshot; reporting target down.")
            componentMetrics['PhysicalServer_Query'].labels(serverAddress_str).set(0)
            return PlainTextResponse(generate_latest(registry))

        # FR-041: an optional, bounded, target-isolated debug artifact of
        # this request's post-reconstruction canonical snapshot — never
        # attempted unless BOTH the operator has enabled it
        # (`Tuning.Debug.WriteNormalizedData`) AND this specific request
        # opted in via `loglevel=debug` (a disabled store's own
        # `enabled()`/`write()` is already a no-op, but the `loglevel` gate
        # is checked first here so a debug-disabled deployment never even
        # constructs the merged-snapshot argument unnecessarily). `merged`
        # is already redacted by construction — it is the SAME
        # canonical/component data `_emit_metrics` below turns into public
        # metric values, never raw headers/tokens/credentials.
        debug_store = _get_debug_artifact_store(profile)
        if effective_loglevel == "debug" and debug_store.write_normalized_data:
            try:
                debug_store.write(canonical, config, "NormalizedData.json", merged)
            except Exception as debug_exc:  # noqa: BLE001 - a debug-artifact failure must never fail the scrape
                logging.warning(
                    "[%s] failed to write normalized debug artifact (event=debug_artifact_write_failed, error=%s)",
                    serverAddress_str, type(debug_exc).__name__,
                )

        _emit_metrics(registry, componentMetrics, merged, serverAddress_str, profile.metrics)

        componentMetrics['PhysicalServer_Query'].labels(serverAddress_str).set(1)
        body = generate_latest(registry)
        # `research.md` Group 3 (item 2): `target.slow.snapshot is not None`
        # stays true FOREVER once Slow ever published once, regardless of
        # whether it has since expired past its own TTL — the same
        # freshness check `merge_canonical_snapshot()` already uses
        # (`is_usable()`) is what determines whether Slow's data actually
        # contributed to `merged`/`body` above; the response-cache's own
        # `slow_generation`/`lane_freshness_deadline` bookkeeping must agree
        # with that, not with a stale "ever published" flag.
        slow_contributed = target.slow.is_usable()
        fast_deadline = (target.fast.last_success_at or 0) + target.fast.ttl_seconds
        if slow_contributed:
            # The response includes both lanes' data — it is only as fresh
            # as the EARLIER of the two lanes' own freshness deadlines, not
            # Fast's alone (`ResponseCache` §"population rule").
            slow_deadline = (target.slow.last_success_at or 0) + target.slow.ttl_seconds
            lane_deadline = min(fast_deadline, slow_deadline)
        else:
            lane_deadline = fast_deadline
        target_registry.response_cache.put(
            cache_key, body,
            fast_generation=target.fast.generation,
            slow_generation=target.slow.generation if slow_contributed else None,
            lane_freshness_deadline=lane_deadline,
            ttl_seconds=profile.tuning.cache.response_ttl_seconds,
        )
        return PlainTextResponse(body)

    except Exception as err:
        componentMetrics['PhysicalServer_Query'].labels(serverAddress_str).set(0)
        # `str(err)` can embed a raw URL/Location/query (e.g. a
        # `SameTargetViolation` or an aiohttp exception built from a
        # request URL) — never interpolated directly; `exc_info=True` also
        # dropped so the traceback (which can include the same raw values
        # in frame locals/repr) never reaches the log either.
        logging.error(f"[{serverAddress_str}] Metric collection failed: {safe_exception_summary(err)}")
        return PlainTextResponse(generate_latest(registry))
    finally:
        target_registry.admission.release()
