"""End-to-end v2 collection pipeline: bootstrap -> select -> execute -> extract.

Pure orchestration over the schema-engine primitives (`bootstrap.py`,
`selection.py`, `executor.py`, `legacy.py`) and an injectable `fetch`
callable — no direct dependency on `aiohttp`/sessions/the dispatcher, so it
is reusable unchanged by the later lane facade (T042) once that supplies a
real, authenticated, dispatcher-bounded `fetch`.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from ..batching import DEFAULT_BATCH_SIZE
from .bootstrap import discover_bootstrap
from .executor import ExecutedRecord, ExecutorError, FetchFunc, TelemetryReducerFunc, execute_children, execute_resource, extract_component
from .legacy import collect_legacy_lane, derive_serverid_from_system_uri, select_legacy_vendor_schema
from .models import CommonSchemaV2, LegacySchema, VendorModelSchemaV2
from .selection import SchemaSelectionError, select_v2_model_schema

logger = logging.getLogger(__name__)


async def collect_v2_resources(
    vendor_schema: VendorModelSchemaV2,
    *,
    lane_resources: tuple[str, ...],
    fetch: FetchFunc,
    context: dict[str, Any],
    max_depth: int = 4,
    telemetry_reducer: Optional[TelemetryReducerFunc] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pagination_next_link_paths: tuple[str, ...] = ('$."Members@odata.nextLink"', '$."@odata.nextLink"'),
    pagination_max_pages: int = 50,
    max_members: int = 4096,
) -> dict[str, list[ExecutedRecord]]:
    """Executes exactly the resources in `lane_resources` (a lane's Fast or
    Slow resource list) and their declared Children, recursively. `batch_size`
    (`Tuning.Crawl.BatchSize`) bounds concurrent member/child fetches inside
    `execute_resource`/`execute_children` — never a list-sized fan-out.
    `pagination_next_link_paths`/`pagination_max_pages`/`max_members` are
    `Safety.Pagination`/`Safety.CollectionLimits` from the selected vendor
    schema's top-level `CommonSchema` (`contracts/schema-format-contract.md`
    §1) — see `executor._resolve_members`/`_collect_paginated_member_refs`."""
    resource_raw_by_name: dict[str, Any] = {}
    executed: dict[str, list[ExecutedRecord]] = {}
    for name in lane_resources:
        resource = vendor_schema.resources[name]
        try:
            records = await execute_resource(
                resource, fetch=fetch, context=context, resource_raw_by_name=resource_raw_by_name,
                telemetry_reducer=telemetry_reducer, batch_size=batch_size,
                pagination_next_link_paths=pagination_next_link_paths,
                pagination_max_pages=pagination_max_pages, max_members=max_members,
            )
        except ExecutorError as exc:
            # `Required` (contract §5): a required resource's failure is
            # lane-scoped-fatal — re-raise unchanged so the caller's existing
            # `except ExecutorError` -> `TargetSelectionError` path applies.
            # An optional resource's failure (the default) must NOT abort
            # the rest of the lane — every other declared resource still
            # gets a chance to collect. Recorded as an empty result, which
            # `extract_v2_components` already treats as "this resource
            # produced nothing" (never as a crash), i.e. the contract's
            # "Optional resource failure omits that resource."
            if resource.required:
                raise
            logger.warning("optional resource %r failed and was omitted: %s", name, exc)
            executed[name] = []
            continue
        await execute_children(
            resource, records, fetch=fetch, context=context, resource_raw_by_name=resource_raw_by_name,
            telemetry_reducer=telemetry_reducer, max_depth=max_depth, batch_size=batch_size,
            pagination_next_link_paths=pagination_next_link_paths,
            pagination_max_pages=pagination_max_pages, max_members=max_members,
        )
        executed[name] = records
        if records:
            resource_raw_by_name[name] = records[0].raw
    return executed


def extract_v2_components(
    vendor_schema: VendorModelSchemaV2, resources: dict[str, list[ExecutedRecord]]
) -> dict[str, list[dict[str, Any]]]:
    """Produces the same `{componentName: [record, ...]}` shape today's
    `dataReconstructor` output has — the emission loop in `prometheus.py`
    is unchanged by whether this or the legacy path produced it."""
    collected: dict[str, list[dict[str, Any]]] = {}
    for name, component in vendor_schema.components.items():
        # Only extract components whose resource was actually collected in
        # this lane pass (Fast lane components reference Fast resources only).
        top_resource = component.records_resource.split(".")[0]
        if top_resource not in resources:
            continue
        collected[name] = extract_component(component, resources=resources, missing_value=vendor_schema.missing_value)
    return collected


class TargetSelectionError(Exception):
    """Clean, target-selection-time failure (Query=0), never a Python crash."""


async def bootstrap_and_select(common: CommonSchemaV2, fetch: FetchFunc) -> tuple[dict[str, Any], Any, Any]:
    """Runs bootstrap discovery + deterministic model selection exactly
    once. Returns `(bootstrap_ctx, system_obj, schema)` so a caller (the
    refresh cycle) can perform this once per authenticated cycle and reuse
    the result for every subsequent Fast/Slow lane invocation instead of
    repeating Service Root/Systems-collection/System discovery per lane per
    generation (`research.md` §C)."""
    try:
        bootstrap_ctx, system_obj = await discover_bootstrap(common, fetch)
    except ExecutorError as exc:
        raise TargetSelectionError(f"bootstrap discovery failed: {exc}") from exc

    try:
        selection = select_v2_model_schema(
            common, manufacturer=bootstrap_ctx["manufacturer"], model=bootstrap_ctx["model"], capabilities=bootstrap_ctx
        )
    except SchemaSelectionError as exc:
        raise TargetSelectionError(str(exc)) from exc

    schema = selection.rule.schema
    if schema is None:
        raise TargetSelectionError(f"selected schema {selection.rule.schema_file!r} could not be loaded")
    return bootstrap_ctx, system_obj, schema


async def collect_v2_target(
    common: CommonSchemaV2,
    *,
    fetch: FetchFunc,
    lane: str,
    legacy_lane_collector: Any = collect_legacy_lane,
    legacy_session: Any = None,
    legacy_semaphore: Any = None,
    legacy_token: str = "",
    legacy_log_level: str = "info",
    server_address: str = "",
    bootstrap_ctx: Optional[dict[str, Any]] = None,
    selected_schema: Any = None,
    system_obj_for_reuse: Any = None,
    legacy_dispatcher: Any = None,
    legacy_cycle_id: Optional[str] = None,
    legacy_deadline: Optional[float] = None,
    legacy_byte_budget: Any = None,
    legacy_timeout_seconds: Optional[float] = None,
    legacy_max_redirects: int = 3,
    legacy_follow_redirects: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_depth: int = 4,
) -> dict[str, list[dict[str, Any]]]:
    """Dispatches to either the v2 executor or the permanent v1 bridge, for
    exactly one lane ("fast" or "slow"). Returns the component-extraction
    result (v2 shape).

    When `bootstrap_ctx`/`selected_schema` are both supplied (the refresh
    cycle's normal path after its first lane call), bootstrap discovery and
    model selection are skipped entirely — the caller already ran
    `bootstrap_and_select()` once for this cycle. When either is omitted
    (legacy direct-call contract still exercised by
    `tests/contract/test_v2_schema_compat.py` and the lane-facade unit
    tests), this performs its own one-shot discovery/selection exactly as
    before, so no existing caller's contract changes.

    `system_obj_for_reuse`, if given, is consumed at most once: if the
    selected v2 schema's Common resource resolves to the exact bootstrap
    `system_uri`, its already-fetched raw body is reused instead of issuing
    a second GET for the same resource — only meaningful for the very first
    Fast collection of a cycle; every later Fast generation must omit this
    (refetch the System URI for freshness, per `research.md` §C) and every
    Slow collection must never receive Common in its own resource list in
    the first place (Fast/Slow are schema-load-time non-overlapping)."""
    if lane not in ("fast", "slow"):
        raise ValueError("lane must be 'fast' or 'slow'")

    if bootstrap_ctx is not None and selected_schema is not None:
        context = bootstrap_ctx
        schema = selected_schema
    else:
        context, _system_obj, schema = await bootstrap_and_select(common, fetch)
        if system_obj_for_reuse is None:
            system_obj_for_reuse = _system_obj

    if isinstance(schema, VendorModelSchemaV2):
        lane_resources = schema.fast_resources if lane == "fast" else schema.slow_resources
        effective_fetch = fetch
        if system_obj_for_reuse is not None:
            effective_fetch = _reuse_once_fetch(fetch, context.get("system_uri"), system_obj_for_reuse)
        try:
            resources = await collect_v2_resources(
                schema, lane_resources=lane_resources, fetch=effective_fetch, context=context,
                batch_size=batch_size, max_depth=max_depth,
                pagination_next_link_paths=common.safety.pagination_next_link_paths,
                pagination_max_pages=common.safety.pagination_max_pages,
                max_members=common.safety.max_members,
            )
        except ExecutorError as exc:
            raise TargetSelectionError(f"resource collection failed: {exc}") from exc
        return extract_v2_components(schema, resources)

    if isinstance(schema, LegacySchema):
        try:
            serverid = derive_serverid_from_system_uri(context["system_uri"])
        except ValueError as exc:
            raise TargetSelectionError(str(exc)) from exc
        return await legacy_lane_collector(
            schema.raw["Metadata"], {"serverid": serverid}, legacy_token, legacy_log_level,
            legacy_session, legacy_semaphore, server_address, lane=lane,
            dispatcher=legacy_dispatcher, cycle_id=legacy_cycle_id, deadline=legacy_deadline,
            byte_budget=legacy_byte_budget, timeout_seconds=legacy_timeout_seconds, batch_size=batch_size,
            max_redirects=legacy_max_redirects, follow_redirects=legacy_follow_redirects,
        )

    raise TargetSelectionError("selected schema could not be loaded")


def _reuse_once_fetch(fetch: FetchFunc, system_uri: Optional[str], system_obj: Any) -> FetchFunc:
    """Wraps `fetch` so the FIRST request for `system_uri` returns the
    already-fetched `system_obj` instead of issuing a real GET; every other
    URL (and every request after the first System hit) goes through the
    real `fetch` unchanged. A plain closure flag, not a cache — this is a
    single-use substitution for one specific resource within one lane
    collection call, never a general response cache."""
    consumed = False

    async def wrapped(url: str) -> Any:
        nonlocal consumed
        if not consumed and system_uri is not None and url == system_uri:
            consumed = True
            return system_obj
        return await fetch(url)

    return wrapped
