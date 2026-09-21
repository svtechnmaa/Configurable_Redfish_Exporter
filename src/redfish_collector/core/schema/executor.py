"""Vendor-neutral v2 schema executor (`contracts/schema-format-contract.md` §4-§6).

Consumes normalized `Resources`/`Strategies`/`Components`/`Fields` from
`schema/models.py`. Contains no HPE/Dell/Lenovo branch or model-name branch.
Decoupled from the concrete HTTP/session/dispatcher layer: callers supply an
async `fetch(url) -> dict` callable (already same-target/redirect-contained,
retried, size-bounded — those are `rawCollector`/`security` concerns, not
the executor's). A `telemetry_reducer` callable is injected for
`Kind: telemetry-reports` strategies; the executor never reduces telemetry
itself (`telemetry/reports.py` owns that, T050).
"""

from __future__ import annotations

import logging

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from jinja2 import Template

from ..batching import DEFAULT_BATCH_SIZE
from .models import ComponentDef, FieldDef, ResourceDef, StrategyDef, VendorModelSchemaV2
from .transforms import TransformError, apply_transforms, validate_type
from .validation import compile_jsonpath

FetchFunc = Callable[[str], Awaitable[Any]]
TelemetryReducerFunc = Callable[[StrategyDef, Any, FetchFunc], Awaitable[list[dict[str, Any]]]]

_ODATA_ID = "@odata.id"

# `Safety.Pagination`/`Safety.CollectionLimits` defaults
# (`contracts/schema-format-contract.md` §1) — mirrors
# `schema/models.py::SafetyConfig`'s own defaults exactly, so a caller that
# doesn't yet thread a real `SafetyConfig` (existing direct-call tests,
# `bootstrap.py`'s own internal collection reads) preserves prior behavior.
_DEFAULT_PAGINATION_NEXT_LINK_PATHS: tuple[str, ...] = (
    '$."Members@odata.nextLink"',
    '$."@odata.nextLink"',
)
_DEFAULT_PAGINATION_MAX_PAGES = 50
_DEFAULT_MAX_MEMBERS = 4096


class ExecutorError(Exception):
    """A resource has no successful strategy, or a required-lane failure."""


def _jsonpath_first(expr: str, data: Any) -> Any:
    matches = compile_jsonpath(expr, context="executor").find(data)
    return matches[0].value if matches else None


def _jsonpath_all(expr: str, data: Any) -> list[Any]:
    return [m.value for m in compile_jsonpath(expr, context="executor").find(data)]


@dataclass
class ExecutedRecord:
    """One raw record (unmodified — `@odata.*` keys are left in place since a
    parent's `@odata.id` links are exactly what a `Children` strategy may
    still need to resolve) plus its child resources, keyed by child resource
    name, each itself a list of `ExecutedRecord` (collections) so nested
    Components can walk the exact parent/child lineage the contract
    requires — no global identity guess."""

    raw: dict[str, Any]
    children: dict[str, list["ExecutedRecord"]] = field(default_factory=dict)
    # Contract §4 `Capture`: values extracted from THIS record's own raw
    # body, available only to this record's own child strategies (merged
    # into their `context` by `execute_children`) — never global, never
    # visible to siblings or to this same strategy's own URI resolution.
    captures: dict[str, Any] = field(default_factory=dict)


async def execute_resource(
    resource: ResourceDef,
    *,
    fetch: FetchFunc,
    context: dict[str, Any],
    resource_raw_by_name: dict[str, Any],
    telemetry_reducer: Optional[TelemetryReducerFunc] = None,
    parent_body: Any = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pagination_next_link_paths: tuple[str, ...] = _DEFAULT_PAGINATION_NEXT_LINK_PATHS,
    pagination_max_pages: int = _DEFAULT_PAGINATION_MAX_PAGES,
    max_members: int = _DEFAULT_MAX_MEMBERS,
) -> list[ExecutedRecord]:
    """Runs `resource`'s Strategies in declared order; the first strategy
    that succeeds wins (no second fallback). Always returns a list — one
    element for an `object`/`inline-collection`-with-one-member resource,
    N elements for a `collection`. Raises `ExecutorError` if every strategy
    fails or is skipped by `When`.

    `pagination_next_link_paths`/`pagination_max_pages`/`max_members` are
    `Safety.Pagination`/`Safety.CollectionLimits` from the schema
    (`contracts/schema-format-contract.md` §1) — see `_resolve_members`."""
    last_error: Optional[str] = None
    for strategy in resource.strategies:
        try:
            records = await _execute_strategy(
                strategy,
                fetch=fetch,
                context=context,
                resource_raw_by_name=resource_raw_by_name,
                telemetry_reducer=telemetry_reducer,
                parent_body=parent_body,
                batch_size=batch_size,
                pagination_next_link_paths=pagination_next_link_paths,
                pagination_max_pages=pagination_max_pages,
                max_members=max_members,
            )
            if records is None:  # `When` gate did not match -> try next strategy
                continue
            return records
        except ExecutorError as exc:
            last_error = str(exc)
            continue
    raise ExecutorError(f"no strategy succeeded (last: {last_error})")


def _resolve_uri(
    strategy: StrategyDef, *, context: dict[str, Any], resource_raw_by_name: dict[str, Any], parent_body: Any = None
) -> Optional[str]:
    if strategy.uri_template is not None:
        return Template(strategy.uri_template).render(context)
    if strategy.uri_from_capture is not None:
        value = context.get(strategy.uri_from_capture)
        if not value:
            raise ExecutorError(f"URIFrom.Capture {strategy.uri_from_capture!r} is empty/absent")
        return value
    if strategy.uri_from_resource is not None:
        source = resource_raw_by_name.get(strategy.uri_from_resource)
        if source is None:
            raise ExecutorError(f"URIFrom.Resource {strategy.uri_from_resource!r} has not been fetched")
        value = _jsonpath_first(strategy.uri_from_path, source)
        if not value:
            raise ExecutorError(f"URIFrom.Resource/Path yielded no URI from {strategy.uri_from_resource!r}")
        return value
    if strategy.uri_from_parent_path is not None:
        # Contract §4: "one URI source for child strategies... forbidden on
        # top-level resources" — `parent_body` is only ever non-None for a
        # child strategy (`execute_children` passes the owning parent
        # record's raw body; a top-level `execute_resource` call never
        # does), so this also enforces that restriction at runtime.
        if parent_body is None:
            raise ExecutorError("URIFrom.ParentPath is forbidden on a top-level resource strategy")
        value = _jsonpath_first(strategy.uri_from_parent_path, parent_body)
        if not value:
            raise ExecutorError(f"URIFrom.ParentPath {strategy.uri_from_parent_path!r} yielded no URI from the parent record")
        return value
    return None  # inline-collection: no root fetch


async def _execute_strategy(
    strategy: StrategyDef,
    *,
    fetch: FetchFunc,
    context: dict[str, Any],
    resource_raw_by_name: dict[str, Any],
    telemetry_reducer: Optional[TelemetryReducerFunc],
    parent_body: Any,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pagination_next_link_paths: tuple[str, ...] = _DEFAULT_PAGINATION_NEXT_LINK_PATHS,
    pagination_max_pages: int = _DEFAULT_PAGINATION_MAX_PAGES,
    max_members: int = _DEFAULT_MAX_MEMBERS,
) -> Optional[list[ExecutedRecord]]:
    if strategy.kind == "telemetry-reports":
        return await _execute_telemetry_reports(strategy, fetch=fetch, context=context, telemetry_reducer=telemetry_reducer)

    if strategy.kind == "inline-collection":
        if strategy.uri_from_parent_path is not None:
            root = _jsonpath_first(strategy.uri_from_parent_path, parent_body)
        else:
            root = parent_body
        # Contract §1: pagination is defined in terms of a fetched
        # collection response's own `NextLinkPaths` — an inline-collection
        # has no such response of its own (its "root" is the parent's
        # already-fetched body), so only `MaxMembers` truncation applies.
        members_raw = _jsonpath_all(strategy.members, root)
        return await _resolve_members(members_raw, fetch=fetch, capture=strategy.capture, batch_size=batch_size, max_members=max_members)

    uri = _resolve_uri(strategy, context=context, resource_raw_by_name=resource_raw_by_name, parent_body=parent_body)
    if uri is None:
        raise ExecutorError(f"strategy {strategy.id!r}: no URI resolved")
    root = await fetch(uri)

    if strategy.when_path is not None:
        value = _jsonpath_first(strategy.when_path, root)
        allowed = strategy.when_equals if isinstance(strategy.when_equals, list) else [strategy.when_equals]
        if value not in allowed:
            return None  # skip this strategy, try the next

    if strategy.kind == "object":
        return [ExecutedRecord(raw=root, captures=_evaluate_captures(strategy.capture, root))]

    # collection
    member_refs = await _collect_paginated_member_refs(
        strategy, root, fetch=fetch,
        next_link_paths=pagination_next_link_paths, max_pages=pagination_max_pages,
    )
    return await _resolve_members(member_refs, fetch=fetch, capture=strategy.capture, batch_size=batch_size, max_members=max_members)


def _first_pagination_link(root: Any, next_link_paths: tuple[str, ...]) -> Optional[str]:
    for path_expr in next_link_paths:
        value = _jsonpath_first(path_expr, root)
        if value:
            return value
    return None


async def _collect_paginated_member_refs(
    strategy: StrategyDef, root: Any, *, fetch: FetchFunc, next_link_paths: tuple[str, ...], max_pages: int,
) -> list[Any]:
    """`Safety.Pagination` (`contracts/schema-format-contract.md` §1):
    follows `NextLinkPaths` (checked in declared order; the first that
    yields a value wins) up to `MaxPages` total pages, appending each
    page's own `strategy.members` matches. A same-target-validated,
    same-target-redirect-contained, size-bounded fetch of the next link —
    `fetch` here is the SAME injected callable every other resource fetch
    in this executor uses, never a separate unguarded HTTP call. Needing
    more pages than `MaxPages` is a bounded-limit violation for this
    resource's fetch (fails, does not silently stop at a partial page
    set) — the whole-scrape-preserving truncation instead applies to
    `MaxMembers`, a distinct bound (`_resolve_members`)."""
    member_refs = list(_jsonpath_all(strategy.members, root))
    current_root = root
    pages_seen = 1
    while True:
        next_link = _first_pagination_link(current_root, next_link_paths)
        if next_link is None:
            return member_refs
        if pages_seen >= max_pages:
            logging.warning(
                "pagination bound exceeded (event=pagination_max_pages_exceeded, max_pages=%s)", max_pages
            )
            raise ExecutorError(f"pagination exceeded MaxPages={max_pages}")
        current_root = await fetch(next_link)
        member_refs.extend(_jsonpath_all(strategy.members, current_root))
        pages_seen += 1


def _evaluate_captures(capture: dict[str, str], body: Any) -> dict[str, Any]:
    return {name: _jsonpath_first(path_expr, body) for name, path_expr in capture.items()}


async def _resolve_members(
    member_refs: list[Any], *, fetch: FetchFunc, capture: Optional[dict[str, str]] = None,
    batch_size: int = DEFAULT_BATCH_SIZE, max_members: int = _DEFAULT_MAX_MEMBERS,
) -> list[ExecutedRecord]:
    """Per contract §4: a member value may be a link (string, or a dict
    carrying only `@odata.id`) that must be fetched, or a fully-inlined
    object retained directly — both are valid for `collection` and
    `inline-collection` alike. `capture`, if given, is evaluated against
    EACH member's own fetched/inline body — never the parent collection
    body — so a per-controller value (e.g. its own `Id`) is available to
    only that specific member's own children.

    Deliberately SEQUENTIAL (`research.md` §C / round-9 finding, `batch_size`
    accepted but unused here): wrapping each member fetch in its own
    concurrent task was tried and reverted in an earlier round — it
    measurably shifted event-loop scheduling order between concurrently-
    running work (this target's own Slow lane; other targets'/lanes'
    cycles sharing the same loop) closely enough to trigger a real,
    silent, no-exception-raised data-drop in `run_as_cycle_owned_request`'s
    ambient-task registration and the legacy `rawDataCollector` keyDict-
    mutation ordering hazard (see `run_as_cycle_owned_request`'s own
    docstring in `refresh_context.py`). `Tuning.Crawl.BatchSize` is instead
    applied to the two genuinely list-sized, flat fan-outs that do not
    share this hazard: `rawCollector.fetch_all` and `schema/legacy.py`'s
    `collect_legacy_lane`.

    `Safety.CollectionLimits.MaxMembers` (contract §1): total members read
    across ALL pages of one collection; exceeding it TRUNCATES at the
    limit and logs a bounded-limit warning — unlike `MaxPages`, this never
    fails the whole scrape."""
    if len(member_refs) > max_members:
        logging.warning(
            "collection member count exceeded MaxMembers, truncating "
            "(event=collection_max_members_truncated, max_members=%s, actual=%s)",
            max_members, len(member_refs),
        )
        member_refs = member_refs[:max_members]
    records: list[ExecutedRecord] = []
    for member in member_refs:
        if isinstance(member, dict) and _ODATA_ID in member:
            member = member[_ODATA_ID]
        if isinstance(member, str):
            member_body = await fetch(member)
            records.append(ExecutedRecord(raw=member_body, captures=_evaluate_captures(capture or {}, member_body)))
        elif isinstance(member, dict):
            records.append(ExecutedRecord(raw=member, captures=_evaluate_captures(capture or {}, member)))
    return records


async def _execute_telemetry_reports(
    strategy: StrategyDef, *, fetch: FetchFunc, context: dict[str, Any], telemetry_reducer: Optional[TelemetryReducerFunc]
) -> list[ExecutedRecord]:
    if telemetry_reducer is None:
        raise ExecutorError("telemetry-reports strategy present but no telemetry_reducer was injected")
    telemetry_service_uri = context.get("telemetry_service")
    if not telemetry_service_uri:
        raise ExecutorError("telemetry_service capability is absent")
    root = await fetch(telemetry_service_uri)
    values = await telemetry_reducer(strategy, root, fetch)
    return [ExecutedRecord(raw=v) for v in values]


async def execute_children(
    resource: ResourceDef, records: list[ExecutedRecord], *, fetch: FetchFunc, context: dict[str, Any],
    resource_raw_by_name: dict[str, Any], telemetry_reducer: Optional[TelemetryReducerFunc], max_depth: int, depth: int = 1,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pagination_next_link_paths: tuple[str, ...] = _DEFAULT_PAGINATION_NEXT_LINK_PATHS,
    pagination_max_pages: int = _DEFAULT_PAGINATION_MAX_PAGES,
    max_members: int = _DEFAULT_MAX_MEMBERS,
) -> None:
    """Populates `record.children[name]` for every declared child resource,
    scoped per-parent-record (the deterministic join rule — no cross-parent
    identity guess). Mutates `records` in place.

    Deliberately SEQUENTIAL (`batch_size` accepted, currently unused here —
    see `_resolve_members`'s docstring for why: converting this to
    concurrent per-(parent,child) dispatch was tried and reverted after it
    reproduced a real, silent data-drop against the golden end-to-end
    fixture baseline, caused by a subtle event-loop-scheduling interaction
    with `run_as_cycle_owned_request`'s ambient-task tracking and the
    legacy `rawDataCollector` keyDict-mutation ordering hazard)."""
    if depth > max_depth:
        return
    child_defs: dict[str, ResourceDef] = {}
    for strategy in resource.strategies:
        child_defs.update(strategy.children)
    if not child_defs:
        return
    for parent_record in records:
        # Contract §4 `Capture`: this record's own captures are visible
        # only to ITS children, merged over (never replacing) the
        # ambient/bootstrap context — never propagated to siblings or back
        # up to the parent's own strategy resolution.
        child_context = {**context, **parent_record.captures} if parent_record.captures else context
        for child_name, child_resource in child_defs.items():
            try:
                child_records = await execute_resource(
                    child_resource,
                    fetch=fetch,
                    context=child_context,
                    resource_raw_by_name=resource_raw_by_name,
                    telemetry_reducer=telemetry_reducer,
                    parent_body=parent_record.raw,
                    batch_size=batch_size,
                    pagination_next_link_paths=pagination_next_link_paths,
                    pagination_max_pages=pagination_max_pages,
                    max_members=max_members,
                )
            except ExecutorError:
                child_records = []
            parent_record.children[child_name] = child_records
            await execute_children(
                child_resource, child_records, fetch=fetch, context=child_context,
                resource_raw_by_name=resource_raw_by_name, telemetry_reducer=telemetry_reducer,
                max_depth=max_depth, depth=depth + 1, batch_size=batch_size,
                pagination_next_link_paths=pagination_next_link_paths,
                pagination_max_pages=pagination_max_pages,
                max_members=max_members,
            )


# ---------------------------------------------------------------------------
# Components / Fields extraction
# ---------------------------------------------------------------------------


def _navigate(dotted_path: str, top_level: dict[str, list[ExecutedRecord]]) -> list[ExecutedRecord]:
    parts = dotted_path.split(".")
    current = top_level.get(parts[0], [])
    for part in parts[1:]:
        next_records: list[ExecutedRecord] = []
        for record in current:
            next_records.extend(record.children.get(part, []))
        current = next_records
    return current


def extract_field(record: ExecutedRecord, field_def: FieldDef, *, missing_value: str) -> Any:
    for selector in field_def.select:
        if selector.path is None:
            continue  # telemetry selectors are resolved before extraction (values are pre-substituted)
        raw_value = _jsonpath_first(selector.path, record.raw)
        try:
            value = apply_transforms(raw_value, selector.transforms)
            return validate_type(value, field_def.type)
        except TransformError:
            continue
    if field_def.type == "status":
        return validate_type(None, "status")
    return missing_value


def extract_component(
    component: ComponentDef, *, resources: dict[str, list[ExecutedRecord]], missing_value: str,
) -> list[dict[str, Any]]:
    """Public entry point: `component.records_resource` is a dotted path
    resolved from the top-level `resources` dict (as produced by
    `collect_v2_resources`)."""
    records = _navigate(component.records_resource, resources)
    return _extract_from_records(component, records, missing_value=missing_value)


def _extract_from_records(
    component: ComponentDef, records: list[ExecutedRecord], *, missing_value: str,
) -> list[dict[str, Any]]:
    if component.records_select == "object":
        records = records[:1]

    output: list[dict[str, Any]] = []
    for record in records:
        entry: dict[str, Any] = {}
        for identity_path in component.identity_select:
            value = _jsonpath_first(identity_path, record.raw)
            if value is not None:
                entry["Id"] = value
                break
        else:
            entry["Id"] = missing_value
        for field_name, field_def in component.fields.items():
            entry[field_name] = extract_field(record, field_def, missing_value=missing_value)
        for child_name, child_component in component.children.items():
            # The child's records were already attached by `execute_children`
            # under this exact parent record (the deterministic join rule) —
            # recurse directly on them, never re-resolving a dotted path.
            child_records = record.children.get(child_name, [])
            entry[child_name] = _extract_from_records(child_component, child_records, missing_value=missing_value)
        output.append(entry)
    return output
