"""Version/Kind discriminated schema loader (`contracts/schema-format-contract.md`).

`Version` absent -> v1-legacy path (`LegacySchema`, raw dict, never
reinterpreted). `Version: 2` -> full structural parse/validate into
`CommonSchemaV2`/`VendorModelSchemaV2`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Union
from urllib.parse import urlsplit

import yaml

from .models import (
    BootstrapBlock,
    CommonSchemaV2,
    ComponentDef,
    FieldDef,
    LegacySchema,
    ModelSchemaRule,
    ResourceDef,
    SafetyConfig,
    SelectorDef,
    StrategyDef,
    TransformDef,
    VendorModelSchemaV2,
)
from ..security.containment import ContainmentError, resolve_schema_path
from .validation import SchemaValidationError, compile_jsonpath, compile_regex, reject_unknown_keys

SUPPORTED_VERSION = 2
_TRANSFORM_OPS = {"to-number", "multiply", "divide", "add", "round", "map"}
_FIELD_TYPES = {"string", "number", "boolean", "status", "object"}
_STRATEGY_KINDS = {"object", "collection", "inline-collection", "telemetry-reports"}


def load_schema_file(path: Path, *, schemas_dir: Path | None = None) -> Union[CommonSchemaV2, VendorModelSchemaV2, LegacySchema]:
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{path.name}: schema file must be a YAML mapping")

    if "Version" not in raw:
        return LegacySchema(path=str(path), raw=raw)

    version = raw["Version"]
    if version != SUPPORTED_VERSION:
        raise SchemaValidationError(f"{path.name}: unsupported Version {version!r} (only {SUPPORTED_VERSION} is supported)")

    kind = raw.get("Kind")
    if kind not in ("CommonSchema", "VendorModelSchema"):
        raise SchemaValidationError(f"{path.name}: Kind must be 'CommonSchema' or 'VendorModelSchema', got {kind!r}")

    if kind == "CommonSchema":
        return _load_common_v2(raw, path, schemas_dir=schemas_dir or path.parent)
    return _load_vendor_model_v2(raw, path)


# ---------------------------------------------------------------------------
# CommonSchema (v2)
# ---------------------------------------------------------------------------

_COMMON_TOP_KEYS = {"Version", "Kind", "Safety", "Bootstrap", "ModelSchemas", "Selection"}


def _load_common_v2(raw: dict[str, Any], path: Path, *, schemas_dir: Path) -> CommonSchemaV2:
    reject_unknown_keys(raw, _COMMON_TOP_KEYS, context=path.name)

    safety = _parse_safety(raw.get("Safety", {}) or {}, path)
    bootstrap = _parse_bootstrap(raw.get("Bootstrap", {}) or {}, path)

    selection_raw = raw.get("Selection", {}) or {}
    reject_unknown_keys(selection_raw, {"RequireUniqueBestMatch", "OnNoMatch"}, context=f"{path.name}: Selection")
    if selection_raw.get("RequireUniqueBestMatch", True) is not True:
        raise SchemaValidationError(f"{path.name}: Selection.RequireUniqueBestMatch must not be set to false")
    on_no_match = selection_raw.get("OnNoMatch", "unsupported-model")
    if on_no_match != "unsupported-model":
        raise SchemaValidationError(f"{path.name}: Selection.OnNoMatch only supports 'unsupported-model'")

    model_schemas_raw = raw.get("ModelSchemas")
    if not isinstance(model_schemas_raw, list) or not model_schemas_raw:
        raise SchemaValidationError(f"{path.name}: ModelSchemas must be a non-empty list")

    seen_ids: set[str] = set()
    rules: list[ModelSchemaRule] = []
    for i, entry in enumerate(model_schemas_raw):
        context = f"{path.name}: ModelSchemas[{i}]"
        if not isinstance(entry, dict):
            raise SchemaValidationError(f"{context} must be a mapping")
        reject_unknown_keys(
            entry,
            {"Id", "Priority", "ManufacturerRegex", "ModelRegex", "RequiredCapabilities", "Schema"},
            context=context,
        )
        for key in ("Id", "Priority", "ManufacturerRegex", "ModelRegex", "Schema"):
            if key not in entry:
                raise SchemaValidationError(f"{context}: missing required key {key!r}")
        rule_id = entry["Id"]
        if rule_id in seen_ids:
            raise SchemaValidationError(f"{context}: duplicate ModelSchemas Id {rule_id!r}")
        seen_ids.add(rule_id)
        compile_regex(entry["ManufacturerRegex"], context=f"{context}.ManufacturerRegex")
        compile_regex(entry["ModelRegex"], context=f"{context}.ModelRegex")

        schema_obj = None
        unavailable_reason = None
        schema_file = entry["Schema"]
        try:
            # Exact basename + single `.yml` suffix (no path separators, no
            # double extension) and realpath-under-`schemas_dir` containment
            # — enforced BEFORE any stat/open, same as the `config` query
            # parameter's containment. A schema file is admin-authored, not
            # attacker-controlled at request time, but a malformed or
            # traversal-shaped `Schema` entry must still fail this one rule
            # closed rather than silently reading/stat-ing outside the
            # schemas directory.
            schema_path = resolve_schema_path(schemas_dir, schema_file)
        except ContainmentError as exc:
            unavailable_reason = f"referenced schema file {schema_file!r} failed containment: {exc}"
        else:
            if not schema_path.is_file() or schema_path.stat().st_size == 0:
                unavailable_reason = f"referenced schema file {schema_file!r} is missing or empty"
            else:
                try:
                    schema_obj = load_schema_file(schema_path, schemas_dir=schemas_dir)
                except SchemaValidationError as exc:
                    unavailable_reason = f"referenced schema file {schema_file!r} failed to load: {exc}"

        rules.append(
            ModelSchemaRule(
                id=rule_id,
                priority=entry["Priority"],
                manufacturer_regex=entry["ManufacturerRegex"],
                model_regex=entry["ModelRegex"],
                schema_file=schema_file,
                required_capabilities=tuple(entry.get("RequiredCapabilities", []) or []),
                schema=schema_obj,
                unavailable_reason=unavailable_reason,
            )
        )

    return CommonSchemaV2(
        path=str(path),
        safety=safety,
        bootstrap=bootstrap,
        model_schemas=tuple(rules),
        require_unique_best_match=True,
        on_no_match=on_no_match,
    )


def _parse_safety(raw: dict[str, Any], path: Path) -> SafetyConfig:
    context = f"{path.name}: Safety"
    reject_unknown_keys(
        raw,
        {"SameOriginLinksOnly", "FollowRedirects", "Pagination", "CollectionLimits", "MaxDepth"},
        context=context,
    )
    if raw.get("SameOriginLinksOnly", True) is not True:
        raise SchemaValidationError(f"{context}.SameOriginLinksOnly must not be set to false")

    redirects = raw.get("FollowRedirects", {}) or {}
    reject_unknown_keys(redirects, {"Enabled", "MaxRedirects"}, context=f"{context}.FollowRedirects")
    max_redirects = redirects.get("MaxRedirects", 3)
    if not (0 <= max_redirects <= 10):
        raise SchemaValidationError(f"{context}.FollowRedirects.MaxRedirects out of range [0,10]: {max_redirects}")

    pagination = raw.get("Pagination", {}) or {}
    reject_unknown_keys(pagination, {"NextLinkPaths", "MaxPages"}, context=f"{context}.Pagination")
    next_link_paths = pagination.get("NextLinkPaths", ['$."Members@odata.nextLink"', '$."@odata.nextLink"'])
    for expr in next_link_paths:
        compile_jsonpath(expr, context=f"{context}.Pagination.NextLinkPaths")
    max_pages = pagination.get("MaxPages", 50)
    if not (1 <= max_pages <= 500):
        raise SchemaValidationError(f"{context}.Pagination.MaxPages out of range [1,500]: {max_pages}")

    collection_limits = raw.get("CollectionLimits", {}) or {}
    reject_unknown_keys(collection_limits, {"MaxMembers"}, context=f"{context}.CollectionLimits")
    max_members = collection_limits.get("MaxMembers", 4096)
    if not (1 <= max_members <= 100000):
        raise SchemaValidationError(f"{context}.CollectionLimits.MaxMembers out of range: {max_members}")

    max_depth = raw.get("MaxDepth", 4)
    if not (1 <= max_depth <= 10):
        raise SchemaValidationError(f"{context}.MaxDepth out of range [1,10]: {max_depth}")

    return SafetyConfig(
        same_origin_links_only=True,
        follow_redirects_enabled=redirects.get("Enabled", True),
        max_redirects=max_redirects,
        pagination_next_link_paths=tuple(next_link_paths),
        pagination_max_pages=max_pages,
        max_members=max_members,
        max_depth=max_depth,
    )


def _validate_bootstrap_fallback_uri(value: Any, *, context: str) -> str:
    """`research.md` Group 2 (item 4): `Bootstrap.Authentication.FallbackURI`
    must be a bare, host-rooted path — never an absolute URL, a scheme-
    relative (`//host/...`) link, or any value carrying its own scheme/
    host — validated at schema LOAD time so a malformed value fails closed
    before any cycle ever builds a login URL from it, rather than only
    being caught (if at all) deep inside a live login attempt. The runtime
    request path still separately validates the constructed initial URL
    and every redirect via `same_target.validate_same_target_url` once the
    actual target host is known — this load-time check can only verify
    shape/rootedness, since no request's target host exists yet."""
    if not isinstance(value, str) or not value:
        raise SchemaValidationError(f"{context} must be a non-empty string")
    parts = urlsplit(value)
    if parts.scheme or parts.netloc:
        raise SchemaValidationError(
            f"{context} must be a host-rooted path, not an absolute or scheme-relative URL: {value!r}"
        )
    if not value.startswith("/"):
        raise SchemaValidationError(f"{context} must be a host-rooted path starting with '/': {value!r}")
    return value


def _parse_bootstrap(raw: dict[str, Any], path: Path) -> BootstrapBlock:
    context = f"{path.name}: Bootstrap"
    reject_unknown_keys(raw, {"ServiceRoot", "Authentication", "System", "Chassis", "Manager"}, context=context)

    service_root = raw.get("ServiceRoot", {}) or {}
    reject_unknown_keys(service_root, {"URI", "Capture", "Fallbacks"}, context=f"{context}.ServiceRoot")
    capture = service_root.get("Capture")
    if not isinstance(capture, dict) or not capture:
        raise SchemaValidationError(f"{context}.ServiceRoot.Capture is required")
    for name, expr in capture.items():
        compile_jsonpath(expr, context=f"{context}.ServiceRoot.Capture.{name}")
    for required_name in ("service_root", "systems_collection"):
        if required_name not in capture:
            raise SchemaValidationError(f"{context}.ServiceRoot.Capture must include {required_name!r}")

    auth = raw.get("Authentication", {}) or {}
    reject_unknown_keys(auth, {"SessionServiceURI", "SessionCollectionPath", "FallbackURI"}, context=f"{context}.Authentication")
    session_collection_path = auth.get("SessionCollectionPath", '$.Sessions."@odata.id"')
    compile_jsonpath(session_collection_path, context=f"{context}.Authentication.SessionCollectionPath")
    session_fallback_uri = _validate_bootstrap_fallback_uri(
        auth.get("FallbackURI", "/redfish/v1/SessionService/Sessions"),
        context=f"{context}.Authentication.FallbackURI",
    )

    system = raw.get("System", {}) or {}
    reject_unknown_keys(system, {"CollectionURI", "Members", "Select", "Capture"}, context=f"{context}.System")
    system_members = system.get("Members", '$.Members[*]."@odata.id"')
    compile_jsonpath(system_members, context=f"{context}.System.Members")
    system_select = system.get("Select", "only")
    if system_select not in ("only", "first"):
        raise SchemaValidationError(f"{context}.System.Select must be 'only' or 'first'")
    system_capture = system.get("Capture")
    if not isinstance(system_capture, dict) or not system_capture:
        raise SchemaValidationError(f"{context}.System.Capture is required")
    for name, expr in system_capture.items():
        compile_jsonpath(expr, context=f"{context}.System.Capture.{name}")
    for required_name in ("system_id", "manufacturer", "model"):
        if required_name not in system_capture:
            raise SchemaValidationError(f"{context}.System.Capture must include {required_name!r}")
    if "system_uri" in system_capture:
        raise SchemaValidationError(f"{context}.System.Capture MUST NOT redefine reserved name 'system_uri'")

    chassis = raw.get("Chassis", {}) or {}
    chassis_capture: dict[str, str] = {}
    chassis_collection_uri = None
    if chassis:
        reject_unknown_keys(chassis, {"CollectionURI", "Members", "Select", "Capture"}, context=f"{context}.Chassis")
        chassis_collection_uri = chassis.get("CollectionURI")
        compile_jsonpath(chassis.get("Members", '$.Members[*]."@odata.id"'), context=f"{context}.Chassis.Members")
        chassis_capture = chassis.get("Capture") or {}
        if not chassis_capture:
            raise SchemaValidationError(f"{context}.Chassis.Capture is required when Chassis block is present")
        if "chassis_uri" not in chassis_capture:
            raise SchemaValidationError(f"{context}.Chassis.Capture must include 'chassis_uri'")
        for name, expr in chassis_capture.items():
            compile_jsonpath(expr, context=f"{context}.Chassis.Capture.{name}")

    manager = raw.get("Manager", {}) or {}
    manager_capture: dict[str, str] = {}
    manager_collection_uri = None
    if manager:
        reject_unknown_keys(manager, {"CollectionURI", "Members", "Select", "Capture"}, context=f"{context}.Manager")
        manager_collection_uri = manager.get("CollectionURI")
        compile_jsonpath(manager.get("Members", '$.Members[*]."@odata.id"'), context=f"{context}.Manager.Members")
        manager_capture = manager.get("Capture") or {}
        if not manager_capture:
            raise SchemaValidationError(f"{context}.Manager.Capture is required when Manager block is present")
        if "manager_uri" not in manager_capture:
            raise SchemaValidationError(f"{context}.Manager.Capture must include 'manager_uri'")
        for name, expr in manager_capture.items():
            compile_jsonpath(expr, context=f"{context}.Manager.Capture.{name}")

    return BootstrapBlock(
        service_root_uri=service_root.get("URI", "/redfish/v1/"),
        service_root_capture=capture,
        service_root_fallbacks=service_root.get("Fallbacks", {}) or {},
        session_service_uri=auth.get("SessionServiceURI", "{{ session_service }}"),
        session_collection_path=session_collection_path,
        session_fallback_uri=session_fallback_uri,
        system_collection_uri=system.get("CollectionURI", "{{ systems_collection }}"),
        system_members=system_members,
        system_select=system_select,
        system_capture=system_capture,
        chassis_collection_uri=chassis_collection_uri,
        chassis_members=chassis.get("Members", '$.Members[*]."@odata.id"') if chassis else '$.Members[*]."@odata.id"',
        chassis_select=chassis.get("Select", "first") if chassis else "first",
        chassis_capture=chassis_capture,
        manager_collection_uri=manager_collection_uri,
        manager_members=manager.get("Members", '$.Members[*]."@odata.id"') if manager else '$.Members[*]."@odata.id"',
        manager_select=manager.get("Select", "first") if manager else "first",
        manager_capture=manager_capture,
    )


# ---------------------------------------------------------------------------
# VendorModelSchema (v2)
# ---------------------------------------------------------------------------

_VENDOR_TOP_KEYS = {
    "Version", "Kind", "Model", "Family", "Defaults", "Collection", "Resources", "Components", "Validation",
}
_STRATEGY_KEYS = {
    "Id", "Kind", "URI", "URIFrom", "When", "Members", "Capture", "Children",
    "EnabledWhen", "ReportsCollectionPath", "ReportMembers", "ReportAllowList", "Report", "Reduce",
}


def _load_vendor_model_v2(raw: dict[str, Any], path: Path) -> VendorModelSchemaV2:
    reject_unknown_keys(raw, _VENDOR_TOP_KEYS, context=path.name)
    if "Model" not in raw:
        raise SchemaValidationError(f"{path.name}: Model is required")

    defaults = raw.get("Defaults", {}) or {}
    reject_unknown_keys(defaults, {"MissingValue", "FieldSelection"}, context=f"{path.name}: Defaults")
    missing_value = defaults.get("MissingValue", "Unknown")
    field_selection = defaults.get("FieldSelection", "first-present")
    if field_selection != "first-present":
        raise SchemaValidationError(f"{path.name}: Defaults.FieldSelection only supports 'first-present'")

    collection = raw.get("Collection", {}) or {}
    reject_unknown_keys(collection, {"Fast", "Slow"}, context=f"{path.name}: Collection")
    fast_block = collection.get("Fast", {}) or {}
    fast_resources = tuple(fast_block.get("Resources", []) or [])
    if not fast_resources:
        raise SchemaValidationError(f"{path.name}: Collection.Fast.Resources is required")

    # `research.md` Group 3 (item 1): `Collection.Slow` and its own
    # `Resources` key must both be explicitly present (an author must state
    # intent, never rely on silently defaulting an entirely-omitted block
    # to empty) — but an explicit `Resources: []` is now a VALID way to
    # declare "this model has no Slow-lane work at all", permanently
    # disabling Slow for every target selecting this schema (`target_state.
    # py`'s `slow_disabled`/`cycle_callbacks.py`'s empty-Slow check already
    # implement the runtime side of this; only this load-time rejection was
    # blocking it from ever being reachable).
    if "Slow" not in collection:
        raise SchemaValidationError(
            f"{path.name}: Collection.Slow is required (use Resources: [] to declare no Slow-lane work)"
        )
    slow_block = collection.get("Slow") or {}
    if "Resources" not in slow_block:
        raise SchemaValidationError(
            f"{path.name}: Collection.Slow.Resources is required (use [] to declare no Slow-lane work)"
        )
    slow_resources = tuple(slow_block.get("Resources") or [])
    overlap = set(fast_resources) & set(slow_resources)
    if overlap:
        raise SchemaValidationError(f"{path.name}: resource(s) {sorted(overlap)} in both Fast and Slow collections")

    resources_raw = raw.get("Resources")
    if not isinstance(resources_raw, dict) or not resources_raw:
        raise SchemaValidationError(f"{path.name}: at least one Resources entry is required")
    resources: dict[str, ResourceDef] = {}
    for name, resource_raw in resources_raw.items():
        resources[name] = _parse_resource(name, resource_raw, context=f"{path.name}: Resources.{name}")

    for resource_name in list(fast_resources) + list(slow_resources):
        if resource_name not in resources:
            raise SchemaValidationError(
                f"{path.name}: Collection references undeclared resource {resource_name!r}"
            )

    components_raw = raw.get("Components")
    if not isinstance(components_raw, dict) or not components_raw:
        raise SchemaValidationError(f"{path.name}: at least one Components entry is required")
    components: dict[str, ComponentDef] = {}
    for name, component_raw in components_raw.items():
        components[name] = _parse_component(name, component_raw, resources, context=f"{path.name}: Components.{name}")

    validation = raw.get("Validation", {}) or {}
    reject_unknown_keys(
        validation,
        {
            "RequireEveryResourceReference", "RequireCompilableJSONPath",
            "RequireCompilableRegex", "RequireUniqueComponentFieldNames", "RejectUnknownTopLevelKeys",
        },
        context=f"{path.name}: Validation",
    )
    for key, value in validation.items():
        if value is not True:
            raise SchemaValidationError(f"{path.name}: Validation.{key} must not be set to false")

    return VendorModelSchemaV2(
        path=str(path),
        model=raw["Model"],
        family=raw.get("Family"),
        missing_value=missing_value,
        fast_resources=fast_resources,
        slow_resources=slow_resources,
        resources=resources,
        components=components,
    )


def _parse_resource(name: str, raw: dict[str, Any], *, context: str) -> ResourceDef:
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{context} must be a mapping")
    reject_unknown_keys(raw, {"Required", "Strategies"}, context=context)
    strategies_raw = raw.get("Strategies")
    if not isinstance(strategies_raw, list) or not strategies_raw:
        raise SchemaValidationError(f"{context}: Strategies must be a non-empty list")
    seen_ids: set[str] = set()
    strategies: list[StrategyDef] = []
    for i, strategy_raw in enumerate(strategies_raw):
        strategy = _parse_strategy(strategy_raw, context=f"{context}.Strategies[{i}]")
        if strategy.id in seen_ids:
            raise SchemaValidationError(f"{context}: duplicate strategy Id {strategy.id!r}")
        seen_ids.add(strategy.id)
        strategies.append(strategy)
    return ResourceDef(name=name, required=raw.get("Required", False), strategies=tuple(strategies))


def _parse_strategy(raw: dict[str, Any], *, context: str) -> StrategyDef:
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{context} must be a mapping")
    reject_unknown_keys(raw, _STRATEGY_KEYS, context=context)
    require_id = "Id" in raw
    if not require_id:
        raise SchemaValidationError(f"{context}: Id is required")
    kind = raw.get("Kind")
    if kind not in _STRATEGY_KINDS:
        raise SchemaValidationError(f"{context}: Kind must be one of {sorted(_STRATEGY_KINDS)}, got {kind!r}")

    uri_template = raw.get("URI")
    uri_from = raw.get("URIFrom", {}) or {}
    uri_from_capture = uri_from.get("Capture")
    uri_from_resource = uri_from.get("Resource")
    uri_from_path = uri_from.get("Path")
    uri_from_parent_path = uri_from.get("ParentPath")

    uri_sources = [
        s for s in (uri_template, uri_from_capture, uri_from_resource, uri_from_parent_path) if s is not None
    ]
    if kind != "inline-collection" and kind != "telemetry-reports":
        if len(uri_sources) != 1:
            raise SchemaValidationError(f"{context}: exactly one URI source is required for Kind {kind!r}")
    if uri_from_resource and not uri_from_path:
        raise SchemaValidationError(f"{context}: URIFrom.Path is required with URIFrom.Resource")
    if kind == "telemetry-reports" and uri_from_capture != "telemetry_service":
        raise SchemaValidationError(f"{context}: telemetry-reports strategies MUST use URIFrom.Capture: telemetry_service")

    when = raw.get("When", {}) or {}
    when_path = when.get("Path")
    when_equals = when.get("Equals")
    if when_path:
        compile_jsonpath(when_path, context=f"{context}.When.Path")
        if when_equals is None:
            raise SchemaValidationError(f"{context}: When.Equals is required with When.Path")

    members = raw.get("Members")
    if kind in ("collection", "inline-collection"):
        if not members:
            raise SchemaValidationError(f"{context}: Members is required for Kind {kind!r}")
        compile_jsonpath(members, context=f"{context}.Members")
    elif members is not None:
        raise SchemaValidationError(f"{context}: Members is forbidden for Kind {kind!r}")

    capture = raw.get("Capture", {}) or {}
    for capture_name, expr in capture.items():
        compile_jsonpath(expr, context=f"{context}.Capture.{capture_name}")

    children_raw = raw.get("Children", {}) or {}
    children: dict[str, ResourceDef] = {}
    for child_name, child_raw in children_raw.items():
        children[child_name] = _parse_resource(child_name, child_raw, context=f"{context}.Children.{child_name}")

    kwargs: dict[str, Any] = dict(
        id=raw["Id"],
        kind=kind,
        uri_template=uri_template,
        uri_from_capture=uri_from_capture,
        uri_from_resource=uri_from_resource,
        uri_from_path=uri_from_path,
        uri_from_parent_path=uri_from_parent_path,
        when_path=when_path,
        when_equals=when_equals,
        members=members,
        capture=capture,
        children=children,
    )

    if kind == "telemetry-reports":
        for key in ("ReportsCollectionPath", "ReportAllowList", "Report"):
            if key not in raw:
                raise SchemaValidationError(f"{context}: {key} is required for Kind 'telemetry-reports'")
        reports_collection_path = raw["ReportsCollectionPath"]
        compile_jsonpath(reports_collection_path, context=f"{context}.ReportsCollectionPath")
        report_members = raw.get("ReportMembers", '$.Members[*]."@odata.id"')
        compile_jsonpath(report_members, context=f"{context}.ReportMembers")
        allow_list = raw["ReportAllowList"]
        if not isinstance(allow_list, list) or not allow_list:
            raise SchemaValidationError(f"{context}: ReportAllowList must be a non-empty list")
        for pattern in allow_list:
            compile_regex(pattern, context=f"{context}.ReportAllowList")
        report = raw["Report"]
        reject_unknown_keys(
            report,
            {"IdPath", "TimestampPath", "ValuesPath", "ValueFields"},
            context=f"{context}.Report",
        )
        for key in ("IdPath", "TimestampPath", "ValuesPath"):
            if key not in report:
                raise SchemaValidationError(f"{context}.Report: {key} is required")
            compile_jsonpath(report[key], context=f"{context}.Report.{key}")
        value_fields = report.get("ValueFields", {})
        for key in ("MetricIdPath", "MetricPropertyPath", "MetricValuePath"):
            if key not in value_fields:
                raise SchemaValidationError(f"{context}.Report.ValueFields: {key} is required")
            compile_jsonpath(value_fields[key], context=f"{context}.Report.ValueFields.{key}")
        kwargs.update(
            reports_collection_path=reports_collection_path,
            report_members=report_members,
            report_allow_list=tuple(allow_list),
            report_id_path=report["IdPath"],
            report_timestamp_path=report["TimestampPath"],
            report_values_path=report["ValuesPath"],
            report_metric_id_path=value_fields["MetricIdPath"],
            report_metric_property_path=value_fields["MetricPropertyPath"],
            report_metric_value_path=value_fields["MetricValuePath"],
        )

    return StrategyDef(**kwargs)


def _parse_component(
    name: str, raw: dict[str, Any], resources: dict[str, ResourceDef], *, context: str,
    _parent_resource_path: str | None = None,
) -> ComponentDef:
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{context} must be a mapping")
    reject_unknown_keys(raw, {"Records", "Identity", "Fields", "Children"}, context=context)
    records = raw.get("Records")
    if not isinstance(records, dict) or "Resource" not in records or "Select" not in records:
        raise SchemaValidationError(f"{context}: Records.Resource and Records.Select are both required")
    reject_unknown_keys(records, {"Resource", "Select"}, context=f"{context}.Records")
    resource_path = records["Resource"]
    _validate_resource_path_exists(resource_path, resources, context=context)
    if _parent_resource_path is not None and not resource_path.startswith(_parent_resource_path + "."):
        raise SchemaValidationError(
            f"{context}: Records.Resource {resource_path!r} must be a descendant of parent {_parent_resource_path!r}"
        )
    select = records["Select"]
    if select not in ("object", "members"):
        raise SchemaValidationError(f"{context}: Records.Select must be 'object' or 'members'")

    identity = raw.get("Identity", {}) or {}
    reject_unknown_keys(identity, {"Type", "Select"}, context=f"{context}.Identity")
    identity_type = identity.get("Type", "string")
    if identity_type != "string":
        raise SchemaValidationError(f"{context}.Identity.Type only supports 'string'")
    identity_select_raw = identity.get("Select")
    if not identity_select_raw:
        raise SchemaValidationError(f"{context}.Identity.Select is required")
    identity_paths: list[str] = []
    for entry in identity_select_raw:
        path_expr = entry["Path"] if isinstance(entry, dict) else entry
        compile_jsonpath(path_expr, context=f"{context}.Identity.Select")
        identity_paths.append(path_expr)

    fields_raw = raw.get("Fields")
    if not isinstance(fields_raw, dict) or not fields_raw:
        raise SchemaValidationError(f"{context}: at least one Fields.<name> entry is required")
    fields: dict[str, FieldDef] = {}
    for field_name, field_raw in fields_raw.items():
        fields[field_name] = _parse_field(field_raw, context=f"{context}.Fields.{field_name}")

    children_raw = raw.get("Children", {}) or {}
    children: dict[str, ComponentDef] = {}
    for child_name, child_raw in children_raw.items():
        children[child_name] = _parse_component(
            child_name, child_raw, resources, context=f"{context}.Children.{child_name}",
            _parent_resource_path=resource_path,
        )

    return ComponentDef(
        records_resource=resource_path,
        records_select=select,
        identity_select=tuple(identity_paths),
        fields=fields,
        children=children,
    )


def _validate_resource_path_exists(dotted_path: str, resources: dict[str, ResourceDef], *, context: str) -> None:
    parts = dotted_path.split(".")
    if parts[0] not in resources:
        raise SchemaValidationError(f"{context}: Records.Resource references undeclared resource {parts[0]!r}")
    current = resources[parts[0]]
    for part in parts[1:]:
        found = None
        for strategy in current.strategies:
            if part in strategy.children:
                found = strategy.children[part]
                break
        if found is None:
            raise SchemaValidationError(
                f"{context}: Records.Resource {dotted_path!r} has no child resource named {part!r} under {current.name!r}"
            )
        current = found


def _parse_field(raw: dict[str, Any], *, context: str) -> FieldDef:
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{context} must be a mapping")
    reject_unknown_keys(raw, {"Type", "Select"}, context=context)
    field_type = raw.get("Type")
    if field_type not in _FIELD_TYPES:
        raise SchemaValidationError(f"{context}: Type must be one of {sorted(_FIELD_TYPES)}, got {field_type!r}")
    select_raw = raw.get("Select")
    if not select_raw:
        raise SchemaValidationError(f"{context}: Select must have at least one entry")
    selectors: list[SelectorDef] = []
    for i, selector_raw in enumerate(select_raw):
        selectors.append(_parse_selector(selector_raw, context=f"{context}.Select[{i}]"))
    return FieldDef(type=field_type, select=tuple(selectors))


def _parse_selector(raw: dict[str, Any], *, context: str) -> SelectorDef:
    if not isinstance(raw, dict):
        raise SchemaValidationError(f"{context} must be a mapping")
    has_path = "Path" in raw
    has_telemetry = "Telemetry" in raw
    if has_path == has_telemetry:
        raise SchemaValidationError(f"{context}: exactly one of Path or Telemetry is required")

    transforms_raw = raw.get("Transforms", []) or []
    transforms = tuple(_parse_transform(t, context=f"{context}.Transforms") for t in transforms_raw)

    if has_path:
        compile_jsonpath(raw["Path"], context=f"{context}.Path")
        return SelectorDef(path=raw["Path"], transforms=transforms)

    telemetry = raw["Telemetry"]
    if "ReportIdRegex" not in telemetry or "MetricIdRegex" not in telemetry:
        raise SchemaValidationError(f"{context}.Telemetry: ReportIdRegex and MetricIdRegex are both required")
    compile_regex(telemetry["ReportIdRegex"], context=f"{context}.Telemetry.ReportIdRegex")
    compile_regex(telemetry["MetricIdRegex"], context=f"{context}.Telemetry.MetricIdRegex")
    metric_property_regex = telemetry.get("MetricPropertyRegex")
    if metric_property_regex:
        compile_regex(metric_property_regex, context=f"{context}.Telemetry.MetricPropertyRegex")
    return SelectorDef(
        telemetry_report_id_regex=telemetry["ReportIdRegex"],
        telemetry_metric_id_regex=telemetry["MetricIdRegex"],
        telemetry_metric_property_regex=metric_property_regex,
        transforms=transforms,
    )


def _parse_transform(raw: dict[str, Any], *, context: str) -> TransformDef:
    if not isinstance(raw, dict) or "Op" not in raw:
        raise SchemaValidationError(f"{context}: transform entry requires 'Op'")
    op = raw["Op"]
    if op not in _TRANSFORM_OPS:
        raise SchemaValidationError(f"{context}: unknown transform Op {op!r}")
    if op == "round":
        digits = raw.get("Digits")
        if digits is None or not (-9 <= digits <= 9):
            raise SchemaValidationError(f"{context}: round requires Digits in [-9,9]")
        reject_unknown_keys(raw, {"Op", "Digits"}, context=context)
        return TransformDef(op=op, digits=digits)
    if op in ("multiply", "add"):
        reject_unknown_keys(raw, {"Op", "Value"}, context=context)
        if "Value" not in raw or not isinstance(raw["Value"], (int, float)):
            raise SchemaValidationError(f"{context}: {op} requires a finite numeric Value")
        return TransformDef(op=op, value=raw["Value"])
    if op == "divide":
        reject_unknown_keys(raw, {"Op", "Value"}, context=context)
        value = raw.get("Value")
        if not isinstance(value, (int, float)) or value == 0:
            raise SchemaValidationError(f"{context}: divide requires a finite non-zero numeric Value")
        return TransformDef(op=op, value=value)
    if op == "map":
        reject_unknown_keys(raw, {"Op", "Values", "Default"}, context=context)
        values = raw.get("Values")
        if not isinstance(values, dict) or not values:
            raise SchemaValidationError(f"{context}: map requires a non-empty Values mapping")
        return TransformDef(op=op, values=values, default=raw.get("Default"))
    # to-number
    reject_unknown_keys(raw, {"Op"}, context=context)
    return TransformDef(op=op)
