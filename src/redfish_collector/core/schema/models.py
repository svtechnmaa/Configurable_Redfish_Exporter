"""Immutable normalized schema models for v2 `CommonSchema`/`VendorModelSchema`.

Structures mirror `contracts/schema-format-contract.md` exactly. v1-legacy
files are NOT modeled here — they are passed through as raw dicts by
`schema/legacy.py`, per the contract's "not reinterpreted" rule.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class SafetyConfig:
    same_origin_links_only: bool = True
    follow_redirects_enabled: bool = True
    max_redirects: int = 3
    pagination_next_link_paths: tuple[str, ...] = (
        '$."Members@odata.nextLink"',
        '$."@odata.nextLink"',
    )
    pagination_max_pages: int = 50
    max_members: int = 4096
    max_depth: int = 4


@dataclass(frozen=True)
class BootstrapBlock:
    service_root_uri: str = "/redfish/v1/"
    service_root_capture: dict[str, str] = field(default_factory=dict)
    service_root_fallbacks: dict[str, str] = field(default_factory=dict)
    session_service_uri: str = "{{ session_service }}"
    session_collection_path: str = '$.Sessions."@odata.id"'
    session_fallback_uri: str = "/redfish/v1/SessionService/Sessions"
    system_collection_uri: str = "{{ systems_collection }}"
    system_members: str = '$.Members[*]."@odata.id"'
    system_select: str = "only"
    system_capture: dict[str, str] = field(default_factory=dict)
    chassis_collection_uri: Optional[str] = None
    chassis_members: str = '$.Members[*]."@odata.id"'
    chassis_select: str = "first"
    chassis_capture: dict[str, str] = field(default_factory=dict)
    manager_collection_uri: Optional[str] = None
    manager_members: str = '$.Members[*]."@odata.id"'
    manager_select: str = "first"
    manager_capture: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelSchemaRule:
    id: str
    priority: int
    manufacturer_regex: str
    model_regex: str
    schema_file: str
    required_capabilities: tuple[str, ...] = ()
    # Populated at CommonSchema-load time by attempting to load `schema_file`.
    # Exactly one of (schema, unavailable_reason) is set, never both.
    schema: Any = None
    unavailable_reason: Optional[str] = None


@dataclass(frozen=True)
class CommonSchemaV2:
    path: str
    safety: SafetyConfig
    bootstrap: BootstrapBlock
    model_schemas: tuple[ModelSchemaRule, ...]
    require_unique_best_match: bool = True
    on_no_match: str = "unsupported-model"


@dataclass(frozen=True)
class TransformDef:
    op: str
    value: Any = None
    digits: Optional[int] = None
    values: Optional[dict[Any, Any]] = None
    default: Any = None


@dataclass(frozen=True)
class SelectorDef:
    path: Optional[str] = None
    telemetry_report_id_regex: Optional[str] = None
    telemetry_metric_id_regex: Optional[str] = None
    telemetry_metric_property_regex: Optional[str] = None
    transforms: tuple[TransformDef, ...] = ()

    @property
    def is_telemetry(self) -> bool:
        return self.telemetry_report_id_regex is not None


@dataclass(frozen=True)
class FieldDef:
    type: str  # string | number | boolean | status | object
    select: tuple[SelectorDef, ...]


@dataclass(frozen=True)
class ComponentDef:
    records_resource: str
    records_select: str  # object | members
    identity_select: tuple[str, ...]
    fields: dict[str, FieldDef]
    children: dict[str, "ComponentDef"] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyDef:
    id: str
    kind: str  # object | collection | inline-collection | telemetry-reports
    uri_template: Optional[str] = None
    uri_from_capture: Optional[str] = None
    uri_from_resource: Optional[str] = None
    uri_from_path: Optional[str] = None
    uri_from_parent_path: Optional[str] = None
    when_path: Optional[str] = None
    when_equals: Any = None
    members: Optional[str] = None
    capture: dict[str, str] = field(default_factory=dict)
    children: dict[str, "ResourceDef"] = field(default_factory=dict)
    # telemetry-reports extras
    reports_collection_path: Optional[str] = None
    report_members: str = '$.Members[*]."@odata.id"'
    report_allow_list: tuple[str, ...] = ()
    report_id_path: Optional[str] = None
    report_timestamp_path: Optional[str] = None
    report_values_path: Optional[str] = None
    report_metric_id_path: Optional[str] = None
    report_metric_property_path: Optional[str] = None
    report_metric_value_path: Optional[str] = None


@dataclass(frozen=True)
class ResourceDef:
    name: str
    required: bool = False
    strategies: tuple[StrategyDef, ...] = ()


@dataclass(frozen=True)
class VendorModelSchemaV2:
    path: str
    model: str
    family: Optional[str]
    missing_value: str
    fast_resources: tuple[str, ...]
    slow_resources: tuple[str, ...]
    resources: dict[str, ResourceDef]
    components: dict[str, ComponentDef]


@dataclass(frozen=True)
class LegacySchema:
    """A v1-shape schema file (no `Version` key) — raw dict, not reinterpreted."""

    path: str
    raw: dict[str, Any]
