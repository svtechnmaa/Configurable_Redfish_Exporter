"""Typed deployment-profile loading and validation.

Implements `contracts/deployment-profile-contract.md` exactly: one file per
deployment profile containing `Auth` + `Tuning` (optional, defaulted) +
`Metrics` together (FR-005) — never split. Every `Tuning` key has one exact
default and one exact validation range; unknown keys and out-of-range values
fail closed with the offending key name only, never secret values.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, get_type_hints

import yaml


class ProfileValidationError(Exception):
    """Raised for any profile-load failure. Message never includes Auth values."""


# ---------------------------------------------------------------------------
# Tuning sub-sections (dataclasses double as the schema: field name -> default)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FastTuning:
    ttl_seconds: int = 30
    deadline_seconds: int = 15


@dataclass(frozen=True)
class SlowTuning:
    ttl_seconds: int = 900
    deadline_seconds: int = 90


@dataclass(frozen=True)
class CycleTuning:
    max_duration_seconds: int = 180


@dataclass(frozen=True)
class CacheTuning:
    max_targets: int = 500
    idle_target_ttl_seconds: int = 3600
    response_ttl_seconds: int = 180
    max_entries: int = 500
    max_total_bytes: int = 52428800
    max_snapshot_bytes_per_lane: int = 10485760
    max_total_snapshot_bytes: int = 104857600
    max_in_flight_candidate_bytes: int = 104857600


@dataclass(frozen=True)
class OverloadTuning:
    retry_after_seconds: int = 5


@dataclass(frozen=True)
class RefreshTuning:
    max_concurrent_refreshes: int = 16
    max_concurrent_cycles_per_bmc: int = 8


@dataclass(frozen=True)
class CrawlTuning:
    batch_size: int = 8
    max_queued_requests_per_lane: int = 64
    fast_batch_limit: int = 4
    # `research.md` Group 5 (item 3): the v2 executor's Children recursion
    # depth bound was previously a hard-coded literal (`pipeline.py`'s
    # `collect_v2_resources(..., max_depth=4)`), not configurable at all.
    max_depth: int = 4


@dataclass(frozen=True)
class IOTuning:
    max_in_flight_response_bytes: int = 67108864


@dataclass(frozen=True)
class LoginTuning:
    timeout_seconds: int = 60
    max_attempts: int = 3


@dataclass(frozen=True)
class RequestTuning:
    timeout_seconds: float = 30
    max_attempts: int = 5
    backoff_base_seconds: float = 0.5
    backoff_cap_seconds: float = 30


@dataclass(frozen=True)
class LogoutTuning:
    timeout_seconds: int = 60


@dataclass(frozen=True)
class ShutdownTuning:
    grace_seconds: int = 30


@dataclass(frozen=True)
class DebugTuning:
    root_directory: str = "/tmp/redfish-data"
    write_raw_data: bool = False
    write_normalized_data: bool = False
    max_file_bytes: int = 5242880
    max_files_per_target: int = 4
    max_total_bytes: int = 104857600
    retention_seconds: int = 3600


@dataclass(frozen=True)
class TelemetryTuning:
    max_future_skew_seconds: int = 300
    max_age_seconds: int = 3600
    max_values_read_per_report: int = 50000


@dataclass(frozen=True)
class Tuning:
    max_parallel_targets: int = 50
    target_concurrency: int = 8
    max_response_bytes: int = 10485760
    fast: FastTuning = field(default_factory=FastTuning)
    slow: SlowTuning = field(default_factory=SlowTuning)
    cycle: CycleTuning = field(default_factory=CycleTuning)
    cache: CacheTuning = field(default_factory=CacheTuning)
    overload: OverloadTuning = field(default_factory=OverloadTuning)
    refresh: RefreshTuning = field(default_factory=RefreshTuning)
    crawl: CrawlTuning = field(default_factory=CrawlTuning)
    io: IOTuning = field(default_factory=IOTuning)
    login: LoginTuning = field(default_factory=LoginTuning)
    request: RequestTuning = field(default_factory=RequestTuning)
    logout: LogoutTuning = field(default_factory=LogoutTuning)
    shutdown: ShutdownTuning = field(default_factory=ShutdownTuning)
    debug: DebugTuning = field(default_factory=DebugTuning)
    telemetry: TelemetryTuning = field(default_factory=TelemetryTuning)


@dataclass(frozen=True)
class Auth:
    username: str
    password: str


@dataclass(frozen=True)
class DeploymentProfile:
    auth: Auth
    tuning: Tuning
    metrics: list[dict[str, Any]]
    name: str


# ---------------------------------------------------------------------------
# Schema: dotted YAML key -> (dataclass field name, nested dataclass or None,
# (min, max) range or None for non-numeric fields).
# ---------------------------------------------------------------------------

_NO_RANGE = None

# Top-level (direct Tuning fields) : yaml_key -> (attr_name, (min,max))
_TOP_LEVEL_RANGES: dict[str, tuple[str, tuple[float, float]]] = {
    "MaxParallelTargets": ("max_parallel_targets", (1, 10000)),
    "TargetConcurrency": ("target_concurrency", (1, 64)),
    "MaxResponseBytes": ("max_response_bytes", (65536, 104857600)),
}

# Nested sections: yaml_section -> (attr_name, dataclass_type, { yaml_key: (attr, (min,max)) })
_SECTIONS: dict[str, tuple[str, type, dict[str, tuple[str, tuple[float, float]]]]] = {
    "Fast": ("fast", FastTuning, {
        "TTLSeconds": ("ttl_seconds", (5, 3600)),
        "DeadlineSeconds": ("deadline_seconds", (1, 300)),
    }),
    "Slow": ("slow", SlowTuning, {
        "TTLSeconds": ("ttl_seconds", (30, 86400)),
        "DeadlineSeconds": ("deadline_seconds", (5, 1800)),
    }),
    "Cycle": ("cycle", CycleTuning, {
        "MaxDurationSeconds": ("max_duration_seconds", (10, 3600)),
    }),
    "Cache": ("cache", CacheTuning, {
        "MaxTargets": ("max_targets", (1, 100000)),
        "IdleTargetTTLSeconds": ("idle_target_ttl_seconds", (60, 604800)),
        "ResponseTTLSeconds": ("response_ttl_seconds", (1, 3600)),
        "MaxEntries": ("max_entries", (1, 100000)),
        "MaxTotalBytes": ("max_total_bytes", (1048576, 1073741824)),
        "MaxSnapshotBytesPerLane": ("max_snapshot_bytes_per_lane", (65536, 104857600)),
        "MaxTotalSnapshotBytes": ("max_total_snapshot_bytes", (1048576, 2147483648)),
        "MaxInFlightCandidateBytes": ("max_in_flight_candidate_bytes", (1048576, 2147483648)),
    }),
    "Overload": ("overload", OverloadTuning, {
        "RetryAfterSeconds": ("retry_after_seconds", (1, 60)),
    }),
    "Refresh": ("refresh", RefreshTuning, {
        "MaxConcurrentRefreshes": ("max_concurrent_refreshes", (1, 4096)),
        "MaxConcurrentCyclesPerBmc": ("max_concurrent_cycles_per_bmc", (1, 1024)),
    }),
    "Crawl": ("crawl", CrawlTuning, {
        "BatchSize": ("batch_size", (1, 64)),
        "MaxQueuedRequestsPerLane": ("max_queued_requests_per_lane", (1, 2048)),
        "FastBatchLimit": ("fast_batch_limit", (1, 64)),
        "MaxDepth": ("max_depth", (1, 20)),
    }),
    "IO": ("io", IOTuning, {
        "MaxInFlightResponseBytes": ("max_in_flight_response_bytes", (1048576, 2147483648)),
    }),
    "Login": ("login", LoginTuning, {
        "TimeoutSeconds": ("timeout_seconds", (1, 300)),
        "MaxAttempts": ("max_attempts", (1, 10)),
    }),
    "Request": ("request", RequestTuning, {
        "TimeoutSeconds": ("timeout_seconds", (1, 300)),
        "MaxAttempts": ("max_attempts", (1, 10)),
        "BackoffBaseSeconds": ("backoff_base_seconds", (0.1, 10)),
        "BackoffCapSeconds": ("backoff_cap_seconds", (1, 300)),
    }),
    "Logout": ("logout", LogoutTuning, {
        "TimeoutSeconds": ("timeout_seconds", (1, 300)),
    }),
    "Shutdown": ("shutdown", ShutdownTuning, {
        "GraceSeconds": ("grace_seconds", (1, 300)),
    }),
    "Debug": ("debug", DebugTuning, {
        "RootDirectory": ("root_directory", None),
        "WriteRawData": ("write_raw_data", None),
        "WriteNormalizedData": ("write_normalized_data", None),
        "MaxFileBytes": ("max_file_bytes", (1024, 104857600)),
        "MaxFilesPerTarget": ("max_files_per_target", (1, 100)),
        "MaxTotalBytes": ("max_total_bytes", (1048576, 10737418240)),
        "RetentionSeconds": ("retention_seconds", (60, 604800)),
    }),
    "Telemetry": ("telemetry", TelemetryTuning, {
        "MaxFutureSkewSeconds": ("max_future_skew_seconds", (0, 3600)),
        "MaxAgeSeconds": ("max_age_seconds", (1, 86400)),
        "MaxValuesReadPerReport": ("max_values_read_per_report", (1, 1000000)),
    }),
}

_KNOWN_TOP_LEVEL_TUNING_KEYS = set(_TOP_LEVEL_RANGES) | set(_SECTIONS)
_TOP_LEVEL_PROFILE_KEYS = {"Auth", "Tuning", "Metrics"}

# The dataclass field annotations above are the one declared source of
# truth for each ranged key's expected numeric type (`int` vs `float`) —
# resolved once here via `get_type_hints` (not `field.type`, which would
# just be the unevaluated annotation string under `from __future__ import
# annotations`) so `_validate_numeric` can actually enforce it below,
# instead of a value merely being numeric-and-in-range regardless of type.
_TOP_LEVEL_FIELD_TYPES: dict[str, type] = get_type_hints(Tuning)
_SECTION_FIELD_TYPES: dict[str, dict[str, type]] = {
    yaml_section: get_type_hints(dataclass_type)
    for yaml_section, (_, dataclass_type, _) in _SECTIONS.items()
}

# Process-wide keys that must be identical across every profile in one
# deployment (dotted path using the YAML section/key names for readable errors).
_PROCESS_WIDE_KEYS: list[tuple[str, ...]] = [
    ("MaxParallelTargets",),
    ("Cache", "MaxTargets"),
    ("Cache", "IdleTargetTTLSeconds"),
    ("Cache", "MaxEntries"),
    ("Cache", "MaxTotalBytes"),
    ("Cache", "MaxSnapshotBytesPerLane"),
    ("Cache", "MaxTotalSnapshotBytes"),
    ("Cache", "MaxInFlightCandidateBytes"),
    ("Overload", "RetryAfterSeconds"),
    ("Refresh", "MaxConcurrentRefreshes"),
    ("Refresh", "MaxConcurrentCyclesPerBmc"),
    ("TargetConcurrency",),
    ("Crawl", "MaxQueuedRequestsPerLane"),
    ("Crawl", "FastBatchLimit"),
    ("MaxResponseBytes",),
    ("IO", "MaxInFlightResponseBytes"),
    ("Shutdown", "GraceSeconds"),
    ("Debug", "RootDirectory"),
    ("Debug", "WriteRawData"),
    ("Debug", "WriteNormalizedData"),
    ("Debug", "MaxFileBytes"),
    ("Debug", "MaxFilesPerTarget"),
    ("Debug", "MaxTotalBytes"),
    ("Debug", "RetentionSeconds"),
]


def _get_nested(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return _MISSING
        current = current[key]
    return current


_MISSING = object()


def _validate_numeric(
    name: str, value: Any, bounds: tuple[float, float] | None, expected_type: type | None
) -> Any:
    """Validate `value` against `bounds` AND `expected_type`, returning the
    value to actually store (coerced to `int` when a whole-number float was
    given for an integer field). A float that is merely numeric-and-in-range
    is not enough for an integer-only setting: e.g. `Tuning.Crawl.BatchSize:
    8.5` previously passed this check (8.5 is numeric and within [1, 64])
    and was stored as-is, only to blow up `range(0, n, 8.5)` in
    `batching.bounded_gather` during request processing, not at startup.
    """
    if bounds is None:
        return value
    low, high = bounds
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProfileValidationError(f"Tuning.{name} must be numeric, got {type(value).__name__}")
    if expected_type is int and isinstance(value, float):
        if not value.is_integer():
            raise ProfileValidationError(
                f"Tuning.{name} must be an integer, got fractional float {value!r}"
            )
        value = int(value)
    elif expected_type is float and isinstance(value, int):
        value = float(value)
    if not (low <= value <= high):
        raise ProfileValidationError(f"Tuning.{name} = {value} is out of range [{low}, {high}]")
    return value


def _build_tuning(raw_tuning: dict[str, Any]) -> Tuning:
    if not isinstance(raw_tuning, dict):
        raise ProfileValidationError("Tuning must be a mapping")

    unknown = set(raw_tuning) - _KNOWN_TOP_LEVEL_TUNING_KEYS
    if unknown:
        raise ProfileValidationError(f"Unknown Tuning key(s): {sorted(unknown)}")

    top_kwargs: dict[str, Any] = {}
    for yaml_key, (attr, bounds) in _TOP_LEVEL_RANGES.items():
        if yaml_key in raw_tuning:
            value = raw_tuning[yaml_key]
            expected_type = _TOP_LEVEL_FIELD_TYPES.get(attr)
            top_kwargs[attr] = _validate_numeric(yaml_key, value, bounds, expected_type)

    for yaml_section, (attr, dataclass_type, key_map) in _SECTIONS.items():
        raw_section = raw_tuning.get(yaml_section, {}) or {}
        if not isinstance(raw_section, dict):
            raise ProfileValidationError(f"Tuning.{yaml_section} must be a mapping")
        unknown_section_keys = set(raw_section) - set(key_map)
        if unknown_section_keys:
            raise ProfileValidationError(
                f"Unknown Tuning.{yaml_section} key(s): {sorted(unknown_section_keys)}"
            )
        section_field_types = _SECTION_FIELD_TYPES[yaml_section]
        section_kwargs: dict[str, Any] = {}
        for yaml_key, (sub_attr, bounds) in key_map.items():
            if yaml_key in raw_section:
                value = raw_section[yaml_key]
                expected_type = section_field_types.get(sub_attr)
                section_kwargs[sub_attr] = _validate_numeric(
                    f"{yaml_section}.{yaml_key}", value, bounds, expected_type
                )
        top_kwargs[attr] = dataclass_type(**section_kwargs)

    return Tuning(**top_kwargs)


def _cross_field_validate(tuning: Tuning) -> None:
    if tuning.fast.deadline_seconds > tuning.cycle.max_duration_seconds:
        raise ProfileValidationError(
            "Tuning.Fast.DeadlineSeconds "
            f"({tuning.fast.deadline_seconds}) must be <= "
            f"Tuning.Cycle.MaxDurationSeconds ({tuning.cycle.max_duration_seconds})"
        )
    if tuning.slow.deadline_seconds > tuning.cycle.max_duration_seconds:
        raise ProfileValidationError(
            "Tuning.Slow.DeadlineSeconds "
            f"({tuning.slow.deadline_seconds}) must be <= "
            f"Tuning.Cycle.MaxDurationSeconds ({tuning.cycle.max_duration_seconds})"
        )
    if tuning.crawl.batch_size > tuning.target_concurrency:
        raise ProfileValidationError(
            f"Tuning.Crawl.BatchSize ({tuning.crawl.batch_size}) must be <= "
            f"Tuning.TargetConcurrency ({tuning.target_concurrency})"
        )
    if tuning.crawl.batch_size > tuning.crawl.max_queued_requests_per_lane:
        raise ProfileValidationError(
            f"Tuning.Crawl.BatchSize ({tuning.crawl.batch_size}) must be <= "
            f"Tuning.Crawl.MaxQueuedRequestsPerLane ({tuning.crawl.max_queued_requests_per_lane})"
        )
    product = tuning.refresh.max_concurrent_cycles_per_bmc * tuning.crawl.batch_size
    if product > tuning.crawl.max_queued_requests_per_lane:
        raise ProfileValidationError(
            "Tuning.Refresh.MaxConcurrentCyclesPerBmc * Tuning.Crawl.BatchSize "
            f"({product}) must be <= Tuning.Crawl.MaxQueuedRequestsPerLane "
            f"({tuning.crawl.max_queued_requests_per_lane})"
        )
    if tuning.cache.max_snapshot_bytes_per_lane > tuning.cache.max_total_snapshot_bytes:
        raise ProfileValidationError(
            "Tuning.Cache.MaxSnapshotBytesPerLane "
            f"({tuning.cache.max_snapshot_bytes_per_lane}) must be <= "
            f"Tuning.Cache.MaxTotalSnapshotBytes ({tuning.cache.max_total_snapshot_bytes})"
        )
    if tuning.cache.max_snapshot_bytes_per_lane > tuning.cache.max_in_flight_candidate_bytes:
        raise ProfileValidationError(
            "Tuning.Cache.MaxSnapshotBytesPerLane "
            f"({tuning.cache.max_snapshot_bytes_per_lane}) must be <= "
            f"Tuning.Cache.MaxInFlightCandidateBytes ({tuning.cache.max_in_flight_candidate_bytes})"
        )
    if tuning.max_response_bytes > tuning.io.max_in_flight_response_bytes:
        raise ProfileValidationError(
            f"Tuning.MaxResponseBytes ({tuning.max_response_bytes}) must be <= "
            f"Tuning.IO.MaxInFlightResponseBytes ({tuning.io.max_in_flight_response_bytes})"
        )


def load_profile(path: Path) -> DeploymentProfile:
    """Load and fully validate one deployment profile file."""
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ProfileValidationError(f"Profile {Path(path).name} does not exist") from exc
    except OSError as exc:
        raise ProfileValidationError(f"Profile {Path(path).name} could not be read: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ProfileValidationError(f"Profile {Path(path).name} is not valid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ProfileValidationError(f"Profile {Path(path).name} must be a YAML mapping")

    unknown_top = set(raw) - _TOP_LEVEL_PROFILE_KEYS
    if unknown_top:
        raise ProfileValidationError(
            f"Profile {Path(path).name} has unknown top-level key(s): {sorted(unknown_top)}"
        )

    if "Auth" not in raw or not isinstance(raw["Auth"], dict):
        raise ProfileValidationError(f"Profile {Path(path).name} is missing required 'Auth' block")
    auth_raw = raw["Auth"]
    if not auth_raw.get("Username") or not auth_raw.get("Password"):
        raise ProfileValidationError(
            f"Profile {Path(path).name}: Auth.Username and Auth.Password are both required"
        )
    auth = Auth(username=auth_raw["Username"], password=auth_raw["Password"])

    if "Metrics" not in raw or not isinstance(raw["Metrics"], list) or not raw["Metrics"]:
        raise ProfileValidationError(f"Profile {Path(path).name} is missing required non-empty 'Metrics' list")

    tuning = _build_tuning(raw.get("Tuning", {}) or {})
    _cross_field_validate(tuning)

    return DeploymentProfile(auth=auth, tuning=tuning, metrics=raw["Metrics"], name=Path(path).stem)


def validate_profiles_directory(directory: Path) -> dict[str, DeploymentProfile]:
    """Load every `*.yml` profile in `directory` and enforce process-wide
    key consistency (contract "Process-wide consistency across profile files").
    """
    directory = Path(directory)
    profiles: dict[str, DeploymentProfile] = {}
    for entry in sorted(directory.glob("*.yml")):
        profiles[entry.stem] = load_profile(entry)

    if len(profiles) <= 1:
        return profiles

    names = list(profiles)
    reference_name = names[0]
    reference = profiles[reference_name]
    for path in _PROCESS_WIDE_KEYS:
        reference_value = _resolve_tuning_path(reference.tuning, path)
        for other_name in names[1:]:
            other_value = _resolve_tuning_path(profiles[other_name].tuning, path)
            if reference_value != other_value:
                key_label = ".".join(path)
                raise ProfileValidationError(
                    f"Process-wide Tuning.{key_label} mismatch between profiles "
                    f"'{reference_name}' and '{other_name}'"
                )
    return profiles


def _resolve_tuning_path(tuning: Tuning, path: tuple[str, ...]) -> Any:
    if len(path) == 1:
        attr, _ = _TOP_LEVEL_RANGES.get(path[0], (None, None))
        if attr is None:
            # Shutdown.GraceSeconds style single nested-only key won't hit this branch.
            raise KeyError(path[0])
        return getattr(tuning, attr)
    section_attr, _, key_map = _SECTIONS[path[0]]
    section = getattr(tuning, section_attr)
    sub_attr, _ = key_map[path[1]]
    return getattr(section, sub_attr)
