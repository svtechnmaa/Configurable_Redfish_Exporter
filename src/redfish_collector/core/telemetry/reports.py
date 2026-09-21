"""Generic, schema-declared, read-only `TelemetryService`/`MetricReports`
reduction (`research.md` §9, `contracts/schema-format-contract.md` §4's
`Kind: telemetry-reports` strategy). Consumes only the normalized schema
strategy — never guesses a vendor URI, never mutates the BMC's telemetry
configuration (FR-032).
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Optional

from ..schema.executor import FetchFunc, _jsonpath_all, _jsonpath_first
from ..schema.models import StrategyDef


def _parse_epoch_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


async def reduce_telemetry_reports(
    strategy: StrategyDef, telemetry_service_body: Any, fetch: FetchFunc,
    *, max_future_skew_seconds: float = 300, max_age_seconds: float = 3600,
    max_values_read_per_report: int = 50000, now: Optional[float] = None,
) -> list[dict[str, Any]]:
    """Discovers MetricReports via the strategy's `ReportsCollectionPath`,
    fetches every report matching `ReportAllowList`, and reduces to one
    value per `(MetricId, MetricProperty)` pair using
    `Reduce.Mode: latest-per-metric-and-property` — the greatest valid
    timestamp wins; ties break by lexicographic `(MetricId, MetricProperty)`.
    Returns a flat list of `{"MetricId", "MetricProperty", "MetricValue",
    "Timestamp"}` dicts — never raises on a nonconforming report; that
    report's values are simply skipped (FR-033: never crash, never
    fabricate)."""
    reports_collection_uri = _jsonpath_first(strategy.reports_collection_path, telemetry_service_body)
    if not reports_collection_uri:
        return []

    reports_collection = await fetch(reports_collection_uri)
    member_refs = _jsonpath_all(strategy.report_members, reports_collection)
    allow_patterns = [re.compile(p) for p in strategy.report_allow_list]
    current_time = time.time() if now is None else now

    best: dict[tuple[str, str], dict[str, Any]] = {}
    for member in member_refs:
        report_uri = member.get("@odata.id") if isinstance(member, dict) else member
        if not isinstance(report_uri, str):
            continue
        try:
            report = await fetch(report_uri)
        except Exception:
            continue  # a single unreachable/malformed report never fails the scrape

        report_id = _jsonpath_first(strategy.report_id_path, report)
        if not isinstance(report_id, str) or not any(p.search(report_id) for p in allow_patterns):
            continue

        report_timestamp = _jsonpath_first(strategy.report_timestamp_path, report)
        values = _jsonpath_all(strategy.report_values_path, report)
        for value_record in values[:max_values_read_per_report]:
            if not isinstance(value_record, dict):
                continue
            metric_id = _jsonpath_first(strategy.report_metric_id_path, value_record)
            metric_property = _jsonpath_first(strategy.report_metric_property_path, value_record)
            metric_value = _jsonpath_first(strategy.report_metric_value_path, value_record)
            timestamp = value_record.get("Timestamp", report_timestamp)
            if metric_id is None or metric_value is None:
                continue

            epoch = _parse_epoch_seconds(timestamp)
            if epoch is not None:
                if epoch > current_time + max_future_skew_seconds:
                    continue  # rejected: too far in the future
                if epoch < current_time - max_age_seconds:
                    continue  # rejected: stale

            key = (str(metric_id), str(metric_property))
            candidate = {
                "MetricId": metric_id, "MetricProperty": metric_property,
                "MetricValue": metric_value, "Timestamp": timestamp,
            }
            existing = best.get(key)
            if existing is None or _is_newer(timestamp, existing.get("Timestamp")):
                best[key] = candidate

    return list(best.values())


def _is_newer(candidate_ts: Any, existing_ts: Any) -> bool:
    """Greatest valid timestamp wins; a non-comparable/missing timestamp
    never displaces an existing valid one."""
    if candidate_ts is None:
        return False
    if existing_ts is None:
        return True
    try:
        return str(candidate_ts) > str(existing_ts)
    except TypeError:
        return False
