"""Deterministic vendor-model schema selection (`research.md` §8).

v2: highest-`Priority` match wins; two matches tied at the highest priority
for the same observed target is a **target-selection-time** error (cannot be
detected at load time, since a real target's Manufacturer/Model isn't known
until then). v1: substring match in **declaration order** — the first match
in the file wins, deterministically, not by accidental dict-iteration luck.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from .models import CommonSchemaV2, ModelSchemaRule


class SchemaSelectionError(Exception):
    """Target-selection-time failure: no match, unavailable match, or a tie."""


@dataclass(frozen=True)
class SelectionResult:
    rule: ModelSchemaRule


def select_v2_model_schema(
    common: CommonSchemaV2, *, manufacturer: str, model: str, capabilities: dict[str, Any]
) -> SelectionResult:
    matches: list[ModelSchemaRule] = []
    for rule in common.model_schemas:
        if not re.search(rule.manufacturer_regex, manufacturer):
            continue
        if not re.search(rule.model_regex, model):
            continue
        if any(not capabilities.get(name) for name in rule.required_capabilities):
            continue
        matches.append(rule)

    if not matches:
        raise SchemaSelectionError(
            f"no ModelSchemas rule matches observed Manufacturer={manufacturer!r} Model={model!r}"
        )

    best_priority = max(r.priority for r in matches)
    best = [r for r in matches if r.priority == best_priority]
    if len(best) > 1:
        raise SchemaSelectionError(
            "ambiguous ModelSchemas selection: rules "
            f"{[r.id for r in best]} tie at Priority={best_priority} for "
            f"Manufacturer={manufacturer!r} Model={model!r}"
        )

    winner = best[0]
    if winner.unavailable_reason is not None:
        raise SchemaSelectionError(
            f"highest-priority match {winner.id!r} is unavailable: {winner.unavailable_reason}"
        )
    return SelectionResult(rule=winner)


def select_v1_model_schema(model_schema_raw: dict[str, Any], *, manufacturer: str, model: str) -> Optional[str]:
    """Replicates today's `dataCollector` substring-match loop exactly:
    declaration order for both the vendor level and the model level, first
    match wins. Returns the schema filename, or None if nothing matches.
    """
    for vendor_key, models in model_schema_raw.items():
        if vendor_key not in manufacturer:
            continue
        for model_key, schema_file in models.items():
            if str(model_key) in str(model):
                return schema_file
    return None
