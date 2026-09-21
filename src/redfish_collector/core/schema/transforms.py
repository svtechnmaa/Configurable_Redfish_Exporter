"""The fixed v2 field-transform vocabulary and final `Type` validation.

Implements `contracts/schema-format-contract.md` §6 exactly. No dynamic
imports or vendor-specific adapter hooks — this is the complete, closed
vocabulary; an unknown `Op` fails schema *load* (`loader.py`), never
appears here at runtime.
"""

from __future__ import annotations

import math
from typing import Any

from .models import TransformDef


class TransformError(Exception):
    """A selector value failed a transform or final Type check — the
    selector is abandoned; the field falls through to its next Select entry
    or `Defaults.MissingValue` if none remain. Never raised to the caller of
    the component/field-extraction pipeline."""


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def apply_transform(value: Any, transform: TransformDef) -> Any:
    op = transform.op
    if op == "to-number":
        if _is_finite_number(value):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError as exc:
                raise TransformError(f"to-number: {value!r} is not a strict decimal string") from exc
        raise TransformError(f"to-number: unsupported input type {type(value).__name__}")

    if op in ("multiply", "add"):
        if not _is_finite_number(value):
            raise TransformError(f"{op}: input {value!r} is not a finite number")
        return (value * transform.value) if op == "multiply" else (value + transform.value)

    if op == "divide":
        if not _is_finite_number(value):
            raise TransformError(f"divide: input {value!r} is not a finite number")
        return value / transform.value

    if op == "round":
        if not _is_finite_number(value):
            raise TransformError(f"round: input {value!r} is not a finite number")
        return round(value, transform.digits)

    if op == "map":
        if value in transform.values:
            return transform.values[value]
        if transform.default is not None:
            return transform.default
        raise TransformError(f"map: {value!r} has no entry and no Default")

    raise TransformError(f"unknown transform op {op!r}")  # unreachable if loader validated


def apply_transforms(value: Any, transforms: tuple[TransformDef, ...]) -> Any:
    for transform in transforms:
        value = apply_transform(value, transform)
    return value


def normalize_status(value: Any) -> dict[str, str]:
    """Exactly matches `dataReconstruction.py`'s existing Status handling
    (`compatibility-baseline.md` §5): a dict fills missing State/Health with
    'Unknown'; a bare string becomes State (Health='Unknown'); None/missing
    becomes both 'Unknown'."""
    if isinstance(value, dict):
        result = dict(value)
        result.setdefault("State", "Unknown")
        result.setdefault("Health", "Unknown")
        return result
    if isinstance(value, str):
        return {"State": value, "Health": "Unknown"}
    if value is None:
        return {"State": "Unknown", "Health": "Unknown"}
    raise TransformError(f"status: unsupported input type {type(value).__name__}")


def validate_type(value: Any, field_type: str) -> Any:
    """Final `Type` check after transforms. Raises `TransformError` (the
    selector fails, falls through) on mismatch; never coerces implicitly."""
    if field_type == "status":
        return normalize_status(value)
    if field_type == "string":
        if isinstance(value, bool) or not isinstance(value, str):
            raise TransformError(f"Type string: got {type(value).__name__}")
        return value
    if field_type == "number":
        if not _is_finite_number(value):
            raise TransformError(f"Type number: got {value!r}")
        return value
    if field_type == "boolean":
        if not isinstance(value, bool):
            raise TransformError(f"Type boolean: got {type(value).__name__}")
        return value
    if field_type == "object":
        if not isinstance(value, (dict, list)):
            raise TransformError(f"Type object: got {type(value).__name__}")
        return value
    raise TransformError(f"unknown field type {field_type!r}")  # unreachable if loader validated
