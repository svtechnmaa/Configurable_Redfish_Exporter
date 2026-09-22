"""Fail-closed validation helpers shared by the v2 schema loader.

Per-rule granularity: a broken/empty referenced schema is marked
unavailable with an actionable error while unrelated valid rules stay
usable (contract §8) — enforced by the caller (`loader.py`/`selection.py`),
not here; this module only raises for a genuinely invalid *file being
loaded right now*.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from jsonpath_ng.ext import parse as jsonpath_parse


class SchemaValidationError(Exception):
    """Raised for any schema-load validation failure. Names file + context."""


@lru_cache(maxsize=None)
def _compile_jsonpath_cached(expression: str) -> Any:
    """`jsonpath_ng.ext.parse` rebuilds its PLY lexer/parser from scratch on
    every call (uncached elsewhere, ~tens of ms each) — both the ~18
    schema-load-time validation call sites in `loader.py` and, far more
    importantly, `executor.py`'s per-field extraction on EVERY request
    (hundreds of calls per scrape) re-parsed the same finite, schema-defined
    expression strings from scratch every time. A parsed `jsonpath_ng`
    expression object is read-only for `.find()`, so caching by expression
    text is safe to share across callers/requests."""
    return jsonpath_parse(expression)


def compile_jsonpath(expression: str, *, context: str) -> Any:
    try:
        return _compile_jsonpath_cached(str(expression))
    except Exception as exc:  # jsonpath_ng raises assorted parser exceptions
        raise SchemaValidationError(f"{context}: JSONPath does not compile: {expression!r} ({exc})") from exc


def compile_regex(pattern: str, *, context: str) -> re.Pattern[str]:
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise SchemaValidationError(f"{context}: regex does not compile: {pattern!r} ({exc})") from exc


def reject_unknown_keys(data: dict[str, Any], allowed: set[str], *, context: str) -> None:
    unknown = set(data) - allowed
    if unknown:
        raise SchemaValidationError(f"{context}: unknown key(s) {sorted(unknown)}")


def require_keys(data: dict[str, Any], required: set[str], *, context: str) -> None:
    missing = required - set(data)
    if missing:
        raise SchemaValidationError(f"{context}: missing required key(s) {sorted(missing)}")
