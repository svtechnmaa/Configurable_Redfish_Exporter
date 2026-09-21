"""Value-safe logging helpers (`research.md` §12, FR-035, SC-018).

Primary rule: construct log messages from explicit safe fields, never raw
payloads/headers/responses/URLs/exception text. `RedactingFilter` is a
defense-in-depth backstop, not the primary mechanism — callers must not
submit secret-bearing objects to logging in the first place.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

REDACTED = "***REDACTED***"

_SECRET_HEADER_NAMES = {"authorization", "x-auth-token", "cookie", "set-cookie"}
_SECRET_PAYLOAD_KEYS = {"password", "username", "token", "x-auth-token", "authorization"}

_PATTERNS = [
    # Order matters: "Bearer <token>" must be redacted before the generic
    # "authorization: <value>" pattern runs — otherwise the generic
    # pattern's value match stops at the first whitespace (on "Bearer"
    # itself) and leaves the real token, after the space, untouched.
    re.compile(r"(Bearer\s+)(\S+)", re.IGNORECASE),
    re.compile(r'("?(?:password|passwd|pwd)"?\s*[:=]\s*)("[^"]*"|\S+)', re.IGNORECASE),
    re.compile(r'("?(?:x-auth-token|authorization|token)"?\s*[:=]\s*)("[^"]*"|\S+)', re.IGNORECASE),
    re.compile(r"(://[^:/@\s]+:)([^@/\s]+)(@)"),  # userinfo in a URL
    # Fail-closed backstop for ANY query-string parameter, regardless of key
    # name: a rejected/discovered URL, Location header, or redirect target
    # embedded in raw exception text may carry an arbitrary secret-bearing
    # key (e.g. `?session_secret=...`) that none of the allow-listed key
    # names above recognize. Every `key=value` pair following `?`/`&` has
    # its value redacted wholesale — this is intentionally an allow-nothing
    # (not an allow-list) rule for query values, since callers must never be
    # trusted to only embed non-secret query keys in a message that reaches
    # this helper.
    re.compile(r"([?&][^=&\s]+=)([^&\s]*)"),
]


def redact_url(url: str) -> str:
    """Strips userinfo and query string; keeps only scheme+host+bounded path."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return REDACTED
    netloc = parts.hostname or ""
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        key: (REDACTED if key.lower() in _SECRET_HEADER_NAMES else value)
        for key, value in headers.items()
    }


def redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (REDACTED if key.lower() in _SECRET_PAYLOAD_KEYS else value)
        for key, value in payload.items()
    }


def safe_exception_summary(exc: BaseException, *, max_length: int = 200) -> str:
    """Never includes the raw `str(exc)` (which may embed a URL with
    userinfo, a payload, or a header value) — only the exception's class
    name plus a redacted, length-bounded rendering of its message. Some
    exception types (e.g. `aiohttp.ClientResponseError`/`ClientConnectorError`
    built with placeholder `None` internals, as fixture/test doubles do)
    raise from their OWN `__str__`; that must never propagate from a
    logging/redaction helper, so it falls back to the class name alone."""
    try:
        message = redact_text(str(exc))
    except Exception:
        return type(exc).__name__
    if len(message) > max_length:
        message = message[:max_length] + "...(truncated)"
    return f"{type(exc).__name__}: {message}"


def redact_text(text: str) -> str:
    for pattern in _PATTERNS:
        text = pattern.sub(lambda m: m.group(1) + REDACTED + (m.group(3) if m.lastindex and m.lastindex >= 3 else ""), text)
    return text


class RedactingFilter(logging.Filter):
    """Defense-in-depth backstop: scrubs known-secret patterns from any log
    record's rendered message before it leaves the process. Callers must
    still construct messages from safe fields — this is not the primary
    mechanism."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # `record.getMessage()` renders `record.msg % record.args`
            # (the %-style substitution `logger.warning("...%s...", val)`
            # relies on) — `str(record.msg)` alone is just the UNRENDERED
            # format string for any %-style call, which corrupted almost
            # every formatted log line once this filter was attached: the
            # raw template (still containing `%s` placeholders) was kept as
            # `record.msg` while `record.args` was cleared to `()`, so any
            # later re-render of the record raised `TypeError` internally
            # (visible as Python logging's own "--- Logging error ---"
            # diagnostic) instead of producing a clean, redacted line.
            record.msg = redact_text(record.getMessage())
            record.args = ()
        except Exception:
            # `record.getMessage()` failing (e.g. a mismatched %-placeholder
            # count) must never fall through to emitting the record
            # UNREDACTED — security-auditor finding: the previous bare
            # `pass` let stdlib's own `Handler.handleError()` print the raw
            # `record.msg`/`record.args` to stderr on that failure, bypassing
            # this filter entirely. Replace with a fixed, safe placeholder
            # instead of ever emitting whatever the original args were.
            record.msg = "<log message redaction failed>"
            record.args = ()
        return True
