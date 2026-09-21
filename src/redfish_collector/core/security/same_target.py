"""Same-target link/redirect enforcement (`research.md` §10).

`allow_redirects=False` is mandatory at every outbound call — aiohttp's
default transparently resends `X-Auth-Token` to a redirect's `Location`
before any application code can inspect it. This module is the single place
that manually inspects a 3xx response and either issues the next hop itself
(after validating it resolves to the exact same canonical host) or drops and
logs a redacted rejection.
"""

from __future__ import annotations

import ipaddress
import zlib
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

DEFAULT_HTTPS_PORT = 443


class SameTargetViolation(Exception):
    """A discovered link, redirect, or Location header did not resolve to
    the target's own canonical host — dropped, never followed."""


def format_host_for_url(host: str) -> str:
    """Brackets an IPv6 literal for use in a URL authority component
    (`https://[::1]/...`); returns IPv4/hostnames unchanged."""
    try:
        if ipaddress.ip_address(host).version == 6:
            return f"[{host}]"
    except ValueError:
        pass
    return host


def validate_same_target_url(location: str, *, canonical_host: str, canonical_port: int = DEFAULT_HTTPS_PORT) -> str:
    """Resolves `location` (absolute or host-relative) against
    `canonical_host`, requiring HTTPS, no userinfo, an exact host match, and
    an exact effective port match (the redirect's own port, or 443 if none
    is specified — never any other port, and never a port silently taken
    from the redirect and trusted). Returns the validated absolute URL;
    raises `SameTargetViolation` for anything else — including scheme
    downgrade, a different host/port, embedded userinfo, or a
    protocol-relative `//other-host/...` link."""
    if not location:
        raise SameTargetViolation("empty Location/link value")
    parts = urlsplit(location)
    if not parts.scheme and not parts.netloc:
        if not location.startswith("/"):
            # Never embed the raw rejected value (it may carry a query
            # string with a secret, e.g. `?session_secret=...`) — a fixed,
            # class/status-only message is sufficient for callers, which
            # must never log more than `type(exc).__name__` for this
            # exception class anyway (see redaction.py's fail-closed
            # backstop for any caller that forgets).
            raise SameTargetViolation("relative link is not host-rooted")
        return f"https://{format_host_for_url(canonical_host)}{location}"
    if parts.scheme != "https":
        raise SameTargetViolation(f"non-https scheme rejected: {parts.scheme!r}")
    if parts.username is not None or parts.password is not None:
        raise SameTargetViolation("userinfo in URL rejected")
    if parts.hostname != canonical_host:
        raise SameTargetViolation(f"host mismatch: {parts.hostname!r} != {canonical_host!r}")
    effective_port = parts.port if parts.port is not None else DEFAULT_HTTPS_PORT
    if effective_port != canonical_port:
        raise SameTargetViolation(f"port mismatch: {effective_port!r} != {canonical_port!r}")
    # Rebuild explicitly rather than `urlunsplit(parts)`: `parts.netloc`
    # would otherwise reproduce the redirect's own (already-validated but
    # possibly differently-formatted, e.g. unbracketed IPv6) authority
    # verbatim; reconstructing from the canonical host/port keeps the
    # returned URL byte-consistent regardless of how the redirect spelled it.
    netloc = format_host_for_url(canonical_host)
    if canonical_port != DEFAULT_HTTPS_PORT:
        netloc = f"{netloc}:{canonical_port}"
    return urlunsplit(("https", netloc, parts.path, parts.query, ""))


class ResponseTooLargeError(Exception):
    """FR-038: the response's `Content-Length` (or decoded body, when that
    header is absent) exceeds the configured bound — a permanent failure for
    this request, not a retryable condition."""


async def fetch_same_target(
    session: Any,
    url: str,
    *,
    canonical_host: str,
    max_redirects: int = 3,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    json_body: Optional[dict[str, Any]] = None,
    timeout: Any = None,
    max_body_bytes: Optional[int] = None,
    return_headers: bool = False,
    allow_empty_body: bool = False,
    follow_redirects: bool = True,
) -> Any:
    """Issues `method` against `url`, manually following up to
    `max_redirects` same-target-validated hops. Every call to the session is
    made with `allow_redirects=False` explicitly. Returns the parsed JSON
    body of the final 2xx response; raises on a non-2xx final status or any
    redirect/link containment violation.

    `return_headers=True` returns `(body, headers)` instead of just `body` —
    used by login (`targets/login.py`), which needs `X-Auth-Token`/
    `Location` from the final response, not only its JSON body, so login can
    share this same same-target-validated/redirect-contained/size-bounded
    engine instead of its own separate direct call (`research.md` Group 2).

    `allow_empty_body=True` returns `None` for a successful response with no
    body (e.g. a `204 No Content` logout DELETE) instead of raising trying
    to JSON-decode nothing — never attempts `.json()` on an empty body
    either way once this is set.

    `follow_redirects=False` (`Safety.FollowRedirects.Enabled` from the
    schema) rejects ANY 3xx response outright — it never inspects or
    validates the `Location` header at all, since a disabled setting means
    the operator does not trust this target to redirect safely."""
    # The initial URL is validated the same way as every subsequent hop —
    # an absolute `https://` URL was previously passed through unchecked
    # (only a same-branch host re-check ran below, which never verified
    # port or userinfo at all).
    current_url = validate_same_target_url(url, canonical_host=canonical_host)

    request_method = getattr(session, method.lower())
    for hop in range(max_redirects + 1):
        kwargs: dict[str, Any] = {"allow_redirects": False, "ssl": False}
        if headers is not None:
            kwargs["headers"] = headers
        if json_body is not None:
            kwargs["json"] = json_body
        if timeout is not None:
            kwargs["timeout"] = timeout
        async with request_method(current_url, **kwargs) as response:
            if 300 <= response.status < 400:
                if not follow_redirects:
                    raise SameTargetViolation("redirect rejected: FollowRedirects disabled by configuration")
                if hop >= max_redirects:
                    raise SameTargetViolation(f"exceeded max_redirects={max_redirects}")
                location = response.headers.get("Location")
                current_url = validate_same_target_url(location, canonical_host=canonical_host)
                continue
            if max_body_bytes is not None:
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    # `research.md` Group 2 (item 5): a malformed (non-
                    # numeric) `Content-Length` from a hostile/misbehaving
                    # BMC must never raise an uncaught `ValueError` out of
                    # this pre-check — it is only a best-effort early
                    # rejection anyway; the real bound is the streaming
                    # check in `_read_json_body_bounded` below, which still
                    # applies regardless of whether this header parses.
                    try:
                        declared_length = int(content_length)
                    except ValueError:
                        declared_length = None
                    if declared_length is not None and declared_length > max_body_bytes:
                        raise ResponseTooLargeError(
                            f"Content-Length {content_length} exceeds max_body_bytes={max_body_bytes}"
                        )
            response.raise_for_status()
            if allow_empty_body and response.status == 204:
                body = None
            else:
                body = await _read_json_body_bounded(response, max_body_bytes, allow_empty_body=allow_empty_body)
            if return_headers:
                return body, dict(response.headers)
            return body
    raise SameTargetViolation(f"exceeded max_redirects={max_redirects}")


class UnsupportedContentEncodingError(Exception):
    """`research.md` Group 2 (item 5): a response declared a `Content-
    Encoding` this engine does not know how to safely, boundedly
    decompress — rejected outright rather than silently handed to
    aiohttp's own implicit (session-level `auto_decompress`, disabled for
    every redesigned-path session) unbounded decompression."""


# Only encodings this engine can incrementally decompress with an
# independent, enforced output bound — `identity` (no `Content-Encoding`
# header, or an explicit "identity" value) needs no decompression at all.
# Any OTHER value (br, compress, zstd, an unknown/typo'd token, ...) is
# rejected before a single byte is decompressed.
_SUPPORTED_CONTENT_ENCODINGS = frozenset({"identity", "gzip", "x-gzip", "deflate"})


def _make_decompressobj(encoding: str):
    """Returns a `zlib.decompressobj` configured for `encoding`, or `None`
    for `identity` (no decompression needed). Both `gzip`/`x-gzip` and
    `deflate` are handled via `zlib` — `gzip` needs the 16-bit `wbits`
    offset for its own header/trailer framing; raw `deflate` (no zlib
    header) is requested with a negative `wbits`, matching what Redfish
    BMCs and aiohttp's own (now-bypassed) auto-decompression both expect."""
    if encoding in ("gzip", "x-gzip"):
        return zlib.decompressobj(zlib.MAX_WBITS | 16)
    if encoding == "deflate":
        return zlib.decompressobj(-zlib.MAX_WBITS)
    return None


async def _read_json_body_bounded(response: Any, max_body_bytes: Optional[int], *, allow_empty_body: bool) -> Any:
    """Reads and JSON-decodes the response body, bounded BEFORE full
    buffering when possible — the actual fix for FR-038/`research.md`
    Group 2's "stream and bound bytes before JSON parsing" requirement: the
    `Content-Length` pre-check above is not sufficient on its own — chunked
    transfer-encoding, a missing `Content-Length`, or a header value a
    hostile/compromised BMC simply lies about all bypass it, so a genuine
    streaming read is the real bound of last resort regardless of what any
    header claimed.

    `research.md` Group 2 (item 5): when `Content-Encoding` is present,
    both the WIRE (still-compressed, as read off the socket) byte count and
    the DECODED (post-decompression) byte count are bounded independently,
    each checked incrementally as data streams in — never after the fact,
    and never by fully materializing either buffer before checking. This
    requires the caller's session to be constructed with
    `auto_decompress=False` (see `refresh_context.py`) so `response.
    content` yields the raw, still-compressed wire bytes for this function
    to bound and decompress itself, rather than aiohttp already having
    silently, implicitly, unboundedly decompressed it beforehand.

    Prefers `response.content.iter_chunked()` (real `aiohttp.ClientResponse`
    always has this) — raises `ResponseTooLargeError` as soon as either
    bound is exceeded, WITHOUT reading further. Degrades gracefully for
    test doubles that only implement `.read()` (whole-body, bound checked
    after the fact, uncompressed only — matches every existing such
    fixture, none of which set `Content-Encoding`) or only `.json()` (the
    original, pre-streaming behavior) — never a hard requirement change for
    any existing caller's own doubles."""
    headers = getattr(response, "headers", None) or {}
    encoding = (headers.get("Content-Encoding") or "identity").strip().lower()
    if encoding not in _SUPPORTED_CONTENT_ENCODINGS:
        raise UnsupportedContentEncodingError(f"unsupported Content-Encoding: {encoding!r}")

    content = getattr(response, "content", None)
    if content is not None and hasattr(content, "iter_chunked"):
        decompressor = _make_decompressobj(encoding)
        wire_bytes = 0
        decoded = bytearray()
        async for chunk in content.iter_chunked(65536):
            wire_bytes += len(chunk)
            if max_body_bytes is not None and wire_bytes > max_body_bytes:
                raise ResponseTooLargeError(
                    f"streamed wire body exceeded max_body_bytes={max_body_bytes} before completing"
                )
            if decompressor is None:
                decoded.extend(chunk)
                if max_body_bytes is not None and len(decoded) > max_body_bytes:
                    raise ResponseTooLargeError(
                        f"streamed body exceeded max_body_bytes={max_body_bytes} before completing"
                    )
                continue
            # Bound each `decompress()` call's own OUTPUT length so a
            # single highly-compressible wire chunk (the classic
            # "decompression bomb" shape) can never allocate more decoded
            # memory in one call than still fits the remaining budget —
            # looping on `unconsumed_tail` to fully drain that chunk's
            # already-buffered input across as many bounded calls as
            # needed, checking the cumulative decoded size after every one.
            pending = chunk
            while pending:
                if max_body_bytes is not None:
                    remaining = max_body_bytes - len(decoded)
                    if remaining < 0:
                        remaining = 0
                    # Round 9 code-review note (accepted tradeoff, no
                    # behavior change): near the bound, `remaining` can
                    # collapse toward 0, making `step_limit` as small as 1
                    # — a highly-compressible tail chunk right at the cap
                    # then takes many single-byte `decompress()` calls
                    # instead of one. Not unbounded (`pending` still shrinks
                    # every iteration; the wire-byte cap above bounds the
                    # total chunk count) — a real but small CPU/latency
                    # cost, accepted in favor of never producing more than
                    # one byte past `max_body_bytes` before rejecting.
                    step_limit = remaining + 1  # +1 so exceeding-by-one is still observable below
                else:
                    step_limit = 65536
                try:
                    piece = decompressor.decompress(bytes(pending), step_limit)
                except zlib.error as exc:
                    raise ResponseTooLargeError(f"failed to decompress {encoding!r} response body") from exc
                decoded.extend(piece)
                if max_body_bytes is not None and len(decoded) > max_body_bytes:
                    raise ResponseTooLargeError(
                        f"decoded body exceeded max_body_bytes={max_body_bytes} before completing"
                    )
                pending = decompressor.unconsumed_tail
        if allow_empty_body and bytes(decoded) == b"":
            return None
        import json as _json

        return _json.loads(decoded.decode("utf-8")) if decoded else None

    read = getattr(response, "read", None)
    if callable(read):
        raw = await read()
        if max_body_bytes is not None and len(raw) > max_body_bytes:
            raise ResponseTooLargeError(f"body {len(raw)} bytes exceeds max_body_bytes={max_body_bytes}")
        if allow_empty_body and raw == b"":
            return None
        import json as _json

        return _json.loads(raw.decode("utf-8")) if raw else None

    # Last resort: a minimal test double exposing only `.json()` — the
    # original pre-streaming behavior, unbounded until decoded.
    body = await response.json()
    if max_body_bytes is not None and body is not None:
        import json as _json

        encoded_size = len(_json.dumps(body).encode("utf-8"))
        if encoded_size > max_body_bytes:
            raise ResponseTooLargeError(
                f"decoded body {encoded_size} bytes exceeds max_body_bytes={max_body_bytes}"
            )
    return body
