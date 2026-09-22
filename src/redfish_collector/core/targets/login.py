"""Redfish session login for the redesigned refresh-cycle path.

Matches today's token-acquisition behavior (`rawCollector.dataCollector`)
exactly in shape — POST to the token URI with `{"UserName", "Password"}`,
read `X-Auth-Token`/`Location` — but is parameterized so it works for both
the v2 `Bootstrap.Authentication` path and the permanent v1 bridge's
`$tokenuri`, and uses `Tuning.Login.*` for attempts/timeout.

`research.md` Group 2: when `dispatcher` is supplied (the redesigned
refresh cycle always supplies one — see `cycle_callbacks.py`), the actual
POST routes through `targets.request_execution.bounded_fetch` — the SAME
dispatcher-integrated, same-target-validated, redirect-contained,
size-bounded, retried engine every v2 GET and (as of this round) the
permanent v1 legacy GET path use. `dispatcher=None` preserves the exact
prior direct-`session.post()` behavior unchanged, for any caller that does
not (yet) supply one.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Optional

from aiohttp import ClientConnectorError, ClientResponseError

from .request_execution import DeadlineExceededError, RequestExecutionError, bounded_fetch
from ..security.redaction import safe_exception_summary
from ..security.same_target import ResponseTooLargeError, SameTargetViolation, validate_same_target_url


class LoginFailedError(Exception):
    """Login exhausted its attempts or received a non-retryable failure —
    terminal for this cycle open attempt."""


async def login_and_get_token(
    session: Any, token_uri: str, *, username: str, password: str,
    canonical_address: str, max_attempts: int = 3, timeout_seconds: float = 60,
    dispatcher: Any = None, cycle_id: Optional[str] = None, deadline: Optional[float] = None,
    max_response_bytes: int = 10485760, backoff_base_seconds: float = 0.5, backoff_cap_seconds: float = 30.0,
    byte_budget: Any = None, semaphore: Optional[asyncio.Semaphore] = None,
    max_redirects: int = 3, follow_redirects: bool = True,
) -> tuple[str, Optional[str]]:
    """Returns `(token, logout_uri)`. `logout_uri` is `None` when the
    response has no `Location` header (existing, preserved behavior).

    `semaphore`, when supplied (the redesigned refresh cycle always
    supplies `context.local_semaphore` — see `cycle_callbacks.py`), is the
    cycle-local semaphore that must be acquired BEFORE the shared per-BMC
    dispatcher lease acquired inside `bounded_fetch` — `research.md` Group
    2 (item 2): "every method acquires the cycle-local semaphore before the
    per-BMC priority dispatcher and releases in reverse order exactly
    once", not only the GET path.

    `research.md` Group 2 (item 6): every production request-handling path
    (`cycle_callbacks.py`'s `login()`) always supplies `dispatcher`.
    Omitting it — explicitly isolated below in
    `_login_via_direct_session_debug_only`, named so it cannot be reached
    by accident — is reachable ONLY by this repo's own regression tests for
    that legacy unguarded-direct-POST behavior; no future routing change
    should ever call it by simply forgetting to pass `dispatcher=`."""
    if dispatcher is not None:
        return await _login_via_unified_engine(
            session, token_uri, username=username, password=password, canonical_address=canonical_address,
            max_attempts=max_attempts, timeout_seconds=timeout_seconds, dispatcher=dispatcher, cycle_id=cycle_id,
            deadline=deadline, max_response_bytes=max_response_bytes, backoff_base_seconds=backoff_base_seconds,
            backoff_cap_seconds=backoff_cap_seconds, byte_budget=byte_budget, semaphore=semaphore,
            max_redirects=max_redirects, follow_redirects=follow_redirects,
        )

    return await _login_via_direct_session_debug_only(
        session, token_uri, username=username, password=password, canonical_address=canonical_address,
        max_attempts=max_attempts, timeout_seconds=timeout_seconds,
    )


async def _login_via_direct_session_debug_only(
    session: Any, token_uri: str, *, username: str, password: str,
    canonical_address: str, max_attempts: int, timeout_seconds: float,
) -> tuple[str, Optional[str]]:
    """`research.md` Group 2 (item 6): the ORIGINAL, unguarded
    `session.post()` fallback — no dispatcher admission, no same-target/
    redirect containment, no shared byte budget, no cycle-local semaphore.
    Explicitly isolated into its own distinctly-named function (never
    inlined into `login_and_get_token()` itself) so it can never be
    reached by a future call site that simply forgets to pass
    `dispatcher=` — every genuine caller of THIS function must name it
    explicitly. The only real caller today is `login_and_get_token()`'s
    own `dispatcher is None` branch, itself only exercised by this repo's
    own regression tests for that legacy behavior — unlike `rawCollector.
    fetch`'s equivalent debug-only fallback, no production entrypoint
    (including the frozen `dataCollector()` monolith, which acquires its
    own token inline rather than via this module) calls this today."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.post(
                token_uri, json={"UserName": username, "Password": password},
                ssl=False, allow_redirects=False, timeout=timeout_seconds,
            ) as response:
                if response.status >= 400:
                    if response.status < 500 and response.status != 429:
                        raise LoginFailedError(f"authentication failed (HTTP {response.status})")
                    last_error = LoginFailedError(f"transient HTTP {response.status}")
                else:
                    token = response.headers.get("X-Auth-Token")
                    if not token:
                        raise LoginFailedError("token response has no X-Auth-Token header")
                    location = response.headers.get("Location")
                    logout_uri = _validate_logout_location(location, canonical_address)
                    return token, logout_uri
        except LoginFailedError:
            raise
        except (ClientConnectorError, asyncio.TimeoutError, OSError) as exc:
            last_error = exc

        if attempt < max_attempts:
            await asyncio.sleep(2 ** (attempt - 1))

    # Security-review finding (round-of-repair, low): never embed the raw
    # exception (`last_error`, an aiohttp/OS exception whose own `str()`
    # can include connector/host/port text) — consistent with every other
    # exception-to-message conversion in this codebase.
    last_error_summary = safe_exception_summary(last_error) if last_error is not None else "no attempts succeeded"
    raise LoginFailedError(f"login failed after {max_attempts} attempts: {last_error_summary}")


def _validate_logout_location(location: Optional[str], canonical_address: str) -> Optional[str]:
    if not location:
        return None
    try:
        return validate_same_target_url(location, canonical_host=canonical_address)
    except SameTargetViolation:
        # A BMC (compromised, misconfigured, or hostile on-path given
        # `ssl=False`) returning a Location that resolves off the
        # canonical host must never receive our token on a later logout
        # DELETE. Fail closed: proceed without a logout URI rather than
        # trust it. The rejected Location/URL is never logged, even
        # redacted — only a fixed, generic notice is emitted (defense in
        # depth alongside `SameTargetViolation`'s own fixed, class/status-
        # only messages and `redaction.py`'s query-string backstop).
        logging.warning(
            "[%s] rejecting a logout Location that failed same-target validation", canonical_address
        )
        return None


class _NullSemaphore:
    """Used only when a caller genuinely supplies no cycle-local semaphore
    (never true in production — `cycle_callbacks.py`'s `login()` always
    passes `context.local_semaphore`) — an `async with` no-op so
    `_login_via_unified_engine` doesn't need a separate branch for that
    case."""

    async def __aenter__(self) -> "_NullSemaphore":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None


async def _login_via_unified_engine(
    session: Any, token_uri: str, *, username: str, password: str, canonical_address: str,
    max_attempts: int, timeout_seconds: float, dispatcher: Any, cycle_id: Optional[str], deadline: Optional[float],
    max_response_bytes: int, backoff_base_seconds: float, backoff_cap_seconds: float, byte_budget: Any,
    semaphore: Optional[asyncio.Semaphore] = None,
    max_redirects: int = 3, follow_redirects: bool = True,
) -> tuple[str, Optional[str]]:
    import time as _time

    from .request_execution import InFlightByteBudget

    if byte_budget is None:
        # `research.md` Group 2 (item 1): this per-call fallback exists ONLY
        # for a caller that genuinely supplies none (never a production
        # path — `cycle_callbacks.py`'s `login()` always passes
        # `context.io_byte_budget`, the one process-wide shared budget) —
        # never a substitute for that shared instance.
        byte_budget = InFlightByteBudget(capacity_bytes=max_response_bytes)
    effective_deadline = deadline if deadline is not None else (_time.monotonic() + timeout_seconds * max_attempts)
    sem = semaphore if semaphore is not None else _NullSemaphore()

    try:
        # `research.md` Group 2 (item 2): the cycle-local semaphore is
        # acquired BEFORE the shared per-BMC dispatcher lease (acquired
        # inside `bounded_fetch` itself) and released in reverse order via
        # this `async with` block's own `__aexit__` — the same order every
        # GET (`rawCollector.fetch`'s dispatcher branch) already uses.
        async with sem:
            body, headers = await bounded_fetch(
                session, token_uri, canonical_host=canonical_address, dispatcher=dispatcher,
                # The dispatcher only recognizes "fast"/"slow" — login blocks
                # the WHOLE cycle (both lanes depend on it), so it is always
                # admitted at "fast" priority regardless of which lane(s) this
                # cycle ultimately serves.
                priority="fast", cycle_id=cycle_id or "login", request_id=str(uuid.uuid4()),
                byte_budget=byte_budget, max_response_bytes=max_response_bytes, max_attempts=max_attempts,
                timeout_seconds=timeout_seconds, backoff_base_seconds=backoff_base_seconds,
                backoff_cap_seconds=backoff_cap_seconds, deadline=effective_deadline,
                method="POST", json_body={"UserName": username, "Password": password},
                return_headers=True, allow_empty_body=True,
                max_redirects=max_redirects, follow_redirects=follow_redirects,
            )
    except SameTargetViolation as exc:
        raise LoginFailedError(f"login response containment violation: {type(exc).__name__}") from exc
    except ResponseTooLargeError as exc:
        raise LoginFailedError(f"login response too large: {type(exc).__name__}") from exc
    except DeadlineExceededError as exc:
        raise LoginFailedError("login deadline exceeded") from exc
    except RequestExecutionError as exc:
        # `bounded_fetch` already classified retryable vs. permanent HTTP
        # statuses and exhausted attempts on our behalf — this is always
        # terminal by the time it reaches us.
        raise LoginFailedError(f"login failed: {exc}") from exc
    except ClientResponseError as exc:
        raise LoginFailedError(f"authentication failed (HTTP {exc.status})") from exc

    token = headers.get("X-Auth-Token")
    if not token:
        raise LoginFailedError("token response has no X-Auth-Token header")
    logout_uri = _validate_logout_location(headers.get("Location"), canonical_address)
    return token, logout_uri
