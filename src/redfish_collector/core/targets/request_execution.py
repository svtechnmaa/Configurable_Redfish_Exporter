"""Bounded, retried, dispatcher-integrated Redfish request execution
(`research.md` §4 batching, §13 retry/deadline/size-bound contract).

This is the "real" `FetchFunc` implementation the schema executor
(`schema/executor.py`) is decoupled from and designed to accept — it wraps
one HTTP call with: a `PriorityRequestDispatcher` lease, same-target/redirect
containment, a process-wide in-flight-response-byte budget, response-size
enforcement, and the exact retry/backoff/deadline policy below.

`research.md` Group 2 (round 7): `rawCollector.py`'s legacy `fetch()`, when
called with a `dispatcher` (the permanent v1-compatibility bridge,
`schema/legacy.py`'s `collect_legacy_lane`, always supplies one), and
`targets/login.py`'s login POST both now route through this SAME engine —
this is the one unified, safe, bounded transport path for login, v2 GET,
permanent v1 GET, and logout alike, not a v2-only concern. The legacy
`dataCollector()` monolithic entrypoint (a frozen, debug-only `__main__`
reference path, never invoked by production request handling) is the one
remaining caller that still uses `fetch()`'s original unguarded
`session.get()` fallback, deliberately unchanged.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from aiohttp import ClientConnectorError, ClientResponseError

from ..security.redaction import safe_exception_summary
from ..security.same_target import ResponseTooLargeError, SameTargetViolation, fetch_same_target
from .dispatcher import PriorityRequestDispatcher


class RequestExecutionError(Exception):
    """Terminal failure for one request attempt (never retried)."""


class DeadlineExceededError(RequestExecutionError):
    """The remaining lane/cycle deadline could not fit another attempt."""


_RETRYABLE_STATUSES_START = 500
_RETRYABLE_STATUSES_END = 599


class InFlightByteBudget:
    """Process-wide raw-response-body budget (`Tuning.IO.MaxInFlightResponseBytes`).
    Reserved before a network call, held through decode/transform/disposal,
    released after. Waiting for a reservation is itself deadline-bound and
    issues no network request while waiting."""

    def __init__(self, capacity_bytes: int) -> None:
        self.capacity_bytes = capacity_bytes
        self._in_use = 0
        self._waiters: list[asyncio.Future[None]] = []

    async def reserve(self, size_bytes: int, *, deadline: float) -> None:
        while True:
            if self._in_use + size_bytes <= self.capacity_bytes:
                self._in_use += size_bytes
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DeadlineExceededError("in-flight response byte budget: deadline exceeded while waiting")
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.append(future)
            try:
                await asyncio.wait_for(future, timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise DeadlineExceededError("in-flight response byte budget: deadline exceeded while waiting") from exc
            finally:
                # `except TimeoutError` alone only removes the waiter on a
                # genuine deadline expiry. If the calling task is instead
                # cancelled while awaiting `future` (asyncio.CancelledError,
                # not TimeoutError), that used to skip cleanup entirely and
                # leak this future in `_waiters` — a `release()` call still
                # sweeps it eventually, but nothing guarantees one happens
                # promptly while the budget stays saturated. `finally`
                # covers every exit (success, timeout, cancellation) and
                # cancellation itself is deliberately left to propagate
                # unchanged, never translated into `DeadlineExceededError`.
                if future in self._waiters:
                    self._waiters.remove(future)

    def release(self, size_bytes: int) -> None:
        self._in_use = max(0, self._in_use - size_bytes)
        for waiter in list(self._waiters):
            if not waiter.done():
                waiter.set_result(None)
        self._waiters.clear()


def _is_retryable_status(status: int) -> bool:
    return status == 429 or (_RETRYABLE_STATUSES_START <= status <= _RETRYABLE_STATUSES_END)


def compute_backoff_seconds(attempt: int, *, base: float, cap: float) -> float:
    return min(base * (2 ** (attempt - 1)), cap)


async def bounded_fetch(
    session: Any,
    url: str,
    *,
    canonical_host: str,
    dispatcher: PriorityRequestDispatcher,
    priority: str,
    cycle_id: str,
    request_id: str,
    byte_budget: InFlightByteBudget,
    max_response_bytes: int,
    max_attempts: int,
    timeout_seconds: float,
    backoff_base_seconds: float,
    backoff_cap_seconds: float,
    deadline: float,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    json_body: Optional[dict[str, Any]] = None,
    max_redirects: int = 3,
    follow_redirects: bool = True,
    return_headers: bool = False,
    allow_empty_body: bool = False,
) -> Any:
    """One logical Redfish request: dispatcher-admitted, same-target-contained,
    byte-budgeted, retried per the exact `research.md` §13 policy, bounded by
    `deadline` (an absolute `time.monotonic()` value)."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeadlineExceededError(f"no remaining deadline before attempt {attempt}")

        try:
            await byte_budget.reserve(max_response_bytes, deadline=deadline)
        except DeadlineExceededError:
            raise

        try:
            async with dispatcher.lease(priority, cycle_id, request_id):
                attempt_timeout = min(timeout_seconds, deadline - time.monotonic())
                if attempt_timeout <= 0:
                    raise DeadlineExceededError("no remaining deadline for this attempt")
                return await asyncio.wait_for(
                    fetch_same_target(
                        session, url, canonical_host=canonical_host, max_redirects=max_redirects,
                        method=method, headers=headers, json_body=json_body,
                        max_body_bytes=max_response_bytes,
                        return_headers=return_headers, allow_empty_body=allow_empty_body,
                        follow_redirects=follow_redirects,
                    ),
                    timeout=attempt_timeout,
                )
        except SameTargetViolation:
            raise  # never retried: a containment violation is always terminal
        except ResponseTooLargeError:
            raise  # FR-038: permanent failure, no retry storm
        except ClientResponseError as exc:
            if not _is_retryable_status(exc.status or 0):
                raise RequestExecutionError(safe_exception_summary(exc)) from exc
            last_error = exc
        except (ClientConnectorError, asyncio.TimeoutError, OSError) as exc:
            last_error = exc
        finally:
            byte_budget.release(max_response_bytes)

        if attempt == max_attempts:
            break
        backoff = compute_backoff_seconds(attempt, base=backoff_base_seconds, cap=backoff_cap_seconds)
        if time.monotonic() + backoff + 0.001 >= deadline:
            break  # backoff + another attempt would not fit; stop now
        await asyncio.sleep(backoff)

    raise RequestExecutionError(
        f"exhausted {max_attempts} attempt(s): {safe_exception_summary(last_error) if last_error else 'deadline exceeded'}"
    )
