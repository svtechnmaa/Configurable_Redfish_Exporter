"""Bounded, order-preserving concurrent dispatch (`research.md` Group 5, item 2).

`asyncio.gather(*coros)` over an entire resource/member/child/component list
fires every one of them onto the event loop (and the underlying HTTP layer)
simultaneously — for a large collection (many Storage/Drive/Controller/...
members, or many external v1 components), this bursts far past the
configured per-BMC concurrency limits the local semaphore is supposed to
enforce. `Tuning.Crawl.BatchSize` exists to bound exactly this: this module
is the one place that actually applies it, replacing a list-sized
`asyncio.gather` with bounded-size batches.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Sequence

DEFAULT_BATCH_SIZE = 8


async def bounded_gather(coros: Sequence[Awaitable[Any]], *, batch_size: int = DEFAULT_BATCH_SIZE) -> list[Any]:
    """Awaits `coros` in batches of at most `batch_size`, awaiting each
    batch to completion before starting the next. Returns results in the
    SAME order `coros` was given — both the batching loop and `gather`
    within a batch preserve order — since callers build parent/child
    lineage and positional output by index."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    coros_list = list(coros)
    if not coros_list:
        return []
    results: list[Any] = []
    for start in range(0, len(coros_list), batch_size):
        batch = coros_list[start : start + batch_size]
        # Tasks, not bare coroutines, are handed to `gather` — `gather`
        # would wrap them into Tasks internally either way, but only a Task
        # we hold a reference to can be cancelled below. Creating them
        # explicitly (instead of relying on `gather`'s implicit wrapping)
        # schedules every one of them onto the loop up front, exactly like
        # the old bare-coroutine `gather(*batch)` did.
        tasks = [asyncio.ensure_future(c) for c in batch]
        try:
            results.extend(await asyncio.gather(*tasks))
        except BaseException:
            # Round-of-repair (code-review finding): when one task in THIS
            # batch fails, `gather` (without `return_exceptions=True`)
            # re-raises immediately but does NOT cancel its siblings — they
            # keep running against the target in the background, entirely
            # untracked by this function, outliving the failed lane. Cancel
            # every sibling that is not already done and await them (with
            # `return_exceptions=True` so a `CancelledError`/any other
            # exception from that drain never masks the original failure)
            # before proceeding, so no task from this batch is still
            # in-flight once this function returns control to its caller.
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # Every coroutine object in every LATER batch is still
            # un-awaited — `asyncio.gather` only ever schedules the batch it
            # was actually given, so this loop would otherwise abandon them,
            # and Python logs a "coroutine ... was never awaited"
            # `RuntimeWarning` for each one at GC time (round-9 code-review
            # finding). Close them explicitly so no such warning fires, then
            # re-raise the original failure unchanged — this method never
            # suppresses or alters it.
            for not_yet_scheduled in coros_list[start + batch_size :]:
                not_yet_scheduled.close()
            raise
    return results
