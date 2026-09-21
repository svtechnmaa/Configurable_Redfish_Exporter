"""Permanent v1-legacy compatibility adapter (`contracts/schema-format-contract.md` §7).

Reuses the existing, unmodified `rawCollector.rawDataCollector`/
`dataReconstruction.dataReconstructor` functions — this module only adds the
corrected Fast/Slow lane split and the v2-Common-selects-v1-model bridge. It
does not reimplement crawling or reconstruction.
"""

from __future__ import annotations

from typing import Any, Optional

from ..batching import DEFAULT_BATCH_SIZE, bounded_gather
from ..rawCollector import getKeyDictFromURLPath, rawDataCollector
from ..dataReconstruction import dataReconstructor
from .selection import select_v1_model_schema

# Corrected legacy lane mapping (`research.md` Group 3, item 4): Fast is
# `Common` ONLY — every other component actually PRESENT in an external v1
# schema's `Metadata` block is Slow, determined dynamically rather than
# from a fixed enumeration. The prior fixed `LEGACY_SLOW_COMPONENTS`
# allow-list silently DROPPED any component name it didn't enumerate (never
# collected by either lane at all) — a real correctness gap for any
# externally-supplied v1 schema with a component name outside that fixed
# set. `Thermal`/`Power` were previously (incorrectly) kept in Fast; they
# are ordinary Slow-owned components like any other now.
LEGACY_FAST_COMPONENTS = frozenset({"Common"})


def derive_serverid_from_system_uri(system_uri: str) -> str:
    """Mixed-version bridge: derive `serverid` from the selected `system_uri`
    leaf segment, using the existing `getKeyDictFromURLPath(..., {">>serverid": -1})`
    semantics — never from the System response's own `Id` (a fixture where
    they differ guards this)."""
    try:
        key = getKeyDictFromURLPath(system_uri, {">>serverid": -1})
    except IndexError as exc:
        raise ValueError(f"malformed system_uri {system_uri!r}: {exc}") from exc
    if "serverid" not in key:
        raise ValueError(f"could not derive serverid from system_uri {system_uri!r}")
    return key["serverid"]


def select_legacy_vendor_schema(model_schema_raw: dict[str, Any], *, manufacturer: str, model: str) -> Optional[str]:
    return select_v1_model_schema(model_schema_raw, manufacturer=manufacturer, model=model)


async def collect_legacy_lane(
    schema_metadata: dict[str, Any],
    key_dict: dict[str, Any],
    token: str,
    log_level: str,
    session: Any,
    semaphore: Any,
    server_address: str,
    *,
    lane: str,
    dispatcher: Any = None,
    cycle_id: Optional[str] = None,
    deadline: Optional[float] = None,
    byte_budget: Any = None,
    timeout_seconds: Optional[float] = None,
    batch_size: Optional[int] = None,
    max_redirects: int = 3,
    follow_redirects: bool = True,
) -> dict[str, Any]:
    """Collects only `lane`'s components ("fast" or "slow") from a v1
    `<vendor-model>.yml` `Metadata` block, reusing `rawDataCollector`
    unchanged. Each component gets its own copy of `key_dict` at this
    fan-out point (unchanged fan-out-copy invariant).

    `dispatcher`/`cycle_id`/`deadline`/`byte_budget`/`timeout_seconds`,
    when supplied by the caller (the redesigned refresh cycle always
    supplies them — see `cycle_callbacks.py`), are threaded through to
    `rawDataCollector` so this permanent v1-compatibility path uses the
    SAME dispatcher-integrated, same-target-validated, size-bounded,
    ONE-shared-I/O-budget, configured-timeout transport engine the v2 path
    uses (`research.md` Group 2), rather than `rawCollector.fetch`'s own
    unguarded `session.get()` fallback or a separate hard-coded budget/
    timeout."""
    if lane not in ("fast", "slow"):
        raise ValueError(f"lane must be 'fast' or 'slow', got {lane!r}")
    # `research.md` Group 3 (item 4): Fast is `Common` only; every OTHER
    # component actually present in this schema's own `Metadata` block is
    # Slow — determined dynamically from what this specific schema
    # declares, never a fixed enumeration that could silently drop an
    # unrecognized component name from both lanes.
    if lane == "fast":
        selected = {name: node for name, node in schema_metadata.items() if name in LEGACY_FAST_COMPONENTS}
    else:
        selected = {name: node for name, node in schema_metadata.items() if name not in LEGACY_FAST_COMPONENTS}

    names = list(selected)
    coros = [
        rawDataCollector(
            server_address, selected[name], dict(key_dict), token, log_level, session, semaphore,
            dispatcher=dispatcher, cycle_id=cycle_id, priority=lane, deadline=deadline,
            byte_budget=byte_budget, timeout_seconds=timeout_seconds, batch_size=batch_size,
            max_redirects=max_redirects, follow_redirects=follow_redirects,
        )
        for name in names
    ]
    # `research.md` Group 5 (item 2): a list-sized `asyncio.gather` over
    # every selected component fires them all onto the event loop/
    # dispatcher at once — bounded here the same way `rawCollector.
    # fetch_all` is, using the same configured `Tuning.Crawl.BatchSize`.
    results = await bounded_gather(coros, batch_size=batch_size or DEFAULT_BATCH_SIZE)
    return dict(zip(names, results))


def reconstruct_legacy(data_raw: dict[str, Any], data_new_schema: dict[str, Any], model_schema_dir: str, server_address: str, log_level: str) -> dict[str, Any]:
    return dataReconstructor(data_raw, data_new_schema, model_schema_dir, server_address, log_level)
