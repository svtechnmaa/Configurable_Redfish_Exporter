"""Fast-lane job body (`data-model.md` FastLaneState; `research.md` §7).

Both v1 (legacy bridge) and v2 dispatch through this one vendor-neutral
entry point — no vendor branch here.
"""

from __future__ import annotations

from typing import Any

from ..schema.pipeline import collect_v2_target
from ..schema.models import CommonSchemaV2
from ..security.redaction import safe_exception_summary

FAST_LANE = "fast"


class FastLaneError(Exception):
    """The Fast lane job failed for this attempt — the caller (refresh
    cycle) records this on `FastLaneState.failed()`, never propagates a
    stale snapshot."""


async def run_fast_lane(
    common_schema: CommonSchemaV2,
    *,
    fetch: Any,
    legacy_lane_collector: Any = None,
    bootstrap_ctx: Any = None,
    selected_schema: Any = None,
    system_obj_for_reuse: Any = None,
    **legacy_kwargs: Any,
) -> dict[str, list[dict[str, Any]]]:
    try:
        return await collect_v2_target(
            common_schema, fetch=fetch, lane=FAST_LANE,
            legacy_lane_collector=legacy_lane_collector,
            bootstrap_ctx=bootstrap_ctx, selected_schema=selected_schema,
            system_obj_for_reuse=system_obj_for_reuse,
            **legacy_kwargs,
        )
    except Exception as exc:  # noqa: BLE001 - translated into the lane's own error type
        raise FastLaneError(safe_exception_summary(exc)) from exc
