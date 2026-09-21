"""v2 `CommonSchema` bootstrap discovery (`contracts/schema-format-contract.md` §1).

Captures `service_root`, `systems_collection`, `system_id`/`manufacturer`/
`model`/`system_uri`, and best-effort `chassis_uri`/`manager_uri` — optional
capabilities are treated as absent (not an error) when their capture/link is
missing, per the contract.
"""

from __future__ import annotations

from typing import Any

from jinja2 import Template

from .executor import ExecutorError, FetchFunc, _jsonpath_all, _jsonpath_first
from .models import CommonSchemaV2


async def discover_bootstrap(common: CommonSchemaV2, fetch: FetchFunc) -> tuple[dict[str, Any], Any]:
    """Returns `(captures, system_obj)` — `system_obj` is the raw System
    body already fetched at `captures["system_uri"]` during discovery, so a
    caller doing a cold Fast collection can reuse it as the Common resource
    body instead of fetching the same URI a second time (`research.md` §C /
    `plan.md`'s bootstrap-dedup decision)."""
    service_root = await fetch(common.bootstrap.service_root_uri)
    captures: dict[str, Any] = {}
    for name, path_expr in common.bootstrap.service_root_capture.items():
        value = _jsonpath_first(path_expr, service_root)
        if value is None:
            value = common.bootstrap.service_root_fallbacks.get(name)
        captures[name] = value

    systems_collection_uri = Template(common.bootstrap.system_collection_uri).render(captures)
    systems_collection = await fetch(systems_collection_uri)
    member_refs = _jsonpath_all(common.bootstrap.system_members, systems_collection)
    member_uris = [m["@odata.id"] if isinstance(m, dict) else m for m in member_refs]
    if not member_uris:
        raise ExecutorError("Bootstrap.System: no System resource found")
    if common.bootstrap.system_select == "only" and len(member_uris) != 1:
        raise ExecutorError(f"Bootstrap.System.Select=only but found {len(member_uris)} System(s)")
    system_uri = member_uris[0]
    captures["system_uri"] = system_uri

    system_obj = await fetch(system_uri)
    for name, path_expr in common.bootstrap.system_capture.items():
        captures[name] = _jsonpath_first(path_expr, system_obj)
    for required_name in ("system_id", "manufacturer", "model"):
        if not captures.get(required_name):
            raise ExecutorError(f"Bootstrap.System.Capture: required capture {required_name!r} resolved to nothing")

    if common.bootstrap.chassis_collection_uri:
        try:
            chassis_collection_uri = Template(common.bootstrap.chassis_collection_uri).render(captures)
            chassis_collection = await fetch(chassis_collection_uri)
            chassis_refs = _jsonpath_all(common.bootstrap.chassis_members, chassis_collection)
            chassis_uris = [m["@odata.id"] if isinstance(m, dict) else m for m in chassis_refs]
            if chassis_uris:
                captures["chassis_uri"] = chassis_uris[0]
                for name, path_expr in common.bootstrap.chassis_capture.items():
                    if name != "chassis_uri":
                        captures[name] = _jsonpath_first(path_expr, await fetch(chassis_uris[0]))
        except ExecutorError:
            pass  # optional capability: absent, not an error

    if common.bootstrap.manager_collection_uri:
        try:
            manager_collection_uri = Template(common.bootstrap.manager_collection_uri).render(captures)
            manager_collection = await fetch(manager_collection_uri)
            manager_refs = _jsonpath_all(common.bootstrap.manager_members, manager_collection)
            manager_uris = [m["@odata.id"] if isinstance(m, dict) else m for m in manager_refs]
            if manager_uris:
                captures["manager_uri"] = manager_uris[0]
        except ExecutorError:
            pass

    return captures, system_obj
