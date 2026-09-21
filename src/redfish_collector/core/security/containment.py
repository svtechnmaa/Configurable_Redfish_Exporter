"""Config/schema path containment + canonical-IP handling (`research.md` §11).

Basename pattern check happens BEFORE any filesystem or network access;
realpath containment is defense-in-depth on top of it, not instead of it.
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
from pathlib import Path
from typing import Union

_CONFIG_STEM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_SCHEMA_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.yml$")


class ContainmentError(Exception):
    """A path/target failed containment validation. Message is safe to log
    (never includes secret content); it may name the offending input since
    `config`/schema names/IPs are not themselves secrets."""


def validate_config_stem(config: str) -> str:
    if not isinstance(config, str) or not _CONFIG_STEM_RE.match(config):
        raise ContainmentError(f"invalid config name: {config!r}")
    return config


def validate_schema_filename(name: str) -> str:
    if not isinstance(name, str) or not _SCHEMA_FILENAME_RE.match(name):
        raise ContainmentError(f"invalid schema filename: {name!r}")
    return name


def resolve_contained_path(base_dir: Union[str, Path], filename: str) -> Path:
    """Joins `filename` under `base_dir` and confirms the resolved realpath
    still has `base_dir`'s realpath as a prefix (defense-in-depth against a
    symlink or an already-bypassed pattern check)."""
    base = Path(base_dir).resolve()
    candidate = (base / filename).resolve()
    if candidate != base and base not in candidate.parents:
        raise ContainmentError(f"path escapes containment root: {filename!r}")
    return candidate


def resolve_config_path(configs_dir: Union[str, Path], config: str) -> Path:
    validate_config_stem(config)
    return resolve_contained_path(configs_dir, f"{config}.yml")


def resolve_schema_path(schemas_dir: Union[str, Path], schema_filename: str) -> Path:
    validate_schema_filename(schema_filename)
    return resolve_contained_path(schemas_dir, schema_filename)


def canonicalize_address(server_address: str) -> str:
    """Normalizes an IP literal (e.g. equivalent IPv6 spellings collapse to
    one canonical string). Raises `ContainmentError` for a non-IP value."""
    try:
        return str(ipaddress.ip_address(server_address))
    except ValueError as exc:
        raise ContainmentError(f"not a valid IP literal: {server_address!r}") from exc


def debug_artifact_dirname(canonical_server_address: str, config: str) -> str:
    """Never raw request input — always this fixed hash (`research.md` §11)."""
    digest = hashlib.sha256(f"{canonical_server_address}\0{config}".encode("utf-8"))
    return digest.hexdigest()


def resolve_debug_artifact_path(root_dir: Union[str, Path], canonical_server_address: str, config: str, filename: str) -> Path:
    root = Path(root_dir)
    if not root.is_absolute():
        raise ContainmentError(f"Debug.RootDirectory must be an absolute path: {root_dir!r}")
    target_dir = root / debug_artifact_dirname(canonical_server_address, config)
    # Resolve strictly under root/<hash-dir>/<filename>, re-checked against root.
    resolved_root = root.resolve()
    resolved = (target_dir / filename).resolve()
    if resolved_root not in resolved.parents:
        raise ContainmentError(f"debug artifact path escapes root: {filename!r}")
    if resolved.is_symlink():
        raise ContainmentError(f"debug artifact path is a symlink: {filename!r}")
    return resolved
