"""Opt-in, bounded, target-isolated debug artifacts (`contracts/deployment-profile-contract.md`
`Tuning.Debug.*`, FR-041). Disabled by default; writes are atomic and
no-follow, under a target/config-hashed directory beneath `RootDirectory`.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from ..security.containment import ContainmentError, debug_artifact_dirname


class DebugArtifactError(Exception):
    pass


@dataclass
class DebugArtifactStore:
    root_directory: str
    write_raw_data: bool = False
    write_normalized_data: bool = False
    max_file_bytes: int = 5242880
    max_files_per_target: int = 4
    max_total_bytes: int = 104857600
    retention_seconds: int = 3600
    _total_bytes: int = field(default=0, init=False)
    _files: dict[str, tuple[float, int]] = field(default_factory=dict, init=False)  # path -> (mtime, size)

    def enabled(self) -> bool:
        return self.write_raw_data or self.write_normalized_data

    def _target_dir(self, canonical_server_address: str, config: str) -> Path:
        root = Path(self.root_directory)
        if not root.is_absolute():
            raise ContainmentError(f"Debug.RootDirectory must be absolute: {self.root_directory!r}")
        return root / debug_artifact_dirname(canonical_server_address, config)

    def write(self, canonical_server_address: str, config: str, filename: str, data: Any, *, now: Optional[float] = None) -> Optional[Path]:
        """Atomically writes `data` (JSON-serialized) under this target's
        isolated, hashed directory. Returns the path written, or `None` if
        disabled. Enforces per-file/per-target/process-wide bounds and
        purges entries older than `retention_seconds` first.

        Round-of-repair: root containment and no-follow semantics are now
        enforced AT THE ACTUAL FILE OPERATIONS, not by a separate
        `Path.resolve()` check performed before the real open/rename — that
        older check-then-use shape left a TOCTOU window: anything replacing
        a directory component (or the target/filename itself) with a
        symlink between the check and the later `mkstemp`/`os.replace` call
        could redirect the write outside `root_directory` undetected. Every
        path component here is opened relative to its own already-open
        parent directory FD with `O_NOFOLLOW` (`openat`/`mkdirat`
        semantics) — a symlink swapped in at any point after (or during)
        this call fails closed (`OSError`) instead of being silently
        followed."""
        if not self.enabled():
            return None
        if not filename or "/" in filename or filename in (".", ".."):
            raise ContainmentError(f"invalid debug artifact filename: {filename!r}")

        current_time = time.time() if now is None else now
        self._purge_expired(current_time)

        root = Path(self.root_directory)
        if not root.is_absolute():
            raise ContainmentError(f"Debug.RootDirectory must be absolute: {self.root_directory!r}")
        dirname = debug_artifact_dirname(canonical_server_address, config)
        target_dir = root / dirname
        final_path = target_dir / filename

        encoded = json.dumps(data, separators=(",", ":")).encode("utf-8")
        if len(encoded) > self.max_file_bytes:
            raise DebugArtifactError(f"artifact for {filename!r} exceeds MaxFileBytes ({len(encoded)} > {self.max_file_bytes})")

        existing_files_for_target = [k for k in self._files if k.startswith(str(target_dir) + os.sep)]
        if final_path not in [Path(p) for p in existing_files_for_target] and len(existing_files_for_target) >= self.max_files_per_target:
            self._evict_oldest_for_target(target_dir)

        while self._total_bytes + len(encoded) > self.max_total_bytes and self._files:
            self._evict_oldest_global()

        root.mkdir(parents=True, exist_ok=True)  # root itself is operator-configured, not attacker-influenced
        root_fd = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY)
        try:
            target_fd = self._open_or_create_subdir(root_fd, dirname)
            try:
                tmp_name = f".tmp-{uuid.uuid4().hex}"
                fd = os.open(
                    tmp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target_fd,
                )
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(encoded)
                    # Atomic rename, no partial file ever visible — both
                    # ends resolved relative to the SAME already-open
                    # directory FD, never re-walked from a path string.
                    os.replace(tmp_name, filename, src_dir_fd=target_fd, dst_dir_fd=target_fd)
                except BaseException:
                    try:
                        os.unlink(tmp_name, dir_fd=target_fd)
                    except OSError:
                        pass
                    raise
            finally:
                os.close(target_fd)
        finally:
            os.close(root_fd)

        key = str(final_path)
        old_size = self._files.get(key, (0, 0))[1]
        self._total_bytes += len(encoded) - old_size
        self._files[key] = (current_time, len(encoded))
        return final_path

    @staticmethod
    def _open_or_create_subdir(parent_fd: int, name: str) -> int:
        """Creates (if needed) and opens `name` directly under the
        directory `parent_fd` refers to — `O_NOFOLLOW` means a symlink
        planted at that name (before or after `mkdir`, which itself
        detects an existing dirent of any kind via `FileExistsError`)
        is never traversed; the open fails closed instead."""
        try:
            os.mkdir(name, dir_fd=parent_fd)
        except FileExistsError:
            pass
        return os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)

    def _evict_oldest_for_target(self, target_dir: Path) -> None:
        candidates = [k for k in self._files if k.startswith(str(target_dir) + os.sep)]
        if not candidates:
            return
        oldest = min(candidates, key=lambda k: self._files[k][0])
        self._remove(oldest)

    def _evict_oldest_global(self) -> None:
        if not self._files:
            return
        oldest = min(self._files, key=lambda k: self._files[k][0])
        self._remove(oldest)

    def _remove(self, path_str: str) -> None:
        _mtime, size = self._files.pop(path_str, (0, 0))
        self._total_bytes -= size
        # Same no-follow-at-the-actual-operation posture as `write()`: open
        # the parent directory with `O_NOFOLLOW` and unlink relative to
        # that FD, rather than `os.unlink(path_str)` re-walking the path
        # string (which a symlink swapped in after eviction was decided
        # could redirect).
        path = Path(path_str)
        try:
            parent_fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError:
            return
        try:
            os.unlink(path.name, dir_fd=parent_fd)
        except OSError:
            pass
        finally:
            os.close(parent_fd)

    def _purge_expired(self, now: float) -> None:
        expired = [k for k, (mtime, _size) in self._files.items() if (now - mtime) > self.retention_seconds]
        for k in expired:
            self._remove(k)
