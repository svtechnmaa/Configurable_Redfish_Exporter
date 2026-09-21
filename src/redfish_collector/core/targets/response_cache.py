"""Whole-response cache (`data-model.md` ResponseCache, `research.md` §3).

Population rule (exact): an entry is written only for an HTTP 200 response
with `PhysicalServer_Query=1` (Fast-only or fully-warm both qualify);
`Query=0` and mid-loop-exception-partial responses never populate or
refresh the cache.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CacheEntry:
    key: tuple[str, str]
    body: bytes
    stored_at: float
    ttl_seconds: int
    size_bytes: int
    fast_generation: int
    slow_generation: Optional[int]
    expires_at: float


class ResponseCache:
    def __init__(self, *, max_entries: int, max_total_bytes: int, default_ttl_seconds: int = 180) -> None:
        self.max_entries = max_entries
        self.max_total_bytes = max_total_bytes
        self.default_ttl_seconds = default_ttl_seconds
        self._entries: dict[tuple[str, str], CacheEntry] = {}
        self._total_bytes = 0

    def get(self, key: tuple[str, str], *, now: Optional[float] = None) -> Optional[bytes]:
        entry = self._entries.get(key)
        if entry is None:
            return None
        current = time.time() if now is None else now
        if current > entry.expires_at:
            self._remove(key)
            return None
        return entry.body

    def put(
        self,
        key: tuple[str, str],
        body: bytes,
        *,
        fast_generation: int,
        slow_generation: Optional[int],
        lane_freshness_deadline: float,
        ttl_seconds: Optional[int] = None,
        now: Optional[float] = None,
    ) -> bool:
        """Returns True if stored, False if the body alone exceeds
        `max_total_bytes` (returned to the caller but never cached)."""
        current = time.time() if now is None else now
        size = len(body)
        if size > self.max_total_bytes:
            return False

        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        expires_at = min(current + ttl, lane_freshness_deadline)

        self._remove(key)  # replacement subtracts the prior entry's bytes first
        self._evict_expired(now=current)
        while (len(self._entries) >= self.max_entries or self._total_bytes + size > self.max_total_bytes) and self._entries:
            oldest_key = min(self._entries, key=lambda k: self._entries[k].stored_at)
            self._remove(oldest_key)

        entry = CacheEntry(
            key=key, body=body, stored_at=current, ttl_seconds=ttl, size_bytes=size,
            fast_generation=fast_generation, slow_generation=slow_generation, expires_at=expires_at,
        )
        self._entries[key] = entry
        self._total_bytes += size
        return True

    def invalidate(self, key: tuple[str, str]) -> None:
        """Called on every successful Fast/Slow publication and on target
        eviction — removes the exact entry before readers can observe the
        new generation."""
        self._remove(key)

    def _remove(self, key: tuple[str, str]) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._total_bytes -= entry.size_bytes

    def _evict_expired(self, *, now: float) -> None:
        expired = [k for k, e in self._entries.items() if now > e.expires_at]
        for k in expired:
            self._remove(k)

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def total_bytes(self) -> int:
        return self._total_bytes
