"""Local JSON cache and in-memory index for the ANAC aerodrome catalog."""

from __future__ import annotations

import json
import logging
import os
from asyncio import Lock
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

from app.intelligence.contracts import AerodromeCatalogEntry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CatalogSnapshot:
    synced_at: datetime
    aerodromes: list[AerodromeCatalogEntry]


class AnacCatalogCache:
    """Persist and query the ANAC aerodrome catalog from a local JSON file."""

    def __init__(self, cache_path: Path) -> None:
        self._cache_path = cache_path
        self._lock = Lock()
        self._loaded = False
        self._synced_at: datetime | None = None
        self._by_local: dict[str, AerodromeCatalogEntry] = {}
        self._by_icao: dict[str, AerodromeCatalogEntry] = {}

    @property
    def cache_path(self) -> Path:
        return self._cache_path

    @property
    def synced_at(self) -> datetime | None:
        return self._synced_at

    @property
    def size(self) -> int:
        return len(self._by_local)

    async def ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            self._load()

    def _load(self) -> None:
        self._by_local.clear()
        self._by_icao.clear()
        self._synced_at = None

        if not self._cache_path.exists():
            self._loaded = True
            logger.info("ANAC catalog cache not found at %s", self._cache_path)
            return

        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
            synced_raw = raw.get("synced_at")
            if synced_raw:
                self._synced_at = datetime.fromisoformat(synced_raw)
            for item in raw.get("aerodromes") or []:
                entry = AerodromeCatalogEntry.model_validate(item)
                self._index_entry(entry)
            logger.info(
                "Loaded %d aerodromes from ANAC catalog cache (%s)",
                len(self._by_local),
                self._cache_path,
            )
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("Failed to load ANAC catalog cache: %s", exc)

        self._loaded = True

    def _index_entry(self, entry: AerodromeCatalogEntry) -> None:
        self._by_local[entry.local_identifier.upper()] = entry
        if entry.icao_code:
            self._by_icao[entry.icao_code.upper()] = entry

    def reload(self) -> None:
        self._loaded = False

    def is_fresh(self, *, ttl_hours: float) -> bool:
        if self._synced_at is None or not self._by_local:
            return False
        age = datetime.now(timezone.utc) - self._synced_at
        return age.total_seconds() < ttl_hours * 3600

    def lookup_by_local(self, local_identifier: str) -> AerodromeCatalogEntry | None:
        if not self._loaded:
            raise RuntimeError("AnacCatalogCache not loaded — call ensure_loaded() first")
        return self._by_local.get(local_identifier.strip().upper())

    def lookup_by_icao(self, icao: str) -> AerodromeCatalogEntry | None:
        if not self._loaded:
            raise RuntimeError("AnacCatalogCache not loaded — call ensure_loaded() first")
        return self._by_icao.get(icao.strip().upper())

    def lookup_code(self, code: str) -> AerodromeCatalogEntry | None:
        normalized = code.strip().upper()
        if len(normalized) == 4:
            hit = self.lookup_by_icao(normalized)
            if hit is not None:
                return hit
        if len(normalized) == 3:
            return self.lookup_by_local(normalized)
        return self.lookup_by_icao(normalized) or self.lookup_by_local(normalized)

    def all_entries(self) -> list[AerodromeCatalogEntry]:
        if not self._loaded:
            raise RuntimeError("AnacCatalogCache not loaded — call ensure_loaded() first")
        return list(self._by_local.values())

    def save(self, snapshot: CatalogSnapshot) -> None:
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "synced_at": snapshot.synced_at.isoformat(),
            "aerodromes": [entry.model_dump(mode="json") for entry in snapshot.aerodromes],
        }
        tmp = Path(NamedTemporaryFile(
            suffix=".json",
            delete=False,
            dir=str(self._cache_path.parent),
        ).name)
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(str(tmp), str(self._cache_path))
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

        self._by_local.clear()
        self._by_icao.clear()
        for entry in snapshot.aerodromes:
            self._index_entry(entry)
        self._synced_at = snapshot.synced_at
        self._loaded = True
        logger.info(
            "Saved %d aerodromes to ANAC catalog cache (%s)",
            len(snapshot.aerodromes),
            self._cache_path,
        )


_global_cache: AnacCatalogCache | None = None


def get_global_catalog_cache(cache_path: Path | None = None) -> AnacCatalogCache:
    global _global_cache
    if _global_cache is None:
        if cache_path is None:
            cache_path = (
                Path(__file__).resolve().parent.parent.parent / "data" / "anac_catalog.json"
            )
        _global_cache = AnacCatalogCache(cache_path)
    return _global_cache


def reload_global_catalog_cache() -> AnacCatalogCache:
    cache = get_global_catalog_cache()
    cache.reload()
    return cache
