"""Orchestrate ANAC MADHEL aerodrome catalog synchronisation."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.config import get_settings
from app.intelligence.contracts import (
    AerodromeCatalogEntry,
    AerodromeCatalogSyncIntent,
    AerodromeCatalogSyncResult,
    Alert,
    AlertLevel,
)
from app.intelligence.geo.anac_catalog_cache import (
    AnacCatalogCache,
    CatalogSnapshot,
    get_global_catalog_cache,
)
from app.intelligence.geo.anac_client import AnacClient
from app.intelligence.geo.anac_mapper import is_helipuerto_list_item, map_list_item_to_entry

logger = logging.getLogger(__name__)


def _build_result_from_entries(
    *,
    aerodromes: list[AerodromeCatalogEntry],
    total_listed: int,
    total_helipuertos_skipped: int,
    source: str,
    synced_at: datetime,
    skipped_unparseable: int = 0,
    messages: list[str] | None = None,
) -> AerodromeCatalogSyncResult:
    alerts: list[Alert] = []
    result_messages = list(messages or [])
    metadata: dict = {}

    if skipped_unparseable:
        metadata["skipped_unparseable"] = skipped_unparseable
        alerts.append(Alert(
            level=AlertLevel.WARNING,
            code="anac_unparseable_list_items",
            message=f"{skipped_unparseable} list items could not be mapped.",
        ))

    without_icao = sum(1 for entry in aerodromes if entry.icao_code is None)

    return AerodromeCatalogSyncResult(
        aerodromes=aerodromes,
        total_listed=total_listed,
        total_aerodromes=len(aerodromes),
        total_helipuertos_skipped=total_helipuertos_skipped,
        total_without_icao=without_icao,
        source=source,  # type: ignore[arg-type]
        synced_at=synced_at,
        alerts=alerts,
        messages=result_messages,
        metadata=metadata,
    )


async def sync_anac_catalog(
    *,
    force_refresh: bool = True,
    cache: AnacCatalogCache | None = None,
) -> AerodromeCatalogSyncResult:
    """Fetch the ANAC list endpoint once, parse aerodromes, and persist the cache."""
    catalog_cache = cache or get_global_catalog_cache()
    await catalog_cache.ensure_loaded()

    client = AnacClient()
    listed = await client.fetch_airports_list()

    aerodromes: list[AerodromeCatalogEntry] = []
    helipuertos_skipped = 0
    skipped_unparseable = 0

    for item in listed:
        if is_helipuerto_list_item(item):
            helipuertos_skipped += 1
            continue

        entry = map_list_item_to_entry(item)
        if entry is None:
            skipped_unparseable += 1
            continue
        aerodromes.append(entry)

    aerodromes.sort(key=lambda entry: entry.local_identifier)
    synced_at = datetime.now(timezone.utc)
    catalog_cache.save(CatalogSnapshot(synced_at=synced_at, aerodromes=aerodromes))

    logger.info(
        "ANAC catalog sync complete: listed=%d aerodromes=%d helipuertos_skipped=%d unparseable=%d",
        len(listed),
        len(aerodromes),
        helipuertos_skipped,
        skipped_unparseable,
    )

    return _build_result_from_entries(
        aerodromes=aerodromes,
        total_listed=len(listed),
        total_helipuertos_skipped=helipuertos_skipped,
        source="fresh_fetch",
        synced_at=synced_at,
        skipped_unparseable=skipped_unparseable,
        messages=["Synced from ANAC list endpoint (single request)."],
    )


async def get_aerodrome_catalog_sync(
    intent: AerodromeCatalogSyncIntent,
) -> AerodromeCatalogSyncResult:
    """Return the full aerodrome catalog, using cache when fresh enough."""
    settings = get_settings()
    cache = get_global_catalog_cache()
    await cache.ensure_loaded()

    if (
        not intent.force_refresh
        and cache.is_fresh(ttl_hours=settings.anac_catalog_cache_ttl_hours)
    ):
        synced_at = cache.synced_at or datetime.now(timezone.utc)
        aerodromes = cache.all_entries()
        return _build_result_from_entries(
            aerodromes=aerodromes,
            total_listed=cache.size,
            total_helipuertos_skipped=0,
            source="cache",
            synced_at=synced_at,
            messages=["Served from local ANAC catalog cache."],
        )

    return await sync_anac_catalog(force_refresh=True, cache=cache)
