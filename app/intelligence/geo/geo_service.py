"""Lightweight aerodrome geo-resolution service backed by the ANAC catalog."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.intelligence.contracts import AerodromeCatalogEntry, AerodromeGeoIntent, GeoCoords
from app.intelligence.geo.anac_catalog_cache import get_global_catalog_cache
from app.intelligence.geo.anac_client import AnacClient
from app.intelligence.geo.anac_mapper import map_detail_to_entry
from app.models.aerodrome import AerodromeDocument, GeoCache

logger = logging.getLogger(__name__)

_M_TO_FT = 3.28084


def _get_cache():
    return get_global_catalog_cache()


def _m_to_ft(m: float | None) -> int | None:
    if m is None:
        return None
    return int(round(m * _M_TO_FT))


def _entry_to_coords(code: str, entry: AerodromeCatalogEntry, *, source: str) -> GeoCoords:
    return GeoCoords(
        icao=code,
        lat=entry.latitude,
        lon=entry.longitude,
        elev_ft=_m_to_ft(entry.elevation_m),
        elev_m=entry.elevation_m,
        source=source,
    )


async def get_aerodrome_geo_intelligence(
    intent: AerodromeGeoIntent,
) -> dict[str, GeoCoords]:
    """Resolve coordinates for one or more ICAO or local identifiers.

    Cache-first (MongoDB ``geo`` field), then local ANAC catalog, then live ANAC detail.
    """
    cache = _get_cache()
    await cache.ensure_loaded()

    codes = _normalize_codes(intent)
    tasks = [_resolve_single(code, intent.force_refresh, cache) for code in codes]
    results = await asyncio.gather(*tasks)
    return {r.icao: r for r in results}


def _normalize_codes(intent: AerodromeGeoIntent) -> list[str]:
    if intent.icao is not None:
        return [intent.icao.strip().upper()]
    if intent.icaos is not None:
        return [code.strip().upper() for code in intent.icaos]
    return []


async def _resolve_single(
    code: str,
    force_refresh: bool,
    cache,
) -> GeoCoords:
    if not code:
        return GeoCoords(icao=code, source="not_found")

    lookup_icao = code if len(code) == 4 else None

    if not force_refresh and lookup_icao:
        doc = await AerodromeDocument.get(lookup_icao)
        if doc is not None and doc.geo is not None:
            logger.debug("[%s] Serving geo from Mongo cache.", code)
            return GeoCoords(
                icao=code,
                lat=doc.geo.lat,
                lon=doc.geo.lon,
                elev_ft=doc.geo.elev_ft,
                elev_m=doc.geo.elev_m,
                source="cache",
            )

    entry = cache.lookup_code(code)
    if entry is not None:
        return _entry_to_coords(code, entry, source="anac_catalog")

    entry = await _fetch_live_entry(code)
    if entry is None:
        logger.debug("[%s] No coordinates found in ANAC catalog.", code)
        return GeoCoords(icao=code, source="not_found")

    if lookup_icao:
        geo = GeoCache(
            lat=entry.latitude,
            lon=entry.longitude,
            elev_ft=_m_to_ft(entry.elevation_m),
            elev_m=entry.elevation_m,
            source="anac",
            cached_at=datetime.now(timezone.utc),
        )
        try:
            doc = await AerodromeDocument.get(lookup_icao)
            if doc is not None:
                doc.geo = geo
                await doc.save()
        except Exception:
            logger.warning("[%s] Failed to persist geo cache to MongoDB.", code)

    return _entry_to_coords(code, entry, source="anac")


async def _fetch_live_entry(code: str) -> AerodromeCatalogEntry | None:
    client = AnacClient()
    local = code if len(code) == 3 else None
    if local is None:
        cached = _get_cache().lookup_by_icao(code)
        if cached is not None:
            local = cached.local_identifier
        else:
            return None

    try:
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            detail = await client.fetch_airport_detail(local, client=http_client)
    except Exception as exc:
        logger.warning("Live ANAC detail fetch failed for %s: %s", code, exc)
        return None

    return map_detail_to_entry(detail)
