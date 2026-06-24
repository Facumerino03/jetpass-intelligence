"""Lightweight aerodrome geo-resolution service (no AIP/NOTAM scraping)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from app.intelligence.contracts import AerodromeGeoIntent, GeoCoords
from app.intelligence.geo.airports_index import (
    AirportCsvIndex,
    get_global_index,
)
from app.models.aerodrome import AerodromeDocument, GeoCache

logger = logging.getLogger(__name__)

_FT_TO_M = 0.3048


def _get_index() -> AirportCsvIndex:
    return get_global_index()


def _ft_to_m(ft: int | None) -> float | None:
    if ft is None:
        return None
    return round(ft * _FT_TO_M, 1)


async def get_aerodrome_geo_intelligence(
    intent: AerodromeGeoIntent,
) -> dict[str, GeoCoords]:
    """Resolve coordinates for one or more ICAO codes.

    Cache-first (MongoDB ``geo`` field), fallback to CSV index.
    Never scrapes AIP or NOTAM.
    """
    index = _get_index()
    await index.ensure_loaded()

    icaos = _normalize_icaos(intent)
    tasks = [_resolve_single(icao, intent.force_refresh, index) for icao in icaos]
    results = await asyncio.gather(*tasks)
    return {r.icao: r for r in results}


def _normalize_icaos(intent: AerodromeGeoIntent) -> list[str]:
    if intent.icao is not None:
        return [intent.icao.strip().upper()]
    if intent.icaos is not None:
        return [icao.strip().upper() for icao in intent.icaos]
    return []


async def _resolve_single(
    icao: str,
    force_refresh: bool,
    index: AirportCsvIndex,
) -> GeoCoords:
    if not icao:
        return GeoCoords(icao=icao, source="not_found")

    if not force_refresh:
        doc = await AerodromeDocument.get(icao)
        if doc is not None and doc.geo is not None:
            logger.debug("[%s] Serving geo from cache.", icao)
            return GeoCoords(
                icao=icao,
                lat=doc.geo.lat,
                lon=doc.geo.lon,
                elev_ft=doc.geo.elev_ft,
                elev_m=doc.geo.elev_m,
                source="cache",
            )

    row = index.lookup(icao)
    if row is not None and row.lat is not None and row.lon is not None:
        geo = GeoCache(
            lat=row.lat,
            lon=row.lon,
            elev_ft=row.elev_ft,
            elev_m=_ft_to_m(row.elev_ft),
            source="csv",
            cached_at=datetime.now(timezone.utc),
        )
        try:
            doc = await AerodromeDocument.get(icao)
            if doc is not None:
                doc.geo = geo
                await doc.save()
        except Exception:
            logger.warning("[%s] Failed to persist geo cache to MongoDB.", icao)

        return GeoCoords(
            icao=icao,
            lat=row.lat,
            lon=row.lon,
            elev_ft=row.elev_ft,
            elev_m=_ft_to_m(row.elev_ft),
            source="csv",
        )

    logger.debug("[%s] No coordinates found in CSV.", icao)
    return GeoCoords(icao=icao, source="not_found")
