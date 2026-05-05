"""NOTAM intelligence service — cache-first fetch with scrape fallback."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.core.config import get_settings
from app.intelligence.contracts import Alert, AlertLevel, NotamIntelResult
from app.models.notam import NotamDocument
from app.repositories import aerodrome_repo, notam_location_repo, notam_repo
from app.tools.notam_scrape_tool import NotamScrapeToolError, scrape

logger = logging.getLogger(__name__)


def _is_stale(doc: NotamDocument) -> bool:
    ttl_hours = get_settings().notam_cache_ttl_hours
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    return doc.fetched_at < cutoff


async def get_notam_intelligence(icao: str, *, force_refresh: bool = False) -> NotamIntelResult:
    """Return NOTAM intelligence, reusing cache when data is still fresh."""
    icao = icao.strip().upper()
    alerts: list[Alert] = []
    messages = [f"[{icao}] Starting NOTAM intelligence pipeline."]

    aerodrome = await aerodrome_repo.get_by_icao(icao)
    if aerodrome is None:
        messages.append(
            f"[{icao}] Aerodrome was not found in 'aerodromes' collection; cannot resolve site dropdown name."
        )
        alerts.append(
            Alert(
                level=AlertLevel.ERROR,
                code="AERODROME_NOT_FOUND",
                message=f"Aerodrome '{icao}' not found in database.",
            )
        )
        return NotamIntelResult(
            icao=icao,
            source="fresh_scrape",
            alerts=alerts,
            messages=messages,
            metadata={"pipeline_stage": "resolve_aerodrome_name"},
        )

    aerodrome_name = (aerodrome.full_name or aerodrome.name).strip().upper()
    messages.append(f"[{icao}] Resolved dropdown name as '{aerodrome_name}'.")
    logger.info("[%s] NOTAM pipeline: resolved aerodrome name '%s'.", icao, aerodrome_name)

    location_map = await notam_location_repo.get_by_icao(icao)
    if location_map is not None:
        site_location = location_map.site_label_exact
        messages.append(
            f"[{icao}] Using exact site location mapping '{site_location}' from 'notam_locations'."
        )
    else:
        site_location = aerodrome_name
        messages.append(
            f"[{icao}] No entry in 'notam_locations'; falling back to aerodrome name '{site_location}'."
        )
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="NOTAM_LOCATION_MAPPING_MISSING",
                message=(
                    f"No NOTAM location mapping found for {icao}. "
                    "Run scripts.sync_notam_locations to avoid label mismatch issues."
                ),
            )
        )

    existing = await notam_repo.get_by_icao(icao)
    if existing is not None and not force_refresh and not _is_stale(existing):
        messages.append(f"[{icao}] Cache hit: returning persisted NOTAM payload.")
        return NotamIntelResult(
            icao=icao,
            aerodrome_name=existing.aerodrome_name,
            site_last_updated_at=existing.site_last_updated_at,
            fetched_at=existing.fetched_at,
            aerodrome_notams=existing.aerodrome_notams,
            fir_notams=existing.fir_notams,
            fir_notams_by_location=existing.fir_notams_by_location,
            source="cache",
            messages=messages,
            metadata={
                "pipeline_stage": "cache_hit",
                "aerodrome_notams_count": len(existing.aerodrome_notams),
                "fir_notams_count": len(existing.fir_notams),
                "fir_locations_count": len(existing.fir_notams_by_location),
            },
        )

    if existing is not None and not force_refresh:
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="STALE_CACHE",
                message=(
                    f"Cached NOTAM data for {icao} is older than "
                    f"{get_settings().notam_cache_ttl_hours} hour(s). Refreshing."
                ),
            )
        )
        messages.append(f"[{icao}] Cache is stale; refreshing via scraper.")
    elif force_refresh:
        messages.append(f"[{icao}] force_refresh=true; bypassing cache and scraping.")
    else:
        messages.append(f"[{icao}] Cache miss; scraping NOTAM website.")

    try:
        logger.info("[%s] NOTAM pipeline: starting scraper.", icao)
        scraped = await scrape(site_location)
    except NotamScrapeToolError as exc:
        alerts.append(Alert(level=AlertLevel.ERROR, code="NOTAM_SCRAPE_FAILED", message=str(exc)))
        messages.append(f"[{icao}] Scrape failed: {exc}")
        return NotamIntelResult(
            icao=icao,
            aerodrome_name=aerodrome_name,
            source="fresh_scrape",
            alerts=alerts,
            messages=messages,
            metadata={
                "pipeline_stage": "scrape",
                "site_location_used": site_location,
            },
        )

    doc = NotamDocument(
        id=icao,
        icao=icao,
        aerodrome_name=aerodrome_name,
        site_last_updated_at=scraped.site_last_updated_at,
        fetched_at=datetime.now(timezone.utc),
        aerodrome_notams=scraped.aerodrome_notams,
        fir_notams=scraped.fir_notams,
        fir_notams_by_location=scraped.fir_notams_by_location,
    )
    if scraped.site_last_updated_at is None:
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="NOTAM_LAST_UPDATE_PARSE_FAILED",
                message="Could not parse site 'Última actualización' timestamp.",
            )
        )
        messages.append(
            f"[{icao}] Could not parse site last update timestamp. "
            f"Raw text: {scraped.site_last_updated_text or '(missing)'}"
        )
    messages.append(
        f"[{icao}] Scrape ok: aerodrome_notams={len(doc.aerodrome_notams)} "
        f"fir_notams={len(doc.fir_notams)}."
    )
    persisted = await notam_repo.upsert(doc)
    messages.append(f"[{icao}] Persisted NOTAM payload to Mongo collection 'notams'.")
    logger.info(
        "[%s] NOTAM scrape completed (aerodrome=%d, fir=%d).",
        icao,
        len(persisted.aerodrome_notams),
        len(persisted.fir_notams),
    )

    return NotamIntelResult(
        icao=icao,
        aerodrome_name=persisted.aerodrome_name,
        site_last_updated_at=persisted.site_last_updated_at,
        fetched_at=persisted.fetched_at,
        aerodrome_notams=persisted.aerodrome_notams,
        fir_notams=persisted.fir_notams,
        fir_notams_by_location=persisted.fir_notams_by_location,
        source="fresh_scrape",
        alerts=alerts,
        messages=messages,
        metadata={
            "pipeline_stage": "persisted",
            "site_location_used": site_location,
            "site_last_updated_text_raw": scraped.site_last_updated_text,
            "aerodrome_notams_count": len(persisted.aerodrome_notams),
            "fir_notams_count": len(persisted.fir_notams),
            "fir_locations_count": len(persisted.fir_notams_by_location),
        },
    )
