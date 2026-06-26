"""ICAO Doc 8643 aircraft type designator validation service."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.core.config import get_settings
from app.intelligence.contracts import (
    AircraftTypeDesignator,
    AircraftTypeValidateIntent,
    AircraftTypeValidationResult,
    Alert,
    AlertLevel,
)
from app.services.aircraft_type_sync_runtime import update_aircraft_type_validation_status
from app.services.scraper.icao_doc8643_scraper import (
    IcaoDoc8643ScraperError,
    IcaoDoc8643ScrapeResult,
    validate_designator,
)

logger = logging.getLogger(__name__)

_cache: dict[str, tuple[AircraftTypeValidationResult, datetime]] = {}


def _normalize_designator(value: str) -> str:
    return value.strip().upper()


def _cache_get(designator: str, *, ttl_seconds: int) -> AircraftTypeValidationResult | None:
    cached = _cache.get(designator)
    if cached is None:
        return None
    result, expires_at = cached
    if datetime.now(timezone.utc) >= expires_at:
        _cache.pop(designator, None)
        return None
    return result.model_copy(update={"source": "cache"})


def _cache_set(designator: str, result: AircraftTypeValidationResult, *, ttl_seconds: int) -> None:
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    _cache[designator] = (result, expires_at)
    update_aircraft_type_validation_status(
        cache_size=len(_cache),
        last_validated_at=datetime.now(timezone.utc),
        last_error=None,
    )


def _to_entry(scrape: IcaoDoc8643ScrapeResult) -> AircraftTypeDesignator | None:
    if not scrape.is_valid:
        return None
    return AircraftTypeDesignator(
        designator=scrape.designator,
        manufacturer=scrape.manufacturer,
        model=scrape.model,
        engine_count=scrape.engine_count,
        engine_type=scrape.engine_type,
        wtc=scrape.wtc,
        aircraft_description=scrape.aircraft_description,
    )


def _build_result(
    scrape: IcaoDoc8643ScrapeResult,
    *,
    source: str,
    alerts: list[Alert] | None = None,
) -> AircraftTypeValidationResult:
    messages = []
    if scrape.is_valid:
        messages.append(f"Designator {scrape.designator} is registered in ICAO Doc 8643.")
    else:
        messages.append(f"Designator {scrape.designator} was not found in ICAO Doc 8643.")

    return AircraftTypeValidationResult(
        designator=scrape.designator,
        is_valid=scrape.is_valid,
        entry=_to_entry(scrape),
        source=source,  # type: ignore[arg-type]
        alerts=alerts or [],
        messages=messages,
        metadata={"engine": "icao_doc8643_scraper"},
    )


def clear_validation_cache() -> None:
    """Clear the in-memory validation cache (primarily for tests)."""
    _cache.clear()
    update_aircraft_type_validation_status(cache_size=0)


async def validate_aircraft_type(
    designator: str,
    *,
    force_refresh: bool = False,
) -> AircraftTypeValidationResult:
    """Validate a designator against ICAO Doc 8643 with a short-lived in-memory cache."""
    settings = get_settings()
    normalized = _normalize_designator(designator)
    if not normalized:
        return AircraftTypeValidationResult(
            designator="",
            is_valid=False,
            source="fresh_fetch",
            alerts=[
                Alert(
                    level=AlertLevel.ERROR,
                    code="aircraft_type_empty_designator",
                    message="Aircraft type designator must not be empty.",
                ),
            ],
            messages=["Aircraft type designator must not be empty."],
        )

    if not force_refresh:
        cached = _cache_get(normalized, ttl_seconds=settings.icao_doc8643_validation_cache_ttl_seconds)
        if cached is not None:
            return cached

    try:
        scrape = await validate_designator(normalized)
    except IcaoDoc8643ScraperError as exc:
        logger.warning("ICAO Doc 8643 validation failed for %s: %s", normalized, exc)
        update_aircraft_type_validation_status(last_error=str(exc))
        return AircraftTypeValidationResult(
            designator=normalized,
            is_valid=False,
            source="fresh_fetch",
            alerts=[
                Alert(
                    level=AlertLevel.ERROR,
                    code="aircraft_type_validation_failed",
                    message=str(exc),
                ),
            ],
            messages=[f"Could not validate designator {normalized} against ICAO Doc 8643."],
        )

    result = _build_result(scrape, source="fresh_fetch")
    _cache_set(normalized, result, ttl_seconds=settings.icao_doc8643_validation_cache_ttl_seconds)
    return result


async def get_aircraft_type_validation(intent: AircraftTypeValidateIntent) -> AircraftTypeValidationResult:
    """Orchestrator entry point for aircraft type validation."""
    return await validate_aircraft_type(
        intent.designator,
        force_refresh=intent.force_refresh,
    )
