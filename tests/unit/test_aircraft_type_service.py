from __future__ import annotations

import pytest

from app.intelligence.aircraft_types.aircraft_type_service import (
    clear_validation_cache,
    get_aircraft_type_validation,
    validate_aircraft_type,
)
from app.intelligence.contracts import AircraftTypeValidateIntent, AircraftTypeValidationResult
from app.services.scraper.icao_doc8643_scraper import (
    IcaoDoc8643ScrapeResult,
    IcaoDoc8643ScraperError,
)


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    clear_validation_cache()


@pytest.mark.asyncio
async def test_validate_aircraft_type_uses_cache(monkeypatch) -> None:
    scrape = IcaoDoc8643ScrapeResult(
        designator="C172",
        is_valid=True,
        manufacturer="CESSNA",
        model="172",
        wtc="L",
    )
    calls = {"count": 0}

    async def fake_validate(designator: str, **kwargs):
        calls["count"] += 1
        return scrape

    monkeypatch.setattr(
        "app.intelligence.aircraft_types.aircraft_type_service.validate_designator",
        fake_validate,
    )

    first = await validate_aircraft_type("c172")
    second = await validate_aircraft_type("C172")

    assert first.is_valid is True
    assert first.source == "fresh_fetch"
    assert second.source == "cache"
    assert calls["count"] == 1


@pytest.mark.asyncio
async def test_validate_aircraft_type_force_refresh_bypasses_cache(monkeypatch) -> None:
    scrape = IcaoDoc8643ScrapeResult(designator="C172", is_valid=True)
    calls = {"count": 0}

    async def fake_validate(designator: str, **kwargs):
        calls["count"] += 1
        return scrape

    monkeypatch.setattr(
        "app.intelligence.aircraft_types.aircraft_type_service.validate_designator",
        fake_validate,
    )

    await validate_aircraft_type("C172")
    await validate_aircraft_type("C172", force_refresh=True)

    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_validate_aircraft_type_invalid_designator(monkeypatch) -> None:
    async def fake_validate(designator: str, **kwargs):
        return IcaoDoc8643ScrapeResult(designator=designator, is_valid=False)

    monkeypatch.setattr(
        "app.intelligence.aircraft_types.aircraft_type_service.validate_designator",
        fake_validate,
    )

    result = await validate_aircraft_type("ZZZZINVALID")

    assert result.is_valid is False
    assert result.entry is None
    assert "not found" in result.messages[0].lower()


@pytest.mark.asyncio
async def test_validate_aircraft_type_scraper_error_returns_alert(monkeypatch) -> None:
    async def fake_validate(designator: str, **kwargs):
        raise IcaoDoc8643ScraperError("browser failed")

    monkeypatch.setattr(
        "app.intelligence.aircraft_types.aircraft_type_service.validate_designator",
        fake_validate,
    )

    result = await validate_aircraft_type("C172")

    assert result.is_valid is False
    assert result.alerts[0].code == "aircraft_type_validation_failed"


@pytest.mark.asyncio
async def test_validate_aircraft_type_empty_designator() -> None:
    result = await validate_aircraft_type("   ")

    assert result.is_valid is False
    assert result.alerts[0].code == "aircraft_type_empty_designator"


@pytest.mark.asyncio
async def test_get_aircraft_type_validation_intent_wrapper(monkeypatch) -> None:
    calls: list[tuple[str, bool]] = []

    async def fake_validate(designator: str, *, force_refresh: bool = False):
        calls.append((designator, force_refresh))
        return AircraftTypeValidationResult(
            designator=designator.upper(),
            is_valid=True,
            source="fresh_fetch",
        )

    monkeypatch.setattr(
        "app.intelligence.aircraft_types.aircraft_type_service.validate_aircraft_type",
        fake_validate,
    )

    result = await get_aircraft_type_validation(
        AircraftTypeValidateIntent(designator="c172", force_refresh=True),
    )

    assert result.designator == "C172"
    assert calls == [("c172", True)]
