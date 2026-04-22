from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.aerodrome import Aerodrome
from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeCreate, RunwayBase


@pytest.mark.asyncio
async def test_get_by_icao_normalizes_value() -> None:
    db = AsyncMock()
    result = MagicMock()
    expected = Aerodrome(
        icao_code="SAMR",
        iata_code="RGL",
        name="Rio Gallegos",
        city="Río Gallegos",
        province="Santa Cruz",
        country="Argentina",
        latitude=-51.60,
        longitude=-69.31,
        elevation_ft=65,
    )
    result.scalar_one_or_none.return_value = expected
    db.execute.return_value = result

    aerodrome = await aerodrome_repo.get_by_icao(db, " samr ")

    assert aerodrome is expected
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_all_returns_aerodromes_list() -> None:
    db = AsyncMock()
    result = MagicMock()
    expected = [
        Aerodrome(
            icao_code="SAEZ",
            iata_code="EZE",
            name="Ezeiza",
            city="Buenos Aires",
            province="Buenos Aires",
            country="Argentina",
            latitude=-34.82,
            longitude=-58.53,
            elevation_ft=67,
        )
    ]
    result.scalars.return_value.all.return_value = expected
    db.execute.return_value = result

    aerodromes = await aerodrome_repo.get_all(db)

    assert aerodromes == expected
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_upsert_creates_new_aerodrome(monkeypatch) -> None:
    db = AsyncMock()
    db.add = MagicMock()
    data = AerodromeCreate(
        icao_code="samr",
        iata_code="rgl",
        name="Rio Gallegos",
        city="Río Gallegos",
        province="Santa Cruz",
        country="Argentina",
        latitude=-51.6089,
        longitude=-69.3126,
        elevation_ft=65,
        runways=[RunwayBase(designator="07/25", length_m=3550, width_m=45, surface_type="ASPH")],
    )

    monkeypatch.setattr(aerodrome_repo, "get_by_icao", AsyncMock(return_value=None))

    aerodrome = await aerodrome_repo.upsert(db, data)

    assert aerodrome.icao_code == "SAMR"
    assert aerodrome.iata_code == "RGL"
    assert len(aerodrome.runways) == 1
    db.add.assert_called_once_with(aerodrome)
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(aerodrome)


@pytest.mark.asyncio
async def test_upsert_updates_existing_aerodrome(monkeypatch) -> None:
    db = AsyncMock()
    db.add = MagicMock()
    existing = Aerodrome(
        icao_code="SAMR",
        iata_code="OLD",
        name="Old Name",
        city="Old City",
        province="Old Province",
        country="Argentina",
        latitude=-50.0,
        longitude=-68.0,
        elevation_ft=10,
    )
    existing.runways = []
    data = AerodromeCreate(
        icao_code="samr",
        iata_code="rgl",
        name="Rio Gallegos",
        city="Río Gallegos",
        province="Santa Cruz",
        country="Argentina",
        latitude=-51.6089,
        longitude=-69.3126,
        elevation_ft=65,
        runways=[RunwayBase(designator="07/25", length_m=3550)],
    )

    monkeypatch.setattr(aerodrome_repo, "get_by_icao", AsyncMock(return_value=existing))

    updated = await aerodrome_repo.upsert(db, data)

    assert updated is existing
    assert updated.iata_code == "RGL"
    assert updated.name == "Rio Gallegos"
    assert len(updated.runways) == 1
    db.add.assert_not_called()
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(existing)
