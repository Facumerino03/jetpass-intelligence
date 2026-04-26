"""Integration tests for aerodrome_import_service.

The scraper is mocked to return known PDF paths.
The parser is mocked (no real PDF parsing, DB or browser needed).
The repository is mocked to avoid DB connections.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.schemas.aerodrome import AerodromeCreate, AerodromeResponse, RunwayBase
from app.services.aerodrome_import_service import (
    AipImportError,
    import_aerodrome_from_aip,
)
from app.services.scraper.aip_scraper import AipScraperError

_SAMR_RESPONSE = AerodromeResponse(
    icao_code="SAMR",
    iata_code="RGL",
    name="Piloto Civil Norberto Fernández",
    city="Río Gallegos",
    province="Santa Cruz",
    country="Argentina",
    latitude=-51.608889,
    longitude=-69.3125,
    elevation_ft=65,
    runways=[
        RunwayBase(designator="07/25", length_m=3550, width_m=45, surface_type="ASPH")
    ],
)


def _make_aerodrome_orm() -> MagicMock:
    aerodrome = MagicMock()
    aerodrome.icao_code = "SAMR"
    aerodrome.iata_code = "RGL"
    aerodrome.name = "Piloto Civil Norberto Fernández"
    aerodrome.city = "Río Gallegos"
    aerodrome.province = "Santa Cruz"
    aerodrome.country = "Argentina"
    aerodrome.latitude = -51.608889
    aerodrome.longitude = -69.3125
    aerodrome.elevation_ft = 65
    runway = MagicMock()
    runway.designator = "07/25"
    runway.length_m = 3550
    runway.width_m = 45
    runway.surface_type = "ASPH"
    aerodrome.runways = [runway]
    return aerodrome


@pytest.mark.asyncio
async def test_import_aerodrome_full_pipeline(tmp_path: Path) -> None:
    """Happy path: scraper returns PDFs, parser extracts data, repo upserts."""
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    aerodrome_data = AerodromeCreate(
        icao_code="SAMR",
        name="Piloto Civil Norberto Fernández",
        city="Río Gallegos",
        province="Santa Cruz",
        country="Argentina",
        latitude=-51.608889,
        longitude=-69.3125,
        elevation_ft=65,
        runways=[
            RunwayBase(
                designator="07/25", length_m=3550, width_m=45, surface_type="ASPH"
            )
        ],
    )

    db = AsyncMock()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documents",
            return_value=aerodrome_data,
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(return_value=_make_aerodrome_orm()),
        ),
    ):
        result = await import_aerodrome_from_aip("SAMR", db, output_dir=tmp_path)

    assert isinstance(result, AerodromeResponse)
    assert result.icao_code == "SAMR"
    assert len(result.runways) == 1


@pytest.mark.asyncio
async def test_import_raises_when_scraper_fails(tmp_path: Path) -> None:
    db = AsyncMock()

    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(side_effect=AipScraperError("browser crash")),
    ):
        with pytest.raises(AipImportError, match="Scraper failed"):
            await import_aerodrome_from_aip("SAMR", db, output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_no_documents_downloaded(tmp_path: Path) -> None:
    """If no AIP PDFs are downloaded, parser stage fails with AipImportError."""
    db = AsyncMock()

    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(return_value=[]),
    ):
        with pytest.raises(AipImportError, match="Parser failed"):
            await import_aerodrome_from_aip("SAMR", db, output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_db_fails(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    aerodrome_data = AerodromeCreate(
        icao_code="SAMR",
        name="Test",
        country="Argentina",
        latitude=-51.0,
        longitude=-69.0,
        runways=[],
    )
    db = AsyncMock()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documents",
            return_value=aerodrome_data,
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ),
    ):
        with pytest.raises(AipImportError, match="Database upsert failed"):
            await import_aerodrome_from_aip("SAMR", db, output_dir=tmp_path)
