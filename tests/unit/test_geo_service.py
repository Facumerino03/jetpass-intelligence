"""Unit tests for geo resolution service and CSV index."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

import pytest

from app.intelligence.contracts import AerodromeGeoIntent
from app.intelligence.geo.airports_index import (
    AirportCsvIndex,
    _parse_row,
    _iter_rows,
)
from app.models.aerodrome import AerodromeDocument, GeoCache

SAMPLE_CSV = """\
"id","ident","type","name","latitude_deg","longitude_deg","elevation_ft","continent","iso_country","iso_region","municipality","scheduled_service","icao_code","iata_code","gps_code","local_code","home_link","wikipedia_link","keywords"
6523,"00A","heliport","Total RF Heliport",40.070985,-74.933689,11,"NA","US","US-PA","Bensalem","no",,,"K00A","00A",,,
30770,"SAEZ","large_airport","Ministro Pistarini",-34.8222,-58.5358,66,"SA","AR","AR-B","Ezeiza","yes","SAEZ","EZE","SAEZ",,,"https://en.wikipedia.org/wiki/Ministro_Pistarini_International_Airport",
30771,"SABE","large_airport","Jorge Newbery",-34.5592,-58.4156,22,"SA","AR","AR-C","Buenos Aires","yes","SABE","AEP","SABE",,,"https://en.wikipedia.org/wiki/Jorge_Newbery_Airpark",
99999,"XXXX","small_airport","Nowhere",,,,"NA","US","US-XX","Nowhere","no","XXXX",,,"XXXX",,,
"""


# ── helpers ───────────────────────────────────────────────────────────────────


@pytest.fixture
def csv_file() -> Path:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(SAMPLE_CSV)
        path = Path(f.name)
    yield path
    os.unlink(path)


@pytest.fixture
def index(csv_file: Path) -> AirportCsvIndex:
    idx = AirportCsvIndex(csv_file)
    # Synchronous load for tests (avoids async fixture issues with pytest-asyncio).
    idx._load()  # type: ignore[attr-defined]
    return idx


# ── _parse_row ────────────────────────────────────────────────────────────────


def test_parse_row_valid():
    raw = {
        "ident": "SAEZ",
        "type": "large_airport",
        "name": "Ministro Pistarini",
        "latitude_deg": "-34.8222",
        "longitude_deg": "-58.5358",
        "elevation_ft": "66",
        "iso_country": "AR",
        "municipality": "Ezeiza",
        "icao_code": "SAEZ",
    }
    row = _parse_row(raw)
    assert row is not None
    assert row.icao_code == "SAEZ"
    assert row.lat == pytest.approx(-34.8222)
    assert row.lon == pytest.approx(-58.5358)
    assert row.elev_ft == 66


def test_parse_row_empty_icao():
    raw = {
        "ident": "00A",
        "icao_code": "",
        "latitude_deg": "40.07",
        "longitude_deg": "-74.93",
    }
    assert _parse_row(raw) is None


def test_parse_row_missing_coords():
    raw = {
        "ident": "XXXX",
        "icao_code": "XXXX",
        "latitude_deg": "",
        "longitude_deg": "",
        "elevation_ft": "",
    }
    row = _parse_row(raw)
    assert row is not None
    assert row.lat is None
    assert row.lon is None
    assert row.elev_ft is None


# ── _iter_rows ────────────────────────────────────────────────────────────────


def test_iter_rows_yields_only_rows_with_icao(csv_file: Path):
    rows = list(_iter_rows(csv_file))
    assert len(rows) == 3  # SAEZ, SABE, XXXX (00A has no icao_code)
    icaos = {r.icao_code for r in rows}
    assert icaos == {"SAEZ", "SABE", "XXXX"}


# ── AirportCsvIndex ────────────────────────────────────────────────────────────


def test_index_lookup_found(index: AirportCsvIndex):
    row = index.lookup("SAEZ")
    assert row is not None
    assert row.icao_code == "SAEZ"
    assert row.lat == pytest.approx(-34.8222)
    assert row.lon == pytest.approx(-58.5358)
    assert row.elev_ft == 66


def test_index_lookup_not_found(index: AirportCsvIndex):
    assert index.lookup("KJFK") is None


def test_index_lookup_case_insensitive(index: AirportCsvIndex):
    row = index.lookup("saez")
    assert row is not None
    assert row.icao_code == "SAEZ"


def test_index_reload(index: AirportCsvIndex, csv_file: Path):
    assert index.size == 3
    with csv_file.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "100000",
            "KJFK",
            "large_airport",
            "John F Kennedy",
            "40.6413",
            "-73.7781",
            "13",
            "NA",
            "US",
            "US-NY",
            "New York",
            "yes",
            "KJFK",
            "JFK",
            "KJFK",
            "",
            "",
            "",
            "",
        ])
    index.reload()
    index._load()  # type: ignore[attr-defined]
    assert index.size == 4
    assert index.lookup("KJFK") is not None


# ── _resolve_single (via get_aerodrome_geo_intelligence) ────────────────────


@pytest.mark.asyncio
async def test_resolve_single_csv_found(monkeypatch, csv_file: Path):
    """First call with a valid ICAO should resolve from CSV and cache to DB."""
    test_idx = AirportCsvIndex(csv_file)
    test_idx._load()
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_index",
        lambda: test_idx,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icao="SAEZ"),
    )
    assert "SAEZ" in result
    coords = result["SAEZ"]
    assert coords.lat == pytest.approx(-34.8222)
    assert coords.lon == pytest.approx(-58.5358)
    assert coords.elev_ft == 66
    assert coords.elev_m == pytest.approx(20.1, rel=0.1)
    assert coords.source in ("csv", "cache")


@pytest.mark.asyncio
async def test_resolve_single_not_found(monkeypatch, csv_file: Path):
    """Unknown ICAO should return nulls."""
    test_idx = AirportCsvIndex(csv_file)
    test_idx._load()
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_index",
        lambda: test_idx,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icao="ZZZZ"),
    )
    assert "ZZZZ" in result
    coords = result["ZZZZ"]
    assert coords.lat is None
    assert coords.lon is None
    assert coords.elev_ft is None
    assert coords.elev_m is None
    assert coords.source == "not_found"


@pytest.mark.asyncio
async def test_resolve_batch(monkeypatch, csv_file: Path):
    """Batch resolution should return all ICAOs."""
    test_idx = AirportCsvIndex(csv_file)
    test_idx._load()
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_index",
        lambda: test_idx,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icaos=["SAEZ", "SABE", "ZZZZ"]),
    )
    assert len(result) == 3
    assert result["SAEZ"].source in ("csv", "cache")
    assert result["SABE"].source in ("csv", "cache")
    assert result["ZZZZ"].source == "not_found"


@pytest.mark.asyncio
async def test_cache_hit_returns_cache_source(monkeypatch, csv_file: Path):
    """When an AerodromeDocument with geo exists, it should be served from cache."""
    from datetime import datetime, timezone

    test_idx = AirportCsvIndex(csv_file)
    test_idx._load()
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_index",
        lambda: test_idx,
    )

    from app.models.aerodrome import AerodromeSnapshot

    doc = AerodromeDocument(
        id="SAEZ",
        icao="SAEZ",
        name="Ministro Pistarini",
        current=AerodromeSnapshot(),
        geo=GeoCache(
            lat=-34.8222,
            lon=-58.5358,
            elev_ft=66,
            elev_m=20.1,
            source="csv",
            cached_at=datetime.now(timezone.utc),
        ),
    )
    await doc.create()

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icao="SAEZ"),
    )
    assert result["SAEZ"].source == "cache"


@pytest.mark.asyncio
async def test_force_refresh_ignores_cache(monkeypatch, csv_file: Path):
    """force_refresh=True should bypass cache and re-resolve from CSV."""
    test_idx = AirportCsvIndex(csv_file)
    test_idx._load()
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_index",
        lambda: test_idx,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    await get_aerodrome_geo_intelligence(AerodromeGeoIntent(icao="SAEZ"))

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icao="SAEZ", force_refresh=True),
    )
    assert result["SAEZ"].source == "csv"


# ── GeoCache model ─────────────────────────────────────────────────────────────


class TestGeoCache:
    def test_geo_cache_defaults(self):
        cache = GeoCache(source="csv")
        assert cache.lat is None
        assert cache.lon is None
        assert cache.elev_ft is None

    def test_geo_cache_with_values(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        cache = GeoCache(
            lat=-34.8222,
            lon=-58.5358,
            elev_ft=66,
            elev_m=20.1,
            source="csv",
            cached_at=now,
        )
        assert cache.lat == -34.8222
        assert cache.source == "csv"
        assert cache.cached_at == now
