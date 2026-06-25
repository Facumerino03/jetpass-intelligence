"""Unit tests for geo resolution service backed by the ANAC catalog."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.intelligence.contracts import AerodromeCatalogEntry, AerodromeGeoIntent
from app.intelligence.geo.anac_catalog_cache import AnacCatalogCache, CatalogSnapshot
from app.models.aerodrome import AerodromeDocument, GeoCache


async def _async_none():
    return None


@pytest.fixture
def cache_file() -> Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = Path(f.name)
    yield path
    os.unlink(path)


@pytest.fixture
def catalog_cache(cache_file: Path) -> AnacCatalogCache:
    cache = AnacCatalogCache(cache_file)
    cache.save(CatalogSnapshot(
        synced_at=datetime.now(timezone.utc),
        aerodromes=[
            AerodromeCatalogEntry(
                local_identifier="SVO",
                icao_code="SAAV",
                name="SANTA FE / SAUCE VIEJO",
                latitude=-31.7108,
                longitude=-60.8114,
                elevation_m=17.0,
                is_controlled=True,
                control_status="CONTROLLED",
                is_active=True,
            ),
            AerodromeCatalogEntry(
                local_identifier="AER",
                icao_code="SABE",
                name="BUENOS AIRES / AEROPARQUE",
                latitude=-34.5592,
                longitude=-58.4156,
                elevation_m=5.0,
                is_controlled=True,
                control_status="CONTROLLED",
                is_active=True,
            ),
        ],
    ))
    return cache


@pytest.mark.asyncio
async def test_resolve_single_catalog_found(monkeypatch, catalog_cache: AnacCatalogCache):
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(AerodromeGeoIntent(icao="SAAV"))
    assert "SAAV" in result
    coords = result["SAAV"]
    assert coords.lat == pytest.approx(-31.7108)
    assert coords.lon == pytest.approx(-60.8114)
    assert coords.elev_m == pytest.approx(17.0)
    assert coords.source == "anac_catalog"


@pytest.mark.asyncio
async def test_resolve_by_local_identifier(monkeypatch, catalog_cache: AnacCatalogCache):
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(AerodromeGeoIntent(icao="SVO"))
    assert result["SVO"].lat == pytest.approx(-31.7108)
    assert result["SVO"].source == "anac_catalog"


@pytest.mark.asyncio
async def test_resolve_single_not_found(monkeypatch, catalog_cache: AnacCatalogCache):
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service._fetch_live_entry",
        lambda code: _async_none(),
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(AerodromeGeoIntent(icao="ZZZZ"))
    assert result["ZZZZ"].source == "not_found"


@pytest.mark.asyncio
async def test_resolve_batch(monkeypatch, catalog_cache: AnacCatalogCache):
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )
    monkeypatch.setattr(
        "app.intelligence.geo.geo_service._fetch_live_entry",
        lambda code: _async_none(),
    )

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icaos=["SAAV", "SABE", "ZZZZ"]),
    )
    assert len(result) == 3
    assert result["SAAV"].source == "anac_catalog"
    assert result["SABE"].source == "anac_catalog"
    assert result["ZZZZ"].source == "not_found"


@pytest.mark.asyncio
async def test_cache_hit_returns_cache_source(monkeypatch, catalog_cache: AnacCatalogCache):
    from app.models.aerodrome import AerodromeSnapshot

    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )

    doc = AerodromeDocument(
        id="SAAV",
        icao="SAAV",
        name="Santa Fe / Sauce Viejo",
        current=AerodromeSnapshot(),
        geo=GeoCache(
            lat=-31.7108,
            lon=-60.8114,
            elev_ft=56,
            elev_m=17.0,
            source="anac",
            cached_at=datetime.now(timezone.utc),
        ),
    )
    await doc.create()

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(AerodromeGeoIntent(icao="SAAV"))
    assert result["SAAV"].source == "cache"


@pytest.mark.asyncio
async def test_force_refresh_ignores_mongo_cache(monkeypatch, catalog_cache: AnacCatalogCache):
    from app.models.aerodrome import AerodromeSnapshot

    monkeypatch.setattr(
        "app.intelligence.geo.geo_service.get_global_catalog_cache",
        lambda: catalog_cache,
    )

    doc = AerodromeDocument(
        id="SAAV",
        icao="SAAV",
        name="Santa Fe / Sauce Viejo",
        current=AerodromeSnapshot(),
        geo=GeoCache(
            lat=0.0,
            lon=0.0,
            source="anac",
            cached_at=datetime.now(timezone.utc),
        ),
    )
    await doc.create()

    from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence

    result = await get_aerodrome_geo_intelligence(
        AerodromeGeoIntent(icao="SAAV", force_refresh=True),
    )
    assert result["SAAV"].source == "anac_catalog"


class TestGeoCache:
    def test_geo_cache_defaults(self):
        cache = GeoCache(source="anac")
        assert cache.lat is None
        assert cache.lon is None
        assert cache.elev_ft is None

    def test_geo_cache_with_values(self):
        now = datetime.now(timezone.utc)
        cache = GeoCache(
            lat=-31.7108,
            lon=-60.8114,
            elev_ft=56,
            elev_m=17.0,
            source="anac",
            cached_at=now,
        )
        assert cache.lat == -31.7108
        assert cache.source == "anac"
        assert cache.cached_at == now
