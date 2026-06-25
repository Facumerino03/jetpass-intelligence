"""Unit tests for the ANAC aerodrome catalog sync service."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.intelligence.contracts import AerodromeCatalogSyncIntent
from app.intelligence.geo.anac_catalog_cache import AnacCatalogCache, CatalogSnapshot
from app.intelligence.aerodromes_catalog_service import (
    get_aerodrome_catalog_sync,
    sync_anac_catalog,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "anac"


@pytest.fixture
def cache_file():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = Path(f.name)
    yield path
    os.unlink(path)


@pytest.mark.asyncio
async def test_sync_anac_catalog_uses_list_only(monkeypatch, cache_file: Path):
    listed = [
        json.loads((FIXTURES / "svo_list_item.json").read_text(encoding="utf-8")),
        json.loads((FIXTURES / "hmd_list_item.json").read_text(encoding="utf-8")),
        json.loads((FIXTURES / "acb_list_item.json").read_text(encoding="utf-8")),
    ]

    class FakeClient:
        async def fetch_airports_list(self, client=None):
            return listed

        async def fetch_all_details(self, *args, **kwargs):
            raise AssertionError("detail fetch should not be called in list-only sync")

    monkeypatch.setattr(
        "app.intelligence.aerodromes_catalog_service.AnacClient",
        lambda: FakeClient(),
    )

    cache = AnacCatalogCache(cache_file)
    result = await sync_anac_catalog(force_refresh=True, cache=cache)

    assert result.total_listed == 3
    assert result.total_aerodromes == 2
    assert result.total_helipuertos_skipped == 1
    assert result.total_without_icao == 1
    assert result.aerodromes[0].local_identifier == "ACB"
    assert result.aerodromes[1].local_identifier == "SVO"
    assert cache.size == 2
    assert "single request" in result.messages[0].lower()


@pytest.mark.asyncio
async def test_get_aerodrome_catalog_sync_serves_cache(monkeypatch, cache_file: Path):
    from app.intelligence.contracts import AerodromeCatalogEntry

    cache = AnacCatalogCache(cache_file)
    entry = AerodromeCatalogEntry(
        local_identifier="SVO",
        icao_code="SAAV",
        name="SANTA FE / SAUCE VIEJO",
        latitude=-31.71,
        longitude=-60.81,
        is_controlled=True,
        control_status="CONTROLLED",
        is_active=True,
    )
    cache.save(CatalogSnapshot(
        synced_at=datetime.now(timezone.utc),
        aerodromes=[entry],
    ))

    monkeypatch.setattr(
        "app.intelligence.aerodromes_catalog_service.get_global_catalog_cache",
        lambda: cache,
    )

    result = await get_aerodrome_catalog_sync(AerodromeCatalogSyncIntent(force_refresh=False))
    assert result.source == "cache"
    assert result.total_aerodromes == 1
    assert result.aerodromes[0].icao_code == "SAAV"
