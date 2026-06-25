"""Unit tests for the ANAC catalog local cache."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.intelligence.contracts import AerodromeCatalogEntry
from app.intelligence.geo.anac_catalog_cache import AnacCatalogCache, CatalogSnapshot


@pytest.fixture
def cache_file() -> Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = Path(f.name)
    yield path
    os.unlink(path)


def _sample_entry(**overrides) -> AerodromeCatalogEntry:
    data = {
        "local_identifier": "SVO",
        "icao_code": "SAAV",
        "name": "SANTA FE / SAUCE VIEJO",
        "latitude": -31.71,
        "longitude": -60.81,
        "is_controlled": True,
        "control_status": "CONTROLLED",
        "is_active": True,
    }
    data.update(overrides)
    return AerodromeCatalogEntry(**data)


def test_cache_save_and_lookup(cache_file: Path):
    cache = AnacCatalogCache(cache_file)
    synced_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cache.save(CatalogSnapshot(synced_at=synced_at, aerodromes=[_sample_entry()]))

    reloaded = AnacCatalogCache(cache_file)
    reloaded._load()
    assert reloaded.size == 1
    assert reloaded.lookup_by_local("SVO") is not None
    assert reloaded.lookup_by_icao("SAAV") is not None
    assert reloaded.lookup_code("SVO") is not None
    assert reloaded.lookup_code("SAAV") is not None


def test_cache_is_fresh(cache_file: Path):
    cache = AnacCatalogCache(cache_file)
    cache.save(CatalogSnapshot(
        synced_at=datetime.now(timezone.utc),
        aerodromes=[_sample_entry()],
    ))
    assert cache.is_fresh(ttl_hours=24) is True
