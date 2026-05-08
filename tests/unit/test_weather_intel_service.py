import json
from types import SimpleNamespace

import pytest

from app.intelligence.weather_intel_service import get_weather_intelligence


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int):
        self.values[key] = value
        self.ttls[key] = ex


def _aerodrome(icao: str = "SAEZ"):
    return SimpleNamespace(icao=icao)


@pytest.mark.asyncio
async def test_weather_requires_aerodrome(monkeypatch):
    async def missing_aerodrome(icao: str):
        return None

    monkeypatch.setattr(
        "app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao",
        missing_aerodrome,
    )

    result = await get_weather_intelligence("SAEZ")

    assert result.icao == "SAEZ"
    assert result.source == "fresh_fetch"
    assert result.alerts[0].code == "AERODROME_NOT_FOUND"


@pytest.mark.asyncio
async def test_weather_requires_redis(monkeypatch):
    async def existing_aerodrome(icao: str):
        return _aerodrome(icao)

    async def missing_redis():
        return None

    monkeypatch.setattr(
        "app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao",
        existing_aerodrome,
    )
    monkeypatch.setattr(
        "app.intelligence.weather_intel_service.get_redis", missing_redis
    )

    result = await get_weather_intelligence("SAEZ")

    assert result.alerts[0].code == "WEATHER_CACHE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_weather_cache_hit_returns_cached_products(monkeypatch):
    redis = FakeRedis()
    redis.values["weather:station:SAEZ"] = json.dumps(
        {"icaoId": "SAEZ", "name": "EZEIZA", "lat": -34.8, "lon": -58.5}
    )
    redis.values["weather:metar:SAEZ"] = json.dumps(
        {"icaoId": "SAEZ", "rawOb": "SAEZ METAR"}
    )
    redis.values["weather:taf:SAEZ"] = json.dumps(
        {"icaoId": "SAEZ", "rawTAF": "SAEZ TAF"}
    )
    redis.values["weather:isigmet:global"] = json.dumps(
        {"type": "FeatureCollection", "features": []}
    )

    async def existing_aerodrome(icao: str):
        return _aerodrome(icao)

    async def fake_redis():
        return redis

    monkeypatch.setattr(
        "app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao",
        existing_aerodrome,
    )
    monkeypatch.setattr(
        "app.intelligence.weather_intel_service.get_redis", fake_redis
    )

    result = await get_weather_intelligence("saez")

    assert result.icao == "SAEZ"
    assert result.source == "cache"
    assert result.station is not None
    assert result.metar is not None
    assert result.taf is not None


@pytest.mark.asyncio
async def test_upstream_error_produces_warning_alert(monkeypatch):
    from app.tools.aviation_weather_tool import AviationWeatherToolError

    redis = FakeRedis()
    async def existing_aerodrome(icao) -> SimpleNamespace:
        return SimpleNamespace(icao=icao)

    async def fake_redis():
        return redis

    async def failing_fetch_metar(self, icao: str, *, hours: float):
        raise AviationWeatherToolError("simulated upstream failure")

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", existing_aerodrome)
    monkeypatch.setattr("app.intelligence.weather_intel_service.get_redis", fake_redis)
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_metar",
        failing_fetch_metar,
    )
    async def _fake_station_info(self, icao):
        return []

    async def _fake_taf(self, icao):
        return []

    async def _fake_isigmet(self):
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_station_info",
        _fake_station_info,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_taf",
        _fake_taf,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_isigmet_geojson",
        _fake_isigmet,
    )

    result = await get_weather_intelligence("SAEZ")

    warning_alerts = [a for a in result.alerts if a.level == "warning"]
    assert any(a.code == "WEATHER_UPSTREAM_ERROR" for a in warning_alerts)


@pytest.mark.asyncio
async def test_force_refresh_bypasses_cache(monkeypatch):
    redis = FakeRedis()
    redis.values["weather:station:SAEZ"] = json.dumps({"icaoId": "SAEZ", "name": "EZEIZA", "lat": -34.8, "lon": -58.5})
    redis.values["weather:metar:SAEZ"] = json.dumps({"icaoId": "SAEZ", "rawOb": "OLD METAR"})
    redis.values["weather:taf:SAEZ"] = json.dumps({"icaoId": "SAEZ", "rawTAF": "OLD TAF"})
    redis.values["weather:isigmet:global"] = json.dumps({"type": "FeatureCollection", "features": []})

    async def existing_aerodrome(icao) -> SimpleNamespace:
        return SimpleNamespace(icao=icao)

    async def fake_redis():
        return redis

    fresh_station = [{"icaoId": "SAEZ", "name": "EZEIZA", "lat": -34.8, "lon": -58.5}]
    fresh_metar = [{"icaoId": "SAEZ", "rawOb": "FRESH METAR"}]
    fresh_taf = [{"icaoId": "SAEZ", "rawTAF": "FRESH TAF"}]
    fresh_sigmet = {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", existing_aerodrome)
    monkeypatch.setattr("app.intelligence.weather_intel_service.get_redis", fake_redis)
    async def _fresh_station(self, icao):
        return fresh_station

    async def _fresh_metar(self, icao, *, hours):
        return fresh_metar

    async def _fresh_taf(self, icao):
        return fresh_taf

    async def _fresh_sigmet(self):
        return fresh_sigmet

    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_station_info",
        _fresh_station,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_metar",
        _fresh_metar,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_taf",
        _fresh_taf,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_isigmet_geojson",
        _fresh_sigmet,
    )

    result = await get_weather_intelligence("SAEZ", force_refresh=True)

    assert result.source == "fresh_fetch"
    assert result.station is not None
    assert result.metar is not None
    assert result.metar.raw == "FRESH METAR"
    assert result.taf is not None
    assert result.taf.raw == "FRESH TAF"


@pytest.mark.asyncio
async def test_metar_hours_back_is_passed_to_fetch_metar(monkeypatch):
    redis = FakeRedis()
    async def existing_aerodrome(icao) -> SimpleNamespace:
        return SimpleNamespace(icao=icao)

    async def fake_redis():
        return redis

    captured_hours = None

    async def capture_metar(self, icao, *, hours):
        nonlocal captured_hours
        captured_hours = hours
        return [{"icaoId": "SAEZ", "rawOb": "METAR WITH CUSTOM HOURS"}]

    fresh_station = [{"icaoId": "SAEZ", "name": "EZEIZA", "lat": -34.8, "lon": -58.5}]
    fresh_taf = [{"icaoId": "SAEZ", "rawTAF": "TAF"}]
    fresh_sigmet = {"type": "FeatureCollection", "features": []}

    async def _fake_station(self, icao):
        return fresh_station

    async def _fake_taf(self, icao):
        return fresh_taf

    async def _fake_sigmet(self):
        return fresh_sigmet

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", existing_aerodrome)
    monkeypatch.setattr("app.intelligence.weather_intel_service.get_redis", fake_redis)
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_metar",
        capture_metar,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_station_info",
        _fake_station,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_taf",
        _fake_taf,
    )
    monkeypatch.setattr(
        "app.tools.aviation_weather_tool.AviationWeatherTool.fetch_isigmet_geojson",
        _fake_sigmet,
    )

    result = await get_weather_intelligence("SAEZ", metar_hours_back=5.0)

    assert captured_hours == 5.0
    assert result.source == "fresh_fetch"
    assert result.metar is not None
    assert result.metar.raw == "METAR WITH CUSTOM HOURS"

    result_2 = await get_weather_intelligence("SAEZ", metar_hours_back=8.0, force_refresh=True)
    assert captured_hours == 8.0
    assert result_2.metar is not None
    assert result_2.metar.raw == "METAR WITH CUSTOM HOURS"
