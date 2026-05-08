import pytest

from app.intelligence.contracts import OrchestratorRequest, WeatherIntelResult
from app.intelligence.orchestrator import run


@pytest.mark.asyncio
async def test_weather_only_request_returns_weather(monkeypatch):
    async def fake_weather(icao: str, *, force_refresh: bool = False, metar_hours_back: float | None = None):
        return WeatherIntelResult(icao=icao, source="cache")

    monkeypatch.setattr("app.intelligence.graph.get_weather_intelligence", fake_weather)

    response = await run(OrchestratorRequest.model_validate({"weather": {"icao": "SAEZ"}}))

    assert response.intent == "weather_context"
    assert response.weather is not None
    assert response.weather.icao == "SAEZ"


@pytest.mark.asyncio
async def test_weather_and_notam_request_builds_combined_intent(monkeypatch):
    async def fake_weather(icao: str, *, force_refresh: bool = False, metar_hours_back: float | None = None):
        return WeatherIntelResult(icao=icao, source="cache")

    async def fake_notam(icao: str, *, force_refresh: bool = False):
        from app.intelligence.contracts import NotamIntelResult

        return NotamIntelResult(icao=icao, source="cache")

    monkeypatch.setattr("app.intelligence.graph.get_weather_intelligence", fake_weather)
    monkeypatch.setattr("app.intelligence.graph.get_notam_intelligence", fake_notam)

    response = await run(
        OrchestratorRequest.model_validate(
            {"notam": {"icao": "SAEZ"}, "weather": {"icao": "SAEZ"}}
        )
    )

    assert response.intent == "notam_context+weather_context"
    assert response.notam is not None
    assert response.weather is not None


@pytest.mark.asyncio
async def test_orchestrator_passes_metar_hours_back_through(monkeypatch):
    captured_kwargs = None

    async def fake_weather(icao: str, *, force_refresh: bool = False, metar_hours_back: float | None = None):
        nonlocal captured_kwargs
        captured_kwargs = {"icao": icao, "force_refresh": force_refresh, "metar_hours_back": metar_hours_back}
        return WeatherIntelResult(icao=icao, source="cache")

    monkeypatch.setattr("app.intelligence.graph.get_weather_intelligence", fake_weather)

    response = await run(
        OrchestratorRequest.model_validate(
            {"weather": {"icao": "SAEZ", "metar_hours_back": 4.0}}
        )
    )

    assert response.weather is not None
    assert captured_kwargs == {"icao": "SAEZ", "force_refresh": False, "metar_hours_back": 4.0}
