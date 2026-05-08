from app.intelligence.contracts import WeatherIntelResult


def test_intelligence_router_accepts_weather_only(client, monkeypatch):
    async def fake_run(request):
        from app.intelligence.contracts import OrchestratorResponse

        return OrchestratorResponse(
            intent="weather_context",
            weather=WeatherIntelResult(icao=request.weather.icao, source="cache"),
        )

    monkeypatch.setattr("app.routers.intelligence_router.run", fake_run)

    response = client.post("/intelligence/run", json={"weather": {"icao": "SAEZ"}})

    assert response.status_code == 200
    assert response.json()["weather"]["icao"] == "SAEZ"


def test_intelligence_router_rejects_empty_intent(client):
    response = client.post("/intelligence/run", json={})

    assert response.status_code == 422
    assert "At least one intent" in response.json()["detail"]
