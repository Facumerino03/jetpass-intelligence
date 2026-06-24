from app.intelligence.contracts import GeoCoords, OrchestratorResponse, WeatherIntelResult


def test_intelligence_router_accepts_weather_only(client, monkeypatch):
    async def fake_run(request):
        return OrchestratorResponse(
            intent="weather_context",
            weather=WeatherIntelResult(icao=request.weather.icao, source="cache"),
        )

    monkeypatch.setattr("app.routers.intelligence_router.run", fake_run)

    response = client.post("/intelligence/run", json={"weather": {"icao": "SAEZ"}})

    assert response.status_code == 200
    assert response.json()["weather"]["icao"] == "SAEZ"


def test_intelligence_router_accepts_aerodrome_geo_only(client, monkeypatch):
    async def fake_run(request):
        return OrchestratorResponse(
            intent="aerodrome_geo",
            aerodrome_geo={
                "SAEZ": GeoCoords(
                    icao="SAEZ",
                    lat=-34.8222,
                    lon=-58.5358,
                    elev_ft=66,
                    elev_m=20.1,
                    source="csv",
                ),
            },
        )

    monkeypatch.setattr("app.routers.intelligence_router.run", fake_run)

    response = client.post(
        "/intelligence/run",
        json={"aerodrome_geo": {"icao": "SAEZ"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "aerodrome_geo"
    assert payload["aerodrome_geo"]["SAEZ"]["lat"] == -34.8222


def test_intelligence_router_accepts_batch_aerodrome_geo(client, monkeypatch):
    async def fake_run(request):
        return OrchestratorResponse(
            intent="aerodrome_geo",
            aerodrome_geo={
                "SAEZ": GeoCoords(icao="SAEZ", lat=-34.8222, lon=-58.5358, source="csv"),
                "SABE": GeoCoords(icao="SABE", lat=-34.5592, lon=-58.4156, source="csv"),
                "XXXX": GeoCoords(icao="XXXX", source="not_found"),
            },
        )

    monkeypatch.setattr("app.routers.intelligence_router.run", fake_run)

    response = client.post(
        "/intelligence/run",
        json={"aerodrome_geo": {"icaos": ["SAEZ", "SABE", "XXXX"]}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["aerodrome_geo"]) == 3
    assert payload["aerodrome_geo"]["XXXX"]["source"] == "not_found"


def test_intelligence_router_rejects_empty_intent(client):
    response = client.post("/intelligence/run", json={})

    assert response.status_code == 422
    assert "At least one intent" in response.json()["detail"]


def test_airports_sync_status_endpoint(client, monkeypatch):
    from app.services.airports_sync_runtime import update_airports_sync_status

    update_airports_sync_status(enabled=True, scheduler_running=True, in_progress=False)

    response = client.get("/intelligence/airports-sync/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["scheduler_running"] is True
