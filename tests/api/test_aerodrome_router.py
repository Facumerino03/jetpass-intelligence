from fastapi.testclient import TestClient

from app.schemas.aerodrome import AerodromeResponse, RunwayResponse
from app.services.aerodrome_service import AerodromeNotFoundError


def test_list_aerodromes(client: TestClient, monkeypatch) -> None:
    async def fake_list_aerodromes(_db):
        return [
            AerodromeResponse(
                icao_code="SAMR",
                iata_code="RGL",
                name="Rio Gallegos",
                city="Río Gallegos",
                province="Santa Cruz",
                country="Argentina",
                latitude=-51.6089,
                longitude=-69.3126,
                elevation_ft=65,
                runways=[
                    RunwayResponse(
                        designator="07/25",
                        length_m=3550,
                        width_m=45,
                        surface_type="ASPH",
                    )
                ],
            )
        ]

    monkeypatch.setattr(
        "app.routers.aerodrome_router.list_aerodromes",
        fake_list_aerodromes,
    )

    response = client.get("/api/v1/aerodromes")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["icao_code"] == "SAMR"


def test_get_aerodrome_by_icao(client: TestClient, monkeypatch) -> None:
    async def fake_get_aerodrome(_db, icao: str):
        return AerodromeResponse(
            icao_code=icao.upper(),
            iata_code="RGL",
            name="Rio Gallegos",
            city="Río Gallegos",
            province="Santa Cruz",
            country="Argentina",
            latitude=-51.6089,
            longitude=-69.3126,
            elevation_ft=65,
            runways=[],
        )

    monkeypatch.setattr(
        "app.routers.aerodrome_router.get_aerodrome",
        fake_get_aerodrome,
    )

    response = client.get("/api/v1/aerodromes/samr")
    assert response.status_code == 200
    assert response.json()["icao_code"] == "SAMR"


def test_get_aerodrome_by_icao_returns_404(client: TestClient, monkeypatch) -> None:
    async def fake_get_aerodrome(_db, _icao: str):
        raise AerodromeNotFoundError("not found")

    monkeypatch.setattr(
        "app.routers.aerodrome_router.get_aerodrome",
        fake_get_aerodrome,
    )

    response = client.get("/api/v1/aerodromes/xxxx")
    assert response.status_code == 404
