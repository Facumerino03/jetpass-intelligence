from fastapi.testclient import TestClient

from app.schemas.aerodrome import (
    AerodromeResponse,
    SectionMetaSchema,
    SectionResponse,
    SectionSchema,
    SnapshotResponse,
)
from app.models.meta import DocumentMeta
from app.services.aerodrome_service import AerodromeNotFoundError, AerodromeSectionNotFoundError


def _snapshot() -> SnapshotResponse:
    return SnapshotResponse(
        ad_sections=[
            SectionSchema(
                section_id="AD 2.1",
                title="AD 2.1",
                raw_text="SAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO",
                data={"indicator": "SAMR"},
                section_meta=SectionMetaSchema(airac_cycle="2026-01", source_page=1),
            )
        ],
        _meta=DocumentMeta(airac_cycle="2026-01", version=1),
    )


def test_list_aerodromes(client: TestClient, monkeypatch) -> None:
    async def fake_list_aerodromes():
        return [
            AerodromeResponse(
                icao="SAMR",
                name="San Rafael",
                full_name="S. A. Santiago Germano",
                current=_snapshot(),
                history=[],
            )
        ]

    monkeypatch.setattr("app.routers.aerodrome_router.list_aerodromes", fake_list_aerodromes)

    response = client.get("/api/v1/aerodromes")
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["icao"] == "SAMR"
    assert payload[0]["current"]["ad_sections"][0]["section_id"] == "AD 2.1"


def test_get_aerodrome_by_icao(client: TestClient, monkeypatch) -> None:
    async def fake_get_aerodrome(_icao: str):
        return AerodromeResponse(
            icao="SAMR",
            name="San Rafael",
            full_name="S. A. Santiago Germano",
            current=_snapshot(),
            history=[],
        )

    monkeypatch.setattr("app.routers.aerodrome_router.get_aerodrome", fake_get_aerodrome)

    response = client.get("/api/v1/aerodromes/samr")
    assert response.status_code == 200
    assert response.json()["icao"] == "SAMR"


def test_get_aerodrome_section(client: TestClient, monkeypatch) -> None:
    async def fake_get_section(_icao: str, _section_id: str):
        return SectionResponse(
            section_id="AD 2.12",
            title="CARACTERISTICAS FISICAS",
            raw_text="RWY 11 ...",
            data={"runways": [{"rwy_designator": "11"}]},
        )

    monkeypatch.setattr("app.routers.aerodrome_router.get_aerodrome_section", fake_get_section)

    response = client.get("/api/v1/aerodromes/samr/sections/AD%202.12")
    assert response.status_code == 200
    assert response.json()["section_id"] == "AD 2.12"


def test_get_aerodrome_by_icao_returns_404(client: TestClient, monkeypatch) -> None:
    async def fake_get_aerodrome(_icao: str):
        raise AerodromeNotFoundError("not found")

    monkeypatch.setattr("app.routers.aerodrome_router.get_aerodrome", fake_get_aerodrome)

    response = client.get("/api/v1/aerodromes/xxxx")
    assert response.status_code == 404


def test_get_aerodrome_section_returns_404(client: TestClient, monkeypatch) -> None:
    async def fake_get_section(_icao: str, _section_id: str):
        raise AerodromeSectionNotFoundError("section not found")

    monkeypatch.setattr("app.routers.aerodrome_router.get_aerodrome_section", fake_get_section)

    response = client.get("/api/v1/aerodromes/samr/sections/AD%202.99")
    assert response.status_code == 404
