"""Unit tests for flexible aerodrome repository."""

import pytest

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeCreate, SectionMetaSchema, SectionSchema


def _sections(cycle: str) -> list[SectionSchema]:
    return [
        SectionSchema(
            section_id=f"AD 2.{idx}",
            title=f"Section {idx}",
            raw_text=f"Raw bilingual text {idx}",
            data={"value": idx},
            section_meta=SectionMetaSchema(airac_cycle=cycle, source_page=idx),
        )
        for idx in range(1, 26)
    ]


def _create_payload(cycle: str = "2026-01") -> AerodromeCreate:
    return AerodromeCreate(
        icao_code="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        airac_cycle=cycle,
        source_document="SAMR_AD-2.0.pdf",
        downloaded_by="parser-agent",
        ad_sections=_sections(cycle),
    )


@pytest.mark.asyncio
async def test_upsert_creates_aerodrome_with_25_sections() -> None:
    doc = await aerodrome_repo.upsert(_create_payload())

    assert doc.id == "SAMR"
    assert len(doc.current.ad_sections) == 25
    assert doc.current.meta.airac_cycle == "2026-01"
    assert doc.history == []


@pytest.mark.asyncio
async def test_upsert_rotates_current_to_history_when_airac_changes() -> None:
    await aerodrome_repo.upsert(_create_payload("2026-01"))
    updated = await aerodrome_repo.upsert(_create_payload("2026-02"))

    assert updated.current.meta.airac_cycle == "2026-02"
    assert len(updated.history) == 1
    assert updated.history[0].meta.status == "superseded"
    assert updated.history[0].meta.airac_cycle == "2026-01"


@pytest.mark.asyncio
async def test_upsert_validates_section_count() -> None:
    payload = _create_payload()
    payload.ad_sections = payload.ad_sections[:-1]

    with pytest.raises(ValueError, match="Expected 25"):
        await aerodrome_repo.upsert(payload)


@pytest.mark.asyncio
async def test_get_section_by_icao_returns_expected_section() -> None:
    await aerodrome_repo.upsert(_create_payload())

    section = await aerodrome_repo.get_section_by_icao("samr", "AD 2.12")

    assert section is not None
    assert section.section_id == "AD 2.12"
