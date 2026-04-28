"""Unit tests for flexible AerodromeDocument model."""

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import DocumentMeta


def _section(idx: int) -> AdSection:
    return AdSection(
        section_id=f"AD 2.{idx}",
        title=f"Section {idx}",
        raw_text=f"Literal text {idx}",
        data={"row": idx},
        section_meta=SectionMeta(airac_cycle="2026-01", source_page=idx),
    )


@pytest.mark.asyncio
async def test_aerodrome_document_current_and_history() -> None:
    current = AerodromeSnapshot(
        ad_sections=[_section(i) for i in range(1, 26)],
        _meta=DocumentMeta(airac_cycle="2026-01", version=1),
    )
    previous = AerodromeSnapshot(
        ad_sections=[_section(i) for i in range(1, 26)],
        _meta=DocumentMeta(airac_cycle="2025-13", version=0, status="superseded"),
    )
    doc = AerodromeDocument(
        id="SAMR",
        icao="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        current=current,
        history=[previous],
    )
    await doc.insert()

    fetched = await AerodromeDocument.get("SAMR")
    assert fetched is not None
    assert fetched.current.meta.airac_cycle == "2026-01"
    assert len(fetched.current.ad_sections) == 25
    assert fetched.history[0].meta.status == "superseded"


@pytest.mark.asyncio
async def test_ad_section_keeps_raw_bilingual_text() -> None:
    section = AdSection(
        section_id="AD 2.1",
        title="INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / AERODROME LOCATION INDICATOR AND NAME",
        raw_text="SAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO\nAEROPUERTO NACIONAL / NATIONAL AIRPORT",
        data={"indicator": "SAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO"},
    )
    doc = AerodromeDocument(
        id="SAMR",
        icao="SAMR",
        name="San Rafael",
        current=AerodromeSnapshot(ad_sections=[section] * 25),
    )
    await doc.insert()

    fetched = await AerodromeDocument.get("SAMR")
    assert fetched is not None
    assert "NATIONAL AIRPORT" in fetched.current.ad_sections[0].raw_text
