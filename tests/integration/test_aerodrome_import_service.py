"""Integration tests for aerodrome_import_service with flexible AD sections."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from app.schemas.aerodrome import AerodromeCreate, SectionSchema
from app.services.aerodrome_import_service import AipImportError, import_aerodrome_from_aip
from app.services.scraper.aip_scraper import AipScraperError


def _sections() -> list[SectionSchema]:
    return [
        SectionSchema(
            section_id=f"AD 2.{idx}",
            title=f"Section {idx}",
            raw_text=f"Raw text {idx}",
            data={"idx": idx},
        )
        for idx in range(1, 26)
    ]


def _aerodrome_create() -> AerodromeCreate:
    return AerodromeCreate(
        icao_code="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        airac_cycle="2026-01",
        source_document="SAMR_AD-2.0.pdf",
        downloaded_by="parser-agent",
        ad_sections=_sections(),
    )


def _aerodrome_doc() -> AerodromeDocument:
    return AerodromeDocument(
        id="SAMR",
        icao="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        current=AerodromeSnapshot(
            ad_sections=[
                AdSection(
                    section_id=s.section_id,
                    title=s.title,
                    raw_text=s.raw_text,
                    data=s.data,
                )
                for s in _sections()
            ],
            _meta=DocumentMeta(airac_cycle="2026-01", version=1),
        ),
    )


@pytest.mark.asyncio
async def test_import_aerodrome_full_pipeline(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad2a_path = tmp_path / "SAMR_AD-2.A.pdf"
    ad20_path.touch()
    ad2a_path.touch()

    parse_mock = patch(
        "app.services.aerodrome_import_service.parse_aerodrome_from_documents",
        return_value=_aerodrome_create(),
    )

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path, ad2a_path]),
        ),
        parse_mock as parse_call,
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(return_value=_aerodrome_doc()),
        ),
        patch(
            "app.services.aerodrome_import_service.enrich_aerodrome_document",
            AsyncMock(side_effect=lambda doc: doc),
        ) as enrich_call,
    ):
        result = await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)

    assert result.icao == "SAMR"
    assert len(result.current.ad_sections) == 25
    parse_call.assert_called_once_with([ad20_path], icao="SAMR")
    enrich_call.assert_called_once()


@pytest.mark.asyncio
async def test_import_raises_when_scraper_fails(tmp_path: Path) -> None:
    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(side_effect=AipScraperError("browser crash")),
    ):
        with pytest.raises(AipImportError, match="Scraper failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_db_fails(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documents",
            return_value=_aerodrome_create(),
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ),
        patch(
            "app.services.aerodrome_import_service.enrich_aerodrome_document",
            AsyncMock(side_effect=lambda doc: doc),
        ),
    ):
        with pytest.raises(AipImportError, match="Database upsert failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_ad20_pdf_missing(tmp_path: Path) -> None:
    ad2a_path = tmp_path / "SAMR_AD-2.A.pdf"
    ad2a_path.touch()

    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(return_value=[ad2a_path]),
    ):
        with pytest.raises(AipImportError, match="does not include required AD-2.0"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_can_skip_enrichment(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documents",
            return_value=_aerodrome_create(),
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(return_value=_aerodrome_doc()),
        ),
        patch(
            "app.services.aerodrome_import_service.enrich_aerodrome_document",
            AsyncMock(side_effect=lambda doc: doc),
        ) as enrich_call,
    ):
        result = await import_aerodrome_from_aip("SAMR", output_dir=tmp_path, enrich=False)

    assert result.icao == "SAMR"
    enrich_call.assert_not_called()
