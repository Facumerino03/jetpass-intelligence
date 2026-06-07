from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.schemas.aerodrome import AerodromeCreate, SectionSchema
from app.services.aerodrome_import_service import AipImportError, import_aerodrome_from_aip
from app.services.aip_extractor import AipExtractorError
from app.services.scraper.aip_scraper import AipScraperError


def _operational_sections() -> list[SectionSchema]:
    return [
        SectionSchema(section_id=section_id, title=section_id, data={"value": section_id})
        for section_id in OPERATIONAL_AD_SECTION_IDS
    ]


def _aerodrome_create() -> AerodromeCreate:
    return AerodromeCreate(
        icao_code="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        airac_cycle="unknown",
        source_document="SAMR_AD-2.0.pdf",
        downloaded_by="ad_extractor",
        ad_sections=_operational_sections(),
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
                    data=s.data,
                )
                for s in _operational_sections()
            ],
            _meta=DocumentMeta(airac_cycle="unknown", version=1),
        ),
    )


@pytest.mark.asyncio
async def test_import_aerodrome_extractor_pipeline(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad2a_path = tmp_path / "SAMR_AD-2.A.pdf"
    ad20_path.touch()
    ad2a_path.touch()

    upsert_mock = AsyncMock(return_value=_aerodrome_doc())

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path, ad2a_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_ad_extractor",
            return_value=_aerodrome_create(),
        ) as parse_call,
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            upsert_mock,
        ),
    ):
        result = await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)

    assert result.icao == "SAMR"
    assert [section.section_id for section in result.current.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
    parse_call.assert_called_once_with(ad20_path, icao="SAMR")
    upsert_payload = upsert_mock.await_args.args[0]
    assert [section.section_id for section in upsert_payload.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)


@pytest.mark.asyncio
async def test_import_raises_when_scraper_fails(tmp_path: Path) -> None:
    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(side_effect=AipScraperError("browser crash")),
    ):
        with pytest.raises(AipImportError, match="Scraper failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_extractor_fails(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_ad_extractor",
            side_effect=AipExtractorError("missing AD 2.19"),
        ),
    ):
        with pytest.raises(AipImportError, match="AIP extraction failed"):
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
            "app.services.aerodrome_import_service.parse_aerodrome_from_ad_extractor",
            return_value=_aerodrome_create(),
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(side_effect=RuntimeError("DB down")),
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
