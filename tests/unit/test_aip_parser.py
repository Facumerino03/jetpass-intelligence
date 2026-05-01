"""Unit tests for AD 2.0 parsing and raw section segmentation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.schemas.aerodrome import AerodromeCreate
from app.services.scraper.aip_parser import (
    DoclingOcrParser,
    ParserConfig,
    ParserExecutionStats,
    PdfFormatError,
    PdfNotReadableError,
    PdfOcrError,
    _get_parser_config,
    parse_aerodrome_from_ad20,
    parse_aerodrome_from_documents,
)


def _ad2_text(*, missing: str | None = None, duplicate: str | None = None) -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        section_id = f"AD 2.{idx}"
        if section_id == missing:
            continue
        chunks.append(
            f"SAMR {section_id} SECTION {idx}\n"
            f"Contenido ES/EN {idx}\n"
            f"Linea\n\n"
        )
        if section_id == duplicate:
            chunks.append(
                f"SAMR {section_id} SECTION DUP\n"
                f"Contenido duplicado {idx}\n\n"
            )
    return "".join(chunks)


def _ad2_text_with_real_ad21_heading() -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        if idx == 1:
            chunks.append(
                "## AD 2.1 INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / "
                "AERODROME LOCATION INDICATOR AND NAME SAMR - SAN RAFAEL / "
                "S. A. SANTIAGO GERMANO\n\n"
                "AEROPUERTO NACIONAL / NATIONAL AIRPORT\n\n"
            )
            continue
        chunks.append(
            f"SAMR AD 2.{idx} SECTION {idx}\n"
            f"Contenido ES/EN {idx}\n\n"
        )
    return "".join(chunks)


def _ad2_text_with_orphan_coordinates_between_210_211() -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        if idx == 10:
            chunks.append(
                "SAMR AD 2.10 OBSTACLES\n"
                "En el área de circuito y en el AD / In circling area and at AD\n"
                "Tipo de obstáculo, Elevación\n"
                "Antena/\n"
                "Antenna, Markings and LGT\n"
                "790.75 m (2.594 ft)\n"
                "Observaciones / Remarks: NIL\n\n"
            )
            continue
        if idx == 11:
            chunks.append(
                "SAMR AD 2.11 METEOROLOGICAL INFORMATION\n"
                "Coordenadas\n\n"
                "/ Coordinates\n\n"
                "343510.2S 0682717.9W\n\n"
                "1 Oficina MET asociada / Associated MET office\n"
                "EMA SAN RAFAEL\n\n"
            )
            continue
        chunks.append(
            f"SAMR AD 2.{idx} SECTION {idx}\n"
            f"Contenido ES/EN {idx}\n\n"
        )
    return "".join(chunks)


def _stats() -> ParserExecutionStats:
    return ParserExecutionStats(
        extraction_seconds=0.01,
        ocr_seconds=0.0,
        ocr_triggered=False,
        parser_strategy="docling_ocr",
    )


def _docling_parser_config(*, ocr_enabled: bool = True, ocr_mode: str = "page") -> ParserConfig:
    return ParserConfig(
        quality_threshold=0.2,
        ocr_enabled=ocr_enabled,
        ocr_mode=ocr_mode,
        timeout_seconds=60,
        max_pages=20,
        docling_do_ocr=False,
        docling_ocr_languages=("es", "en"),
        docling_force_full_page_ocr=True,
        tesseract_lang="spa+eng",
        tesseract_psm=6,
        docling_do_table_structure=False,
        docling_table_mode="fast",
        docling_table_cell_matching=False,
    )


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_from_documents_returns_structured_sections(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    assert isinstance(result, AerodromeCreate)
    assert result.icao_code == "SAMR"
    assert len(result.ad_sections) == 25
    assert result.ad_sections[0].section_id == "AD 2.1"
    assert result.ad_sections[-1].section_id == "AD 2.25"
    assert result.ad_sections[0].raw_text.startswith("SAMR AD 2.1")


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_extracts_name_from_ad21_heading(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text_with_real_ad21_heading(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    assert result.name == "San Rafael"
    assert result.full_name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_from_documents_preserves_multiline_content(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    text = _ad2_text().replace("Contenido ES/EN 5\nLinea", "Contenido ES/EN 5\n\nLinea")
    mock_extract_text.return_value = (text, _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    section_5 = next(s for s in result.ad_sections if s.section_id == "AD 2.5")
    assert "Contenido ES/EN 5" in section_5.raw_text
    assert "Linea" in section_5.raw_text


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_distinguishes_ad21_from_ad210(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    section_1 = next(s for s in result.ad_sections if s.section_id == "AD 2.1")
    section_10 = next(s for s in result.ad_sections if s.section_id == "AD 2.10")
    assert "AD 2.10" not in section_1.raw_text.splitlines()[0]
    assert "AD 2.10" in section_10.raw_text.splitlines()[0]


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_raises_with_missing_section_diagnostics(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(missing="AD 2.12"), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    with pytest.raises(PdfFormatError, match=r"missing=\['AD 2.12'\]"):
        parse_aerodrome_from_documents([pdf_path], icao="SAMR")


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_tolerates_duplicate_headers_using_first_match(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(duplicate="AD 2.19"), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")
    assert len(result.ad_sections) == 25
    section_19 = next(s for s in result.ad_sections if s.section_id == "AD 2.19")
    assert section_19.raw_text.startswith("SAMR AD 2.19 SECTION 19")
    assert "Contenido duplicado 19" in section_19.raw_text


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_rebalances_orphan_coordinates_between_sections(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text_with_orphan_coordinates_between_210_211(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    section_10 = next(s for s in result.ad_sections if s.section_id == "AD 2.10")
    section_11 = next(s for s in result.ad_sections if s.section_id == "AD 2.11")
    assert "Antena/Antenna 790.75 m (2.594 ft) 343510.2S 0682717.9W" in section_10.raw_text
    assert "Coordenadas" not in section_11.raw_text
    assert "343510.2S 0682717.9W" not in section_11.raw_text


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_from_ad20_infers_icao_from_filename(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_ad20(pdf_path)

    assert result.icao_code == "SAMR"


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_raises_on_empty_pdf(mock_extract_text: MagicMock, tmp_path: Path) -> None:
    mock_extract_text.side_effect = PdfNotReadableError("unreadable")
    pdf_path = tmp_path / "SAMR_empty.pdf"
    pdf_path.touch()

    with pytest.raises(PdfNotReadableError):
        parse_aerodrome_from_documents([pdf_path], icao="SAMR")


def test_get_parser_config_from_settings() -> None:
    with patch(
        "app.services.scraper.aip_parser.get_settings",
        return_value=MagicMock(
            aip_parser_docling_quality_threshold=0.2,
            aip_parser_ocr_enabled=True,
            aip_parser_ocr_mode="page",
            aip_parser_timeout_seconds=60,
            aip_parser_max_pages=20,
            aip_parser_docling_do_ocr=False,
            aip_parser_docling_ocr_languages="es,en",
            aip_parser_docling_force_full_page_ocr=True,
            aip_parser_tesseract_lang="spa+eng",
            aip_parser_tesseract_psm=6,
            aip_parser_docling_do_table_structure=False,
            aip_parser_docling_table_mode="fast",
            aip_parser_docling_table_cell_matching=False,
        ),
    ):
        config = _get_parser_config()
        assert config.quality_threshold == 0.2
        assert config.ocr_enabled is True
        assert config.ocr_mode == "page"
        assert config.docling_do_ocr is False
        assert config.docling_ocr_languages == ("es", "en")
        assert config.tesseract_lang == "spa+eng"
        assert config.docling_do_table_structure is False


def test_docling_parser_uses_ocr_for_low_quality_pages(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    pdf_path.touch()
    parser = DoclingOcrParser(_docling_parser_config())

    with (
        patch.object(parser, "_validate_operational_limits"),
        patch.object(parser, "_extract_pages_with_docling", return_value=[""]),
        patch.object(parser, "_apply_ocr_fallback", return_value=["decoded by ocr"]),
    ):
        text, stats = parser.extract_text(pdf_path)

    assert text == "decoded by ocr"
    assert stats.ocr_triggered is True


def test_docling_parser_raises_when_ocr_disabled_and_needed(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    pdf_path.touch()
    parser = DoclingOcrParser(_docling_parser_config(ocr_enabled=False))

    with (
        patch.object(parser, "_validate_operational_limits"),
        patch.object(parser, "_extract_pages_with_docling", return_value=[""]),
    ):
        with pytest.raises(PdfOcrError, match="disabled"):
            parser.extract_text(pdf_path)


def test_apply_ocr_fallback_document_mode_ocrs_all_rendered_pages(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    pdf_path.touch()
    parser = DoclingOcrParser(_docling_parser_config(ocr_enabled=True, ocr_mode="document"))

    with (
        patch("app.services.scraper.aip_parser.convert_from_path", return_value=[object(), object()]),
        patch(
            "app.services.scraper.aip_parser.pytesseract.image_to_string",
            side_effect=["first page text", "second page text"],
        ),
    ):
        pages = parser._apply_ocr_fallback(pdf_path, pages=[""], low_quality_indexes=[0])

    assert pages == ["first page text", "second page text"]


def test_apply_ocr_fallback_uses_configured_tesseract_language_and_psm(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    pdf_path.touch()
    parser = DoclingOcrParser(_docling_parser_config(ocr_enabled=True, ocr_mode="page"))
    image = object()

    with (
        patch("app.services.scraper.aip_parser.convert_from_path", return_value=[image]),
        patch(
            "app.services.scraper.aip_parser.pytesseract.image_to_string",
            return_value="texto con acentos",
        ) as mock_ocr,
    ):
        pages = parser._apply_ocr_fallback(pdf_path, pages=[""], low_quality_indexes=[0])

    assert pages == ["texto con acentos"]
    mock_ocr.assert_called_once_with(image, lang="spa+eng", config="--psm 6")
