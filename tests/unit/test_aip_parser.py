"""Unit tests for deterministic AD 2.0 parsing and segmentation."""

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
    _postprocess_canonical_section,
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


def _ad2_text_missing_211_with_meteo_hint() -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        if idx == 11:
            chunks.append("| 9 | Informacion meteorologica suministrada / Meteorological information provided | TWR |\n\n")
            continue
        chunks.append(
            f"SAMR AD 2.{idx} SECTION {idx}\n"
            f"Contenido ES/EN {idx}\n\n"
        )
    return "".join(chunks)


def _ad2_text_missing_211_with_full_meteo_block() -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        if idx == 11:
            chunks.append(
                "1 Oficina MET asociada / Associated MET office EMA SAN RAFAEL, OMA/OVM MENDOZA\n"
                "2 Horas de servicio / Hours of service H24\n"
                "9 Dependencias ATS a las cuales se suministra información meteorológica / "
                "The ATS units provided with meteorological information TWR\n\n"
            )
            continue
        chunks.append(
            f"SAMR AD 2.{idx} SECTION {idx}\n"
            f"Contenido ES/EN {idx}\n\n"
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


def _ad2_text_with_ad210_and_ad211_boundary_noise() -> str:
    chunks: list[str] = []
    for idx in range(1, 26):
        if idx == 10:
            chunks.append(
                "SAMR AD 2.10 OBSTACULOS DEL AERODROMO / AERODROME OBSTACLES\n"
                "Antena/Antenna, 790.75 m (2.594 ft) 343510.2S 0682717.9W\n"
                "Observaciones / Remarks: NIL\n\n"
            )
            continue
        if idx == 11:
            chunks.append(
                "SAMR AD 2.11 INFORMACION METEOROLOGICA PROPORCIONADA / METEOROLOGICAL INFORMATION PROVIDED\n"
                "1 Oficina MET asociada / Associated MET office EMA SAN RAFAEL\n"
                "2 Horas de servicio / Hours of service H24\n\n"
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
    assert result.ad_sections[0].raw_text.startswith("SECTION: AD 2.1 |")


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

    with pytest.raises(PdfFormatError, match=r'"missing_sections": \["AD 2.12"\]'):
        parse_aerodrome_from_documents([pdf_path], icao="SAMR")


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_recovers_ad211_from_meteorological_hint(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text_missing_211_with_meteo_hint(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    assert len(result.ad_sections) == 25
    section_11 = next(s for s in result.ad_sections if s.section_id == "AD 2.11")
    assert "meteorological" in section_11.raw_text.lower() or "meteorologica" in section_11.raw_text.lower()


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_recovers_ad211_from_first_meteo_row(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text_missing_211_with_full_meteo_block(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    section_11 = next(s for s in result.ad_sections if s.section_id == "AD 2.11")
    assert "Oficina MET asociada" in section_11.raw_text
    assert "Dependencias ATS" in section_11.raw_text


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_keeps_ad210_coordinates_out_of_ad211(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text_with_ad210_and_ad211_boundary_noise(), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    section_10 = next(s for s in result.ad_sections if s.section_id == "AD 2.10")
    section_11 = next(s for s in result.ad_sections if s.section_id == "AD 2.11")
    assert "343510.2S 0682717.9W" in section_10.raw_text
    assert "343510.2S 0682717.9W" not in section_11.raw_text


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_raises_with_duplicate_section_diagnostics(
    mock_extract_text: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_ad2_text(duplicate="AD 2.19"), _stats())
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    with pytest.raises(PdfFormatError, match=r'"duplicate_sections": \["AD 2.19"\]'):
        parse_aerodrome_from_documents([pdf_path], icao="SAMR")


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
        ),
    ):
        config = _get_parser_config()
        assert config.quality_threshold == 0.2
        assert config.ocr_enabled is True
        assert config.ocr_mode == "page"


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


def test_postprocess_ad212_fixes_duplicated_second_no_row() -> None:
    raw = "\n".join(
        [
            "SECTION: AD 2.12 | CARACTERISTICAS",
            "ITEM: ROW | No",
            "VALUE: 2.222x280 (*) | 90x60 | No | No | NIL",
            "ITEM: ROW | No",
            "VALUE: 2.222x280 (*) | 90x60 | No | No | NIL",
        ]
    )

    out = _postprocess_canonical_section("AD 2.12", raw)
    assert "VALUE: No | 2.222x280 (*) | 90x60 | No | No | NIL" in out
    assert "VALUE: No | - | 90x60 | No | No | NIL" in out
