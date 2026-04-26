"""Unit tests for app.services.scraper.aip_parser."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.schemas.aerodrome import AerodromeCreate, RunwayBase
from app.services.scraper.aip_parser import (
    AipParserError,
    DoclingOcrParser,
    ParserConfig,
    ParserExecutionStats,
    PdfFormatError,
    PdfNotReadableError,
    PdfOcrError,
    _get_parser_config,
    parse_aerodrome_from_ad20,
    parse_runways_from_ad20,
)

# ── fixtures ───────────────────────────────────────────────────────────────────

_SAMR_TEXT = (
    "SAMR\n"
    "Aeropuerto Internacional Piloto Civil Norberto Fernández\n"
    "Ciudad: Río Gallegos\n"
    "Provincia: Santa Cruz\n"
    "COORD ARP 343516S 0682413W\n"
    "65 FT AMSL\n"
    "Pista 07/25 Long: 3550 m  Ancho: 45 m  Sup: ASPH\n"
    "Pista 01/19 Long: 1750 m  Ancho: 30 m  Sup: ASPH\n"
)

_SAMR_AERODROME = AerodromeCreate(
    icao_code="SAMR",
    name="Aeropuerto Internacional Piloto Civil Norberto Fernández",
    city="Río Gallegos",
    province="Santa Cruz",
    country="Argentina",
    latitude=-34.587778,
    longitude=-68.403611,
    elevation_ft=65,
    runways=[
        RunwayBase(designator="07/25", length_m=3550, width_m=45, surface_type="ASPH"),
        RunwayBase(designator="01/19", length_m=1750, width_m=30, surface_type="ASPH"),
    ],
)


def _mock_llm_client(return_value: AerodromeCreate) -> MagicMock:
    """Return an instructor-like mock whose .create() returns return_value."""
    client = MagicMock()
    client.create.return_value = return_value
    return client


def _docling_parser_config(*, ocr_enabled: bool = True, ocr_mode: str = "page") -> ParserConfig:
    return ParserConfig(
        quality_threshold=0.2,
        ocr_enabled=ocr_enabled,
        ocr_mode=ocr_mode,
        timeout_seconds=60,
        max_pages=20,
    )


def _stats() -> ParserExecutionStats:
    return ParserExecutionStats(
        extraction_seconds=0.01,
        ocr_seconds=0.0,
        ocr_triggered=False,
        parser_strategy="docling_ocr",
    )


# ── parse_aerodrome_from_ad20 ──────────────────────────────────────────────────


@patch("app.services.scraper.aip_parser._get_llm_client")
@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_from_ad20_returns_aerodrome_create(
    mock_extract_text: MagicMock, mock_get_client: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_SAMR_TEXT, _stats())
    mock_get_client.return_value = _mock_llm_client(_SAMR_AERODROME)
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    result = parse_aerodrome_from_ad20(pdf_path)

    assert isinstance(result, AerodromeCreate)
    assert result.icao_code == "SAMR"
    assert result.name == "Aeropuerto Internacional Piloto Civil Norberto Fernández"
    assert result.city == "Río Gallegos"
    assert result.province == "Santa Cruz"
    assert result.latitude == pytest.approx(-34.587778, abs=1e-4)
    assert result.longitude == pytest.approx(-68.403611, abs=1e-4)
    assert result.elevation_ft == 65
    assert len(result.runways) == 2


@patch("app.services.scraper.aip_parser._get_llm_client")
@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_calls_llm_with_extracted_text(
    mock_extract_text: MagicMock, mock_get_client: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_SAMR_TEXT, _stats())
    mock_client = _mock_llm_client(_SAMR_AERODROME)
    mock_get_client.return_value = mock_client
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    parse_aerodrome_from_ad20(pdf_path)

    mock_client.create.assert_called_once()
    call_kwargs = mock_client.create.call_args.kwargs
    assert call_kwargs["response_model"] is AerodromeCreate
    assert call_kwargs["max_retries"] == 3
    messages = call_kwargs["messages"]
    assert any(_SAMR_TEXT in msg["content"] for msg in messages)


@patch("app.services.scraper.aip_parser._get_llm_client")
@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_raises_pdf_format_error_when_llm_fails(
    mock_extract_text: MagicMock, mock_get_client: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_SAMR_TEXT, _stats())
    mock_client = MagicMock()
    mock_client.create.side_effect = ValueError("LLM validation error")
    mock_get_client.return_value = mock_client
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    with pytest.raises(PdfFormatError, match="LLM failed"):
        parse_aerodrome_from_ad20(pdf_path)


@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_aerodrome_raises_on_empty_pdf(mock_extract_text: MagicMock, tmp_path: Path) -> None:
    mock_extract_text.side_effect = PdfNotReadableError("unreadable")
    pdf_path = tmp_path / "empty.pdf"
    pdf_path.touch()

    with pytest.raises(PdfNotReadableError):
        parse_aerodrome_from_ad20(pdf_path)


# ── parse_runways_from_ad20 ────────────────────────────────────────────────────


@patch("app.services.scraper.aip_parser._get_llm_client")
@patch("app.services.scraper.aip_parser.DoclingOcrParser.extract_text")
def test_parse_runways_from_ad20_returns_runways(
    mock_extract_text: MagicMock, mock_get_client: MagicMock, tmp_path: Path
) -> None:
    mock_extract_text.return_value = (_SAMR_TEXT, _stats())
    mock_get_client.return_value = _mock_llm_client(_SAMR_AERODROME)
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.touch()

    runways = parse_runways_from_ad20(pdf_path)

    assert len(runways) == 2
    designators = {r.designator for r in runways}
    assert "07/25" in designators
    assert "01/19" in designators


# ── _get_llm_client ────────────────────────────────────────────────────────────


def test_get_llm_client_raises_when_api_key_missing() -> None:
    from app.services.scraper.aip_parser import _get_llm_client
    from unittest.mock import patch as _patch

    with _patch(
        "app.services.scraper.aip_parser.get_settings",
        return_value=MagicMock(openrouter_api_key=None),
    ):
        with pytest.raises(AipParserError, match="OPENROUTER_API_KEY"):
            _get_llm_client()


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


def test_docling_parser_raises_when_unrecoverable_after_ocr(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    pdf_path.touch()
    parser = DoclingOcrParser(_docling_parser_config())

    with (
        patch.object(parser, "_validate_operational_limits"),
        patch.object(parser, "_extract_pages_with_docling", return_value=[""]),
        patch.object(parser, "_apply_ocr_fallback", return_value=["   "]),
    ):
        with pytest.raises(PdfNotReadableError, match="unreadable"):
            parser.extract_text(pdf_path)


# ── AerodromeCreate validators ─────────────────────────────────────────────────


def test_aerodrome_create_validates_icao_prefix() -> None:
    with pytest.raises(Exception, match='must start with "SA"'):
        AerodromeCreate(
            icao_code="KLAX",
            name="Test",
            latitude=34.0,
            longitude=-118.0,
        )


def test_aerodrome_create_accepts_sa_prefix() -> None:
    aerodrome = AerodromeCreate(
        icao_code="SAMR",
        name="Test",
        latitude=-34.0,
        longitude=-68.0,
    )
    assert aerodrome.icao_code == "SAMR"


def test_aerodrome_create_rejects_negative_elevation() -> None:
    with pytest.raises(Exception):
        AerodromeCreate(
            icao_code="SAMR",
            name="Test",
            latitude=-34.0,
            longitude=-68.0,
            elevation_ft=-10,
        )


def test_runway_base_rejects_out_of_range_length() -> None:
    with pytest.raises(Exception):
        RunwayBase(designator="07/25", length_m=100)  # below min 300


def test_runway_base_rejects_excessive_length() -> None:
    with pytest.raises(Exception):
        RunwayBase(designator="07/25", length_m=9999)  # above max 6000
