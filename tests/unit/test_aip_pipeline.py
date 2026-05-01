"""Unit tests for AD 2.0 pipeline orchestration."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.scraper.aip_parser import ParserConfig, PdfFormatError
from app.services.scraper.aip_pipeline import extract_and_segment_sections


def _config(*, ocr_enabled: bool = True) -> ParserConfig:
    return ParserConfig(
        quality_threshold=0.2,
        ocr_enabled=ocr_enabled,
        ocr_mode="page",
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


def test_pipeline_happy_path_without_retry() -> None:
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)

    result = extract_and_segment_sections(
        pdf_paths=[Path("SAMR_AD-2.0.pdf")],
        icao="SAMR",
        config=_config(ocr_enabled=False),
        parser_factory=lambda cfg: object(),
        extract_documents_text=lambda parser, paths: ("doc text", 0.1, 0.0, False),
        segment_sections=lambda text, source, icao: ["AD 2.1"],
        forced_config_builder=lambda cfg: cfg,
        format_error=PdfFormatError,
        logger=logger,
    )

    assert result.text == "doc text"
    assert result.sections == ["AD 2.1"]


def test_pipeline_retries_with_forced_ocr_on_format_error() -> None:
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)
    calls = {"n": 0}

    def segment_sections(text: str, source: Path, icao: str) -> list[str]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise PdfFormatError("first failed")
        return ["AD 2.1", "AD 2.2"]

    result = extract_and_segment_sections(
        pdf_paths=[Path("SAMR_AD-2.0.pdf")],
        icao="SAMR",
        config=_config(ocr_enabled=True),
        parser_factory=lambda cfg: object(),
        extract_documents_text=lambda parser, paths: (
            "retry text" if calls["n"] else "first text",
            0.1,
            0.2,
            True,
        ),
        segment_sections=segment_sections,
        forced_config_builder=lambda cfg: cfg,
        format_error=PdfFormatError,
        logger=logger,
    )

    assert calls["n"] == 2
    assert result.text == "retry text"
    assert result.sections == ["AD 2.1", "AD 2.2"]


def test_pipeline_wraps_non_format_exception() -> None:
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)

    with pytest.raises(PdfFormatError) as exc_info:
        extract_and_segment_sections(
            pdf_paths=[Path("SAMR_AD-2.0.pdf")],
            icao="SAMR",
            config=_config(ocr_enabled=True),
            parser_factory=lambda cfg: object(),
            extract_documents_text=lambda parser, paths: ("doc text", 0.1, 0.0, False),
            segment_sections=lambda text, source, icao: (_ for _ in ()).throw(RuntimeError("boom")),
            forced_config_builder=lambda cfg: cfg,
            format_error=PdfFormatError,
            logger=logger,
        )

    assert "Deterministic AD 2.0 segmentation failed" in str(exc_info.value)
