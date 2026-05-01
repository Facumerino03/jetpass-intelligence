"""Extraction helpers for Docling/OCR document text assembly."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.scraper.aip_parser import DoclingOcrParser, ParserConfig


def extract_documents_text(
    parser: "DoclingOcrParser",
    pdf_paths: list[Path],
) -> tuple[str, float, float, bool]:
    assembled_blocks: list[str] = []
    total_extraction_seconds = 0.0
    total_ocr_seconds = 0.0
    any_ocr_triggered = False

    for pdf_path in pdf_paths:
        text, stats = parser.extract_text(pdf_path)
        total_extraction_seconds += stats.extraction_seconds
        total_ocr_seconds += stats.ocr_seconds
        any_ocr_triggered = any_ocr_triggered or stats.ocr_triggered
        assembled_blocks.append(f"### DOCUMENT: {pdf_path.name}\n{text}")

    return "\n\n".join(assembled_blocks), total_extraction_seconds, total_ocr_seconds, any_ocr_triggered


def forced_document_ocr_config(base: "ParserConfig") -> "ParserConfig":
    from app.services.scraper.aip_parser import ParserConfig

    return ParserConfig(
        quality_threshold=1.1,
        ocr_enabled=base.ocr_enabled,
        ocr_mode="document",
        timeout_seconds=base.timeout_seconds,
        max_pages=base.max_pages,
        docling_do_ocr=base.docling_do_ocr,
        docling_ocr_languages=base.docling_ocr_languages,
        docling_force_full_page_ocr=base.docling_force_full_page_ocr,
        tesseract_lang=base.tesseract_lang,
        tesseract_psm=base.tesseract_psm,
        docling_do_table_structure=base.docling_do_table_structure,
        docling_table_mode=base.docling_table_mode,
        docling_table_cell_matching=base.docling_table_cell_matching,
    )
