"""AIP PDF parser — deterministic AD 2.0 extraction and segmentation."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytesseract
from docling.document_converter import DocumentConverter
from pdf2image import convert_from_path

from app.core.config import get_settings
from app.schemas.aerodrome import AerodromeCreate
from app.services.scraper.aip_extraction import extract_documents_text, forced_document_ocr_config
from app.services.scraper.aip_metadata import extract_airac_cycle, extract_name_fields
from app.services.scraper.aip_pipeline import extract_and_segment_sections
from app.services.scraper.aip_segmenter import segment_ad2_sections

logger = logging.getLogger(__name__)


class AipParserError(Exception):
    """Base error for the AIP parser module."""


class PdfNotReadableError(AipParserError):
    """PDF cannot be opened or contains no extractable text."""


class PdfFormatError(AipParserError):
    """LLM could not produce a valid structured response from the PDF content."""


class PdfOcrError(AipParserError):
    """OCR stage failed or produced unusable output."""


@dataclass(frozen=True)
class ParserExecutionStats:
    extraction_seconds: float
    ocr_seconds: float
    ocr_triggered: bool
    parser_strategy: str


@dataclass(frozen=True)
class ParserConfig:
    quality_threshold: float
    ocr_enabled: bool
    ocr_mode: str
    timeout_seconds: int
    max_pages: int
    docling_do_ocr: bool
    docling_ocr_languages: tuple[str, ...]
    docling_force_full_page_ocr: bool
    tesseract_lang: str
    tesseract_psm: int
    docling_do_table_structure: bool
    docling_table_mode: str
    docling_table_cell_matching: bool


class DoclingOcrParser:
    """Docling-first parser with configurable OCR fallback."""

    def __init__(self, config: ParserConfig) -> None:
        self._config = config

    def extract_text(self, pdf_path: Path) -> tuple[str, ParserExecutionStats]:
        self._validate_operational_limits(pdf_path)
        extraction_start = time.monotonic()
        pages = self._extract_pages_with_docling(pdf_path)
        extraction_elapsed = time.monotonic() - extraction_start

        low_quality_indexes = [
            idx for idx, page_text in enumerate(pages) if _page_quality(page_text) < self._config.quality_threshold
        ]
        needs_ocr = len(low_quality_indexes) == len(pages) if self._config.ocr_mode == "document" else bool(low_quality_indexes)

        ocr_elapsed = 0.0
        if needs_ocr:
            if not self._config.ocr_enabled:
                raise PdfOcrError(
                    f"OCR is required but disabled by configuration for '{pdf_path}'."
                )
            ocr_start = time.monotonic()
            pages = self._apply_ocr_fallback(pdf_path, pages, low_quality_indexes)
            ocr_elapsed = time.monotonic() - ocr_start

        normalized = _normalize_text("\n".join(page for page in pages if page.strip()))
        if not normalized.strip():
            raise PdfNotReadableError(
                f"PDF is empty or unreadable after Docling/OCR processing: {pdf_path}"
            )

        return normalized, ParserExecutionStats(
            extraction_seconds=extraction_elapsed,
            ocr_seconds=ocr_elapsed,
            ocr_triggered=needs_ocr,
            parser_strategy="docling_ocr",
        )

    def _validate_operational_limits(self, pdf_path: Path) -> None:
        if self._config.timeout_seconds <= 0:
            raise PdfNotReadableError("Invalid parser timeout configuration.")
        if not pdf_path.exists():
            raise PdfNotReadableError(f"PDF file does not exist: {pdf_path}")
        if self._config.max_pages <= 0:
            raise PdfNotReadableError("Invalid max pages parser configuration.")
        # Docling currently returns a document-level export in this flow.
        # We keep max_pages as guardrail config for future page-aware extraction.
        if pdf_path.stat().st_size == 0:
            raise PdfNotReadableError(
                f"PDF is empty or unreadable before Docling processing: {pdf_path}"
            )

    def _extract_pages_with_docling(self, pdf_path: Path) -> list[str]:
        try:
            converter = self._build_docling_converter()
            result = converter.convert(str(pdf_path))
            markdown = _normalize_text(result.document.export_to_markdown())
        except Exception as exc:
            raise PdfNotReadableError(f"Docling failed for '{pdf_path}': {exc}") from exc

        # Docling exports document-level markdown; we keep compatibility with a page-list API.
        return [markdown] if markdown else [""]

    def _build_docling_converter(self) -> DocumentConverter:
        try:
            from docling.datamodel.base_models import InputFormat
            from docling.datamodel.pipeline_options import EasyOcrOptions, PdfPipelineOptions, TableFormerMode
            from docling.document_converter import PdfFormatOption

            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = self._config.docling_do_ocr
            if self._config.docling_do_ocr:
                pipeline_options.ocr_options = EasyOcrOptions(
                    lang=list(self._config.docling_ocr_languages),
                )
                pipeline_options.ocr_options.force_full_page_ocr = self._config.docling_force_full_page_ocr
            pipeline_options.do_table_structure = self._config.docling_do_table_structure
            if self._config.docling_do_table_structure:
                pipeline_options.table_structure_options.mode = (
                    TableFormerMode.ACCURATE
                    if self._config.docling_table_mode == "accurate"
                    else TableFormerMode.FAST
                )
                pipeline_options.table_structure_options.do_cell_matching = (
                    self._config.docling_table_cell_matching
                )

            return DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                }
            )
        except Exception:
            return DocumentConverter()

    def _apply_ocr_fallback(
        self,
        pdf_path: Path,
        pages: list[str],
        low_quality_indexes: list[int],
    ) -> list[str]:
        try:
            images = convert_from_path(str(pdf_path))
        except Exception as exc:
            raise PdfOcrError(f"OCR image conversion failed for '{pdf_path}': {exc}") from exc

        if not images:
            raise PdfOcrError(f"OCR image conversion yielded no pages for '{pdf_path}'.")

        # Docling currently provides a document-level markdown blob as a single entry.
        # To avoid losing non-first pages during OCR fallback, OCR all rendered pages
        # when operating in document mode or when page mapping is ambiguous.
        if self._config.ocr_mode == "document" or (len(pages) == 1 and len(images) > 1):
            ocr_pages: list[str] = []
            for image in images:
                ocr_pages.append(_normalize_text(self._tesseract_to_string(image)))
            if any(page.strip() for page in ocr_pages):
                return ocr_pages
            return pages

        targets = low_quality_indexes

        updated_pages = pages[:]
        for idx in targets:
            if idx >= len(images):
                continue
            ocr_text = _normalize_text(self._tesseract_to_string(images[idx]))
            if ocr_text.strip():
                updated_pages[idx] = ocr_text
        return updated_pages

    def _tesseract_to_string(self, image: object) -> str:
        return pytesseract.image_to_string(
            image,
            lang=self._config.tesseract_lang,
            config=f"--psm {self._config.tesseract_psm}",
        )


# ── public interface ───────────────────────────────────────────────────────────


def parse_aerodrome_from_ad20(pdf_path: Path) -> AerodromeCreate:
    """Backward-compatible single-document entrypoint for AD-2.0 parsing."""
    inferred_icao = _infer_icao_from_path(pdf_path)
    if inferred_icao is None:
        raise PdfFormatError(
            "Could not infer ICAO from PDF file path. "
            "Use parse_aerodrome_from_documents(..., icao='SAXX')."
        )
    return parse_aerodrome_from_documents([pdf_path], icao=inferred_icao)


def parse_aerodrome_from_documents(pdf_paths: list[Path], icao: str) -> AerodromeCreate:
    """Extract deterministic AD 2.0 sections from one or more AIP PDFs."""
    if not pdf_paths:
        raise PdfNotReadableError("No AIP PDF documents were provided for parsing.")

    config = _get_parser_config()
    pipeline = extract_and_segment_sections(
        pdf_paths=pdf_paths,
        icao=icao,
        config=config,
        parser_factory=lambda cfg: DoclingOcrParser(config=cfg),
        extract_documents_text=extract_documents_text,
        segment_sections=lambda text, source_path, code: segment_ad2_sections(
            text,
            source_path,
            code,
            logger=logger,
            format_error=PdfFormatError,
        ),
        forced_config_builder=forced_document_ocr_config,
        format_error=PdfFormatError,
        logger=logger,
    )
    text = pipeline.text
    sections = pipeline.sections

    name, full_name = extract_name_fields(sections)
    now = datetime.now(timezone.utc)
    source_document = ", ".join(path.name for path in pdf_paths)
    airac_cycle = extract_airac_cycle(text) or "unknown"

    return AerodromeCreate(
        icao_code=icao.strip().upper(),
        name=name,
        full_name=full_name,
        airac_cycle=airac_cycle,
        airac_effective_date=now,
        airac_expiry_date=now,
        source_document=source_document,
        downloaded_by="parser-agent",
        ad_sections=sections,
    )


# ── private helpers ────────────────────────────────────────────────────────────


def _get_parser_config() -> ParserConfig:
    settings = get_settings()
    docling_ocr_languages = tuple(
        part.strip() for part in settings.aip_parser_docling_ocr_languages.split(",") if part.strip()
    ) or ("es", "en")
    return ParserConfig(
        quality_threshold=settings.aip_parser_docling_quality_threshold,
        ocr_enabled=settings.aip_parser_ocr_enabled,
        ocr_mode=settings.aip_parser_ocr_mode,
        timeout_seconds=settings.aip_parser_timeout_seconds,
        max_pages=settings.aip_parser_max_pages,
        docling_do_ocr=settings.aip_parser_docling_do_ocr,
        docling_ocr_languages=docling_ocr_languages,
        docling_force_full_page_ocr=settings.aip_parser_docling_force_full_page_ocr,
        tesseract_lang=settings.aip_parser_tesseract_lang,
        tesseract_psm=settings.aip_parser_tesseract_psm,
        docling_do_table_structure=settings.aip_parser_docling_do_table_structure,
        docling_table_mode=settings.aip_parser_docling_table_mode,
        docling_table_cell_matching=settings.aip_parser_docling_table_cell_matching,
    )


def _normalize_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return normalized.replace("\x00", "")


def _page_quality(text: str) -> float:
    if not text:
        return 0.0
    meaningful_chars = sum(1 for ch in text if ch.isalnum())
    return meaningful_chars / max(len(text), 1)


def _infer_icao_from_path(pdf_path: Path) -> str | None:
    candidates = re.findall(r"SA[A-Z0-9]{2}", pdf_path.name.upper())
    if candidates:
        return candidates[0]
    return None
