"""AIP PDF parser — extracts flexible AD 2.0 sections from ANAC AIP PDFs."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import instructor
import pytesseract
from docling.document_converter import DocumentConverter
from openai import OpenAI
from pdf2image import convert_from_path

from app.core.config import get_settings
from app.schemas.aerodrome import AerodromeCreate

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an aviation AD 2.0 extraction specialist for Argentine AIP documents.

Return only valid JSON.
Do not include markdown, explanations, comments, or code fences.
Do not invent values.
Preserve literal published wording whenever possible.

Goal:
- Build one flexible aerodrome payload with AD 2.0 sections AD 2.1 ... AD 2.25.
- Keep each section raw text bilingual (ES/EN) exactly as extracted.
- Add flexible `data` objects for machine use.

Required output shape:
{
  "icao_code": "SAMR",
  "name": "...",
  "full_name": "... or null",
  "airac_cycle": "...",
  "airac_effective_date": "ISO-8601",
  "airac_expiry_date": "ISO-8601",
  "source_document": "...",
  "source_url": "... or null",
  "downloaded_by": "parser-agent",
  "ad_sections": [
    {
      "section_id": "AD 2.1",
      "title": "...",
      "raw_text": "literal bilingual content",
      "data": {"free": "structure"},
      "anchors": null,
      "section_meta": {
        "airac_cycle": "... or null",
        "source_page": 1
      }
    }
  ]
}

Rules:
- Include exactly AD 2.1 to AD 2.25 in ad_sections.
- `raw_text` must be non-empty in every section.
- If a section is NIL, keep that literal value in `raw_text`.
- `data` can be flexible and partial, but must be an object.
- `section_meta.airac_cycle` is optional and should be set when page-level AIRAC differs.
"""


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
            converter = DocumentConverter()
            result = converter.convert(str(pdf_path))
            markdown = _normalize_text(result.document.export_to_markdown())
        except Exception as exc:
            raise PdfNotReadableError(f"Docling failed for '{pdf_path}': {exc}") from exc

        # Docling exports document-level markdown; we keep compatibility with a page-list API.
        return [markdown] if markdown else [""]

    def _apply_ocr_fallback(
        self,
        pdf_path: Path,
        pages: list[str],
        low_quality_indexes: list[int],
    ) -> list[str]:
        if self._config.ocr_mode == "document":
            targets = list(range(len(pages)))
        else:
            targets = low_quality_indexes

        try:
            images = convert_from_path(str(pdf_path))
        except Exception as exc:
            raise PdfOcrError(f"OCR image conversion failed for '{pdf_path}': {exc}") from exc

        if not images:
            raise PdfOcrError(f"OCR image conversion yielded no pages for '{pdf_path}'.")

        updated_pages = pages[:]
        for idx in targets:
            if idx >= len(images):
                continue
            ocr_text = _normalize_text(pytesseract.image_to_string(images[idx]))
            if ocr_text.strip():
                updated_pages[idx] = ocr_text
        return updated_pages


# ── public interface ───────────────────────────────────────────────────────────


def parse_aerodrome_from_ad20(pdf_path: Path) -> AerodromeCreate:
    """Backward-compatible single-document entrypoint for AD-2.0 parsing."""
    return parse_aerodrome_from_documents([pdf_path])


def parse_aerodrome_from_documents(pdf_paths: list[Path]) -> AerodromeCreate:
    """Extract a flexible AD 2.0 aerodrome payload from one or more AIP PDFs."""
    if not pdf_paths:
        raise PdfNotReadableError("No AIP PDF documents were provided for parsing.")

    parser = DoclingOcrParser(config=_get_parser_config())
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

    text = "\n\n".join(assembled_blocks)
    client = _get_llm_client()
    settings = get_settings()
    logger.info(
        "aip.parser.extraction",
        extra={
            "strategy": "docling_ocr",
            "ocr_triggered": any_ocr_triggered,
            "documents_count": len(pdf_paths),
            "extraction_seconds": round(total_extraction_seconds, 3),
            "ocr_seconds": round(total_ocr_seconds, 3),
        },
    )

    structuring_start = time.monotonic()
    try:
        result: AerodromeCreate = client.create(
            model=settings.openrouter_model,
            response_model=AerodromeCreate,
            max_retries=3,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
        )
        logger.info(
            "aip.parser.structuring",
            extra={"structuring_seconds": round(time.monotonic() - structuring_start, 3)},
        )
        return result
    except Exception as exc:
        raise PdfFormatError(
            f"LLM failed to extract structured data from '{pdf_path}': {exc}"
        ) from exc


# ── private helpers ────────────────────────────────────────────────────────────


def _get_parser_config() -> ParserConfig:
    settings = get_settings()
    return ParserConfig(
        quality_threshold=settings.aip_parser_docling_quality_threshold,
        ocr_enabled=settings.aip_parser_ocr_enabled,
        ocr_mode=settings.aip_parser_ocr_mode,
        timeout_seconds=settings.aip_parser_timeout_seconds,
        max_pages=settings.aip_parser_max_pages,
    )


def _normalize_text(text: str) -> str:
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _page_quality(text: str) -> float:
    if not text:
        return 0.0
    meaningful_chars = sum(1 for ch in text if ch.isalnum())
    return meaningful_chars / max(len(text), 1)


def _get_llm_client() -> instructor.Instructor:
    """Return an instructor-patched OpenAI client pointed at OpenRouter."""
    settings = get_settings()
    if not settings.openrouter_api_key:
        raise AipParserError(
            "OPENROUTER_API_KEY is required for LLM-based parsing. "
            "Set it in your .env file."
        )
    return instructor.from_openai(
        OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
        )
    )
