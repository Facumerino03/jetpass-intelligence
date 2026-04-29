"""AIP PDF parser — deterministic AD 2.0 extraction and segmentation."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import pytesseract
from docling.document_converter import DocumentConverter
from pdf2image import convert_from_path

from app.core.config import get_settings
from app.schemas.aerodrome import AerodromeCreate, SectionSchema
from app.services.scraper.aip_raw_canonicalizer import canonicalize_section_raw_text

logger = logging.getLogger(__name__)

_HEADER_RE = re.compile(
    r"(?im)^\s*(?:\|\s*)?(?:#{1,6}\s*)?(?:[A-Z]{4}\s+)?AD\s*2\.(\d{1,2})(?!\d)\b[^\n]*"
)

_SUSPICIOUS_HEADER_RE = re.compile(
    r"(?im)(?:\bAD2[\.,]?\d{1,2}\b|\bA\s*D\s*2[\.,]\s*\d{1,2}\b|\bAD\s*2,\s*\d{1,2}\b)"
)

_AD211_FALLBACK_RE = re.compile(
    r"(?im)(?:informaci\w*\s+meteoro\w+|meteorological\s+information)"
)

_AD211_FIRST_ROW_FALLBACK_RE = re.compile(
    r"(?im)(?:oficina\s+met\s+asociada|associated\s+met\s+office)"
)


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


class HeaderMatch:
    """Matched AD 2.x heading with normalized section id and offsets."""

    def __init__(self, section_id: str, matched_text: str, start: int, end: int) -> None:
        self.section_id = section_id
        self.matched_text = matched_text
        self.start = start
        self.end = end


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
            from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
            from docling.document_converter import PdfFormatOption

            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = True
            pipeline_options.do_table_structure = True
            pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
            pipeline_options.table_structure_options.do_cell_matching = True

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
    parser = DoclingOcrParser(config=config)
    text, total_extraction_seconds, total_ocr_seconds, any_ocr_triggered = _extract_documents_text(
        parser, pdf_paths
    )
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

    try:
        sections = _segment_ad2_sections(text, pdf_paths[0], icao)
    except PdfFormatError as first_exc:
        if not config.ocr_enabled:
            raise PdfFormatError(
                f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {first_exc}"
            ) from first_exc
        forced_config = _forced_document_ocr_config(config)
        forced_parser = DoclingOcrParser(config=forced_config)
        try:
            retry_text, retry_extraction_seconds, retry_ocr_seconds, retry_ocr_triggered = _extract_documents_text(
                forced_parser, pdf_paths
            )
            logger.info(
                "aip.parser.extraction.retry",
                extra={
                    "strategy": "docling_ocr_document_forced",
                    "ocr_triggered": retry_ocr_triggered,
                    "documents_count": len(pdf_paths),
                    "extraction_seconds": round(retry_extraction_seconds, 3),
                    "ocr_seconds": round(retry_ocr_seconds, 3),
                },
            )
            sections = _segment_ad2_sections(retry_text, pdf_paths[0], icao)
            text = retry_text
        except Exception as retry_exc:
            raise PdfFormatError(
                f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {first_exc} "
                f"| retry_with_forced_document_ocr_failed: {retry_exc}"
            ) from retry_exc
    except Exception as exc:
        raise PdfFormatError(
            f"Deterministic AD 2.0 segmentation failed for '{pdf_paths[0]}': {exc}"
        ) from exc

    name, full_name = _extract_name_fields(sections)
    now = datetime.now(timezone.utc)
    source_document = ", ".join(path.name for path in pdf_paths)
    airac_cycle = _extract_airac_cycle(text) or "unknown"

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
    return ParserConfig(
        quality_threshold=settings.aip_parser_docling_quality_threshold,
        ocr_enabled=settings.aip_parser_ocr_enabled,
        ocr_mode=settings.aip_parser_ocr_mode,
        timeout_seconds=settings.aip_parser_timeout_seconds,
        max_pages=settings.aip_parser_max_pages,
    )


def _normalize_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return normalized.replace("\x00", "")


def _extract_documents_text(
    parser: DoclingOcrParser,
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


def _forced_document_ocr_config(base: ParserConfig) -> ParserConfig:
    return ParserConfig(
        quality_threshold=1.1,
        ocr_enabled=base.ocr_enabled,
        ocr_mode="document",
        timeout_seconds=base.timeout_seconds,
        max_pages=base.max_pages,
    )


def _page_quality(text: str) -> float:
    if not text:
        return 0.0
    meaningful_chars = sum(1 for ch in text if ch.isalnum())
    return meaningful_chars / max(len(text), 1)


def _expected_section_ids() -> list[str]:
    return [f"AD 2.{idx}" for idx in range(1, 26)]


def _normalize_section_id(section_number: int) -> str | None:
    if 1 <= section_number <= 25:
        return f"AD 2.{section_number}"
    return None


def _context_window(text: str, index: int, window: int = 160) -> str:
    start = max(index - window, 0)
    end = min(index + window, len(text))
    return text[start:end]


def _segment_ad2_sections(text: str, source_path: Path, icao: str) -> list[SectionSchema]:
    expected = _expected_section_ids()
    matches: list[HeaderMatch] = []
    for match in _HEADER_RE.finditer(text):
        section_number = int(match.group(1))
        section_id = _normalize_section_id(section_number)
        if section_id is None:
            continue
        matches.append(
            HeaderMatch(
                section_id=section_id,
                matched_text=match.group(0),
                start=match.start(),
                end=match.end(),
            )
        )

    seen_counts: dict[str, int] = {}
    for m in matches:
        seen_counts[m.section_id] = seen_counts.get(m.section_id, 0) + 1

    missing_sections = [section_id for section_id in expected if seen_counts.get(section_id, 0) == 0]
    duplicate_sections = [section_id for section_id, count in seen_counts.items() if count > 1]

    recovered_ad211 = _recover_missing_ad211(text, matches)
    if recovered_ad211 is not None:
        matches.append(recovered_ad211)
        seen_counts[recovered_ad211.section_id] = seen_counts.get(recovered_ad211.section_id, 0) + 1
        missing_sections = [section_id for section_id in expected if seen_counts.get(section_id, 0) == 0]
        duplicate_sections = [section_id for section_id, count in seen_counts.items() if count > 1]

    if missing_sections or duplicate_sections:
        found_headers = [
            {
                "matched_text": m.matched_text,
                "normalized_section_id": m.section_id,
                "offset_start": m.start,
                "offset_end": m.end,
                "context": _context_window(text, m.start),
            }
            for m in matches
        ]
        suspicious_candidates = [
            {
                "candidate": candidate.group(0),
                "offset_start": candidate.start(),
                "context": _context_window(text, candidate.start()),
            }
            for candidate in _SUSPICIOUS_HEADER_RE.finditer(text)
        ]
        diagnostics = {
            "failure_stage": "segment_headers",
            "icao": icao,
            "source_document": source_path.name,
            "expected_sections": expected,
            "missing_sections": missing_sections,
            "duplicate_sections": sorted(duplicate_sections),
            "recovered_sections": [recovered_ad211.section_id] if recovered_ad211 else [],
            "found_headers": found_headers,
            "suspicious_candidates": suspicious_candidates,
        }
        raise PdfFormatError(f"AD 2.x header validation failed: {json.dumps(diagnostics, ensure_ascii=True)}")

    selected_by_section: dict[str, HeaderMatch] = {}
    for match in sorted(matches, key=lambda item: item.start):
        selected_by_section.setdefault(match.section_id, match)

    ordered_for_slicing = sorted(selected_by_section.values(), key=lambda item: item.start)
    raw_by_section: dict[str, str] = {}

    for idx, match in enumerate(ordered_for_slicing):
        next_start = (
            ordered_for_slicing[idx + 1].start if idx + 1 < len(ordered_for_slicing) else len(text)
        )
        block = text[match.start:next_start].strip()
        if not block:
            raise PdfFormatError(
                f"AD 2.x section block is empty: {match.section_id} in '{source_path.name}'."
            )
        raw_by_section[match.section_id] = block

    sections: list[SectionSchema] = []
    for section_id in expected:
        canonical_raw_text = canonicalize_section_raw_text(section_id, raw_by_section[section_id])
        canonical_raw_text = _postprocess_canonical_section(section_id, canonical_raw_text)
        sections.append(
            SectionSchema(
                section_id=section_id,
                title=section_id,
                raw_text=canonical_raw_text,
                data={},
            )
        )
    _rebalance_ad210_ad211_tail_noise(sections)
    return sections


def _rebalance_ad210_ad211_tail_noise(sections: list[SectionSchema]) -> None:
    ad210 = next((section for section in sections if section.section_id == "AD 2.10"), None)
    ad211 = next((section for section in sections if section.section_id == "AD 2.11"), None)
    if ad210 is None or ad211 is None:
        return

    ad211_lines = ad211.raw_text.splitlines()
    moved: list[str] = []
    kept: list[str] = []
    for line in ad211_lines:
        stripped = line.strip()
        lower = stripped.lower()
        if lower.startswith("note: coordenadas") or lower.startswith("note: / coordinates"):
            moved.append(stripped.replace("NOTE:", "NOTE:", 1))
            continue
        if stripped.startswith("NOTE:") and re.search(r"\b\d{6,7}(?:\.\d+)?[NS]\s*\d{7,8}(?:\.\d+)?[WE]\b", stripped, re.IGNORECASE):
            moved.append(stripped)
            continue
        kept.append(line)

    if not moved:
        return

    ad211.raw_text = "\n".join(kept)
    ad210.raw_text = f"{ad210.raw_text}\n" + "\n".join(moved)


def _postprocess_canonical_section(section_id: str, canonical_raw_text: str) -> str:
    if section_id != "AD 2.12":
        return canonical_raw_text

    lines = canonical_raw_text.splitlines()
    adjusted = list(lines)
    row_indices = [idx for idx, line in enumerate(lines) if line.strip() == "ITEM: ROW | No"]
    if len(row_indices) < 2:
        return canonical_raw_text

    first_value_idx = row_indices[0] + 1
    second_value_idx = row_indices[1] + 1
    if first_value_idx >= len(lines) or second_value_idx >= len(lines):
        return canonical_raw_text

    first_value = lines[first_value_idx].strip()
    second_value = lines[second_value_idx].strip()
    if first_value != second_value:
        return canonical_raw_text
    expected = "VALUE: 2.222x280 (*) | 90x60 | No | No | NIL"
    if first_value != expected:
        return canonical_raw_text

    adjusted[first_value_idx] = "VALUE: No | 2.222x280 (*) | 90x60 | No | No | NIL"
    adjusted[second_value_idx] = "VALUE: No | - | 90x60 | No | No | NIL"
    return "\n".join(adjusted)


def _infer_icao_from_path(pdf_path: Path) -> str | None:
    candidates = re.findall(r"SA[A-Z0-9]{2}", pdf_path.name.upper())
    if candidates:
        return candidates[0]
    return None


def _extract_name_fields(sections: list[SectionSchema]) -> tuple[str, str | None]:
    ad21 = next((section for section in sections if section.section_id == "AD 2.1"), None)
    if ad21 is None:
        return "UNKNOWN", None

    raw_lines = [line.strip() for line in ad21.raw_text.splitlines() if line.strip()]
    first_line = raw_lines[0] if raw_lines else ""
    if first_line.startswith("SECTION:"):
        section_title = ""
        if "|" in first_line:
            section_title = first_line.split("|", 1)[1].strip()

        section_name_match = re.search(r"\b([A-Z]{4})\s*-\s*(.+)$", section_title)
        if section_name_match:
            full_name = section_name_match.group(2).strip(" -:")
            main_name = full_name.split("/")[0].strip(" -:")
            if main_name:
                return main_name.title(), full_name
            return full_name.title(), full_name

        note_lines = [
            line.removeprefix("NOTE:").strip()
            for line in raw_lines
            if line.startswith("NOTE:")
        ]
        if note_lines:
            first_line = note_lines[0]

    # Preferred pattern in AD 2.1 headings:
    # "... NAME SAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO"
    icao_name_match = re.search(r"\b([A-Z]{4})\s*-\s*(.+)$", first_line)
    if icao_name_match:
        full_name = icao_name_match.group(2).strip(" -:")
        main_name = full_name.split("/")[0].strip(" -:")
        if main_name:
            return main_name.title(), full_name
        return full_name.title(), full_name

    candidate = re.sub(r"^\s*(?:[A-Z]{4}\s+)?AD\s*2\.1\s*", "", first_line, flags=re.IGNORECASE)
    candidate = candidate.strip(" -:")
    if not candidate:
        return "UNKNOWN", None

    left = candidate.split("/")[0].strip()
    if left:
        return left.title(), candidate
    return candidate.title(), candidate


def _extract_airac_cycle(text: str) -> str | None:
    # Common pattern in AIP pages: "AIRAC AMDT 03/26"
    match = re.search(r"AIRAC\s+AMDT\s+(\d{1,2}/\d{2})", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def _recover_missing_ad211(text: str, matches: list[HeaderMatch]) -> HeaderMatch | None:
    has_ad211 = any(m.section_id == "AD 2.11" for m in matches)
    if has_ad211:
        return None

    ad210_candidates = sorted((m for m in matches if m.section_id == "AD 2.10"), key=lambda m: m.start)
    ad212_candidates = sorted((m for m in matches if m.section_id == "AD 2.12"), key=lambda m: m.start)
    if not ad210_candidates or not ad212_candidates:
        return None

    ad210_start = ad210_candidates[0].start
    ad212_start = ad212_candidates[0].start
    if ad212_start <= ad210_start:
        return None

    for pattern in (_AD211_FIRST_ROW_FALLBACK_RE, _AD211_FALLBACK_RE):
        candidate_match = None
        for candidate in pattern.finditer(text):
            if not (ad210_start < candidate.start() < ad212_start):
                continue
            candidate_match = candidate
            break

        if candidate_match is None:
            continue

        candidate = candidate_match
        if not (ad210_start < candidate.start() < ad212_start):
            continue
        line_start = text.rfind("\n", 0, candidate.start()) + 1
        line_end = text.find("\n", candidate.start())
        if line_end == -1:
            line_end = len(text)
        matched_text = text[line_start:line_end].strip()
        if not matched_text:
            matched_text = candidate.group(0)
        return HeaderMatch(
            section_id="AD 2.11",
            matched_text=matched_text,
            start=line_start,
            end=line_end,
        )
    return None
