"""AIP AD 2.0 parser based on PyMuPDF layout and table extraction."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymupdf

from app.repositories.pre_llm_artifacts_repo import upsert_pre_llm_sections, upsert_raw_extraction
from app.schemas.aerodrome import AerodromeCreate
from app.services.scraper.aip_metadata import extract_airac_cycle, extract_name_fields
from app.services.scraper.aip_segmenter import (
    LAYOUT_ENGINE,
    LAYOUT_SCHEMA_VERSION,
    sectionize_layout_artifact,
)

logger = logging.getLogger(__name__)

_PAGE_HEADER_Y = 58.0
_PAGE_FOOTER_MARGIN = 58.0


class AipParserError(Exception):
    pass


class PdfNotReadableError(AipParserError):
    pass


class PdfFormatError(AipParserError):
    pass


class PdfOcrError(AipParserError):
    """Reserved for future OCR support. OCR is intentionally disabled in v1."""


@dataclass(frozen=True)
class ParserExecutionStats:
    extraction_seconds: float
    ocr_seconds: float
    ocr_triggered: bool
    parser_strategy: str


@dataclass(frozen=True)
class ParserConfig:
    """PyMuPDF parser configuration."""

    max_pages: int | None = None


class PyMuPdfAipParser:
    """Extract ANAC AIP AD 2.0 layout blocks and tables from native PDF text."""

    def __init__(self, config: ParserConfig | None = None) -> None:
        self._config = config or ParserConfig()
        self._artifacts: dict[str, Any] = {}

    def get_artifacts(self) -> dict[str, Any]:
        return dict(self._artifacts)

    def extract_layout(self, pdf_path: Path) -> tuple[dict[str, Any], ParserExecutionStats]:
        self._validate_pdf(pdf_path)
        started = time.monotonic()

        try:
            doc = pymupdf.open(str(pdf_path))
        except Exception as exc:
            raise PdfNotReadableError(f"Could not open PDF '{pdf_path}': {exc}") from exc

        try:
            page_count = doc.page_count
            if page_count <= 0:
                raise PdfNotReadableError(f"PDF has no pages: {pdf_path}")
            max_pages = self._config.max_pages or page_count
            pages_limit = min(max_pages, page_count)
            pages = [_extract_page_layout(doc[index], page_number=index + 1) for index in range(pages_limit)]
        finally:
            doc.close()

        plain_text = "\n\n".join(
            str(element.get("text") or "")
            for page in pages
            for element in page.get("elements", [])
            if isinstance(element, dict) and str(element.get("text") or "").strip()
        )
        if not plain_text.strip():
            raise PdfNotReadableError(
                f"PDF has no embedded text usable by PyMuPDF: {pdf_path}. OCR is disabled for this parser."
            )

        elapsed = time.monotonic() - started
        artifact = {
            "schema_version": LAYOUT_SCHEMA_VERSION,
            "engine": LAYOUT_ENGINE,
            "source_path": str(pdf_path),
            "extraction_seconds": elapsed,
            "metadata": {
                "page_count": page_count,
                "pages_extracted": len(pages),
                "ocr_enabled": False,
            },
            "pages": pages,
        }
        self._artifacts = {"raw_extraction": artifact}
        return artifact, ParserExecutionStats(
            extraction_seconds=elapsed,
            ocr_seconds=0.0,
            ocr_triggered=False,
            parser_strategy=LAYOUT_ENGINE,
        )

    def _validate_pdf(self, pdf_path: Path) -> None:
        if not pdf_path.exists():
            raise PdfNotReadableError(f"PDF file does not exist: {pdf_path}")
        if pdf_path.stat().st_size == 0:
            raise PdfNotReadableError(f"PDF is empty or unreadable before extraction: {pdf_path}")


PymupdfParser = PyMuPdfAipParser


def parse_aerodrome_from_ad20(pdf_path: Path) -> AerodromeCreate:
    inferred_icao = _infer_icao_from_path(pdf_path)
    if inferred_icao is None:
        raise PdfFormatError(
            "Could not infer ICAO from PDF file path. "
            "Use parse_aerodrome_from_documents(..., icao='XXXX')."
        )
    return parse_aerodrome_from_documents([pdf_path], icao=inferred_icao)


def parse_aerodrome_from_documents(pdf_paths: list[Path], icao: str) -> AerodromeCreate:
    if not pdf_paths:
        raise PdfNotReadableError("No AIP PDF documents were provided for parsing.")

    parser = PyMuPdfAipParser(_get_parser_config())
    layouts: list[dict[str, Any]] = []
    source_names: list[str] = []
    total_extraction_seconds = 0.0
    for pdf_path in pdf_paths:
        layout, stats = parser.extract_layout(pdf_path)
        layouts.append(layout)
        source_names.append(pdf_path.name)
        total_extraction_seconds += stats.extraction_seconds

    merged_layout = _merge_layouts(layouts, source_names=source_names)
    logger.info(
        "aip.parser.extraction",
        extra={
            "strategy": LAYOUT_ENGINE,
            "ocr_triggered": False,
            "documents_count": len(pdf_paths),
            "extraction_seconds": round(total_extraction_seconds, 3),
            "ocr_seconds": 0.0,
        },
    )

    sectionized = sectionize_layout_artifact(
        layout_artifact=merged_layout,
        icao=icao,
        source_path=pdf_paths[0],
        logger=logger,
        format_error=PdfFormatError,
    )
    sections = sectionized.sections

    full_text = _layout_plain_text(merged_layout)
    name, full_name = extract_name_fields(sections)
    now = datetime.now(timezone.utc)
    source_document = ", ".join(source_names)
    airac_cycle = extract_airac_cycle(full_text) or "unknown"

    parser_artifacts = {
        "raw_extraction": merged_layout,
        "pre_llm_sections": sectionized.pre_llm_sections,
    }
    _persist_intermediate_artifacts(
        icao=icao,
        airac_cycle=airac_cycle,
        source_filename=pdf_paths[0].name,
        parser_artifacts=parser_artifacts,
    )

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


def _extract_page_layout(page: pymupdf.Page, *, page_number: int) -> dict[str, Any]:
    text_blocks = _page_text_blocks(page, page_number=page_number)
    tables = _page_tables(page, page_number=page_number)
    table_bboxes = [table["bbox"] for table in tables if isinstance(table.get("bbox"), list)]

    elements: list[dict[str, Any]] = []
    for block in text_blocks:
        if _is_in_any_bbox(block["bbox"], table_bboxes):
            continue
        elements.append(block)
    elements.extend(tables)
    elements.sort(key=lambda e: (float(e["bbox"][1]), float(e["bbox"][0]), int(e["order"])))
    for order, element in enumerate(elements, start=1):
        element["order"] = order

    return {
        "page": page_number,
        "width": float(page.rect.width),
        "height": float(page.rect.height),
        "elements": elements,
    }


def _page_text_blocks(page: pymupdf.Page, *, page_number: int) -> list[dict[str, Any]]:
    data = page.get_text("dict", sort=True)
    blocks: list[dict[str, Any]] = []
    for idx, block in enumerate(data.get("blocks", []), start=1):
        if block.get("type") != 0:
            continue
        bbox = _bbox(block.get("bbox"))
        if bbox is None or _is_page_header_or_footer(bbox, page_height=float(page.rect.height)):
            continue
        text = _text_from_block(block)
        if not text:
            continue
        blocks.append(
            {
                "type": "text",
                "text": text,
                "page": page_number,
                "bbox": bbox,
                "order": idx,
            }
        )
    return blocks


def _page_tables(page: pymupdf.Page, *, page_number: int) -> list[dict[str, Any]]:
    try:
        found = page.find_tables()
    except Exception as exc:
        logger.warning("aip.parser.table_detection_failed", extra={"page": page_number, "error": str(exc)})
        return []

    tables: list[dict[str, Any]] = []
    for idx, table in enumerate(found.tables, start=1):
        bbox = _bbox(table.bbox)
        if bbox is None or _is_page_header_or_footer(bbox, page_height=float(page.rect.height)):
            continue
        raw_rows = table.extract()
        normalized = _normalize_table(raw_rows)
        if not normalized["rows"] and not normalized["label"]:
            continue
        text_lines = [normalized["label"]] if normalized["label"] else []
        for row in normalized["rows"]:
            text_lines.append(
                " | ".join(
                    value
                    for value in (
                        str(row.get("item") or "").strip(),
                        str(row.get("label") or "").strip(),
                        str(row.get("value") or "").strip(),
                    )
                    if value
                )
            )
        tables.append(
            {
                "type": "table",
                "text": "\n".join(line for line in text_lines if line).strip(),
                "page": page_number,
                "bbox": bbox,
                "order": idx,
                "table": {
                    "label": normalized["label"],
                    "columns": ["item", "label", "value"],
                    "rows": normalized["rows"],
                    "cells": normalized["cells"],
                    "raw_rows": normalized["raw_rows"],
                },
            }
        )
    return tables


def _normalize_table(raw_rows: list[list[Any]]) -> dict[str, Any]:
    rows = [_clean_cells(row) for row in raw_rows if isinstance(row, list)]
    rows = [row for row in rows if any(row)]
    label = ""
    data_rows: list[dict[str, str]] = []
    cells: list[str] = []

    for row in rows:
        nonempty = [cell for cell in row if cell]
        if not nonempty:
            continue
        joined = " ".join(nonempty)
        cells.extend(nonempty)
        if not label and re.search(r"(?i)\bAD\s*2\.\d{1,2}\b", joined):
            label = joined
            continue

        item = ""
        values = nonempty
        if re.fullmatch(r"\d{1,2}", nonempty[0]):
            item = nonempty[0]
            values = nonempty[1:]

        if not values:
            data_rows.append({"item": item, "label": "", "value": ""})
            continue
        if len(values) == 1:
            data_rows.append({"item": item, "label": "", "value": values[0]})
            continue
        data_rows.append(
            {
                "item": item,
                "label": values[0],
                "value": " ".join(values[1:]).strip(),
            }
        )

    return {
        "label": label,
        "columns": ["item", "label", "value"],
        "rows": data_rows,
        "cells": cells,
        "raw_rows": rows,
    }


def _clean_cells(row: list[Any]) -> list[str]:
    return [_clean_text(str(cell or "")) for cell in row]


def _text_from_block(block: dict[str, Any]) -> str:
    lines: list[str] = []
    for line in block.get("lines", []):
        spans = line.get("spans", []) if isinstance(line, dict) else []
        text = "".join(str(span.get("text") or "") for span in spans if isinstance(span, dict))
        text = _clean_text(text)
        if text:
            lines.append(text)
    return "\n".join(lines).strip()


def _bbox(value: Any) -> list[float] | None:
    if not isinstance(value, (tuple, list)) or len(value) != 4:
        return None
    try:
        return [float(v) for v in value]
    except (TypeError, ValueError):
        return None


def _is_page_header_or_footer(bbox: list[float], *, page_height: float) -> bool:
    return bbox[1] < _PAGE_HEADER_Y or bbox[3] > page_height - _PAGE_FOOTER_MARGIN


def _is_in_any_bbox(bbox: list[float], candidates: list[list[float]]) -> bool:
    cx = (bbox[0] + bbox[2]) / 2
    cy = (bbox[1] + bbox[3]) / 2
    for candidate in candidates:
        if candidate[0] <= cx <= candidate[2] and candidate[1] <= cy <= candidate[3]:
            return True
    return False


def _merge_layouts(layouts: list[dict[str, Any]], *, source_names: list[str]) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    offset = 0
    for layout, source_name in zip(layouts, source_names, strict=False):
        for page in layout.get("pages", []):
            if not isinstance(page, dict):
                continue
            page_copy = dict(page)
            page_copy["source_document"] = source_name
            page_copy["page"] = int(page_copy.get("page", 0) or 0) + offset
            for element in page_copy.get("elements", []):
                if isinstance(element, dict):
                    element["source_document"] = source_name
                    element["page"] = page_copy["page"]
            pages.append(page_copy)
        offset += int(layout.get("metadata", {}).get("pages_extracted", 0) or len(layout.get("pages", [])))
    return {
        "schema_version": LAYOUT_SCHEMA_VERSION,
        "engine": LAYOUT_ENGINE,
        "source_documents": source_names,
        "metadata": {
            "documents_count": len(layouts),
            "pages_extracted": len(pages),
            "ocr_enabled": False,
        },
        "pages": pages,
    }


def _layout_plain_text(layout: dict[str, Any]) -> str:
    return "\n\n".join(
        str(element.get("text") or "")
        for page in layout.get("pages", [])
        if isinstance(page, dict)
        for element in page.get("elements", [])
        if isinstance(element, dict) and str(element.get("text") or "").strip()
    )


def _persist_intermediate_artifacts(*, icao: str, airac_cycle: str, source_filename: str, parser_artifacts: dict[str, object]) -> None:
    raw_extraction = parser_artifacts.get("raw_extraction") if isinstance(parser_artifacts, dict) else None
    pre_llm = parser_artifacts.get("pre_llm_sections") if isinstance(parser_artifacts, dict) else None
    if not isinstance(raw_extraction, dict) or not isinstance(pre_llm, dict):
        return
    try:
        import asyncio

        async def _save() -> None:
            await upsert_raw_extraction(
                icao=icao,
                airac_cycle=airac_cycle,
                source_filename=source_filename,
                payload=raw_extraction,
            )
            await upsert_pre_llm_sections(
                icao=icao,
                airac_cycle=airac_cycle,
                source_filename=source_filename,
                payload=pre_llm,
            )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_save())
        except RuntimeError:
            try:
                asyncio.run(_save())
            except Exception as exc:
                if exc.__class__.__name__ == "CollectionWasNotInitialized":
                    logger.debug("aip.parser.persist_intermediate_skipped_beanie_not_initialized")
                    return
                raise
    except Exception:
        logger.exception("aip.parser.persist_intermediate_failed", extra={"icao": icao, "source": source_filename})


def _get_parser_config() -> ParserConfig:
    return ParserConfig()


def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return "\n".join(line.strip() for line in text.splitlines()).strip()


def _infer_icao_from_path(pdf_path: Path) -> str | None:
    """Pick first 4-letter alphabetic token (underscore is a word char in regex \\b)."""
    name = pdf_path.stem.upper()
    for token in re.split(r"[^A-Z]+", name):
        if len(token) == 4 and token.isalpha():
            return token
    return None
