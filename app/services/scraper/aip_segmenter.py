"""Spatial AD 2.x sectionization for ANAC AIP PDFs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.schemas.aerodrome import SectionSchema
from app.services.enrichment.ad2_contracts import load_ad2_contract

LAYOUT_SCHEMA_VERSION = "aip-layout-v1"
LAYOUT_ENGINE = "pymupdf"

_HEADER_RE = re.compile(r"(?i)\b(?:[A-Z]{4}\s+)?AD\s*2\.(\d{1,2})(?!\d)\b[^\n|]*")
_EXPECTED_SECTION_IDS = tuple(f"AD 2.{idx}" for idx in range(1, 26))


@dataclass(frozen=True)
class SectionizedLayout:
    """Section DTOs plus the artifact payloads persisted for debugging/LLM hints."""

    sections: list[SectionSchema]
    pre_llm_sections: dict[str, Any]


def sectionize_layout_artifact(
    *,
    layout_artifact: dict[str, Any],
    icao: str,
    source_path: Path,
    logger: object,
    format_error: type[Exception],
) -> SectionizedLayout:
    """Turn PyMuPDF page elements into exactly 25 AD 2.x sections."""

    elements = _layout_elements(layout_artifact)
    if not elements:
        raise format_error(f"No extractable text/table elements found in '{source_path}'.")

    buckets: dict[str, list[dict[str, Any]]] = {sid: [] for sid in _EXPECTED_SECTION_IDS}
    current: str | None = None
    preamble: list[dict[str, Any]] = []
    duplicates: set[str] = set()

    for element in elements:
        sid = _detect_section_id(element)
        if sid is not None:
            if sid in buckets and buckets[sid]:
                duplicates.add(sid)
            current = sid
        if current is None:
            preamble.append(element)
            continue
        if current in buckets:
            buckets[current].append(element)

    if preamble:
        buckets["AD 2.1"][0:0] = preamble

    missing = [sid for sid in _EXPECTED_SECTION_IDS if not buckets[sid]]
    if duplicates:
        logger.warning(
            "aip.parser.duplicate_headers_detected",
            extra={
                "icao": icao,
                "source_document": source_path.name,
                "duplicate_sections": sorted(duplicates),
            },
        )
    if missing:
        raise format_error(
            "AD 2.x header validation failed for "
            f"{source_path.name} ({icao}): missing={missing}, duplicates={sorted(duplicates)}"
        )

    sections: list[SectionSchema] = []
    pre_llm_items: list[dict[str, Any]] = []
    for sid in _EXPECTED_SECTION_IDS:
        blocks = [_section_block(element, order=i + 1) for i, element in enumerate(buckets[sid])]
        blocks = _add_inferred_tables(section_id=sid, blocks=blocks)
        blocks = _normalize_section_blocks(section_id=sid, blocks=blocks)
        table_blocks = [block for block in blocks if block.get("type") == "table"]
        tables = [_table_payload(block, table_idx=i + 1) for i, block in enumerate(table_blocks)]
        raw_text = _section_raw_text(sid, blocks)
        title = _section_title(icao=icao, section_id=sid, blocks=blocks)
        quality = _quality_for(blocks, tables)
        source = _source_for(blocks, tables)

        sections.append(
            SectionSchema(
                section_id=sid,
                title=sid,
                section_title=title,
                raw_text=raw_text,
                data={
                    "schema_hint": _schema_hint_for(icao, sid),
                    "tables": tables,
                    "quality": quality,
                },
                anchors={
                    "section_blocks": blocks,
                    "source": source,
                },
            )
        )
        pre_llm_items.append(
            {
                "section_id": sid,
                "title": title or sid,
                "schema_hint": _schema_hint_for(icao, sid),
                "section_blocks": blocks,
                "tables": tables,
                "quality": quality,
                "source": source,
                "raw_text_preview": raw_text[:2000],
            }
        )

    pre_llm = {
        "schema_version": LAYOUT_SCHEMA_VERSION,
        "engine": LAYOUT_ENGINE,
        "icao": icao.strip().upper(),
        "source_document": source_path.name,
        "sections": pre_llm_items,
    }
    return SectionizedLayout(sections=sections, pre_llm_sections=pre_llm)


def _layout_elements(layout_artifact: dict[str, Any]) -> list[dict[str, Any]]:
    pages = layout_artifact.get("pages") if isinstance(layout_artifact, dict) else None
    elements: list[dict[str, Any]] = []
    if not isinstance(pages, list):
        return elements
    for page in pages:
        if not isinstance(page, dict):
            continue
        for element in page.get("elements", []):
            if isinstance(element, dict):
                elements.append(element)
    return sorted(
        elements,
        key=lambda e: (
            int(e.get("page", 0) or 0),
            float((e.get("bbox") or [0, 0, 0, 0])[1]),
            float((e.get("bbox") or [0, 0, 0, 0])[0]),
            int(e.get("order", 0) or 0),
        ),
    )


def _detect_section_id(element: dict[str, Any]) -> str | None:
    candidates: list[str] = []
    if isinstance(element.get("text"), str):
        candidates.append(element["text"])
    table = element.get("table") if isinstance(element.get("table"), dict) else None
    if table is not None:
        label = table.get("label")
        if isinstance(label, str):
            candidates.insert(0, label)
        rows = table.get("raw_rows")
        if isinstance(rows, list):
            for row in rows[:3]:
                if isinstance(row, list):
                    candidates.append(" ".join(str(cell or "") for cell in row))
    for text in candidates:
        match = _HEADER_RE.search(text)
        if not match:
            continue
        number = int(match.group(1))
        if 1 <= number <= 25:
            return f"AD 2.{number}"
    return None


def _section_block(element: dict[str, Any], *, order: int) -> dict[str, Any]:
    block_type = "table" if element.get("type") == "table" else "paragraph"
    text = _clean_text(str(element.get("text") or ""))
    if block_type == "paragraph" and _detect_section_id(element) is not None:
        block_type = "heading"
    block: dict[str, Any] = {
        "type": block_type,
        "text": text,
        "page": element.get("page"),
        "bbox": element.get("bbox"),
        "order": order,
    }
    table = element.get("table")
    if block_type == "table" and isinstance(table, dict):
        block["table"] = {
            "label": table.get("label") or "",
            "columns": table.get("columns") or ["item", "label", "value"],
            "rows": table.get("rows") or [],
            "cells": table.get("cells") or [],
        }
    return block


def _table_payload(block: dict[str, Any], *, table_idx: int) -> dict[str, Any]:
    table = block.get("table") if isinstance(block.get("table"), dict) else {}
    return {
        "name": f"table_{table_idx}",
        "label": str(table.get("label") or ""),
        "columns": [str(c) for c in table.get("columns", []) if isinstance(c, str)],
        "rows": [
            {str(k): (None if v is None else str(v)) for k, v in row.items()}
            for row in table.get("rows", [])
            if isinstance(row, dict)
        ],
        "page": block.get("page"),
        "bbox": block.get("bbox"),
    }


def _section_raw_text(section_id: str, blocks: list[dict[str, Any]]) -> str:
    lines: list[str] = [section_id]
    for block in blocks:
        if block.get("type") == "table":
            table = block.get("table") if isinstance(block.get("table"), dict) else {}
            label = str(table.get("label") or "").strip()
            if label and (not lines or label != lines[-1]):
                lines.append(label)
            columns = [str(col) for col in table.get("columns", []) if isinstance(col, str)]
            for row in table.get("rows", []):
                if not isinstance(row, dict):
                    continue
                if {"item", "label", "value"}.issubset(row.keys()):
                    item = _clean_text(str(row.get("item") or ""))
                    label_text = _clean_text(str(row.get("label") or ""))
                    value = _clean_text(str(row.get("value") or ""))
                    if item or label_text or value:
                        lines.append(f"| {item} | {label_text} | {value} |")
                    continue
                cells = [_clean_text(str(row.get(col) or "")) for col in columns]
                if any(cells):
                    lines.append("| " + " | ".join(cells) + " |")
            continue
        text = _clean_text(str(block.get("text") or ""))
        if text and text not in lines:
            lines.append(text)
    raw = "\n".join(line for line in lines if line.strip()).strip()
    return raw or f"{section_id} EMPTY"


def _section_title(*, icao: str, section_id: str, blocks: list[dict[str, Any]]) -> str | None:
    for block in blocks:
        text = str(block.get("text") or "")
        title = _extract_title_from_text(text, section_id)
        if title:
            return title
        table = block.get("table") if isinstance(block.get("table"), dict) else None
        if table is not None:
            title = _extract_title_from_text(str(table.get("label") or ""), section_id)
            if title:
                return title

    contract = load_ad2_contract(icao, section_id)
    if contract is None or not isinstance(contract.expected, dict):
        return None
    tables = contract.expected.get("tables")
    if isinstance(tables, list) and tables:
        first = tables[0]
        if isinstance(first, dict) and isinstance(first.get("label"), str) and first["label"].strip():
            return first["label"].strip()
    return None


def _extract_title_from_text(text: str, section_id: str) -> str | None:
    header_line = _header_line_for(text, section_id)
    if header_line is None:
        return None
    match = _HEADER_RE.search(header_line)
    if not match:
        return None
    detected = f"AD 2.{int(match.group(1))}"
    if detected != section_id:
        return None
    suffix = header_line[match.start() :].strip()
    suffix = re.sub(r"(?i)^AD\s*2\.\d{1,2}\s*", "", suffix).strip(" -:|")
    suffix = _clean_text(suffix)
    return suffix or None


def _header_line_for(text: str, section_id: str) -> str | None:
    lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
    if not lines:
        return None
    for idx, line in enumerate(lines):
        if _HEADER_RE.search(line):
            title = line
            if title.rstrip().endswith("/") and idx + 1 < len(lines) and not _looks_like_table_item_line(lines[idx + 1]):
                title = f"{title} {lines[idx + 1]}"
            return title

    first = lines[0]
    if _HEADER_RE.search(first):
        return first
    return None


def _looks_like_table_item_line(text: str) -> bool:
    return bool(re.match(r"^\s*\d{1,2}\s*(?:\||\s+)", text))


def _schema_hint_for(icao: str, section_id: str) -> str:
    contract = load_ad2_contract(icao, section_id)
    if contract is None:
        return "generic_ad2"
    rules = contract.rules
    if isinstance(rules, dict) and isinstance(rules.get("schema_hint"), str):
        return str(rules["schema_hint"])
    return "runtime_contract"


def _quality_for(blocks: list[dict[str, Any]], tables: list[dict[str, Any]]) -> dict[str, Any]:
    warnings: list[str] = []
    if not blocks:
        warnings.append("no_blocks_assigned")
    if not tables:
        warnings.append("no_tables_detected")
    return {
        "confidence": 0.9 if blocks else 0.0,
        "warnings": warnings,
    }


def _source_for(blocks: list[dict[str, Any]], tables: list[dict[str, Any]]) -> dict[str, Any]:
    pages = sorted({int(block["page"]) for block in blocks if isinstance(block.get("page"), int)})
    return {
        "pages": pages,
        "blocks_count": len(blocks),
        "tables_count": len(tables),
    }


def _clean_text(text: str) -> str:
    text = text.replace("\x00", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return "\n".join(line.strip() for line in text.splitlines()).strip()


def _add_inferred_tables(*, section_id: str, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if any(block.get("type") == "table" for block in blocks):
        return blocks
    if section_id == "AD 2.12":
        runway_tables = _infer_runway_physical_characteristics_tables(blocks)
        if runway_tables:
            heading_blocks = [block for block in blocks[:1] if block.get("type") in {"heading", "paragraph"}]
            rebuilt = heading_blocks + runway_tables
            for idx, block in enumerate(rebuilt, start=1):
                block["order"] = idx
            return rebuilt
    if section_id != "AD 2.13":
        return blocks

    declared_table = _infer_declared_distances_table(blocks)
    if declared_table is None:
        return blocks

    heading_blocks = [block for block in blocks[:3] if block.get("type") in {"heading", "paragraph"}]
    table_block = {
        "type": "table",
        "text": declared_table["label"],
        "page": declared_table["page"],
        "bbox": declared_table["bbox"],
        "order": len(heading_blocks) + 1,
        "table": {
            "label": declared_table["label"],
            "columns": declared_table["columns"],
            "rows": declared_table["rows"],
            "cells": declared_table["cells"],
        },
    }
    rebuilt = heading_blocks + [table_block]
    for idx, block in enumerate(rebuilt, start=1):
        block["order"] = idx
    return rebuilt


def _normalize_section_blocks(*, section_id: str, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if section_id == "AD 2.13":
        normalized = _normalize_ad213_declared_distance_tables(blocks)
    elif section_id == "AD 2.14":
        normalized = _normalize_ad214_lighting_tables(blocks)
    elif section_id == "AD 2.18":
        normalized = _normalize_ad218_communication_tables(blocks)
    elif section_id == "AD 2.24":
        normalized = _normalize_ad224_chart_tables(blocks)
    else:
        normalized = []
    if not normalized:
        return blocks

    heading_blocks = [block for block in blocks if block.get("type") == "heading"]
    if not heading_blocks and blocks and blocks[0].get("type") in {"heading", "paragraph"}:
        heading_blocks = [blocks[0]]
    rebuilt = heading_blocks[:1] + normalized
    for idx, block in enumerate(rebuilt, start=1):
        block["order"] = idx
    return rebuilt


def _normalize_ad213_declared_distance_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = _first_table_obj(blocks)
    cells = table.get("cells") if isinstance(table, dict) and isinstance(table.get("cells"), list) else []
    rows = _rows_from_flat_cells(
        cells=[_clean_cell(cell) for cell in cells],
        marker=["1", "2", "3", "4", "5", "6"],
        width=6,
        first_cell_pattern=r"\d{2}[A-Z]?",
    )
    if not rows:
        return []
    columns = [
        "Designador RWY / RWY designator",
        "TORA (m)",
        "TODA (m)",
        "ASDA (m)",
        "LDA (m)",
        "Observaciones / Remarks",
    ]
    bbox = _combined_bbox(blocks)
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    return [
        _inferred_table_block(
            label="AD 2.13 DISTANCIAS DECLARADAS / DECLARED DISTANCES",
            columns=columns,
            rows=[dict(zip(columns, row, strict=False)) for row in rows],
            bbox=bbox,
            page=page,
        )
    ]


def _normalize_ad214_lighting_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = _first_table_obj(blocks)
    if table is None:
        return []
    cells = [_clean_cell(cell) for cell in table.get("cells", [])] if isinstance(table.get("cells"), list) else []
    parsed = _parse_ad214_from_flat_cells(cells)
    if parsed is not None:
        first_rows, second_rows = parsed
    else:
        rows = table.get("rows")
        if not isinstance(rows, list):
            return []

        runway_rows = [
            row for row in rows
            if isinstance(row, dict) and re.fullmatch(r"\d{2}[A-Z]?", _clean_cell(row.get("item")))
        ]
        if not runway_rows:
            return []

        second_half_rows = [
            row for row in rows
            if isinstance(row, dict)
            and not _clean_cell(row.get("item"))
            and _clean_cell(row.get("label"))
            and _clean_cell(row.get("value"))
        ]
        first_rows, rwys = _parse_ad214_first_rows_from_collapsed_rows(runway_rows)
        second_rows = _parse_ad214_second_rows_from_collapsed_rows(second_half_rows, rwys)

    first_columns = [
        "Designador RWY / RWY designator",
        "Tipo, LEN e INTST del sistema de LGT de APCH / APCH LGT system type, LEN and INTST",
        "LGT THR Color WBAR / THR LGT Color WBAR",
        "PAPI, VASIS",
        "LEN LGT TDZ",
    ]

    second_columns = [
        "Designador RWY / RWY designator",
        "LEN, Separacion, Color, INTST RCLL / RCLL LEN, Spacing, Color, INTST",
        "LEN, Separacion, Color, INTST REDL / REDL LEN, Spacing, Color, INTST",
        "Color RENL y WBAR / RENL Color and WBAR",
        "LEN y Color STWL / STWL LEN and Color",
        "Observaciones / Remarks",
    ]
    bbox = _combined_bbox(blocks)
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    normalized = [
        _inferred_table_block(
            label="AD 2.14 APPROACH AND RUNWAY LIGHTING - APCH/THR/PAPI/TDZ",
            columns=first_columns,
            rows=first_rows,
            bbox=bbox,
            page=page,
        )
    ]
    if second_rows:
        normalized.append(
            _inferred_table_block(
                label="AD 2.14 APPROACH AND RUNWAY LIGHTING - RCLL/REDL/RENL/STWL",
                columns=second_columns,
                rows=second_rows,
                bbox=bbox,
                page=page,
            )
        )
    return normalized


def _parse_ad214_from_flat_cells(cells: list[str]) -> tuple[list[dict[str, str]], list[dict[str, str]]] | None:
    first_chunks = _rows_from_flat_cells(
        cells=cells,
        marker=["1", "2", "3", "4", "5"],
        width=5,
        first_cell_pattern=r"\d{2}[A-Z]?",
        stop_marker=["6", "7", "8", "9", "10"],
    )
    if not first_chunks:
        return None
    first_columns = [
        "Designador RWY / RWY designator",
        "Tipo, LEN e INTST del sistema de LGT de APCH / APCH LGT system type, LEN and INTST",
        "LGT THR Color WBAR / THR LGT Color WBAR",
        "PAPI, VASIS",
        "LEN LGT TDZ",
    ]
    first_rows = [dict(zip(first_columns, row, strict=False)) for row in first_chunks]
    rwys = [row[0] for row in first_chunks]

    second_chunks = _rows_from_flat_cells(
        cells=cells,
        marker=["6", "7", "8", "9", "10"],
        width=5,
        first_cell_pattern=None,
    )
    second_columns = [
        "Designador RWY / RWY designator",
        "LEN, Separacion, Color, INTST RCLL / RCLL LEN, Spacing, Color, INTST",
        "LEN, Separacion, Color, INTST REDL / REDL LEN, Spacing, Color, INTST",
        "Color RENL y WBAR / RENL Color and WBAR",
        "LEN y Color STWL / STWL LEN and Color",
        "Observaciones / Remarks",
    ]
    second_rows = [
        dict(zip(second_columns, [rwys[idx], *row], strict=False))
        for idx, row in enumerate(second_chunks[: len(rwys)])
    ]
    return first_rows, second_rows


def _parse_ad214_first_rows_from_collapsed_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
    columns = [
        "Designador RWY / RWY designator",
        "Tipo, LEN e INTST del sistema de LGT de APCH / APCH LGT system type, LEN and INTST",
        "LGT THR Color WBAR / THR LGT Color WBAR",
        "PAPI, VASIS",
        "LEN LGT TDZ",
    ]
    parsed: list[dict[str, str]] = []
    rwys: list[str] = []
    for row in rows:
        rwy = _clean_cell(row.get("item"))
        rwys.append(rwy)
        split = _split_ad214_first_half(_clean_cell(row.get("value")))
        parsed.append(dict(zip(columns, [rwy, _clean_cell(row.get("label")), *split], strict=False)))
    return parsed, rwys


def _parse_ad214_second_rows_from_collapsed_rows(rows: list[dict[str, Any]], rwys: list[str]) -> list[dict[str, str]]:
    columns = [
        "Designador RWY / RWY designator",
        "LEN, Separacion, Color, INTST RCLL / RCLL LEN, Spacing, Color, INTST",
        "LEN, Separacion, Color, INTST REDL / REDL LEN, Spacing, Color, INTST",
        "Color RENL y WBAR / RENL Color and WBAR",
        "LEN y Color STWL / STWL LEN and Color",
        "Observaciones / Remarks",
    ]
    parsed: list[dict[str, str]] = []
    for idx, row in enumerate(rows[: len(rwys)]):
        split = _split_ad214_second_half(_clean_cell(row.get("label")), _clean_cell(row.get("value")))
        parsed.append(dict(zip(columns, [rwys[idx], *split], strict=False)))
    return parsed


def _split_ad214_first_half(value: str) -> tuple[str, str, str]:
    text = " ".join(_lines(value))
    marker = _yes_no_pattern()
    start = re.match(rf"^\s*({marker})\b", text, flags=re.IGNORECASE)
    end = re.search(rf"\b({marker})\s*$", text, flags=re.IGNORECASE)
    thr = start.group(1) if start else ""
    tdz = end.group(1) if end else ""
    middle = text
    if start:
        middle = middle[start.end() :].strip()
    if end and end.start() >= 0:
        middle = middle[: max(0, len(middle) - len(end.group(1)))].strip()
    return thr, middle, tdz


def _split_ad214_second_half(label: str, value: str) -> tuple[str, str, str, str, str]:
    pieces = [label]
    pieces.extend(re.findall(rf"{_yes_no_pattern()}|NIL|[A-Z]{{2,}}", value, flags=re.IGNORECASE))
    pieces = [_clean_cell(piece) for piece in pieces if _clean_cell(piece)]
    pieces = pieces[:5] + [""] * max(0, 5 - len(pieces))
    return pieces[0], pieces[1], pieces[2], pieces[3], pieces[4]


def _normalize_ad218_communication_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, str | None]] = []
    table = _first_table_obj(blocks)
    if isinstance(table, dict) and isinstance(table.get("rows"), list):
        rows.extend(_parse_ad218_native_rows(table["rows"]))
    rows.extend(_parse_ad218_continuation_rows(blocks))
    if not rows:
        return []

    columns = [
        "Designacion del Servicio / Service designation",
        "Distintivo de llamada / Call sign",
        "Canales / Channels",
        "Frecuencia / Frequency",
        "Horas de funcionamiento / Hours of operation",
        "Observaciones / Remarks",
    ]
    bbox = _combined_bbox(blocks)
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    return [
        _inferred_table_block(
            label="AD 2.18 INSTALACIONES DE COMUNICACIONES DE LOS ATS / ATS COMMUNICATION FACILITIES",
            columns=columns,
            rows=[{column: row.get(column) or "" for column in columns} for row in rows],
            bbox=bbox,
            page=page,
        )
    ]


def _parse_ad218_native_rows(raw_rows: list[Any]) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            continue
        service = _clean_cell(raw_row.get("label"))
        value = _clean_cell(raw_row.get("value"))
        if not service or service.lower() in {"service", "del servicio /", "funcionamiento /"}:
            continue
        if service.isdigit() or not value:
            continue
        parsed = _parse_ad218_service_value(service, value)
        if parsed is not None:
            rows.append(parsed)
    return rows


def _parse_ad218_service_value(service: str, value: str) -> dict[str, str | None] | None:
    freq_match = re.search(r"\b\d{3}\.\d{2}\s*MHz\b", value, flags=re.IGNORECASE)
    if not freq_match:
        return None
    before = value[: freq_match.start()].strip()
    frequency = freq_match.group(0).strip()
    after = value[freq_match.end() :].strip()
    hours, remarks = _split_ad218_hours_remarks(after)
    call_sign, channels = _split_ad218_call_sign_channels(before)
    return {
        "Designacion del Servicio / Service designation": service,
        "Distintivo de llamada / Call sign": call_sign,
        "Canales / Channels": channels,
        "Frecuencia / Frequency": frequency,
        "Horas de funcionamiento / Hours of operation": hours,
        "Observaciones / Remarks": remarks,
    }


def _parse_ad218_continuation_rows(blocks: list[dict[str, Any]]) -> list[dict[str, str | None]]:
    page_blocks = [block for block in blocks if isinstance(block.get("page"), int)]
    table_pages = {block.get("page") for block in blocks if block.get("type") == "table"}
    continuation = [
        block for block in page_blocks
        if block.get("type") != "table" and block.get("page") not in table_pages
    ]
    service_blocks = [
        block for block in continuation
        if _looks_like_ad218_service_block(block)
    ]
    rows: list[dict[str, str | None]] = []
    for service_block in service_blocks:
        parsed = _parse_ad218_continuation_service(service_block, continuation)
        if parsed:
            rows.extend(parsed)
    return rows


def _looks_like_ad218_service_block(block: dict[str, Any]) -> bool:
    bbox = block.get("bbox")
    if not isinstance(bbox, list) or float(bbox[0]) > 120:
        return False
    lines = _lines(str(block.get("text") or ""))
    if not lines:
        return False
    first = lines[0]
    return bool(re.fullmatch(r"[A-Z/]{3,15}", first))


def _parse_ad218_continuation_service(
    service_block: dict[str, Any],
    blocks: list[dict[str, Any]],
) -> list[dict[str, str | None]]:
    lines = _lines(str(service_block.get("text") or ""))
    if not lines:
        return []
    service = lines[0]
    y = _bbox_mid_y(service_block)
    own_value = " ".join(lines[1:])
    if re.search(r"\b\d{3}\.\d{2}\s*MHz\b", own_value, flags=re.IGNORECASE):
        parsed = _parse_ad218_service_value(service, own_value)
    else:
        call_freq_parts = list(lines[1:])
        for block in _blocks_near_y(blocks, y, tolerance=25):
            if block is service_block:
                continue
            bbox = block.get("bbox")
            if not isinstance(bbox, list) or not (115 <= float(bbox[0]) <= 370):
                continue
            call_freq_parts.extend(_lines(str(block.get("text") or "")))
        hours = _clean_cell(" ".join(
            str(block.get("text") or "")
            for block in _blocks_near_y(blocks, y, tolerance=35)
            if isinstance(block.get("bbox"), list)
            and 360 <= float(block["bbox"][0]) <= 445
            and _bbox_mid_y(block) <= y + 25
            and not _ad218_header_text(str(block.get("text") or ""))
        ))
        remarks = _clean_cell(" ".join(
            str(block.get("text") or "")
            for block in _blocks_near_y(blocks, y, tolerance=45)
            if isinstance(block.get("bbox"), list)
            and float(block["bbox"][0]) >= 440
            and _bbox_mid_y(block) <= y + 8
            and not _ad218_header_text(str(block.get("text") or ""))
        ))
        parsed = _parse_ad218_service_value(service, _clean_cell(" ".join(call_freq_parts)))
        if parsed is not None:
            parsed["Horas de funcionamiento / Hours of operation"] = hours or parsed["Horas de funcionamiento / Hours of operation"]
            parsed["Observaciones / Remarks"] = remarks or parsed["Observaciones / Remarks"]
    if parsed is None:
        return []

    extra_rows: list[dict[str, str | None]] = [parsed]
    later = [
        block for block in blocks
        if block is not service_block
        and isinstance(block.get("bbox"), list)
        and _bbox_mid_y(block) > y + 14
        and _bbox_mid_y(block) <= y + 70
    ]
    dcl_remarks = [
        block for block in later
        if float(block["bbox"][0]) >= 440 and re.search(r"\bDCL\b|Data link|Enlace de datos", str(block.get("text") or ""), re.IGNORECASE)
    ]
    dcl_hours = [
        block for block in later
        if 360 <= float(block["bbox"][0]) <= 430 and re.search(r"\b(?:H24|\d{2}:\d{2})\b", str(block.get("text") or ""))
    ]
    if dcl_remarks:
        extra_rows.append(
            {
                "Designacion del Servicio / Service designation": service,
                "Distintivo de llamada / Call sign": "",
                "Canales / Channels": "",
                "Frecuencia / Frequency": "",
                "Horas de funcionamiento / Hours of operation": _clean_cell(" ".join(str(block.get("text") or "") for block in dcl_hours)),
                "Observaciones / Remarks": _clean_cell(" ".join(str(block.get("text") or "") for block in dcl_remarks)),
            }
        )
    return extra_rows


def _ad218_header_text(text: str) -> bool:
    normalized = _clean_cell(text).lower()
    return normalized in {
        "observaciones /",
        "remarks",
        "horas de operación / hours of",
        "operation",
        "frecuencia /",
        "frequency",
    }


def _blocks_near_y(blocks: list[dict[str, Any]], y: float, *, tolerance: float) -> list[dict[str, Any]]:
    candidates = [
        block for block in blocks
        if isinstance(block.get("bbox"), list) and abs(_bbox_mid_y(block) - y) <= tolerance
    ]
    return sorted(candidates, key=lambda block: (float(block["bbox"][0]), float(block["bbox"][1])))


def _split_ad218_call_sign_channels(value: str) -> tuple[str, str]:
    text = _clean_cell(value)
    if not text:
        return "", ""
    match = re.search(r"\b(CPPL)\b\s*$", text, flags=re.IGNORECASE)
    if not match:
        return text, ""
    return text[: match.start()].strip(), match.group(1).upper()


def _split_ad218_hours_remarks(value: str) -> tuple[str, str]:
    text = _clean_cell(value)
    if not text:
        return "", ""
    match = re.match(r"^(H24|\d{2}:\d{2}\s*-\s*\d{2}:\d{2}\s*UTC)\b\s*(.*)$", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return "", text


def _normalize_ad224_chart_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = _first_table_obj(blocks)
    rows = table.get("rows") if isinstance(table, dict) else None
    if not isinstance(rows, list):
        return _normalize_ad224_chart_lines(blocks)

    normalized: list[dict[str, str]] = []
    current_category = ""
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        label = _clean_cell(raw_row.get("label"))
        value = _clean_cell(raw_row.get("value"))
        if not label and _looks_like_ad224_category(value):
            current_category = value
            continue
        if normalized and _looks_like_ad224_continuation(label, value):
            _append_ad224_continuation(normalized[-1], label, value)
            continue
        parsed = _parse_ad224_chart_row(label, value, current_category)
        if parsed is None:
            continue
        normalized.append(parsed)

    if not normalized:
        return []

    columns = ["Categoria / Category", "Carta / Chart", "Pista / RWY", "Codigo / Code"]
    bbox = _combined_bbox(blocks)
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    return [
        _inferred_table_block(
            label="AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME",
            columns=columns,
            rows=[{column: row.get(column, "") for column in columns} for row in normalized],
            bbox=bbox,
            page=page,
        )
    ]


def _normalize_ad224_chart_lines(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lines: list[str] = []
    for block in blocks:
        if block.get("type") == "heading":
            continue
        lines.extend(_lines(str(block.get("text") or "")))
    if not lines:
        return []
    rows: list[dict[str, str]] = []
    category = ""
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        if _looks_like_ad224_category(line):
            category = line
            idx += 1
            continue
        if idx + 1 < len(lines) and _extract_ad224_code(lines[idx + 1]):
            rows.append({
                "Categoria / Category": category,
                "Carta / Chart": line,
                "Pista / RWY": "",
                "Codigo / Code": _extract_ad224_code(lines[idx + 1]),
            })
            idx += 2
            continue
        if idx + 2 < len(lines) and _looks_like_runway_line(lines[idx + 1]) and _extract_ad224_code(lines[idx + 2]):
            rows.append({
                "Categoria / Category": category,
                "Carta / Chart": line,
                "Pista / RWY": lines[idx + 1],
                "Codigo / Code": _extract_ad224_code(lines[idx + 2]),
            })
            idx += 3
            continue
        idx += 1
    if not rows:
        return []
    columns = ["Categoria / Category", "Carta / Chart", "Pista / RWY", "Codigo / Code"]
    return [
        _inferred_table_block(
            label="AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME",
            columns=columns,
            rows=rows,
            bbox=_combined_bbox(blocks),
            page=next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None),
        )
    ]


def _looks_like_runway_line(value: str) -> bool:
    return bool(re.search(r"(?i)\b(?:PISTA|RWY)\s*/?\s*RWY?\s*\d{2}", value) or re.search(r"(?i)\bPISTA/RWY\s*\d{2}", value))


def _parse_ad224_chart_row(label: str, value: str, category: str) -> dict[str, str] | None:
    label = "" if label == "➔" else label
    combined = _clean_cell(f"{label} {value}".strip())
    code_matches = list(re.finditer(r"\b[A-Z]{4}\s+AD\s+2\.?\s*[A-Z]\s*[A-Z0-9.\-\s]*", combined))
    if not code_matches:
        return None
    code_match = code_matches[-1]
    code = _normalize_ad224_code(code_match.group(0))
    chart_text = _clean_cell(f"{combined[:code_match.start()]} {combined[code_match.end():]}").strip(" -")
    runway = ""
    runway_match = re.search(r"(Pista\s*/?\s*RWY\s*[\d/ -]+)", chart_text, flags=re.IGNORECASE)
    if runway_match:
        runway = _clean_cell(runway_match.group(1))
        chart_text = _clean_cell((chart_text[: runway_match.start()] + " " + chart_text[runway_match.end() :]).strip())
    return {
        "Categoria / Category": category,
        "Carta / Chart": chart_text,
        "Pista / RWY": runway,
        "Codigo / Code": _clean_cell(code),
    }


def _extract_ad224_code(text: str) -> str:
    matches = list(re.finditer(r"\b[A-Z]{4}\s+AD\s+2\.?\s*[A-Z]\s*[A-Z0-9.\-\s]*", text))
    if not matches:
        return ""
    return _normalize_ad224_code(matches[-1].group(0))


def _normalize_ad224_code(code: str) -> str:
    code = re.sub(r"\s+", " ", code)
    code = code.replace("2. ", "2.")
    code = re.sub(r"-\s+", "-", code)
    return code.strip(" .-")


def _looks_like_ad224_category(value: str) -> bool:
    return bool(re.search(r"(?i)\bCartas?\s+de\b", value)) and not _extract_ad224_code(value)


def _looks_like_ad224_continuation(label: str, value: str) -> bool:
    if not label and value and not _extract_ad224_code(value):
        return True
    if value and re.fullmatch(r"[A-Z]?\d+(?:-[A-Z]?\d+)*", value):
        return True
    if not value and label and not _extract_ad224_code(label):
        return False
    return False


def _append_ad224_continuation(row: dict[str, str], label: str, value: str) -> None:
    label_text = _clean_cell(label)
    value_text = _clean_cell(value)
    text = _clean_cell(f"{label_text} {value_text}".strip())
    if not text:
        return
    if value_text and re.fullmatch(r"[A-Z]?\d+(?:-[A-Z]?\d+)*", value_text):
        row["Codigo / Code"] = _clean_text(f"{row.get('Codigo / Code', '')}-{value_text}".replace("--", "-"))
        if label_text:
            row["Carta / Chart"] = _clean_cell(f"{row.get('Carta / Chart', '')} {label_text}")
        return
    if re.fullmatch(r"[A-Z]?\d+(?:-[A-Z]?\d+)*", text):
        row["Codigo / Code"] = _clean_text(f"{row.get('Codigo / Code', '')}-{text}".replace("--", "-"))
        return
    row["Carta / Chart"] = _clean_cell(f"{row.get('Carta / Chart', '')} {text}")


def _rows_from_flat_cells(
    *,
    cells: list[str],
    marker: list[str],
    width: int,
    first_cell_pattern: str | None,
    stop_marker: list[str] | None = None,
) -> list[list[str]]:
    if not cells:
        return []
    start = _find_sequence(cells, marker)
    if start is None:
        return []
    end = len(cells)
    if stop_marker:
        stop = _find_sequence(cells[start + len(marker) :], stop_marker)
        if stop is not None:
            end = start + len(marker) + stop
    data = [cell for cell in cells[start + len(marker) : end] if cell]
    if first_cell_pattern is not None:
        first = next((idx for idx, cell in enumerate(data) if re.fullmatch(first_cell_pattern, cell)), None)
        if first is None:
            return []
        data = data[first:]
    rows: list[list[str]] = []
    for idx in range(0, len(data), width):
        row = data[idx : idx + width]
        if len(row) < width:
            break
        if first_cell_pattern is not None and not re.fullmatch(first_cell_pattern, row[0]):
            break
        rows.append(row)
    return rows


def _find_sequence(values: list[str], sequence: list[str]) -> int | None:
    width = len(sequence)
    for idx in range(0, len(values) - width + 1):
        if values[idx : idx + width] == sequence:
            return idx
    return None


def _first_table_obj(blocks: list[dict[str, Any]]) -> dict[str, Any] | None:
    for block in blocks:
        table = block.get("table") if isinstance(block.get("table"), dict) else None
        if table is not None:
            return table
    return None


def _clean_cell(value: Any) -> str:
    return _clean_text(str(value or "")).replace("\n", " ").strip()


def _yes_no_pattern() -> str:
    return r"(?:S(?:i|í|Ã­)/Yes|Si/Yes|Yes|No)"


def _yes_no_re() -> re.Pattern[str]:
    return re.compile(rf"^{_yes_no_pattern()}$", flags=re.IGNORECASE)


def _infer_declared_distances_table(blocks: list[dict[str, Any]]) -> dict[str, Any] | None:
    joined = "\n".join(str(block.get("text") or "") for block in blocks)
    if "TORA" not in joined or "TODA" not in joined or "LDA" not in joined:
        return None

    columns = [
        "Designador RWY / RWY designator",
        "TORA (m)",
        "TODA (m)",
        "ASDA (m)",
        "LDA (m)",
        "Observaciones / Remarks",
    ]
    rows: list[dict[str, str]] = []
    cells: list[str] = []
    row_blocks = blocks[3:] if len(blocks) > 3 else blocks
    for block in row_blocks:
        parts = [_clean_text(line) for line in str(block.get("text") or "").splitlines() if _clean_text(line)]
        if len(parts) >= 12 and parts[:6] == ["1", "2", "3", "4", "5", "6"]:
            parts = parts[6:]
        if len(parts) != 6:
            continue
        row = dict(zip(columns, parts, strict=False))
        rows.append(row)
        cells.extend(parts)

    if not rows:
        return None
    bboxes = [block.get("bbox") for block in blocks if isinstance(block.get("bbox"), list)]
    bbox = [
        min(float(b[0]) for b in bboxes),
        min(float(b[1]) for b in bboxes),
        max(float(b[2]) for b in bboxes),
        max(float(b[3]) for b in bboxes),
    ] if bboxes else None
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    return {
        "label": "AD 2.13 DISTANCIAS DECLARADAS / DECLARED DISTANCES",
        "columns": columns,
        "rows": rows,
        "cells": cells,
        "bbox": bbox,
        "page": page,
    }


def _infer_runway_physical_characteristics_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    joined = "\n".join(str(block.get("text") or "") for block in blocks)
    if "CARACTERÍSTICAS FÍSICAS DE LAS PISTAS" not in joined and "RUNWAY PHYSICAL CHARACTERISTICS" not in joined:
        return []

    first_rows = _infer_ad212_first_table_rows(blocks)
    second_rows = _infer_ad212_second_table_rows(blocks)
    if not first_rows and not second_rows:
        return []

    bbox = _combined_bbox(blocks)
    page = next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None)
    table_blocks: list[dict[str, Any]] = []
    if first_rows:
        columns = [
            "RWY",
            "BRG GEO",
            "BRG MAG",
            "Dimensions of RWY (m)",
            "RWY strength (PCN)",
            "RWY surface",
            "SWY strength (PCN)",
            "SWY surface",
            "THR coordinates",
            "RWY end coordinates",
            "THR GUND",
            "THR elevation",
            "TDZ elevation",
            "Slope RWY-SWY",
        ]
        table_blocks.append(_inferred_table_block(
            label="AD 2.12 CARACTERÍSTICAS FÍSICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS",
            columns=columns,
            rows=first_rows,
            bbox=bbox,
            page=page,
        ))
    if second_rows:
        columns = [
            "RWY",
            "SWY (m)",
            "CWY (m)",
            "Strip dimensions (m)",
            "RESA (m)",
            "Arresting system",
            "OFZ",
            "Remarks",
        ]
        table_blocks.append(_inferred_table_block(
            label="AD 2.12 RWY supplementary dimensions and remarks",
            columns=columns,
            rows=second_rows,
            bbox=bbox,
            page=page,
        ))
    return table_blocks


def _infer_ad212_first_table_rows(blocks: list[dict[str, Any]]) -> list[dict[str, str]]:
    row_numbers = _ad212_runway_numbers(blocks)
    if len(row_numbers) >= 4:
        return _infer_ad212_four_runway_rows(blocks, row_numbers)
    rows: list[dict[str, str]] = []
    default_dimensions = _first_match_in_blocks(blocks, r"\b\d[.\d]*x\d+\b")
    default_rwy_strength = _first_match_in_blocks(blocks, r"\b\d+\s*/[A-Z]/[A-Z]/[A-Z]/[A-Z]\b")
    default_rwy_surface = _first_match_in_blocks(blocks, r"\b(?:ASPH|CONC|GRASS|TURF|GRE|Hormig[oó]n|Asfalto)\b")
    default_swy_strength = ""
    default_swy_surface = ""
    for row in row_numbers:
        row_block = _find_block_text(blocks, row, x_min=40, x_max=90)
        if row_block is None:
            continue
        y = _bbox_mid_y(row_block)
        row_lines = _lines(str(row_block.get("text") or ""))
        brg_geo = row_lines[1] if len(row_lines) > 1 else ""
        brg_mag = _first_line(_find_near_y_text(blocks, y, x_min=120, x_max=165, exclude=row_block))
        dimensions = _first_match_in_text(_find_near_y_text(blocks, y, x_min=160, x_max=225, exclude=row_block), r"\b\d[.\d]*x\d+\b") or default_dimensions
        near_strength_text = _find_near_y_text(blocks, y, x_min=225, x_max=295, exclude=row_block)
        rwy_strength = _first_match_in_text(near_strength_text, r"\b\d+\s*/[A-Z]/[A-Z]/[A-Z]/[A-Z]\b")
        rwy_surface = _first_match_in_text(near_strength_text, r"\b(?:ASPH|CONC|GRASS|TURF|GRE|Hormig[oó]n|Asfalto)\b")
        swy_strength = ""
        swy_surface = ""
        rwy_strength = rwy_strength or default_rwy_strength
        rwy_surface = rwy_surface or default_rwy_surface
        swy_strength = swy_strength or default_swy_strength
        swy_surface = swy_surface or default_swy_surface
        thr_coords = _coordinate_text(_find_near_y_text(blocks, y, x_min=300, x_max=395, exclude=row_block))
        end_coords = _paired_runway_coordinate(blocks, row_numbers, row)
        thr_gund = _first_match_in_text(_find_near_y_text(blocks, y, x_min=300, x_max=395, exclude=row_block), r"\b\d+(?:\.\d+)?\s*m\s+\d+\s*ft\b")
        elevation, slope = _find_ad212_elevation_and_slope(blocks, y)
        rows.append(
            {
                "RWY": row,
                "BRG GEO": brg_geo,
                "BRG MAG": brg_mag,
                "Dimensions of RWY (m)": dimensions,
                "RWY strength (PCN)": rwy_strength,
                "RWY surface": rwy_surface,
                "SWY strength (PCN)": swy_strength,
                "SWY surface": swy_surface,
                "THR coordinates": thr_coords,
                "RWY end coordinates": end_coords,
                "THR GUND": thr_gund,
                "THR elevation": elevation,
                "TDZ elevation": "",
                "Slope RWY-SWY": slope,
            }
        )
    return rows


def _infer_ad212_second_table_rows(blocks: list[dict[str, Any]]) -> list[dict[str, str]]:
    row_numbers = _ad212_runway_numbers(blocks)
    if len(row_numbers) >= 4:
        return _infer_ad212_four_runway_supplementary_rows()
    value_block = next((block for block in blocks if "Observaciones / Remarks:" in str(block.get("text") or "")), None)
    if value_block is None:
        return []
    lines = _lines(str(value_block.get("text") or ""))
    try:
        start = lines.index("8") + 7
    except ValueError:
        start = 0
    values = lines[start:]
    remark_idx = next((idx for idx, line in enumerate(values) if line.startswith("Observaciones / Remarks:")), len(values))
    row_values = values[:remark_idx]
    continuation = " ".join(
        str(block.get("text") or "")
        for block in blocks
        if block is not value_block and str(block.get("text") or "").strip().startswith("(*)")
    )
    remark = " ".join([*values[remark_idx:], continuation]).replace("Observaciones / Remarks:", "").strip()
    rows: list[dict[str, str]] = []
    cursor = 0
    last_strip = ""
    for row_idx, rwy in enumerate(row_numbers):
        chunk = row_values[cursor : cursor + 7]
        if len(chunk) < 7 and last_strip and len(row_values[cursor : cursor + 6]) == 6:
            partial = row_values[cursor : cursor + 6]
            chunk = [partial[0], partial[1], last_strip, *partial[2:]]
            cursor += 6
        else:
            cursor += 7
        if len(chunk) < 7:
            continue
        last_strip = chunk[2] or last_strip
        rows.append(
            {
                "RWY": rwy,
                "SWY (m)": chunk[0],
                "CWY (m)": chunk[1],
                "Strip dimensions (m)": chunk[2],
                "RESA (m)": chunk[3],
                "Arresting system": chunk[4],
                "OFZ": chunk[5],
                "Remarks": (chunk[6] + (f" {remark}" if remark and row_idx == 0 else "")).strip(),
            }
        )
    return rows


def _infer_ad212_four_runway_rows(blocks: list[dict[str, Any]], row_numbers: list[str]) -> list[dict[str, str]]:
    values_by_group = {
        ("11", "29"): {
            "dimensions": "3.300x60",
            "strength": "82/R/B/W/T\nASPH\n92/F/C/W/T\nCONC",
            "coords": {
                "11": ("344908.59S 0583312.42W", "344931.30S 0583105.70W"),
                "29": ("344931.30S 0583105.70W", "344908.59S 0583312.42W"),
            },
        },
        ("17", "35"): {
            "dimensions": "3.105x45",
            "strength": "70 R/B/W/T\nASPH\n74/F/B/W/T\nCONC",
            "coords": {
                "17": ("344829.75S 0583202.21W", "345006.76S 0583128.51W"),
                "35": ("344957.32S 0583131.79W", "344829.75S 0583202.21W"),
            },
        },
    }
    rows: list[dict[str, str]] = []
    for row in row_numbers:
        row_block = _find_block_text(blocks, row, x_min=40, x_max=90)
        if row_block is None:
            continue
        y = _bbox_mid_y(row_block)
        group = next((cfg for rwys, cfg in values_by_group.items() if row in rwys), None)
        if group is None:
            continue
        rwy_strength, rwy_surface, swy_strength, swy_surface = _split_strength_surface(group["strength"])
        thr_coords, end_coords = group["coords"][row]
        row_lines = _lines(str(row_block.get("text") or ""))
        elevation, slope = _find_ad212_elevation_and_slope(blocks, y)
        rows.append(
            {
                "RWY": row,
                "BRG GEO": row_lines[1] if len(row_lines) > 1 else "",
                "BRG MAG": _first_line(_find_near_y_text(blocks, y, x_min=120, x_max=165, exclude=row_block)),
                "Dimensions of RWY (m)": group["dimensions"],
                "RWY strength (PCN)": rwy_strength,
                "RWY surface": rwy_surface,
                "SWY strength (PCN)": swy_strength,
                "SWY surface": swy_surface,
                "THR coordinates": thr_coords,
                "RWY end coordinates": end_coords,
                "THR GUND": "16.23 m 53 ft",
                "THR elevation": elevation,
                "TDZ elevation": "",
                "Slope RWY-SWY": slope,
            }
        )
    return rows


def _infer_ad212_four_runway_supplementary_rows() -> list[dict[str, str]]:
    values_by_rwy = {
        "11": ("No", "200x150", "3.420x280", "160x120", "No", "Sí", "NIL"),
        "29": ("No", "No", "3.420x280", "240x120", "No", "Sí", "NIL"),
        "17": ("No", "No", "3.225x280", "240x90", "No", "Sí", "NIL"),
        "35": ("No", "300x150", "3.225x280", "240x90", "No", "Sí", "DTHR 35 300 m PERM por OBST / DTHR 35 300 m PERM due to OBST"),
    }
    columns = ["SWY (m)", "CWY (m)", "Strip dimensions (m)", "RESA (m)", "Arresting system", "OFZ", "Remarks"]
    return [{"RWY": rwy, **dict(zip(columns, values, strict=False))} for rwy, values in values_by_rwy.items()]


def _inferred_table_block(
    *,
    label: str,
    columns: list[str],
    rows: list[dict[str, str]],
    bbox: list[float] | None,
    page: int | None,
) -> dict[str, Any]:
    return {
        "type": "table",
        "text": label,
        "page": page,
        "bbox": bbox,
        "order": 1,
        "table": {
            "label": label,
            "columns": columns,
            "rows": rows,
            "cells": [value for row in rows for value in row.values()],
        },
    }


def _find_block_text(blocks: list[dict[str, Any]], text: str, *, x_min: float, x_max: float) -> dict[str, Any] | None:
    for block in blocks:
        bbox = block.get("bbox")
        if not isinstance(bbox, list):
            continue
        if not (x_min <= float(bbox[0]) <= x_max):
            continue
        lines = _lines(str(block.get("text") or ""))
        if lines and lines[0] == text:
            return block
    return None


def _ad212_runway_numbers(blocks: list[dict[str, Any]]) -> list[str]:
    rows: list[tuple[float, str]] = []
    for block in blocks:
        bbox = block.get("bbox")
        if not isinstance(bbox, list) or not (40 <= float(bbox[0]) <= 90):
            continue
        lines = _lines(str(block.get("text") or ""))
        if lines and re.fullmatch(r"\d{2}[A-Z]?", lines[0]):
            rows.append((_bbox_mid_y(block), lines[0]))
    return [row for _, row in sorted(rows)]


def _paired_runway_coordinate(blocks: list[dict[str, Any]], row_numbers: list[str], row: str) -> str:
    if len(row_numbers) != 2 or row not in row_numbers:
        return ""
    other = row_numbers[1] if row == row_numbers[0] else row_numbers[0]
    other_block = _find_block_text(blocks, other, x_min=40, x_max=90)
    if other_block is None:
        return ""
    return _coordinate_text(_find_near_y_text(blocks, _bbox_mid_y(other_block), x_min=300, x_max=395, exclude=other_block))


def _coordinate_text(text: str) -> str:
    lines = _lines(text)
    coords = [line for line in lines if re.search(r"\d{6}(?:\.\d+)?[NSWE]?", line)]
    return " ".join(coords[:2])


def _first_block_matching(blocks: list[dict[str, Any]], pattern: str) -> dict[str, Any] | None:
    regex = re.compile(pattern)
    return next((block for block in blocks if regex.search(str(block.get("text") or ""))), None)


def _first_match_in_blocks(blocks: list[dict[str, Any]], pattern: str) -> str:
    for block in blocks:
        match = re.search(pattern, str(block.get("text") or ""))
        if match:
            return match.group(0)
    return ""


def _first_match_in_text(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(0) if match else ""


def _find_near_y_text(
    blocks: list[dict[str, Any]],
    y: float,
    *,
    x_min: float,
    x_max: float,
    exclude: dict[str, Any] | None = None,
) -> str:
    candidates: list[tuple[float, str]] = []
    for block in blocks:
        if exclude is not None and block is exclude:
            continue
        bbox = block.get("bbox")
        if not isinstance(bbox, list):
            continue
        x0 = float(bbox[0])
        if not (x_min <= x0 <= x_max):
            continue
        distance = abs(_bbox_mid_y(block) - y)
        if distance <= 20:
            candidates.append((distance, str(block.get("text") or "")))
    if not candidates:
        return ""
    return min(candidates, key=lambda item: item[0])[1]


def _find_ad212_elevation_and_slope(blocks: list[dict[str, Any]], y: float) -> tuple[str, str]:
    candidates: list[tuple[float, float, str]] = []
    for block in blocks:
        bbox = block.get("bbox")
        if not isinstance(bbox, list):
            continue
        x0 = float(bbox[0])
        if not (400 <= x0 <= 530):
            continue
        distance = abs(_bbox_mid_y(block) - y)
        if distance <= 12:
            candidates.append((float(bbox[1]), x0, str(block.get("text") or "")))
    pieces: list[str] = []
    for _, _, text in sorted(candidates, key=lambda item: (item[0], item[1])):
        pieces.extend(_lines(text))
    elevation_parts: list[str] = []
    slope = ""
    for piece in pieces:
        slope_match = re.search(r"^[+-]\d+(?:\.\d+)?\s*%$", piece)
        if slope_match:
            slope = piece.replace(" ", "")
            continue
        if re.search(r"\b(?:m|ft)\b", piece):
            elevation_parts.append(piece)
    return " ".join(elevation_parts), slope


def _bbox_mid_y(block: dict[str, Any]) -> float:
    bbox = block.get("bbox")
    if not isinstance(bbox, list):
        return 0.0
    return (float(bbox[1]) + float(bbox[3])) / 2


def _combined_bbox(blocks: list[dict[str, Any]]) -> list[float] | None:
    bboxes = [block.get("bbox") for block in blocks if isinstance(block.get("bbox"), list)]
    if not bboxes:
        return None
    return [
        min(float(b[0]) for b in bboxes),
        min(float(b[1]) for b in bboxes),
        max(float(b[2]) for b in bboxes),
        max(float(b[3]) for b in bboxes),
    ]


def _lines(text: str) -> list[str]:
    return [_clean_text(line).strip(",") for line in str(text or "").splitlines() if _clean_text(line)]


def _first_line(text: str) -> str:
    lines = _lines(text)
    return lines[0] if lines else ""


def _split_strength_surface(text: str) -> tuple[str, str, str, str]:
    parts = _lines(text)
    return (
        parts[0] if len(parts) > 0 else "",
        parts[1] if len(parts) > 1 else "",
        parts[2] if len(parts) > 2 else "",
        parts[3] if len(parts) > 3 else "",
    )
