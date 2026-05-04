"""General source model for AD 2.x section preprocessing."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


@dataclass(frozen=True)
class SourceRow:
    kind: str
    raw: str
    cells: list[str]


@dataclass(frozen=True)
class SectionSource:
    section_id: str
    rows: list[SourceRow]


def build_section_source(section_id: str, raw_text: str) -> SectionSource:
    rows: list[SourceRow] = []
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("|") and line.endswith("|"):
            cells = [cell.strip() for cell in line.strip("|").split("|") if cell.strip()]
            rows.append(SourceRow(kind="table", raw=line, cells=cells))
            continue
        if line.startswith("-") or line.startswith("*"):
            rows.append(SourceRow(kind="bullet", raw=line, cells=[]))
            continue
        rows.append(SourceRow(kind="text", raw=line, cells=[]))
    return SectionSource(section_id=section_id, rows=rows)


def merge_continuation_rows(rows: list[SourceRow]) -> list[SourceRow]:
    merged: list[SourceRow] = []
    i = 0
    while i < len(rows):
        current = rows[i]
        if (
            current.kind == "table"
            and i + 1 < len(rows)
            and rows[i + 1].kind == "text"
            and rows[i + 1].raw
        ):
            nxt = rows[i + 1]
            cells = current.cells[:]
            if cells:
                cells[-1] = f"{cells[-1]} {nxt.raw}".strip()
            merged.append(SourceRow(kind="table", raw=f"{current.raw} {nxt.raw}", cells=cells))
            i += 2
            continue
        merged.append(current)
        i += 1
    return merged


def normalize_bilingual_pairs(value: str) -> str:
    value = re.sub(r"(?<=\S)\s*/\s+(?=\S)", " / ", value)
    value = re.sub(r"(?<=\S)\s+/\s*(?=\S)", " / ", value)
    return " ".join(value.split())


def normalize_bullet_text(value: str) -> str:
    return re.sub(r"^[\-*]\s*", "", value).strip()


def build_section_blocks(raw_text: str) -> list[dict[str, Any]]:
    rows = merge_continuation_rows(build_section_source("", raw_text).rows)
    blocks: list[dict[str, Any]] = []
    order = 1
    for row in rows:
        raw = row.raw.strip()
        if not raw:
            continue
        block_type = "paragraph"
        text = raw
        table_payload = None
        if row.kind == "table":
            block_type = "table"
            text = " | ".join(row.cells) if row.cells else raw
            cols = [f"col_{i}" for i in range(1, len(row.cells) + 1)]
            table_payload = {
                "columns": cols,
                "rows": [dict(zip(cols, row.cells, strict=False))],
                "cells": row.cells,
            }
        elif row.kind == "bullet":
            block_type = "list"
            text = normalize_bullet_text(raw)
        elif re.match(r"^\s*2\.\d{1,2}(?:\.\d+)?\s+", raw):
            block_type = "heading"

        block: dict[str, Any] = {
            "type": block_type,
            "text": text,
            "page": None,
            "bbox": None,
            "order": order,
        }
        if table_payload is not None:
            block["table"] = table_payload
        blocks.append(block)
        order += 1
    return blocks


def extract_table_groups_from_blocks(section_blocks: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not isinstance(section_blocks, list):
        return []

    # First, consume native table payloads that already provide columns+rows.
    direct_tables: list[dict[str, Any]] = []
    for idx, block in enumerate(section_blocks, start=1):
        if not isinstance(block, dict) or block.get("type") != "table":
            continue
        table_obj = block.get("table") if isinstance(block.get("table"), dict) else {}
        cols = table_obj.get("columns") if isinstance(table_obj.get("columns"), list) else []
        rows = table_obj.get("rows") if isinstance(table_obj.get("rows"), list) else []
        if cols and rows and all(isinstance(c, str) for c in cols):
            label = table_obj.get("label") if isinstance(table_obj.get("label"), str) else ""
            safe_rows: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    safe_rows.append({str(k): (None if v is None else str(v)) for k, v in row.items()})
            if safe_rows:
                direct_tables.append(
                    {
                        "name": f"table_{len(direct_tables)+1}",
                        "label": label,
                        "columns": [str(c) for c in cols],
                        "rows": safe_rows,
                    }
                )

    if direct_tables:
        return direct_tables

    grouped: list[list[list[str]]] = []
    current: list[list[str]] = []
    for block in section_blocks:
        if not isinstance(block, dict) or block.get("type") != "table":
            if current:
                grouped.append(current)
                current = []
            continue
        table_obj = block.get("table") if isinstance(block.get("table"), dict) else {}
        cells = table_obj.get("cells") if isinstance(table_obj.get("cells"), list) else []
        row = [str(c).strip() for c in cells if str(c).strip()]
        if row:
            current.append(row)
    if current:
        grouped.append(current)

    tables: list[dict[str, Any]] = []
    for idx, rows in enumerate(grouped, start=1):
        if len(rows) < 2:
            continue
        columns = rows[0]
        width = len(columns)
        if width < 2:
            continue
        out_rows: list[dict[str, Any]] = []
        for r in rows[1:]:
            norm = r[:width] + [""] * max(0, width - len(r))
            out_rows.append(dict(zip(columns, norm, strict=False)))
        tables.append({"name": f"table_{idx}", "label": "", "columns": columns, "rows": out_rows})
    return tables
