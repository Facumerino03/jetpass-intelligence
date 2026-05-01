"""General source model for AD 2.x section preprocessing."""

from __future__ import annotations

from dataclasses import dataclass
import re


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
