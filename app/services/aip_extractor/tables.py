"""Extract structured tables from AIP PDF files using pdfplumber.

Tables in AIP documents map directly to AD sections (AD 2.12, AD 2.13, etc.).
Using extract_tables() instead of extract_text() preserves column boundaries,
which prevents the LLM from confusing adjacent columns in wide tables.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pdfplumber

# Matches "AD 2.12" or "AD 2.2" anywhere inside a cell string
_SECTION_RE = re.compile(r"\bAD\s+2\.(\d+)\b", re.IGNORECASE)

# Sections where table extraction is preferred over plain text.
# All others fall back to the text-based splitter.
TABLE_PREFERRED_SECTIONS: frozenset[str] = frozenset(
    {
        "AD 2.2",
        "AD 2.3",
        "AD 2.4",
        "AD 2.12",
        "AD 2.13",
        "AD 2.14",
        "AD 2.18",
        "AD 2.19",
    }
)

Row = list[Any]
Table = list[Row]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _detect_section_id(table: Table) -> str | None:
    """Return 'AD 2.X' if the first row contains a recognisable section title."""
    if not table:
        return None
    for cell in table[0]:
        if cell:
            m = _SECTION_RE.search(str(cell))
            if m:
                return f"AD 2.{m.group(1)}"
    return None


def _compact_table(rows: Table) -> Table:
    """Remove columns where every cell is None or empty string."""
    if not rows:
        return rows

    n_cols = max(len(row) for row in rows)

    # Pad all rows to the same width
    padded: Table = [list(row) + [None] * (n_cols - len(row)) for row in rows]

    keep: list[int] = [
        col
        for col in range(n_cols)
        if any(padded[r][col] not in (None, "") for r in range(len(padded)))
    ]

    return [[row[i] for i in keep] for row in padded]


def _is_column_number_row(row: Row) -> bool:
    """Return True when a row contains only single-digit column index labels.

    AIP tables routinely include a row like ``['', '1', '', '2', '', '3', ...]``
    to number the columns.  Forward-filling these numbers into subsequent data
    rows would pollute the data, so we detect and exclude such rows from
    providing seed values for the fill.
    """
    non_empty = [str(c).strip() for c in row if c not in (None, "")]
    if not non_empty:
        return False
    return all(v.isdigit() and len(v) <= 2 for v in non_empty)


def _forward_fill_columns(rows: Table) -> Table:
    """Propagate non-None values downward in each column.

    pdfplumber represents vertically merged (rowspan) cells as ``None`` in
    every row below the first of the merged group.  Forward-filling restores
    the implied repeated value so the LLM receives complete information.

    Rules:
    - Only ``None`` is filled; empty strings are treated as intentional blanks.
    - "Column number rows" (rows whose only content is digit labels such as
      ``1, 2, 3``) reset the fill buffer so their values never seed data rows.
    """
    if not rows:
        return rows

    n_cols = max(len(row) for row in rows)
    filled: Table = [list(row) + [None] * (n_cols - len(row)) for row in rows]

    # Per-column buffer: None means "nothing to propagate yet"
    last_vals: list[Any] = [None] * n_cols

    for row_idx, row in enumerate(filled):
        if _is_column_number_row(row):
            # Reset the buffer — column labels must not bleed into data rows
            last_vals = [None] * n_cols
            continue

        for col in range(n_cols):
            cell = row[col]
            if cell is not None:
                last_vals[col] = cell
            elif last_vals[col] is not None:
                filled[row_idx][col] = last_vals[col]

    return filled


def _clean_cell(value: Any) -> str:
    """Normalise a cell value to a plain string safe for Markdown."""
    if value is None:
        return ""
    # Replace in-cell newlines with a space so they don't break table rows
    return str(value).replace("\n", " ").strip()


def _table_to_markdown(rows: Table) -> str:
    """
    Convert a list-of-rows to a GitHub-Flavoured Markdown table.

    The first row is used as the header row.
    """
    if not rows:
        return ""

    cleaned: list[list[str]] = [[_clean_cell(c) for c in row] for row in rows]
    n_cols = max(len(row) for row in cleaned)

    # Pad rows to uniform width
    cleaned = [row + [""] * (n_cols - len(row)) for row in cleaned]

    lines: list[str] = []
    for i, row in enumerate(cleaned):
        lines.append("| " + " | ".join(row) + " |")
        if i == 0:
            lines.append("|" + "|".join(["---"] * n_cols) + "|")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_sections_as_tables(pdf_path: str | Path) -> dict[str, str]:
    """
    Scan every page of *pdf_path*, detect tables that belong to an AD 2.X
    section, compact empty columns, and return a dict mapping section_id
    (e.g. ``"AD 2.12"``) to a Markdown-table string.

    Tables that span multiple pages (e.g. AD 2.18 pages 8-9) are merged
    automatically: both tables carry the same section title, so their rows
    are concatenated.

    Only section IDs present in :data:`TABLE_PREFERRED_SECTIONS` are returned.
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {path}")

    # Accumulate raw rows per section (including the title row of each table
    # so the LLM gets the full context on the first encounter, and skips
    # duplicate title rows on continuations).
    accumulated: dict[str, Table] = {}

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                section_id = _detect_section_id(table)
                if section_id is None or section_id not in TABLE_PREFERRED_SECTIONS:
                    continue

                if section_id not in accumulated:
                    # First encounter: keep the title row for context
                    accumulated[section_id] = list(table)
                else:
                    # Continuation: skip the title row to avoid repetition,
                    # but keep the column-header rows that follow (they help
                    # the LLM re-orient in the new page).
                    accumulated[section_id].extend(table[1:])

    result: dict[str, str] = {}
    for section_id, rows in accumulated.items():
        compacted = _compact_table(rows)
        filled = _forward_fill_columns(compacted)
        md = _table_to_markdown(filled)
        if md:
            result[section_id] = md

    return result
