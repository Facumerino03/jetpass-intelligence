"""Deterministic canonicalization for AD 2.x raw text blocks."""

from __future__ import annotations

import re


_SECTION_HEADER_RE = re.compile(
    r"^\s*(?:\|\s*)?(?:#{1,6}\s*)?(?:[A-Z]{4}\s+)?AD\s*2\.(\d{1,2})\s*(.*)$",
    re.IGNORECASE,
)
_ITEM_RE = re.compile(r"^\s*(\d{1,2})\s+(.+?)\s*$")
_CHART_REF_RE = re.compile(r"^(.*?)\s+(SAMR\s+AD\s+2\.[A-Z0-9.-]+)\s*$", re.IGNORECASE)
_SECTION_TITLE_FALLBACKS: dict[str, str] = {
    "AD 2.11": "INFORMACION METEOROLOGICA PROPORCIONADA / METEOROLOGICAL INFORMATION PROVIDED",
}


def canonicalize_section_raw_text(section_id: str, raw_text: str) -> str:
    """Normalize a section to canonical SECTION/ITEM/VALUE/NOTE blocks."""
    normalized = _normalize_noise(raw_text)
    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    if not lines:
        return f"SECTION: {section_id}"

    title = _extract_title(lines, section_id)
    content_lines = _drop_redundant_headers(lines, section_id)

    output: list[str] = [f"SECTION: {section_id} | {title}" if title else f"SECTION: {section_id}"]

    pending_item: tuple[str, str] | None = None
    pending_value_parts: list[str] = []
    ad214_primary_runway_ids: list[str] = []
    ad214_secondary_row_idx = 0
    ad214_secondary_block = False

    def flush_pending() -> None:
        nonlocal pending_item, pending_value_parts
        if pending_item is None:
            return
        item_id, label = pending_item
        output.append(f"ITEM: {item_id} | {label}")
        value = " ".join(part for part in pending_value_parts if part).strip()
        if value:
            output.append(f"VALUE: {value}")
        pending_item = None
        pending_value_parts = []

    for line in content_lines:
        if _is_section_header(line):
            continue
        if _is_rule_line(line):
            continue
        if _is_numeric_index_pipe_row(line):
            continue

        scalar_pipe_value = _single_pipe_scalar_value(line)
        if scalar_pipe_value is not None:
            flush_pending()
            output.append(f"VALUE: {scalar_pipe_value}")
            continue

        parsed_pipe = _parse_pipe_line(line, section_id)
        if parsed_pipe is not None:
            flush_pending()
            kind, label, value = parsed_pipe
            if kind == "ROW" and _looks_like_table_header_row(section_id, label, value):
                output.append(f"NOTE: {label} | {value}")
                if section_id == "AD 2.14" and "rcll" in f"{label} {value}".lower():
                    ad214_secondary_block = True
                continue

            if section_id == "AD 2.14" and kind == "ROW":
                if label in {"11", "29"} and not ad214_secondary_block:
                    ad214_primary_runway_ids.append(label)
                elif ad214_secondary_block and label.lower() == "no" and ad214_primary_runway_ids:
                    if ad214_secondary_row_idx < len(ad214_primary_runway_ids):
                        if not value.lower().startswith("no |"):
                            value = f"No | {value}"
                        label = ad214_primary_runway_ids[ad214_secondary_row_idx]
                        ad214_secondary_row_idx += 1

            if section_id == "AD 2.18" and kind == "ROW":
                value_cells = [cell.strip() for cell in value.split("|")]
                if value_cells and value_cells[0].endswith("/"):
                    value_cells[0] = value_cells[0].removesuffix("/").rstrip()
                    value = " | ".join(value_cells)
            output.append(f"ITEM: {kind} | {label}")
            output.append(f"VALUE: {value}")
            continue

        note_from_pipe = _pipe_line_to_note(line)
        if note_from_pipe is not None:
            flush_pending()
            output.append(f"NOTE: {note_from_pipe}")
            continue

        chart_row = _parse_chart_row(line)
        if chart_row is not None:
            flush_pending()
            label, value = chart_row
            output.append(f"ITEM: CHART | {label}")
            output.append(f"VALUE: {value}")
            continue

        if line.startswith("-"):
            flush_pending()
            output.append(f"NOTE: {line}")
            continue

        item_match = _ITEM_RE.match(line)
        if item_match:
            flush_pending()
            item_id, remainder = item_match.group(1), item_match.group(2)
            label, inline_value = _split_label_and_value(remainder)
            pending_item = (item_id, label)
            if inline_value:
                pending_value_parts.append(inline_value)
            continue

        if pending_item is not None:
            pending_value_parts.append(line)
        elif line.lower() in {"no", "nil"}:
            output.append(f"VALUE: {line}")
        else:
            output.append(f"NOTE: {line}")

    flush_pending()
    return "\n".join(output)


def _normalize_noise(text: str) -> str:
    text = text.replace("\x00", "")
    text = text.replace("\ufffe", "")
    text = text.replace("\u2013", "-")
    text = text.replace("\u2014", "-")
    text = text.replace("e- mail", "e-mail")
    text = text.replace("Secu r ity", "Security")
    text = text.replace("C learance", "Clearance")
    text = text.replace("EANASA", "EANA SA")
    text = text.replace("OMAMENDOZA", "OMA MENDOZA")
    text = text.replace("OMACÓRDOBA", "OMA CÓRDOBA")
    text = text.replace("San Rafael Torre / |", "San Rafael Torre |")
    return text


def _extract_title(lines: list[str], section_id: str) -> str:
    for line in lines:
        title_from_pipe = _extract_title_from_pipe_header(line, section_id)
        if title_from_pipe:
            return title_from_pipe

    for line in lines:
        if section_id.lower().replace(" ", "") not in line.lower().replace(" ", ""):
            continue
        match = _SECTION_HEADER_RE.match(line)
        if not match:
            continue
        raw_title = match.group(2).strip("|- ")
        if "|" in raw_title:
            raw_title = raw_title.split("|", 1)[0].strip()
        return raw_title
    return _SECTION_TITLE_FALLBACKS.get(section_id, "")


def _drop_redundant_headers(lines: list[str], section_id: str) -> list[str]:
    result: list[str] = []
    section_compact = section_id.lower().replace(" ", "")
    for line in lines:
        compact = _strip_leading_table_or_markdown_prefix(line).lower().replace(" ", "")
        if compact.startswith(section_compact):
            continue
        if compact.startswith("aipargentina") or compact.startswith("departamentoinformacion"):
            continue
        result.append(line)
    return result


def _split_label_and_value(remainder: str) -> tuple[str, str]:
    # Most rows are bilingual labels separated by " / " and then inline value.
    if " / " not in remainder:
        return remainder.strip(), ""

    left, right = remainder.split(" / ", 1)
    tokens = right.split()
    split_at = _find_value_start(tokens)
    if split_at is None:
        return remainder.strip(), ""

    label_right = " ".join(tokens[:split_at]).strip()
    value = " ".join(tokens[split_at:]).strip()
    label = f"{left.strip()} / {label_right}".strip()
    return label, value


def _find_value_start(tokens: list[str]) -> int | None:
    markers = {"no", "nil", "si", "sí", "h24", "yes", "(*)"}
    for idx, token in enumerate(tokens):
        clean = token.strip().strip(",;")
        if clean.lower() in markers:
            return idx
        if any(char.isdigit() for char in clean):
            return idx
        if clean.startswith("("):
            return idx
    return None


def _is_section_header(line: str) -> bool:
    return bool(_SECTION_HEADER_RE.match(line))


def _parse_pipe_line(line: str, section_id: str) -> tuple[str, str, str] | None:
    cells = _split_pipe_cells(line)
    if cells is None:
        return None
    if not cells:
        return None

    if cells[0].isdigit() and len(cells) >= 3:
        if all(cell.isdigit() for cell in cells):
            return None
        if len(cells) >= 5:
            return "ROW", cells[0], " | ".join(cells[1:])
        label = cells[1]
        value = " | ".join(cells[2:])
        if value.startswith(f"{label} | "):
            value = value[len(label) + 3 :]
        if not label or not value:
            return None
        return cells[0], label, value

    if len(cells) >= 4:
        label = cells[0]
        value_cells = cells[1:]
        if value_cells and all(cell == label for cell in value_cells):
            return None
        if value_cells and value_cells[0] == label:
            value_cells = value_cells[1:]
        value = " | ".join(value_cells)
        return "ROW", label, value

    if len(cells) == 3:
        col1, col2, col3 = cells
        if _is_rule_like_pipe_row(col1, col2, col3):
            return None
        if col1.lower().startswith("ad 2."):
            return None
        if col1 == col2 == col3:
            return None
        if section_id == "AD 2.24":
            label = col1 if col1 == col2 else f"{col1} | {col2}"
            return "CHART", label, col3
        if not any(char.isdigit() for char in col1 + col2 + col3):
            return None
        return "ROW", col1, f"{col2} | {col3}"

    return None


def _parse_chart_row(line: str) -> tuple[str, str] | None:
    match = _CHART_REF_RE.match(line)
    if not match:
        return None
    label = match.group(1).strip()
    value = match.group(2).strip()
    if not label or not value:
        return None
    return label, value


def _split_pipe_cells(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    cells = [cell.strip() for cell in stripped.strip("|").split("|")]
    return [cell for cell in cells if cell != ""]


def _pipe_line_to_note(line: str) -> str | None:
    cells = _split_pipe_cells(line)
    if cells is None or not cells:
        return None
    return " | ".join(cells)


def _single_pipe_scalar_value(line: str) -> str | None:
    cells = _split_pipe_cells(line)
    if cells is None:
        return None
    if len(cells) != 1:
        return None
    value = cells[0].strip()
    if value.lower() in {"no", "nil"}:
        return value
    return None


def _extract_title_from_pipe_header(line: str, section_id: str) -> str:
    cells = _split_pipe_cells(line)
    if cells is None or len(cells) < 2:
        return ""
    section_compact = section_id.lower().replace(" ", "")
    if section_compact not in cells[0].lower().replace(" ", ""):
        return ""

    parts: list[str] = []
    for cell in cells:
        normalized = cell.strip()
        if not normalized:
            continue
        if section_compact in normalized.lower().replace(" ", ""):
            normalized = _SECTION_HEADER_RE.sub(lambda m: m.group(2).strip(), normalized).strip("|- ")
        if normalized and normalized not in parts:
            parts.append(normalized)
    return " ".join(parts).strip()


def _looks_like_table_header_row(section_id: str, label: str, value: str) -> bool:
    text = f"{label} | {value}".lower()
    header_markers = [
        "designador",
        "designation",
        "frecuencia",
        "frequency",
        "horas de operación",
        "hours of operation",
        "observaciones",
        "remarks",
        "dimensiones",
        "dimensions",
        "resistencia",
        "coord",
        "elev",
        "pendiente",
        "len",
        "intst",
        "papi",
    ]
    if any(marker in text for marker in header_markers) and not any(ch.isdigit() for ch in label):
        return True
    if section_id == "AD 2.10" and "en las áreas de aproximación" in text:
        return True
    return False


def _is_numeric_index_pipe_row(line: str) -> bool:
    cells = _split_pipe_cells(line)
    if cells is None or not cells:
        return False
    return all(cell.isdigit() for cell in cells)


def _is_rule_like_pipe_row(col1: str, col2: str, col3: str) -> bool:
    cols = (col1, col2, col3)
    return all(not any(ch.isalnum() for ch in col.replace("-", "")) for col in cols)


def _is_rule_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("|") and stripped.endswith("|"):
        inner = stripped.strip("|").replace("|", "").replace("-", "").strip()
        return inner == ""
    return False


def _strip_leading_table_or_markdown_prefix(line: str) -> str:
    value = line.lstrip()
    if value.startswith("|"):
        value = value[1:].lstrip()
    if value.startswith("#"):
        value = value.lstrip("#").lstrip()
    return value
