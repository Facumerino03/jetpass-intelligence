"""Deterministic AD 2.x section segmentation for raw text persistence."""

from __future__ import annotations

import re
from pathlib import Path

from app.schemas.aerodrome import SectionSchema
from app.services.enrichment.ad2_contracts import load_ad2_contract

_HEADER_RE = re.compile(
    r"(?im)^\s*(?:\|\s*)?(?:#{1,6}\s*)?(?:[A-Z]{4}\s+)?AD\s*2\.(\d{1,2})(?!\d)\b[^\n]*"
)

class HeaderMatch:
    """Matched AD 2.x heading with normalized section id and offsets."""

    def __init__(self, section_id: str, matched_text: str, start: int, end: int) -> None:
        self.section_id = section_id
        self.matched_text = matched_text
        self.start = start
        self.end = end


def segment_ad2_sections(
    text: str,
    source_path: Path,
    icao: str,
    *,
    logger: object,
    format_error: type[Exception],
) -> list[SectionSchema]:
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
    for match in matches:
        seen_counts[match.section_id] = seen_counts.get(match.section_id, 0) + 1

    missing_sections = [section_id for section_id in expected if seen_counts.get(section_id, 0) == 0]
    duplicate_sections = sorted(section_id for section_id, count in seen_counts.items() if count > 1)

    if duplicate_sections:
        logger.warning(
            "aip.parser.duplicate_headers_detected",
            extra={
                "icao": icao,
                "source_document": source_path.name,
                "duplicate_sections": duplicate_sections,
            },
        )

    if missing_sections:
        raise format_error(
            "AD 2.x header validation failed for "
            f"{source_path.name} ({icao}): missing={missing_sections}, duplicates={duplicate_sections}"
        )

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
            raise format_error(
                f"AD 2.x section block is empty: {match.section_id} in '{source_path.name}'."
            )
        raw_by_section[match.section_id] = block

    _rebalance_orphan_coordinate_prefixes(raw_by_section, expected)
    _normalize_cross_line_table_rows(raw_by_section)
    _rebalance_orphan_coordinate_blocks(raw_by_section, expected)

    return [
        SectionSchema(
            section_id=section_id,
            title=section_id,
            section_title=_resolve_section_title(
                icao=icao,
                section_id=section_id,
                matched_text=selected_by_section[section_id].matched_text,
            ),
            raw_text=raw_by_section[section_id],
            data={},
        )
        for section_id in expected
    ]


def _resolve_section_title(*, icao: str, section_id: str, matched_text: str) -> str | None:
    extracted = _extract_title_from_header(matched_text, section_id)
    if extracted:
        return extracted
    contract = load_ad2_contract(icao, section_id)
    if contract is None:
        return None
    expected = contract.expected
    if not isinstance(expected, dict):
        return None
    tables = expected.get("tables")
    if isinstance(tables, list) and tables:
        first = tables[0]
        if isinstance(first, dict) and isinstance(first.get("label"), str) and first["label"].strip():
            return first["label"].strip()
    fields = expected.get("fields")
    if isinstance(fields, list) and fields:
        first = fields[0]
        if isinstance(first, dict) and isinstance(first.get("label"), str) and first["label"].strip():
            return first["label"].strip()
    return None


def _extract_title_from_header(matched_text: str, section_id: str) -> str | None:
    compact = section_id.lower().replace(" ", "")
    text = matched_text.strip().lstrip("#").strip().strip("|").strip()
    if not text:
        return None
    lowered = text.lower().replace(" ", "")
    idx = lowered.find(compact)
    if idx == -1:
        return None
    suffix = text[idx:]
    # remove leading AD 2.x token and separators
    suffix = re.sub(r"(?i)^AD\s*2\.\d{1,2}\s*", "", suffix).strip(" -:|")
    if not suffix:
        return None
    return suffix


def _expected_section_ids() -> list[str]:
    return [f"AD 2.{idx}" for idx in range(1, 26)]


def _normalize_section_id(section_number: int) -> str | None:
    if 1 <= section_number <= 25:
        return f"AD 2.{section_number}"
    return None


def _rebalance_orphan_coordinate_prefixes(raw_by_section: dict[str, str], ordered_ids: list[str]) -> None:
    """Move OCR-orphan coordinate prefaces from a section to the previous one.

    Some OCR/layout flows can place the tail of a table row (typically coordinates)
    right after the next section heading. This keeps section boundaries stable by
    moving that prefix back to the previous section when confidently detected.
    """

    for idx in range(1, len(ordered_ids)):
        prev_id = ordered_ids[idx - 1]
        curr_id = ordered_ids[idx]
        prev_block = raw_by_section.get(prev_id, "")
        curr_block = raw_by_section.get(curr_id, "")
        if not prev_block or not curr_block:
            continue

        orphan_prefix, remainder = _split_orphan_coordinate_prefix(curr_block)
        if orphan_prefix is None:
            continue
        if not _looks_like_table_tail_context(prev_block):
            continue

        raw_by_section[prev_id] = _insert_before_remarks(prev_block, orphan_prefix.strip())
        raw_by_section[curr_id] = remainder


def _split_orphan_coordinate_prefix(block: str) -> tuple[str | None, str]:
    lines = block.splitlines()
    if len(lines) < 3:
        return None, block

    header_idx = 0
    first_content_idx = 1
    while first_content_idx < len(lines) and not lines[first_content_idx].strip():
        first_content_idx += 1
    if first_content_idx >= len(lines):
        return None, block

    numbered_item_idx = None
    for i in range(first_content_idx, len(lines)):
        if re.match(r"^\s*\d{1,2}\s+\S", lines[i]):
            numbered_item_idx = i
            break
    if numbered_item_idx is None:
        return None, block

    candidate_lines = lines[first_content_idx:numbered_item_idx]
    candidate_text = "\n".join(candidate_lines).strip()
    if not candidate_text:
        return None, block

    has_coordinates_label = bool(re.search(r"(?im)\bcoordenadas\b|\bcoordinates\b", candidate_text))
    has_coordinate_value = bool(
        re.search(r"\b\d{6}(?:\.\d+)?[NS]\s*\d{7}(?:\.\d+)?[EW]\b", candidate_text)
    )
    if not (has_coordinates_label or has_coordinate_value):
        return None, block

    rebuilt = lines[: first_content_idx] + lines[numbered_item_idx:]
    remainder = "\n".join(rebuilt).strip()
    if not remainder:
        return None, block
    return candidate_text, remainder


def _looks_like_table_tail_context(previous_block: str) -> bool:
    return bool(
        re.search(
            r"(?im)\b(obst[aá]culos|coordenadas|coordinates|area affected|in circling area)\b",
            previous_block,
        )
    )


def _normalize_cross_line_table_rows(raw_by_section: dict[str, str]) -> None:
    """Apply generic OCR row stitching inside each section block.

    This avoids section-specific logic while fixing common OCR line breaks:
    - token split around slash across two lines (e.g., "Antena/" + "Antenna,...")
    - coordinate values moved to next line after obstacle/elevation text
    """

    for section_id, block in raw_by_section.items():
        raw_by_section[section_id] = _stitch_block_rows(block)


def _stitch_block_rows(block: str) -> str:
    lines = block.splitlines()
    stitched: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            stitched.append(line)
            i += 1
            continue

        # Generic pattern: trailing slash with continuation in next non-empty line.
        # Example: "Antena/" + "Antenna, Markings and LGT"
        if stripped.endswith("/"):
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                nxt = lines[j].strip()
                if nxt and not _looks_like_new_header(nxt):
                    nxt = _strip_column_header_noise(nxt)
                    line = f"{stripped}{nxt}"
                    i = j

        # Generic pattern: obstacle/elevation row followed by coordinate-only line.
        # Join into one row to preserve field association.
        j = i + 1
        while j < len(lines) and not lines[j].strip():
            j += 1
        if j < len(lines):
            nxt = lines[j].strip()
            if _looks_like_row_descriptor(stripped) and _looks_like_elevation_fragment(nxt):
                line = f"{line.strip()} {nxt}"
                i = j
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                nxt = lines[j].strip() if j < len(lines) else ""
            if _looks_like_coordinate_only_line(nxt) and _looks_like_row_needing_coordinate(stripped):
                line = f"{line.strip()} {nxt}"
                i = j
            elif _looks_like_row_needing_coordinate(stripped):
                coord_idx, coord_val = _find_coordinate_after_optional_label(lines, j)
                if coord_idx is not None and coord_val is not None:
                    line = f"{line.strip()} {coord_val}"
                    i = coord_idx

        stitched.append(line)
        i += 1

    return "\n".join(stitched).strip()


def _looks_like_coordinate_only_line(text: str) -> bool:
    return bool(re.fullmatch(r"\d{6}(?:\.\d+)?[NS]\s*\d{7}(?:\.\d+)?[EW]", text))


def _looks_like_row_needing_coordinate(text: str) -> bool:
    return bool(
        re.search(
            r"(?i)(obstacle|obst[aá]culo|antenna|antena|tree|[aá]rbol|grove|elevation|elevaci[oó]n|\bft\b|\bm\b)",
            text,
        )
    )


def _looks_like_row_descriptor(text: str) -> bool:
    return bool(re.search(r"(?i)(antenna|antena|obstacle|obst[aá]culo|tree|[aá]rbol|grove)", text))


def _looks_like_elevation_fragment(text: str) -> bool:
    return bool(re.search(r"\b\d+(?:\.\d+)?\s*m\s*\(\d+(?:\.\d+)?\s*ft\)", text, re.IGNORECASE))


def _looks_like_new_header(text: str) -> bool:
    return bool(re.match(r"(?i)^(AD\s*2\.|\d{1,2}\s+)", text))


def _find_coordinate_after_optional_label(lines: list[str], start_idx: int) -> tuple[int | None, str | None]:
    max_scan = min(len(lines), start_idx + 8)
    saw_label = False
    for idx in range(start_idx, max_scan):
        token = lines[idx].strip()
        if not token:
            continue
        if re.search(r"(?i)^(coordenadas|coordinates|/\s*coordinates)$", token):
            saw_label = True
            continue
        if _looks_like_coordinate_only_line(token):
            if saw_label:
                return idx, token
            return None, None
        if _looks_like_new_header(token) or re.search(r"(?i)observaciones\s*/\s*remarks", token):
            return None, None
    return None, None


def _insert_before_remarks(block: str, orphan_prefix: str) -> str:
    match = re.search(r"(?im)^\s*observaciones\s*/\s*remarks\b", block)
    if not match:
        return f"{block.rstrip()}\n\n{orphan_prefix}"
    return f"{block[:match.start()].rstrip()}\n\n{orphan_prefix}\n\n{block[match.start():].lstrip()}"


def _rebalance_orphan_coordinate_blocks(raw_by_section: dict[str, str], ordered_ids: list[str]) -> None:
    """Move coordinate label blocks from current section to previous row when orphaned.

    OCR can place trailing coordinate labels/values in the next section block instead of
    completing the last row of the previous section. This function detects coordinate
    blocks in the current section and, when safe, appends the coordinate value to the
    last coordinate-needing row in the previous section.
    """

    for idx in range(1, len(ordered_ids)):
        prev_id = ordered_ids[idx - 1]
        curr_id = ordered_ids[idx]
        prev_block = raw_by_section.get(prev_id, "")
        curr_block = raw_by_section.get(curr_id, "")
        if not prev_block or not curr_block:
            continue
        if not _looks_like_table_tail_context(prev_block):
            continue

        coord_value, cleaned_curr = _extract_coordinate_label_block(curr_block)
        if coord_value is None:
            continue

        updated_prev = _append_coordinate_to_last_row(prev_block, coord_value)
        if updated_prev is None:
            continue

        raw_by_section[prev_id] = updated_prev
        raw_by_section[curr_id] = cleaned_curr


def _extract_coordinate_label_block(block: str) -> tuple[str | None, str]:
    lines = block.splitlines()
    if not lines:
        return None, block

    idx = 0
    while idx < len(lines) and not lines[idx].strip():
        idx += 1

    # allow heading line first
    if idx < len(lines) and _looks_like_new_header(lines[idx].strip()):
        idx += 1

    scan_limit = min(len(lines), idx + 10)
    coord_idx = None
    saw_coord_label = False
    for i in range(idx, scan_limit):
        token = lines[i].strip()
        if not token:
            continue
        if re.search(r"(?i)^(coordenadas|coordinates|/\s*coordinates)$", token):
            saw_coord_label = True
            continue
        if _looks_like_coordinate_only_line(token):
            if saw_coord_label:
                coord_idx = i
                break
            return None, block
        if re.match(r"^\s*\d{1,2}\s+", token):
            return None, block

    if coord_idx is None:
        return None, block

    coord_value = lines[coord_idx].strip()
    consumed = set()
    for i in range(idx, coord_idx + 1):
        token = lines[i].strip()
        if not token:
            consumed.add(i)
            continue
        if re.search(r"(?i)^(coordenadas|coordinates|/\s*coordinates)$", token) or token == coord_value:
            consumed.add(i)

    rebuilt = [line for i, line in enumerate(lines) if i not in consumed]
    cleaned = "\n".join(rebuilt).strip()
    return coord_value, cleaned


def _append_coordinate_to_last_row(block: str, coordinate: str) -> str | None:
    lines = block.splitlines()
    for i in range(len(lines) - 1, -1, -1):
        token = lines[i].strip()
        if not token:
            continue
        if _looks_like_new_header(token):
            break
        if _looks_like_row_needing_coordinate(token) and not _contains_coordinate(token):
            lines[i] = f"{token} {coordinate}"
            return "\n".join(lines).strip()
    return None


def _contains_coordinate(text: str) -> bool:
    return bool(re.search(r"\d{6}(?:\.\d+)?[NS]\s*\d{7}(?:\.\d+)?[EW]", text))


def _strip_column_header_noise(text: str) -> str:
    cleaned = re.sub(r"(?i),\s*markings\s+and\s+lgt\b", "", text)
    cleaned = re.sub(r"(?i),\s*se[nñ]ales\s+y\s+lgt\b", "", cleaned)
    return cleaned.strip()
