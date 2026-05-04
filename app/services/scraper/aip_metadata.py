"""Metadata extraction helpers from canonical AD 2.x sections/text."""

from __future__ import annotations

import re

from app.schemas.aerodrome import SectionSchema


def extract_name_fields(sections: list[SectionSchema]) -> tuple[str, str | None]:
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

    icao_name_match = re.search(r"\b([A-Z]{4})\s*[-–]\s*(.+)$", first_line)
    if icao_name_match:
        full_name = icao_name_match.group(2).strip(" -:")
        main_name = full_name.split("/")[0].strip(" -:")
        if main_name:
            return main_name.title(), full_name
        return full_name.title(), full_name

    for line in raw_lines[1:]:
        icao_name_match = re.search(r"\b([A-Z]{4})\s*[-–]\s*(.+)$", line)
        if not icao_name_match:
            continue
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


def extract_airac_cycle(text: str) -> str | None:
    match = re.search(r"AIRAC\s+AMDT\s+(\d{1,2}/\d{2})", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)
