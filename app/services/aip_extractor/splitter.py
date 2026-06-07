"""Split raw AIP text into sections keyed by AD 2.X identifiers."""

import re

# Section headers appear at line start: "AD 2.12 CARACTERÍSTICAS..."
_SECTION_HEADER = re.compile(r"^AD\s+(2\.\d+)\b", re.MULTILINE)
_SECTION_SPLIT = re.compile(r"(?=^AD\s+2\.\d+\b)", re.MULTILINE)


def _section_key(chunk: str) -> str | None:
    """Extract 'AD 2.X' from the first line of a chunk."""
    first_line = chunk.strip().split("\n", 1)[0]
    match = _SECTION_HEADER.match(first_line)
    if match:
        return f"AD {match.group(1)}"
    return None


def split_sections(raw_text: str) -> dict[str, str]:
    """
    Split AIP text into sections keyed by 'AD 2.X'.

    Duplicate sections (e.g. continuations marked '(cont.)') are merged.
    """
    chunks = [c.strip() for c in _SECTION_SPLIT.split(raw_text) if c.strip()]
    sections: dict[str, str] = {}

    for chunk in chunks:
        key = _section_key(chunk)
        if key is None:
            continue
        if key in sections:
            sections[key] = f"{sections[key]}\n\n{chunk}"
        else:
            sections[key] = chunk

    return sections
