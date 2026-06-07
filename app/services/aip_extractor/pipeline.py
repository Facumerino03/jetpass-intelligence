"""Orchestrate PDF extraction, section splitting, and LLM parsing."""

from pathlib import Path
from typing import Any

from .llm import extract_section_as_dict
from .normalize import (
    collect_field_errors,
    normalize_extracted_data,
)
from .pdf import extract_text
from .schemas import SECTION_REGISTRY, SUPPORTED_SECTIONS
from .splitter import split_sections
from .tables import extract_sections_as_tables


def run(
    pdf_path: str | Path,
    *,
    sections: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run the full extraction pipeline.

    For tabular sections (AD 2.12, AD 2.13, AD 2.18, AD 2.19, etc.) the
    content fed to the LLM is a Markdown table extracted directly with
    pdfplumber's table engine.  This preserves column boundaries and
    eliminates spatial-alignment hallucinations.

    For all other sections, or when table extraction yields nothing, the
    pipeline falls back to raw text from pdfplumber's text extractor.

    Returns a dict with metadata and extracted section data keyed by AD 2.X.
    """
    path = Path(pdf_path)
    raw_text = extract_text(path)
    all_sections = split_sections(raw_text)

    # Best-effort table extraction; never raises — failures are silently ignored
    # so the text fallback kicks in automatically.
    try:
        table_sections = extract_sections_as_tables(path)
    except Exception:  # noqa: BLE001
        table_sections = {}

    target_sections = sections or list(SUPPORTED_SECTIONS)
    unknown = [s for s in target_sections if s not in SECTION_REGISTRY]
    if unknown:
        raise ValueError(
            f"Unknown sections: {unknown}. "
            f"Supported: {', '.join(SUPPORTED_SECTIONS)}"
        )

    result: dict[str, Any] = {
        "source_pdf": str(path.resolve()),
        "sections_found": sorted(all_sections.keys()),
        "sections_extracted": {},
        "sections_skipped": [],
        "errors": {},
        "field_errors": [],
    }

    for section_id in target_sections:
        # Prefer Markdown table when available; fall back to raw text.
        content = table_sections.get(section_id) or all_sections.get(section_id)
        if not content:
            result["sections_skipped"].append(section_id)
            continue

        schema_model = SECTION_REGISTRY[section_id]
        try:
            raw_section = extract_section_as_dict(
                section_id,
                content,
                schema_model,
            )
            normalized = normalize_extracted_data(raw_section, omit_none=False)
            result["sections_extracted"][section_id] = normalized
            result["field_errors"].extend(
                collect_field_errors(normalized, section_id=section_id)
            )
        except Exception as exc:  # noqa: BLE001 — collect per-section errors
            result["errors"][section_id] = str(exc)

    if not result["field_errors"]:
        del result["field_errors"]

    return result
