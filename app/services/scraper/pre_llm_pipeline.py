"""Build per-section LLM hint payloads from PyMuPDF layout artifacts."""

from __future__ import annotations

from typing import Any

from app.services.enrichment.ad2_contracts import load_ad2_contract
from app.services.scraper.aip_segmenter import LAYOUT_SCHEMA_VERSION


def build_pre_llm_sections_payload(*, icao: str, raw_extraction: dict[str, Any]) -> dict[str, Any]:
    """Return a versioned 25-section payload from an ``aip-layout-v1`` artifact.

    The parser normally builds this payload while sectionizing. This function remains
    as a compatibility entry point for callers/tests that only have the raw layout
    artifact.
    """

    if raw_extraction.get("schema_version") != LAYOUT_SCHEMA_VERSION:
        return {"schema_version": LAYOUT_SCHEMA_VERSION, "icao": icao, "sections": [_empty_section(icao, f"AD 2.{i}") for i in range(1, 26)]}

    section_map: dict[str, dict[str, Any]] = {}
    sections = raw_extraction.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if isinstance(section, dict) and isinstance(section.get("section_id"), str):
                section_map[section["section_id"]] = section

    out = []
    for idx in range(1, 26):
        sid = f"AD 2.{idx}"
        section = section_map.get(sid)
        if section is None:
            out.append(_empty_section(icao, sid))
            continue
        out.append(
            {
                "section_id": sid,
                "title": section.get("title") or sid,
                "schema_hint": section.get("schema_hint") or _schema_hint_for(icao, sid),
                "section_blocks": section.get("section_blocks", []),
                "tables": section.get("tables", []),
                "quality": section.get("quality", {}),
                "source": section.get("source", {}),
                "raw_text_preview": section.get("raw_text_preview", ""),
            }
        )

    return {
        "schema_version": LAYOUT_SCHEMA_VERSION,
        "engine": raw_extraction.get("engine", "pymupdf"),
        "icao": icao.strip().upper(),
        "sections": out,
    }


def _schema_hint_for(icao: str, section_id: str) -> str:
    contract = load_ad2_contract(icao, section_id)
    if contract is None:
        return "generic_ad2"
    rules = contract.rules
    if isinstance(rules, dict) and isinstance(rules.get("schema_hint"), str):
        return str(rules["schema_hint"])
    return "runtime_contract"


def _empty_section(icao: str, section_id: str) -> dict[str, Any]:
    return {
        "section_id": section_id,
        "title": section_id,
        "schema_hint": _schema_hint_for(icao, section_id),
        "section_blocks": [],
        "tables": [],
        "quality": {"confidence": 0.0, "warnings": ["no_layout_section"]},
        "source": {"pages": [], "blocks_count": 0, "tables_count": 0},
        "raw_text_preview": "",
    }
