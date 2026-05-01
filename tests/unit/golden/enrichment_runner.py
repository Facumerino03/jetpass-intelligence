"""Helpers to run enrichment for one AD 2.x section."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import unicodedata
from typing import Any

from app.models.aerodrome import AerodromeDocument
from app.services.enrichment import enrich_aerodrome_document
from app.services.enrichment.llm_providers import get_llm_provider


@dataclass(frozen=True)
class SectionEnrichmentResult:
    section_id: str
    raw_text: str
    actual_data: dict[str, Any]
    extraction: dict[str, Any]


async def run_section_enrichment(doc: AerodromeDocument, section_id: str) -> SectionEnrichmentResult:
    updated = await enrich_aerodrome_document(doc, section_ids=[section_id])
    section = next(s for s in updated.current.ad_sections if s.section_id == section_id)
    extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
    return SectionEnrichmentResult(
        section_id=section.section_id,
        raw_text=section.raw_text,
        actual_data=section.data,
        extraction=extraction,
    )


async def run_section_enrichment_with_golden_schema(
    *,
    icao: str,
    section_id: str,
    raw_text: str,
    expected_data: dict[str, Any],
) -> SectionEnrichmentResult:
    provider = get_llm_provider()
    schema = _json_schema_from_example(expected_data)
    content = provider.chat_structured(
        icao=icao,
        section_id=section_id,
        raw_text=raw_text,
        schema=schema,
        contract=expected_data,
    )
    actual_data = json.loads(_coerce_json_payload(content))
    actual_data = _repair_with_source_orthography(actual_data, raw_text)
    extraction = {
        "engine": provider.engine_name,
        "model": provider.model_name,
        "status": "ok",
        "mode": "golden_schema",
    }
    return SectionEnrichmentResult(
        section_id=section_id,
        raw_text=raw_text,
        actual_data=actual_data,
        extraction=extraction,
    )


def _repair_with_source_orthography(payload: Any, raw_text: str) -> Any:
    if isinstance(payload, dict):
        return {key: _repair_with_source_orthography(value, raw_text) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_repair_with_source_orthography(item, raw_text) for item in payload]
    if not isinstance(payload, str):
        return payload
    repaired = _lift_orthography_from_raw(payload, raw_text)
    return repaired if repaired is not None else payload


def _lift_orthography_from_raw(value: str, raw_text: str) -> str | None:
    normalized_value = _normalize_for_match(value)
    normalized_raw = _normalize_for_match(raw_text)
    if normalized_value not in normalized_raw:
        return None

    start = normalized_raw.find(normalized_value)
    end = start + len(normalized_value)
    candidate = raw_text[start:end]

    if _strip_diacritics(candidate) == _strip_diacritics(value) and candidate != value:
        return candidate
    return None


def _normalize_for_match(value: str) -> str:
    text = value.upper().replace("–", "-").replace("—", "-").replace("’", "'")
    text = " ".join(text.split())
    return _strip_diacritics(text)


def _strip_diacritics(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _json_schema_from_example(example: Any) -> dict[str, Any]:
    if isinstance(example, dict):
        properties = {key: _json_schema_from_example(value) for key, value in example.items()}
        return {
            "type": "object",
            "properties": properties,
            "required": list(example.keys()),
            "additionalProperties": False,
        }
    if isinstance(example, list):
        item_schema = _json_schema_from_example(example[0]) if example else {"type": "string"}
        return {"type": "array", "items": item_schema}
    if isinstance(example, bool):
        return {"type": "boolean"}
    if isinstance(example, int):
        return {"type": "integer"}
    if isinstance(example, float):
        return {"type": "number"}
    if example is None:
        return {"type": ["string", "null"]}
    return {"type": "string"}


def _coerce_json_payload(content: str) -> str:
    text = content.strip()

    fenced = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()

    values = _extract_all_json_values(text)
    if values:
        # If model emits schema + answer, keep the last JSON value (usually the answer).
        return values[-1]
    return text


def _extract_all_json_values(text: str) -> list[str]:
    values: list[str] = []
    i = 0
    while i < len(text):
        start_candidates = [pos for pos in (text.find("{", i), text.find("[", i)) if pos != -1]
        if not start_candidates:
            break
        start = min(start_candidates)
        opener = text[start]
        closer = "}" if opener == "{" else "]"
        depth = 0
        in_string = False
        escaped = False
        end = None
        for idx in range(start, len(text)):
            ch = text[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
                continue
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        if end is None:
            break
        values.append(text[start : end + 1].strip())
        i = end + 1
    return values
