"""LLM enrichment tool for AD 2.0 section structured data."""

from __future__ import annotations
import asyncio
from difflib import SequenceMatcher
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from app.models.aerodrome import AerodromeDocument
from app.services.enrichment.ad2_contracts import load_ad2_contract
from app.services.enrichment.aip_section_schemas import SECTION_SCHEMA_REGISTRY
from app.services.enrichment.llm_providers import get_llm_provider
from app.services.scraper.aip_source_model import build_section_blocks, extract_table_groups_from_blocks

logger = logging.getLogger(__name__)

PROMPT_VERSION = "ad2-v1"
TARGET_SECTION_IDS = tuple(f"AD 2.{idx}" for idx in range(1, 26))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _extract_for_section(
    icao: str,
    section_id: str,
    raw_text: str,
    *,
    provider: Any,
    section_blocks: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    registry_entry = SECTION_SCHEMA_REGISTRY.get(section_id)
    if registry_entry is None:
        raise ValueError(f"Unsupported enrichment section: {section_id}")
    key, model = registry_entry

    contract = load_ad2_contract(icao, section_id)
    contract_payload = contract.expected if contract is not None else None

    content = provider.chat_structured(
        icao=icao,
        section_id=section_id,
        raw_text=raw_text,
        schema=model.model_json_schema(),
        contract=contract_payload,
        section_blocks=section_blocks,
    )
    parsed = model.model_validate(_sanitize_llm_payload(_coerce_json_payload(content), section_id=section_id))
    payload = parsed.model_dump(mode="json", by_alias=True)
    payload = _inject_layout_tables(payload, section_blocks, section_id=section_id, raw_text=raw_text)
    payload = _postprocess_section_payload(
        section_id=section_id,
        payload=payload,
        contract_expected=contract_payload,
    )
    return key, payload


def _postprocess_section_payload(
    *, section_id: str, payload: dict[str, Any], contract_expected: dict[str, Any] | None
) -> dict[str, Any]:
    aligned = _align_tables_with_contract(payload, contract_expected)
    if section_id == "AD 2.18":
        _fill_down_column(
            aligned,
            table_index=0,
            column_name="Horas de operación / Hours of operation",
        )
    if section_id == "AD 2.20":
        _collapse_fields_to_single_entry(aligned)
    if section_id == "AD 2.24":
        _normalize_ad224_chart_rows(aligned)
    if section_id == "AD 2.25":
        _normalize_single_answer_section(aligned, label_hint="PENETRACIÓN DE LA SUPERFICIE DEL TRAMO VISUAL (VSS) / VISUAL SEGMENT SURFACE (VSS) PENETRATION")
    return aligned


def _align_tables_with_contract(payload: dict[str, Any], contract_expected: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(contract_expected, dict):
        return payload
    expected_tables = contract_expected.get("tables")
    actual_tables = payload.get("tables")
    if not isinstance(expected_tables, list) or not isinstance(actual_tables, list):
        return payload

    for table_idx, expected_table in enumerate(expected_tables):
        if table_idx >= len(actual_tables):
            break
        if not isinstance(expected_table, dict) or not isinstance(actual_tables[table_idx], dict):
            continue
        expected_columns = expected_table.get("columns")
        actual_rows = actual_tables[table_idx].get("rows")
        if not isinstance(expected_columns, list) or not isinstance(actual_rows, list):
            continue

        normalized_rows: list[dict[str, Any]] = []
        for row in actual_rows:
            if not isinstance(row, dict):
                normalized_rows.append({col: None for col in expected_columns})
                continue
            mapped: dict[str, Any] = {col: None for col in expected_columns}
            used_actual: set[str] = set()
            for exp_col in expected_columns:
                if exp_col in row:
                    mapped[exp_col] = row.get(exp_col)
                    used_actual.add(exp_col)
                    continue
                best_key = None
                best_score = 0.0
                for act_col in row.keys():
                    if not isinstance(act_col, str) or act_col in used_actual:
                        continue
                    score = SequenceMatcher(
                        a=_norm_token(exp_col),
                        b=_norm_token(act_col),
                    ).ratio()
                    if score > best_score:
                        best_score = score
                        best_key = act_col
                if best_key is not None and best_score >= 0.72:
                    mapped[exp_col] = row.get(best_key)
                    used_actual.add(best_key)
            normalized_rows.append(mapped)
        actual_tables[table_idx]["columns"] = expected_columns
        actual_tables[table_idx]["rows"] = normalized_rows
    payload["tables"] = actual_tables
    return payload


def _norm_token(value: str) -> str:
    text = value.lower()
    text = text.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    text = text.replace("ñ", "n")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _fill_down_column(payload: dict[str, Any], *, table_index: int, column_name: str) -> None:
    tables = payload.get("tables")
    if not isinstance(tables, list) or table_index >= len(tables):
        return
    table = tables[table_index]
    if not isinstance(table, dict):
        return
    rows = table.get("rows")
    if not isinstance(rows, list):
        return
    last_value: str | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = row.get(column_name)
        if isinstance(value, str) and value.strip() and value.strip() != "-":
            last_value = value
            continue
        if value in (None, "", "-") and last_value is not None:
            row[column_name] = last_value


def _collapse_fields_to_single_entry(payload: dict[str, Any]) -> None:
    fields = payload.get("fields")
    if not isinstance(fields, list) or len(fields) <= 1:
        return
    values: list[str] = []
    base_label = None
    for item in fields:
        if not isinstance(item, dict):
            continue
        if base_label is None and isinstance(item.get("label"), str):
            base_label = item["label"]
        value = item.get("value")
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    if not values:
        return
    payload["fields"] = [
        {
            "field": "item_1",
            "label": base_label or "REGLAMENTO LOCAL DEL AERÓDROMO / LOCAL AERODROME REGULATIONS",
            "value": " ".join(values),
        }
    ]


def _normalize_ad224_chart_rows(payload: dict[str, Any]) -> None:
    tables = payload.get("tables")
    if not isinstance(tables, list) or not tables:
        return
    table = tables[0]
    if not isinstance(table, dict):
        return
    columns = table.get("columns")
    if isinstance(columns, list) and {
        "Categoria / Category",
        "Carta / Chart",
        "Pista / RWY",
        "Codigo / Code",
    }.issubset({str(column) for column in columns}):
        return
    rows = table.get("rows")
    if not isinstance(rows, list):
        return
    chart_key = None
    for k in table.get("columns", []):
        if isinstance(k, str) and "chart" in k.lower():
            chart_key = k
            break
    if chart_key is None:
        return

    prefix = "Cartas de aproximacion por instrumentos - OACI / Instrument Approach Chart - ICAO"
    for row in rows:
        if not isinstance(row, dict):
            continue
        chart = row.get(chart_key)
        if not isinstance(chart, str):
            continue
        if "VOR" in chart and prefix not in chart:
            row[chart_key] = f"{prefix} {chart}".strip()

    base_rows: list[dict[str, Any]] = []
    approach_rows: list[dict[str, Any]] = []
    code_key = next((k for k in table.get("columns", []) if isinstance(k, str) and "code" in k.lower()), None)
    if code_key is None:
        return

    for row in rows:
        if not isinstance(row, dict):
            continue
        chart = row.get(chart_key)
        code = row.get(code_key)
        if not isinstance(chart, str):
            continue
        normalized = " ".join(chart.split())
        vor_match = re.search(r"\b(VOR\s*Z?|NDB|RNAV)\b.*?(PISTA\s*/?\s*RWY\s*\d{2})", normalized, re.IGNORECASE)
        if vor_match and isinstance(code, str):
            procedure = vor_match.group(1).upper().replace("  ", " ").strip()
            rw = vor_match.group(2).upper().replace("PISTA / RWY", "PISTA/RWY").replace("PISTA/ RWY", "PISTA/RWY").strip()
            approach_rows.append(
                {
                    "Procedimiento / Procedure": procedure,
                    "Pista / RWY": rw,
                    "Codigo / Code": code,
                }
            )
            continue
        base_rows.append(
            {
                "Carta / Chart": chart,
                "Codigo / Code": code,
            }
        )

    table["columns"] = ["Carta / Chart", "Codigo / Code"]
    table["rows"] = base_rows
    table["label"] = "CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME"

    if approach_rows:
        payload["tables"] = [
            table,
            {
                "name": "table_2",
                "label": "Cartas de aproximacion por instrumentos - OACI / Instrument Approach Chart - ICAO",
                "columns": ["Procedimiento / Procedure", "Pista / RWY", "Codigo / Code"],
                "rows": approach_rows,
            },
        ]


def _coerce_json_payload(content: str) -> str:
    text = content.strip()

    fenced = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()

    if text.startswith("{") or text.startswith("["):
        return text

    object_start = text.find("{")
    object_end = text.rfind("}")
    if object_start != -1 and object_end != -1 and object_end > object_start:
        return text[object_start : object_end + 1].strip()

    array_start = text.find("[")
    array_end = text.rfind("]")
    if array_start != -1 and array_end != -1 and array_end > array_start:
        return text[array_start : array_end + 1].strip()

    return text


def _sanitize_llm_payload(content: str, *, section_id: str) -> dict[str, Any]:
    """Coerce common LLM nulls into schema-compatible empty strings.

    OpenAI-compatible models sometimes emit ``null`` for required documentary
    string metadata such as ``schema`` or table ``label``. The actual row values
    may still be unknown, so those remain nullable.
    """

    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("Structured LLM response must be a JSON object")

    if payload.get("section_id") is None:
        payload["section_id"] = section_id
    if payload.get("schema") is None:
        payload["schema"] = "generic-field-value-v1"

    fields = payload.get("fields")
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            for key in ("field", "label", "value"):
                if field.get(key) is None:
                    field[key] = ""

    tables = payload.get("tables")
    if isinstance(tables, list):
        for idx, table in enumerate(tables, start=1):
            if not isinstance(table, dict):
                continue
            if table.get("name") is None:
                table["name"] = f"table_{idx}"
            if table.get("label") is None:
                table["label"] = ""
            columns = table.get("columns")
            if isinstance(columns, list):
                table["columns"] = ["" if col is None else str(col) for col in columns]
    return payload


async def enrich_aerodrome_document(
    aerodrome_doc: AerodromeDocument,
    section_ids: list[str] | None = None,
    *,
    save: bool = True,
) -> AerodromeDocument:
    selected = section_ids or list(TARGET_SECTION_IDS)
    provider = get_llm_provider()

    for section in aerodrome_doc.current.ad_sections:
        if section.section_id not in selected:
            continue

        section_blocks = _resolve_section_blocks(section)

        resolved_title = _resolve_section_title_runtime(
            icao=aerodrome_doc.icao,
            section_id=section.section_id,
            raw_text=section.raw_text,
        )
        if resolved_title and getattr(section, "section_title", None) != resolved_title:
            section.section_title = resolved_title

        source_hash = _sha256(section.raw_text)
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        if extraction.get("raw_text_sha256") == source_hash and extraction.get("status") == "ok":
            continue

        metadata = {
            "engine": provider.engine_name,
            "model": provider.model_name,
            "prompt_version": PROMPT_VERSION,
            "raw_text_sha256": source_hash,
            "extracted_at": _utc_iso(),
            "status": "ok",
            "error": None,
        }

        try:
            key, payload = await asyncio.to_thread(
                _extract_for_section,
                aerodrome_doc.icao,
                section.section_id,
                section.raw_text,
                provider=provider,
                section_blocks=section_blocks,
            )
            if _is_partial_extraction(section.section_id, payload, section_blocks):
                metadata["status"] = "partial"
                metadata["error"] = "layout_contains_tables_but_payload_has_no_structured_content"
            if key == "__self__":
                section.data = {**payload, "_layout": {"version": "v1", "source": _layout_source(section), "section_blocks": section_blocks}, "_extraction": metadata}
            else:
                section.data = {key: payload, "_layout": {"version": "v1", "source": _layout_source(section), "section_blocks": section_blocks}, "_extraction": metadata}
        except ValidationError as exc:
            metadata["status"] = "error"
            metadata["error"] = f"validation_error: {exc}"
            section.data = {"_layout": {"version": "v1", "source": _layout_source(section), "section_blocks": section_blocks}, "_extraction": metadata}
            logger.warning(
                "aerodrome.enrichment.section_failed icao=%s section=%s validation: %s",
                aerodrome_doc.icao,
                section.section_id,
                exc,
            )
        except Exception as exc:
            metadata["status"] = "error"
            metadata["error"] = str(exc)
            section.data = {"_layout": {"version": "v1", "source": _layout_source(section), "section_blocks": section_blocks}, "_extraction": metadata}
            logger.warning(
                "aerodrome.enrichment.section_failed icao=%s section=%s: %s",
                aerodrome_doc.icao,
                section.section_id,
                exc,
            )

    if save:
        await aerodrome_doc.save()
    return aerodrome_doc


def _resolve_section_title_runtime(*, icao: str, section_id: str, raw_text: str) -> str | None:
    # Prefer title extracted from current raw section header.
    first_line = ""
    for line in raw_text.splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break
    if first_line:
        compact_id = section_id.lower().replace(" ", "")
        lowered = first_line.lower().replace(" ", "")
        idx = lowered.find(compact_id)
        if idx != -1:
            suffix = first_line[idx:]
            suffix = re.sub(r"(?i)^\|?\s*AD\s*2\.\d{1,2}\s*", "", suffix).strip(" |-:")
            # Stop at first numbered item marker to avoid capturing full raw content.
            suffix = re.split(r"\s{2,}\d{1,2}\s+", suffix, maxsplit=1)[0].strip()
            if suffix:
                return suffix

    # Fallback to contract table label only (never first field label).
    contract = load_ad2_contract(icao, section_id)
    if contract is not None and isinstance(contract.expected, dict):
        tables = contract.expected.get("tables")
        if isinstance(tables, list) and tables:
            first = tables[0]
            if isinstance(first, dict) and isinstance(first.get("label"), str) and first["label"].strip():
                return first["label"].strip()
    return None


async def enrich_aerodrome(icao: str, section_ids: list[str] | None = None) -> AerodromeDocument | None:
    doc = await AerodromeDocument.get(icao.strip().upper())
    if doc is None:
        return None
    return await enrich_aerodrome_document(doc, section_ids=section_ids)


def _inject_layout_tables(
    payload: dict[str, Any],
    section_blocks: list[dict[str, Any]] | None,
    *,
    section_id: str,
    raw_text: str,
) -> dict[str, Any]:
    if not _raw_text_looks_tabular(raw_text):
        return payload

    candidates = extract_table_groups_from_blocks(section_blocks)
    if not candidates:
        return payload

    if section_id in {"AD 2.12", "AD 2.14", "AD 2.18", "AD 2.24"}:
        patched = dict(payload)
        patched["fields"] = []
        patched["tables"] = candidates
        patched["schema"] = payload.get("schema") or "GenericAd2SectionData"
        return patched

    current = payload.get("tables")
    if not isinstance(current, list) or not current:
        patched = dict(payload)
        patched["tables"] = candidates
        return patched

    numeric_like = 0
    total = 0
    for table in current:
        if not isinstance(table, dict):
            continue
        cols = table.get("columns") if isinstance(table.get("columns"), list) else []
        for c in cols:
            if isinstance(c, str):
                total += 1
                if c.strip().isdigit() or c.strip().lower().startswith("col_"):
                    numeric_like += 1
    if total > 0 and numeric_like / total >= 0.6:
        patched = dict(payload)
        patched["tables"] = candidates
        return patched
    return payload


def _resolve_section_blocks(section: Any) -> list[dict[str, Any]]:
    anchors = getattr(section, "anchors", None)
    if isinstance(anchors, dict):
        blocks = anchors.get("section_blocks")
        if isinstance(blocks, list) and blocks:
            normalized: list[dict[str, Any]] = []
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                tagged_sid = b.get("section_id")
                if isinstance(tagged_sid, str) and tagged_sid != getattr(section, "section_id", None):
                    continue
                btype = "paragraph"
                if b.get("type") in {"TableItem", "table"}:
                    btype = "table"
                elif str(b.get("label", "")).upper() == "SECTION_HEADER" or b.get("source_class") == "section-header":
                    btype = "heading"
                text = b.get("text") if isinstance(b.get("text"), str) else ""
                obj: dict[str, Any] = {
                    "type": btype,
                    "text": text,
                    "page": b.get("page"),
                    "bbox": b.get("bbox"),
                    "order": int(b.get("index", len(normalized) + 1)),
                }
                if btype == "table" and isinstance(b.get("table"), dict):
                    table = b["table"]
                    label = table.get("label") if isinstance(table.get("label"), str) else ""
                    cols = table.get("columns") if isinstance(table.get("columns"), list) else []
                    rows = table.get("rows") if isinstance(table.get("rows"), list) else []
                    cells = [str(v) for r in rows if isinstance(r, dict) for v in r.values()]
                    obj["table"] = {"label": label, "columns": cols, "rows": rows, "cells": cells}
                normalized.append(obj)
            if normalized:
                return normalized
    return build_section_blocks(section.raw_text)


def _layout_source(section: Any) -> str:
    anchors = getattr(section, "anchors", None)
    if isinstance(anchors, dict) and isinstance(anchors.get("section_blocks"), list) and anchors.get("section_blocks"):
        return "section_blocks"
    return "raw_text"


def _raw_text_looks_tabular(raw_text: str) -> bool:
    if not isinstance(raw_text, str):
        return False
    text = raw_text
    if "|" in text:
        return True
    if re.search(r"(?im)^\s*\d+\s+\d+\s+\d+", text):
        return True
    if re.search(r"(?i)\b(rwy|designador|pista|frecuencia|observaciones|remarks|coordinates|coordenadas)\b", text):
        return True
    return False


def _normalize_single_answer_section(payload: dict[str, Any], *, label_hint: str) -> None:
    fields = payload.get("fields") if isinstance(payload.get("fields"), list) else []
    if fields:
        payload["fields"] = [_normalize_single_answer_field(fields[0], label_hint=label_hint)]
        payload["tables"] = []
        return
    tables = payload.get("tables") if isinstance(payload.get("tables"), list) else []
    if not tables:
        return
    first_table = tables[0] if isinstance(tables[0], dict) else {}
    rows = first_table.get("rows") if isinstance(first_table.get("rows"), list) else []
    if not rows:
        return
    first_row = rows[0] if isinstance(rows[0], dict) else {}
    value = next((v for v in first_row.values() if isinstance(v, str) and v.strip()), None)
    if value is None:
        return
    payload["fields"] = [{"field": "item_1", "label": label_hint, "value": value.strip()}]
    payload["tables"] = []


def _normalize_single_answer_field(field: Any, *, label_hint: str) -> dict[str, str | None]:
    if not isinstance(field, dict):
        return {"field": "item_1", "label": label_hint, "value": None}
    value = field.get("value")
    if not isinstance(value, str) or not value.strip():
        value = field.get("label") if isinstance(field.get("label"), str) else None
    return {
        "field": str(field.get("field") or "item_1"),
        "label": label_hint,
        "value": value.strip() if isinstance(value, str) else None,
    }


def _is_partial_extraction(section_id: str, payload: dict[str, Any], section_blocks: list[dict[str, Any]] | None) -> bool:
    if not isinstance(section_blocks, list) or not section_blocks:
        return False
    has_table_blocks = any(isinstance(b, dict) and b.get("type") == "table" for b in section_blocks)
    if not has_table_blocks:
        return False
    fields = payload.get("fields") if isinstance(payload.get("fields"), list) else []
    tables = payload.get("tables") if isinstance(payload.get("tables"), list) else []
    has_fields = any(isinstance(f, dict) and (f.get("label") or f.get("value")) for f in fields)
    has_table_rows = any(isinstance(t, dict) and isinstance(t.get("rows"), list) and len(t.get("rows")) > 0 for t in tables)
    if section_id == "AD 2.17":
        return not has_table_rows
    return (not has_fields) and (not has_table_rows)
