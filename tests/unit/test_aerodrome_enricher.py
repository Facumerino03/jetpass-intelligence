"""Unit tests for aerodrome enrichment tool."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from app.services.enrichment.aerodrome_enricher import (
    _coerce_json_payload,
    _inject_layout_tables,
    _postprocess_section_payload,
    _sanitize_llm_payload,
    enrich_aerodrome_document,
)


def _doc() -> AerodromeDocument:
    sections = []
    for idx in range(1, 26):
        sections.append(
            AdSection(
                section_id=f"AD 2.{idx}",
                title=f"AD 2.{idx}",
                raw_text=f"Raw text for AD 2.{idx}",
                data={},
            )
        )
    return AerodromeDocument(
        id="SAMR",
        icao="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        current=AerodromeSnapshot(ad_sections=sections, _meta=DocumentMeta(airac_cycle="2026-01")),
        history=[],
    )


@pytest.mark.asyncio
async def test_enricher_writes_data_for_target_section() -> None:
    doc = _doc()

    def fake_extract(*_args, **_kwargs):
        return "__self__", {
            "section_id": "AD 2.12",
            "schema": "generic-field-value-v1",
            "fields": [{"field": "item_1", "label": "Label", "value": "Value"}],
            "tables": [],
        }

    with patch(
        "app.services.enrichment.aerodrome_enricher._extract_for_section",
        side_effect=fake_extract,
    ):
        updated = await enrich_aerodrome_document(doc, section_ids=["AD 2.12"])

    section = next(s for s in updated.current.ad_sections if s.section_id == "AD 2.12")
    assert section.data["section_id"] == "AD 2.12"
    assert section.data["fields"][0]["field"] == "item_1"
    assert section.data["_extraction"]["status"] == "ok"


@pytest.mark.asyncio
async def test_enricher_skips_section_when_hash_matches() -> None:
    doc = _doc()
    target = next(s for s in doc.current.ad_sections if s.section_id == "AD 2.12")
    target.data = {
        "section_id": "AD 2.12",
        "schema": "generic-field-value-v1",
        "fields": [{"field": "item_1", "label": "Label", "value": "old"}],
        "tables": [],
        "_extraction": {
            "status": "ok",
            "raw_text_sha256": "",
        },
    }

    from app.services.enrichment.aerodrome_enricher import _sha256

    target.data["_extraction"]["raw_text_sha256"] = _sha256(target.raw_text)

    with patch("app.services.enrichment.aerodrome_enricher._extract_for_section") as extract_call:
        updated = await enrich_aerodrome_document(doc, section_ids=["AD 2.12"])

    extract_call.assert_not_called()
    section = next(s for s in updated.current.ad_sections if s.section_id == "AD 2.12")
    assert section.data["fields"][0]["value"] == "old"


@pytest.mark.asyncio
async def test_enricher_records_error_without_raising() -> None:
    doc = _doc()

    with patch(
        "app.services.enrichment.aerodrome_enricher._extract_for_section",
        side_effect=RuntimeError("model not found"),
    ):
        updated = await enrich_aerodrome_document(doc, section_ids=["AD 2.19"])

    section = next(s for s in updated.current.ad_sections if s.section_id == "AD 2.19")
    assert section.data["_extraction"]["status"] == "error"
    assert "model not found" in section.data["_extraction"]["error"]


def test_coerce_json_payload_strips_markdown_fence() -> None:
    raw = """```json
{"location_indicator":"SAMR","aerodrome_name":"San Rafael"}
```"""

    payload = _coerce_json_payload(raw)

    assert payload == '{"location_indicator":"SAMR","aerodrome_name":"San Rafael"}'


def test_coerce_json_payload_extracts_json_from_preamble() -> None:
    raw = "Here is the extracted data:\n```json\n{\"served_city\":\"San Rafael\"}\n```"

    payload = _coerce_json_payload(raw)

    assert payload == '{"served_city":"San Rafael"}'


def test_sanitize_llm_payload_coerces_null_required_strings() -> None:
    raw = '{"section_id": null, "schema": null, "fields": [], "tables": [{"name": null, "label": null, "columns": [null, "A"], "rows": []}]}'

    payload = _sanitize_llm_payload(raw, section_id="AD 2.9")

    assert payload["section_id"] == "AD 2.9"
    assert payload["schema"] == "generic-field-value-v1"
    assert payload["tables"][0]["name"] == "table_1"
    assert payload["tables"][0]["label"] == ""
    assert payload["tables"][0]["columns"] == ["", "A"]


def test_postprocess_ad225_keeps_single_field_and_drops_table() -> None:
    payload = {
        "section_id": "AD 2.25",
        "schema": "generic-field-value-v1",
        "fields": [{"field": "vss", "label": "", "value": "No"}],
        "tables": [{"name": "table_1", "label": "", "columns": ["value"], "rows": [{"value": "No"}]}],
    }

    out = _postprocess_section_payload(
        section_id="AD 2.25",
        payload=payload,
        contract_expected=None,
    )

    assert out["tables"] == []
    assert out["fields"][0]["value"] == "No"
    assert "VISUAL SEGMENT SURFACE" in out["fields"][0]["label"]


def test_postprocess_ad224_preserves_canonical_layout_table() -> None:
    payload = {
        "section_id": "AD 2.24",
        "schema": "generic-field-value-v1",
        "fields": [],
        "tables": [
            {
                "name": "table_1",
                "label": "AD 2.24 CARTAS RELATIVAS AL AERODROMO / CHARTS RELATED TO THE AERODROME",
                "columns": ["Categoria / Category", "Carta / Chart", "Pista / RWY", "Codigo / Code"],
                "rows": [
                    {
                        "Categoria / Category": "Cartas de salida normalizada por instrumentos",
                        "Carta / Chart": "ATOVO 3A-BIVAM 3A-LANDA 3A",
                        "Pista / RWY": "Pista/RWY 11",
                        "Codigo / Code": "SAEZ AD 2.I1-I2",
                    }
                ],
            }
        ],
    }

    out = _postprocess_section_payload(
        section_id="AD 2.24",
        payload=payload,
        contract_expected=None,
    )

    assert out["tables"][0]["columns"] == ["Categoria / Category", "Carta / Chart", "Pista / RWY", "Codigo / Code"]
    assert out["tables"][0]["rows"][0]["Pista / RWY"] == "Pista/RWY 11"


def test_inject_layout_tables_overrides_ad212_llm_payload() -> None:
    payload = {
        "section_id": "AD 2.12",
        "schema": "GenericAd2SectionData",
        "fields": [{"field": "bad", "label": "bad", "value": "bad"}],
        "tables": [{"name": "bad", "label": "bad", "columns": ["bad"], "rows": []}],
    }
    blocks = [
        {
            "type": "table",
            "text": "AD 2.12",
            "table": {
                "label": "AD 2.12 CARACTERÍSTICAS FÍSICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS",
                "columns": ["RWY", "Dimensions of RWY (m)"],
                "rows": [{"RWY": "35", "Dimensions of RWY (m)": "3.105x45"}],
            },
        }
    ]

    out = _inject_layout_tables(
        payload,
        blocks,
        section_id="AD 2.12",
        raw_text="AD 2.12 | 35 | 3.105x45 |",
    )

    assert out["fields"] == []
    assert out["tables"][0]["label"].startswith("AD 2.12")
    assert out["tables"][0]["rows"][0]["Dimensions of RWY (m)"] == "3.105x45"
