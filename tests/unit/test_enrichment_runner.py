"""Unit tests for section-level enrichment runner."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from tests.unit.golden.enrichment_runner import (
    _coerce_json_payload,
    run_section_enrichment,
    run_section_enrichment_with_golden_schema,
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
async def test_run_section_enrichment_returns_section_data_and_metadata() -> None:
    doc = _doc()

    def fake_extract(*_args, **_kwargs):
        return "runways", {"runways": [{"designator": "09/27"}]}

    with patch("app.services.enrichment.aerodrome_enricher._extract_for_section", side_effect=fake_extract):
        result = await run_section_enrichment(doc, "AD 2.12")

    assert result.section_id == "AD 2.12"
    assert result.raw_text == "Raw text for AD 2.12"
    assert result.actual_data["runways"]["runways"][0]["designator"] == "09/27"
    assert result.extraction["status"] == "ok"


@pytest.mark.asyncio
async def test_run_section_enrichment_with_golden_schema_uses_expected_structure() -> None:
    class _FakeProvider:
        engine_name = "fake"
        model_name = "fake-model"
        _calls = 0

        def chat_structured(self, **_kwargs):
            self._calls += 1
            if self._calls == 1:
                return '{"identity":{"location_indicator":"SAMR"}}'
            return '{"identity":{"location_indicator":"SAMR"}}'

    with patch(
        "tests.unit.golden.enrichment_runner.get_llm_provider",
        return_value=_FakeProvider(),
    ):
        result = await run_section_enrichment_with_golden_schema(
            icao="SAMR",
            section_id="AD 2.1",
            raw_text="raw",
            expected_data={"identity": {"location_indicator": "SAMR"}},
        )

    assert result.actual_data["identity"]["location_indicator"] == "SAMR"
    assert result.extraction["mode"] == "golden_schema"


@pytest.mark.asyncio
async def test_run_section_enrichment_with_golden_schema_repairs_orthography() -> None:
    class _FakeProvider:
        engine_name = "fake"
        model_name = "fake-model"

        def __init__(self) -> None:
            self.calls = 0

        def chat_structured(self, **_kwargs):
            self.calls += 1
            return '{"administrative":{"ad_head_office":{"name_es":"ADMINISTRACION NACIONAL DE AVIACION CIVIL"}}}'

    fake_provider = _FakeProvider()

    with patch(
        "tests.unit.golden.enrichment_runner.get_llm_provider",
        return_value=fake_provider,
    ):
        result = await run_section_enrichment_with_golden_schema(
            icao="SAMR",
            section_id="AD 2.2",
            raw_text="ADMINISTRACIÓN NACIONAL DE AVIACIÓN CIVIL",
            expected_data={"administrative": {"ad_head_office": {"name_es": "ADMINISTRACIÓN"}}},
        )

    assert fake_provider.calls == 1
    assert (
        result.actual_data["administrative"]["ad_head_office"]["name_es"]
        == "ADMINISTRACIÓN NACIONAL DE AVIACIÓN CIVIL"
    )


def test_coerce_json_payload_handles_fenced_json() -> None:
    raw = """```json
{"vss_penetration":{"value":"No"}}
```"""
    assert _coerce_json_payload(raw) == '{"vss_penetration":{"value":"No"}}'


def test_coerce_json_payload_handles_extra_preface_and_schema_blob() -> None:
    raw = (
        '{"type":"object","properties":{"x":{"type":"string"}}}\n'
        '{"x":"value"}'
    )
    # Keep last JSON value when schema + answer are concatenated.
    assert _coerce_json_payload(raw) == '{"x":"value"}'
