"""Golden evaluation suite for AD 2.x sections using real LLM enrichment."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from tests.unit.golden.enrichment_runner import run_section_enrichment_with_golden_schema
from tests.unit.golden.evaluator import evaluate_section
from tests.unit.golden.fixtures_loader import load_expected, load_rules, validate_fixture_is_meaningful
from tests.unit.golden.reporting import write_section_report_json

SECTION_IDS = [f"AD 2.{idx}" for idx in range(1, 26)]


def _is_enabled() -> bool:
    return os.getenv("RUN_LLM_GOLDEN", "0") == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("section_id", SECTION_IDS)
async def test_llm_golden_section_evaluation(section_id: str) -> None:
    if not _is_enabled():
        pytest.skip("Set RUN_LLM_GOLDEN=1 to run real LLM golden tests")

    icao = os.getenv("AIP_GOLDEN_ICAO", "SAMR").strip().upper()
    raw_path = Path("tests/golden") / icao / f"{section_id.replace(' ', '_').replace('.', '_')}.raw.txt"
    if not raw_path.exists():
        pytest.skip(f"Raw text fixture missing: {raw_path}")

    rules = load_rules(icao, section_id)
    expected_data = load_expected(icao, section_id)
    validate_fixture_is_meaningful(expected_data, rules)
    raw_text = raw_path.read_text(encoding="utf-8")
    # Keep a document object creation for compatibility with existing test DB setup,
    # but use golden-schema extraction to force canonical output structure.
    doc = AerodromeDocument(
        id=icao,
        icao=icao,
        name=icao,
        full_name=icao,
        current=AerodromeSnapshot(
            ad_sections=[AdSection(section_id=section_id, title=section_id, raw_text=raw_text, data={})],
            _meta=DocumentMeta(airac_cycle="golden-test"),
        ),
        history=[],
    )
    await doc.insert()

    run_result = await run_section_enrichment_with_golden_schema(
        icao=icao,
        section_id=section_id,
        raw_text=raw_text,
        expected_data=expected_data,
    )
    report = evaluate_section(
        icao=icao,
        rules=rules,
        actual_data=run_result.actual_data,
        raw_text=run_result.raw_text,
        expected_data=expected_data,
    )
    write_section_report_json(
        report,
        output_dir=Path("tmp/reports") / icao,
    )

    assert report.summary["high_failed"] == 0, (
        f"{icao} {section_id} has high-critical failures. "
        f"See tmp/reports/{icao}/{section_id.replace(' ', '_').replace('.', '_')}.report.json"
    )
