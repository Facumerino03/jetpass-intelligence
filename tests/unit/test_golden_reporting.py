"""Unit tests for section report generation."""

from __future__ import annotations

from pathlib import Path

from tests.unit.golden.contracts import FieldRule, RulesFile
from tests.unit.golden.evaluator import evaluate_section
from tests.unit.golden.reporting import section_report_to_dict, write_section_report_json


def test_evaluate_section_sets_warn_without_high_failures() -> None:
    rules = RulesFile(
        section_id="AD 2.1",
        version="1.0",
        fields=[
            FieldRule(field="identity.location_indicator", type="text", criticality="high", expected="SAMR"),
            FieldRule(field="identity.name", type="text", criticality="medium", expected="SAN RAFAEL"),
        ],
    )
    report = evaluate_section(
        icao="SAMR",
        rules=rules,
        actual_data={"identity": {"location_indicator": "SAMR", "name": "BAD"}},
        raw_text="... SAN RAFAEL ...",
    )
    assert report.status == "warn"
    assert report.summary["high_failed"] == 0
    assert report.summary["medium_failed"] == 1


def test_write_section_report_json(tmp_path: Path) -> None:
    rules = RulesFile(
        section_id="AD 2.1",
        version="1.0",
        fields=[FieldRule(field="identity.location_indicator", type="text", criticality="high", expected="SAMR")],
    )
    report = evaluate_section(
        icao="SAMR",
        rules=rules,
        actual_data={"identity": {"location_indicator": "SAMR"}},
        raw_text="SAMR",
    )

    payload = section_report_to_dict(report)
    path = write_section_report_json(report, output_dir=tmp_path)

    assert payload["status"] == "pass"
    assert path.exists()
