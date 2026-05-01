"""Reporting helpers for golden section evaluation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.unit.golden.contracts import SectionReport


def section_report_to_dict(report: SectionReport) -> dict[str, Any]:
    return {
        "icao": report.icao,
        "section_id": report.section_id,
        "rules_version": report.rules_version,
        "status": report.status,
        "summary": report.summary,
        "results": [
            {
                "field": result.field,
                "status": result.status,
                "expected": result.expected,
                "actual": result.actual,
                "normalized_expected": result.normalized_expected,
                "normalized_actual": result.normalized_actual,
                "type": result.type,
                "criticality": result.criticality,
                "probable_cause": result.probable_cause,
                "evidence_in_raw_text": result.evidence_in_raw_text,
                "message": result.message,
            }
            for result in report.results
        ],
    }


def write_section_report_json(report: SectionReport, *, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{report.section_id.replace(' ', '_').replace('.', '_')}.report.json"
    output_path = output_dir / file_name
    output_path.write_text(json.dumps(section_report_to_dict(report), ensure_ascii=True, indent=2), encoding="utf-8")
    return output_path
