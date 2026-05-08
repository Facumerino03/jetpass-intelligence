from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.services.documentai.aip_documentai import (
    DocumentAiAipError,
    build_aerodrome_from_schema_tree,
)


FIXTURE_PATH = Path(__file__).resolve().parents[2] / "SAMR_AD-2.0_extracted.json"


def _schema_tree() -> dict:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return payload["schema_tree"]


def test_build_aerodrome_from_schema_tree_filters_operational_sections() -> None:
    result = build_aerodrome_from_schema_tree(
        _schema_tree(),
        icao="SAMR",
        source_document="SAMR_AD-2.0.pdf",
        processor_id="processor-1",
        processor_version_id="version-1",
    )

    assert result.icao_code == "SAMR"
    assert result.name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.full_name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.source_document == "SAMR_AD-2.0.pdf"
    assert result.downloaded_by == "documentai"
    assert [section.section_id for section in result.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
    assert "runway" in result.ad_sections[4].data
    assert result.ad_sections[4].raw_text is None
    assert result.ad_sections[4].anchors is None


def test_build_aerodrome_from_schema_tree_preserves_documentai_payload() -> None:
    result = build_aerodrome_from_schema_tree(
        _schema_tree(),
        icao="SAMR",
        source_document="SAMR_AD-2.0.pdf",
        processor_id="processor-1",
        processor_version_id="version-1",
    )

    by_id = {section.section_id: section for section in result.ad_sections}
    ad_2_13 = by_id["AD 2.13"]

    assert ad_2_13.section_title == "DISTANCIAS DECLARADAS / DECLARED DISTANCES"
    assert ad_2_13.data["declared_distance"][0]["rwy_designator"]["mention_text"] == "11"
    assert ad_2_13.data["_extraction"] == {
        "engine": "documentai",
        "processor_id": "processor-1",
        "processor_version_id": "version-1",
        "source_document": "SAMR_AD-2.0.pdf",
        "status": "ok",
    }


def test_build_aerodrome_from_schema_tree_rejects_missing_required_section() -> None:
    schema_tree = dict(_schema_tree())
    schema_tree.pop("ad_2_19")

    with pytest.raises(DocumentAiAipError, match="Missing required operational AD 2.x sections"):
        build_aerodrome_from_schema_tree(
            schema_tree,
            icao="SAMR",
            source_document="SAMR_AD-2.0.pdf",
            processor_id="processor-1",
            processor_version_id=None,
        )


def test_build_aerodrome_from_schema_tree_rejects_icao_mismatch() -> None:
    with pytest.raises(DocumentAiAipError, match="Document AI ICAO mismatch"):
        build_aerodrome_from_schema_tree(
            _schema_tree(),
            icao="SAEZ",
            source_document="SAMR_AD-2.0.pdf",
            processor_id="processor-1",
            processor_version_id=None,
        )
