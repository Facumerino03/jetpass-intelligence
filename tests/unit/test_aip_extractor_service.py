from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.services.aip_extractor.service import (
    AipExtractorError,
    parse_aerodrome_from_ad_extractor,
)


def _pipeline_result(*, sections_extracted: dict | None = None) -> dict:
    extracted = sections_extracted or {
        "AD 2.1": {
            "location_indicator": "SAMR",
            "aerodrome_name": "SAN RAFAEL / S. A. SANTIAGO GERMANO",
            "airport_type": "AD",
            "remarks": None,
        },
        "AD 2.2": {"arp_coordinates": "345000S 0683000W"},
        "AD 2.3": {"services": []},
        "AD 2.4": {"facilities": []},
        "AD 2.12": {"runways": []},
        "AD 2.13": {"entries": []},
        "AD 2.18": {"facilities": []},
        "AD 2.19": {"aids": []},
    }
    return {
        "source_pdf": "/tmp/SAMR_AD-2.0.pdf",
        "sections_found": sorted(extracted.keys()),
        "sections_extracted": extracted,
        "sections_skipped": [],
        "errors": {},
    }


def test_parse_aerodrome_from_ad_extractor_maps_sections() -> None:
    pdf_path = Path("SAMR_AD-2.0.pdf")

    with patch(
        "app.services.aip_extractor.service.run_pipeline",
        return_value=_pipeline_result(),
    ):
        result = parse_aerodrome_from_ad_extractor(pdf_path, icao="SAMR")

    assert result.icao_code == "SAMR"
    assert result.name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.full_name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.source_document == "SAMR_AD-2.0.pdf"
    assert result.downloaded_by == "ad_extractor"
    assert [section.section_id for section in result.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
    assert result.ad_sections[0].section_title == "AERODROME LOCATION AND NAME"
    assert result.ad_sections[0].data["_extraction"] == {
        "engine": "ad_extractor",
        "source_document": "SAMR_AD-2.0.pdf",
        "status": "ok",
    }


def test_parse_aerodrome_from_ad_extractor_uses_icao_fallback_when_name_missing() -> None:
    pdf_path = Path("SAMR_AD-2.0.pdf")
    payload = _pipeline_result()
    payload["sections_extracted"]["AD 2.1"] = {"location_indicator": "SAMR"}

    with patch(
        "app.services.aip_extractor.service.run_pipeline",
        return_value=payload,
    ):
        result = parse_aerodrome_from_ad_extractor(pdf_path, icao="SAMR")

    assert result.name == "SAMR"
    assert result.full_name == "SAMR"


def test_parse_aerodrome_from_ad_extractor_raises_when_no_sections_extracted() -> None:
    pdf_path = Path("SAMR_AD-2.0.pdf")

    with patch(
        "app.services.aip_extractor.service.run_pipeline",
        return_value={
            "source_pdf": str(pdf_path),
            "sections_found": [],
            "sections_extracted": {},
            "sections_skipped": list(OPERATIONAL_AD_SECTION_IDS),
            "errors": {},
        },
    ):
        with pytest.raises(AipExtractorError, match="No AD 2.x sections extracted"):
            parse_aerodrome_from_ad_extractor(pdf_path, icao="SAMR")


def test_parse_aerodrome_from_ad_extractor_allows_partial_sections() -> None:
    pdf_path = Path("SAMR_AD-2.0.pdf")
    payload = _pipeline_result()
    payload["sections_extracted"] = {
        "AD 2.1": payload["sections_extracted"]["AD 2.1"],
        "AD 2.12": payload["sections_extracted"]["AD 2.12"],
    }
    payload["sections_skipped"] = ["AD 2.2", "AD 2.3"]
    payload["errors"] = {"AD 2.19": "LLM validation failed"}

    with patch(
        "app.services.aip_extractor.service.run_pipeline",
        return_value=payload,
    ):
        result = parse_aerodrome_from_ad_extractor(pdf_path, icao="SAMR")

    assert [section.section_id for section in result.ad_sections] == ["AD 2.1", "AD 2.12"]
