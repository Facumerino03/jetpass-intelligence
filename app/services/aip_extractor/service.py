"""Adapter from AIP extractor pipeline output to AerodromeCreate."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.schemas.aerodrome import AerodromeCreate, SectionSchema
from app.services.aip_extractor.pipeline import run as run_pipeline

logger = logging.getLogger(__name__)

AD_SECTION_TITLES = {
    "AD 2.1": "AERODROME LOCATION AND NAME",
    "AD 2.2": "AERODROME GEOGRAPHICAL AND ADMINISTRATIVE DATA",
    "AD 2.3": "OPERATIONAL HOURS",
    "AD 2.4": "HANDLING AND LOADING SERVICES AND FACILITIES",
    "AD 2.12": "RUNWAY PHYSICAL CHARACTERISTICS",
    "AD 2.13": "DECLARED DISTANCES",
    "AD 2.18": "ATS COMMUNICATION FACILITIES",
    "AD 2.19": "RADIO NAVIGATION AND LANDING AIDS",
}


class AipExtractorError(Exception):
    """Raised when AIP extractor output cannot be converted to AerodromeCreate."""


def _extraction_meta(*, source_document: str) -> dict[str, Any]:
    return {
        "engine": "ad_extractor",
        "source_document": source_document,
        "status": "ok",
    }


def _with_extraction(payload: dict[str, Any], extraction: dict[str, Any]) -> dict[str, Any]:
    return {**payload, "_extraction": extraction}


def _section_name(ad_2_1: dict[str, Any] | None, fallback: str) -> str:
    if not isinstance(ad_2_1, dict):
        return fallback
    name = ad_2_1.get("aerodrome_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return fallback


def _log_pipeline_warnings(icao: str, result: dict[str, Any]) -> None:
    for section_id, message in result.get("errors", {}).items():
        logger.warning("[%s] AIP extraction error in %s: %s", icao, section_id, message)

    for skipped in result.get("sections_skipped", []):
        logger.warning("[%s] AIP section skipped (not found in PDF): %s", icao, skipped)

    for field_error in result.get("field_errors", []):
        logger.warning(
            "[%s] AIP field extraction error in %s.%s: %s",
            icao,
            field_error.get("section"),
            field_error.get("field"),
            field_error.get("message"),
        )


def _build_sections(
    sections_extracted: dict[str, Any],
    *,
    source_document: str,
) -> list[SectionSchema]:
    extraction = _extraction_meta(source_document=source_document)
    sections: list[SectionSchema] = []

    for section_id in OPERATIONAL_AD_SECTION_IDS:
        payload = sections_extracted.get(section_id)
        if not isinstance(payload, dict):
            continue
        sections.append(
            SectionSchema(
                section_id=section_id,
                title=section_id,
                section_title=AD_SECTION_TITLES.get(section_id),
                data=_with_extraction(payload, extraction),
            )
        )

    return sections


def parse_aerodrome_from_ad_extractor(pdf_path: Path, *, icao: str) -> AerodromeCreate:
    """Run the AIP extractor pipeline and build AerodromeCreate for persistence."""
    requested_icao = icao.strip().upper()
    result = run_pipeline(pdf_path)
    _log_pipeline_warnings(requested_icao, result)

    sections_extracted = result.get("sections_extracted", {})
    if not isinstance(sections_extracted, dict) or not sections_extracted:
        raise AipExtractorError(
            f"No AD 2.x sections extracted from {pdf_path.name}"
        )

    ad_2_1 = sections_extracted.get("AD 2.1")
    name = _section_name(ad_2_1 if isinstance(ad_2_1, dict) else None, requested_icao)
    source_document = pdf_path.name

    return AerodromeCreate(
        icao_code=requested_icao,
        name=name,
        full_name=name,
        source_document=source_document,
        downloaded_by="ad_extractor",
        ad_sections=_build_sections(sections_extracted, source_document=source_document),
    )
