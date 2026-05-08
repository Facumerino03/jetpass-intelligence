from __future__ import annotations

import pytest

from app.schemas.ad_sections import (
    FLIGHT_PLANNING_AD_SECTION_IDS,
    OPERATIONAL_AD_SECTION_IDS,
    filter_operational_sections,
    normalize_ad_section_id,
    validate_operational_section_ids,
)
from app.schemas.aerodrome import SectionSchema


def _section(section_id: str) -> SectionSchema:
    return SectionSchema(
        section_id=section_id,
        title=section_id,
        raw_text=f"{section_id} raw text",
    )


def test_operational_section_ids_are_product_scope() -> None:
    assert OPERATIONAL_AD_SECTION_IDS == FLIGHT_PLANNING_AD_SECTION_IDS
    assert OPERATIONAL_AD_SECTION_IDS == (
        "AD 2.1",
        "AD 2.2",
        "AD 2.3",
        "AD 2.4",
        "AD 2.12",
        "AD 2.13",
        "AD 2.19",
    )


def test_normalize_ad_section_id_handles_case_and_spacing() -> None:
    assert normalize_ad_section_id(" ad   2.12 ") == "AD 2.12"
    assert normalize_ad_section_id("AD  2.18") == "AD 2.18"


def test_filter_operational_sections_preserves_product_order() -> None:
    sections = [_section(f"AD 2.{idx}") for idx in range(1, 26)]

    filtered = filter_operational_sections(sections)

    assert [section.section_id for section in filtered] == list(OPERATIONAL_AD_SECTION_IDS)


def test_filter_operational_sections_accepts_non_canonical_input_order() -> None:
    sections = [
        _section("AD 2.24"),
        _section("ad 2.19"),
        _section("AD 2.1"),
        _section("AD 2.12"),
    ]

    filtered = filter_operational_sections(sections)

    assert [section.section_id for section in filtered] == ["AD 2.1", "AD 2.12", "ad 2.19"]


def test_validate_operational_section_ids_reports_missing_and_unexpected() -> None:
    ids = [sid for sid in OPERATIONAL_AD_SECTION_IDS if sid != "AD 2.19"]
    ids.append("AD 2.24")

    with pytest.raises(
        ValueError,
        match=r"Missing required operational AD 2\.x sections: \['AD 2\.19'\].*Unexpected non-operational AD 2\.x sections: \['AD 2\.24'\]",
    ):
        validate_operational_section_ids(ids)
