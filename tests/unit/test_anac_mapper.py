"""Unit tests for ANAC MADHEL payload mapping."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.intelligence.geo.anac_mapper import (
    map_detail_to_entry,
    map_list_item_to_entry,
    parse_control_from_identifier,
    parse_display_name,
    parse_icao_from_identifier,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "anac"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_parse_display_name():
    assert parse_display_name(
        "SANTA FE / SAUCE VIEJO - (SVO / SAAV) - DRCE - PÚBLICO CONTROLADO",
    ) == "SANTA FE / SAUCE VIEJO"


def test_parse_icao_from_identifier_with_slash():
    assert parse_icao_from_identifier(
        "SANTA FE / SAUCE VIEJO - (SVO / SAAV) - DRCE - PÚBLICO CONTROLADO",
    ) == "SAAV"


def test_parse_icao_from_identifier_without_slash():
    assert parse_icao_from_identifier(
        "CORONEL BOGADO / AGROSERVICIOS - (ACB) - DRCE - PRIVADO NO CONTROLADO",
    ) is None


def test_parse_control_from_identifier():
    assert parse_control_from_identifier(
        "GENERAL ACHA - (ACH / SAEA) - DRCE - PÚBLICO NO CONTROLADO",
    ) == "NON-CONTROLLED"
    assert parse_control_from_identifier(
        "SANTA FE / SAUCE VIEJO - (SVO / SAAV) - DRCE - PÚBLICO CONTROLADO",
    ) == "CONTROLLED"


def test_map_list_item_controlled_with_icao():
    entry = map_list_item_to_entry(_load("svo_list_item.json"))
    assert entry is not None
    assert entry.local_identifier == "SVO"
    assert entry.icao_code == "SAAV"
    assert entry.is_controlled is True
    assert entry.control_status == "CONTROLLED"
    assert entry.latitude == pytest.approx(-31.7108333333333)
    assert entry.longitude == pytest.approx(-60.8113888888889)


def test_map_list_item_non_controlled_with_optional_icao_in_text():
    entry = map_list_item_to_entry(_load("ach_list_item.json"))
    assert entry is not None
    assert entry.local_identifier == "ACH"
    assert entry.icao_code == "SAEA"
    assert entry.is_controlled is False


def test_map_list_item_without_icao():
    entry = map_list_item_to_entry(_load("acb_list_item.json"))
    assert entry is not None
    assert entry.local_identifier == "ACB"
    assert entry.icao_code is None
    assert entry.is_controlled is False


def test_map_list_item_excludes_helipuerto():
    assert map_list_item_to_entry(_load("hmd_list_item.json")) is None


def test_map_controlled_aerodrome_with_icao_detail():
    entry = map_detail_to_entry(_load("svo_detail.json"))
    assert entry is not None
    assert entry.local_identifier == "SVO"
    assert entry.icao_code == "SAAV"
    assert entry.iata_code == "SFN"
    assert entry.is_controlled is True
    assert entry.control_status == "CONTROLLED"
    assert entry.traffic_type == "NTL"
    assert entry.is_active is True
    assert entry.latitude == pytest.approx(-31.7108333333333)
    assert entry.longitude == pytest.approx(-60.8113888888889)


def test_map_non_controlled_without_icao_detail():
    entry = map_detail_to_entry(_load("ach_detail.json"))
    assert entry is not None
    assert entry.local_identifier == "ACH"
    assert entry.icao_code is None
    assert entry.is_controlled is False
    assert entry.control_status == "NON-CONTROLLED"


def test_helipuerto_is_filtered_out_detail():
    assert map_detail_to_entry(_load("hmd_detail.json")) is None


def test_closed_aerodrome_is_inactive_detail():
    detail = _load("ach_detail.json")
    detail["human_readable_identifier"] += " [** AD CERRADO (CLSD) **]"
    entry = map_detail_to_entry(detail)
    assert entry is not None
    assert entry.is_active is False
