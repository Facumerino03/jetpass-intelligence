from __future__ import annotations

from app.intelligence.fpl_rules.coordinates import format_oaci_coordinates


def test_format_oaci_coordinates_southern_western_hemisphere() -> None:
    assert format_oaci_coordinates(-32.8317, -68.7928) == "3250S06848W"


def test_format_oaci_coordinates_northern_eastern_hemisphere() -> None:
    assert format_oaci_coordinates(35.6762, 139.6503) == "3541N13939E"
