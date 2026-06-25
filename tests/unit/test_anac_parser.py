"""Unit tests for ANAC human-readable identifier parsing."""

from app.intelligence.geo.anac_mapper import parse_display_name


def test_parse_display_name_strips_metadata_suffix():
    assert parse_display_name(
        "CORONEL BOGADO / AGROSERVICIOS - (ACB) - DRCE - PRIVADO NO CONTROLADO",
    ) == "CORONEL BOGADO / AGROSERVICIOS"


def test_parse_icao_from_identifier():
    from app.intelligence.geo.anac_mapper import parse_icao_from_identifier

    assert parse_icao_from_identifier(
        "GENERAL ACHA - (ACH / SAEA) - DRCE - PÚBLICO NO CONTROLADO",
    ) == "SAEA"


def test_parse_display_name_handles_closed_tag():
    assert parse_display_name(
        "MORTEROS / CAMPO SAN JOSÉ - (CSJ) - DRNO - PRIVADO [** USO AGROAÉREO **]",
    ) == "MORTEROS / CAMPO SAN JOSÉ"
