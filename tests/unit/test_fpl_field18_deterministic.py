from __future__ import annotations

import pytest

from app.intelligence.contracts import (
    Field18AerodromeContexts,
    Field18Intent,
    FlightPlanFields,
    FplAerodromeContext,
)
from app.intelligence.fpl_field18_intel_service import get_field18_intelligence
from app.intelligence.fpl_rules.coordinates import format_oaci_coordinates
from app.intelligence.fpl_rules.deterministic import compute_mandatory_indicators

MZA_LAT = -32.8317
MZA_LON = -68.7928
COR_LAT = -31.3236
COR_LON = -64.2081


def _mza_context() -> FplAerodromeContext:
    return FplAerodromeContext(
        fpl_code="MZA",
        local_identifier="MZA",
        icao_code=None,
        is_controlled=False,
        latitude=MZA_LAT,
        longitude=MZA_LON,
        name="Mendoza El Plumerillo",
    )


def _cor_context() -> FplAerodromeContext:
    return FplAerodromeContext(
        fpl_code="COR",
        local_identifier="COR",
        icao_code=None,
        is_controlled=False,
        latitude=COR_LAT,
        longitude=COR_LON,
        name="Córdoba",
    )


def _controlled_destination() -> FplAerodromeContext:
    return FplAerodromeContext(
        fpl_code="SAMR",
        local_identifier="MZA",
        icao_code="SAMR",
        is_controlled=True,
        latitude=MZA_LAT,
        longitude=MZA_LON,
        name="Mendoza",
    )


def test_typ_required_when_aircraft_type_is_zzzz() -> None:
    fields = FlightPlanFields(aircraft_type="ZZZZ", typ_detail="C172")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "TYP/"
    assert result.suggestions[0].full_field == "TYP/C172"
    assert result.suggestions[0].is_mandatory is True
    assert result.fpl_updates == []


def test_typ_not_generated_when_aircraft_type_is_known() -> None:
    fields = FlightPlanFields(aircraft_type="C172", typ_detail="SHOULD NOT APPEAR")
    result = compute_mandatory_indicators(fields)
    assert result.suggestions == []
    assert result.fpl_updates == []


def test_dep_required_when_departure_is_zzzz() -> None:
    fields = FlightPlanFields(departure_aerodrome="ZZZZ", dep_detail="4620S06630W")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "DEP/"
    assert result.suggestions[0].full_field == "DEP/4620S06630W"


def test_dep_required_when_departure_is_afil() -> None:
    fields = FlightPlanFields(departure_aerodrome="AFIL", dep_detail="4620S06630W")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "DEP/"
    assert "AFIL" in result.suggestions[0].reason


def test_dest_required_when_destination_is_zzzz() -> None:
    fields = FlightPlanFields(destination_aerodrome="ZZZZ", dest_detail="SAN RAFAEL")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "DEST/"
    assert result.suggestions[0].full_field == "DEST/SAN RAFAEL"


def test_altn_required_when_alternate_is_zzzz() -> None:
    fields = FlightPlanFields(alternate_aerodrome_1="ZZZZ", altn_detail="MENDOZA")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "ALTN/"
    assert result.suggestions[0].full_field == "ALTN/MENDOZA"


def test_altn_required_when_second_alternate_is_zzzz() -> None:
    fields = FlightPlanFields(alternate_aerodrome_2="ZZZZ", altn_detail="CORDOBA")
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "ALTN/"


def test_reg_required_when_registration_differs_from_identification() -> None:
    fields = FlightPlanFields(
        aircraft_identification="ARG1234",
        registration="LV-ABC",
    )
    result = compute_mandatory_indicators(fields)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "REG/"
    assert result.suggestions[0].full_field == "REG/LV-ABC"


def test_reg_not_generated_when_registration_matches_identification() -> None:
    fields = FlightPlanFields(
        aircraft_identification="ARG1234",
        registration="arg1234",
    )
    result = compute_mandatory_indicators(fields)
    assert result.suggestions == []



def test_multiple_mandatory_indicators_sorted_canonically() -> None:
    fields = FlightPlanFields(
        aircraft_type="ZZZZ",
        typ_detail="C172",
        departure_aerodrome="AFIL",
        dep_detail="4620S06630W",
        destination_aerodrome="SAMR",
    )
    result = compute_mandatory_indicators(fields)

    assert [item.indicator for item in result.suggestions] == ["DEP/", "TYP/"]


def test_format_oaci_coordinates_example_from_raac() -> None:
    assert format_oaci_coordinates(-46.333333, -66.5) == "4620S06630W"


def test_dep_non_controlled_local_generates_dep_and_fpl_update() -> None:
    fields = FlightPlanFields(departure_aerodrome="MZA", destination_aerodrome="SAMR")
    aerodromes = Field18AerodromeContexts(
        departure=_mza_context(),
        destination=_controlled_destination(),
    )
    expected_coords = format_oaci_coordinates(MZA_LAT, MZA_LON)

    result = compute_mandatory_indicators(fields, aerodromes)

    assert len(result.suggestions) == 1
    assert result.suggestions[0].indicator == "DEP/"
    assert result.suggestions[0].full_field == f"DEP/MZA{expected_coords}"
    assert len(result.fpl_updates) == 1
    assert result.fpl_updates[0].field == "departure_aerodrome"
    assert result.fpl_updates[0].from_value == "MZA"
    assert result.fpl_updates[0].to_value == "ZZZZ"


def test_controlled_icao_destination_generates_no_patch() -> None:
    fields = FlightPlanFields(departure_aerodrome="MZA", destination_aerodrome="SAMR")
    aerodromes = Field18AerodromeContexts(
        departure=_mza_context(),
        destination=_controlled_destination(),
    )

    result = compute_mandatory_indicators(fields, aerodromes)

    assert all(update.field != "destination_aerodrome" for update in result.fpl_updates)
    assert all(item.indicator != "DEST/" for item in result.suggestions)


def test_dest_non_controlled_local_generates_dest_and_fpl_update() -> None:
    fields = FlightPlanFields(destination_aerodrome="COR")
    aerodromes = Field18AerodromeContexts(destination=_cor_context())
    expected_coords = format_oaci_coordinates(COR_LAT, COR_LON)

    result = compute_mandatory_indicators(fields, aerodromes)

    assert result.suggestions[0].full_field == f"DEST/COR{expected_coords}"
    assert result.fpl_updates[0].field == "destination_aerodrome"
    assert result.fpl_updates[0].to_value == "ZZZZ"


def test_altn_non_controlled_local_generates_altn_and_fpl_update() -> None:
    fields = FlightPlanFields(alternate_aerodrome_1="COR")
    aerodromes = Field18AerodromeContexts(alternate_1=_cor_context())
    expected_coords = format_oaci_coordinates(COR_LAT, COR_LON)

    result = compute_mandatory_indicators(fields, aerodromes)

    assert result.suggestions[0].full_field == f"ALTN/COR{expected_coords}"
    assert result.fpl_updates[0].field == "alternate_aerodrome_1"


def test_non_controlled_without_aerodrome_context_is_ignored() -> None:
    fields = FlightPlanFields(departure_aerodrome="MZA")
    result = compute_mandatory_indicators(fields)
    assert result.suggestions == []
    assert result.fpl_updates == []


def test_departure_and_alternate_non_controlled_both_generate_updates() -> None:
    fields = FlightPlanFields(departure_aerodrome="MZA", alternate_aerodrome_1="COR")
    aerodromes = Field18AerodromeContexts(
        departure=_mza_context(),
        alternate_1=_cor_context(),
    )

    result = compute_mandatory_indicators(fields, aerodromes)

    assert [item.indicator for item in result.suggestions] == ["DEP/", "ALTN/"]
    assert len(result.fpl_updates) == 2
    assert {update.field for update in result.fpl_updates} == {
        "departure_aerodrome",
        "alternate_aerodrome_1",
    }


@pytest.mark.asyncio
async def test_get_field18_intelligence_assembles_computed_field18() -> None:
    intent = Field18Intent(
        fpl_fields=FlightPlanFields(
            aircraft_type="ZZZZ",
            typ_detail="C172",
            departure_aerodrome="AFIL",
            dep_detail="4620S06630W",
        ),
    )

    result = await get_field18_intelligence(intent)

    assert result.computed_field18 == "DEP/4620S06630W TYP/C172"
    assert len(result.suggestions) == 2
    assert result.metadata["engine"] == "deterministic"
    assert result.fpl_updates == []


@pytest.mark.asyncio
async def test_get_field18_intelligence_returns_zero_when_no_suggestions() -> None:
    intent = Field18Intent(
        fpl_fields=FlightPlanFields(
            aircraft_type="C172",
            departure_aerodrome="SAMR",
            destination_aerodrome="SAEZ",
        ),
    )

    result = await get_field18_intelligence(intent)

    assert result.computed_field18 == "0"
    assert result.suggestions == []
    assert result.fpl_updates == []


@pytest.mark.asyncio
async def test_get_field18_intelligence_appends_to_current_field18() -> None:
    intent = Field18Intent(
        fpl_fields=FlightPlanFields(aircraft_type="ZZZZ", typ_detail="C172"),
        current_field18="DOF/260324",
    )

    result = await get_field18_intelligence(intent)

    assert result.computed_field18 == "DOF/260324 TYP/C172"


@pytest.mark.asyncio
async def test_get_field18_intelligence_non_controlled_departure() -> None:
    expected_coords = format_oaci_coordinates(MZA_LAT, MZA_LON)
    intent = Field18Intent(
        fpl_fields=FlightPlanFields(
            departure_aerodrome="MZA",
            destination_aerodrome="SAMR",
        ),
        aerodromes=Field18AerodromeContexts(
            departure=_mza_context(),
            destination=_controlled_destination(),
        ),
    )

    result = await get_field18_intelligence(intent)

    assert result.computed_field18 == f"DEP/MZA{expected_coords}"
    assert len(result.fpl_updates) == 1
    assert result.fpl_updates[0].to_value == "ZZZZ"
    assert any("FPL field update" in message for message in result.messages)
