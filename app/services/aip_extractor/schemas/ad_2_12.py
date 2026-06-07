"""AD 2.12 — Runway physical characteristics."""

from pydantic import BaseModel, Field


class PCNEntry(BaseModel):
    """One PCN strength rating for a runway surface."""

    pcn_value: str | None = Field(
        None,
        description=(
            "PCN numerical value and classification components, "
            "e.g. '82/R/B/W/T'. Format: number/pavement/subgrade/tyre_pressure/evaluation."
        ),
    )
    surface_type: str | None = Field(
        None,
        description="Surface material: ASPH (asphalt), CONC (concrete), GRVL (gravel), etc.",
    )


class RunwayPhysicalCharacteristics(BaseModel):
    """Physical characteristics for one runway direction (one THR end)."""

    designator: str | None = Field(
        None,
        description="RWY designator (e.g. '11', '29', '17L', '35R')",
    )
    dimensions_m: str | None = Field(
        None,
        description="RWY dimensions as LengthxWidth in metres (e.g. '3300x60')",
    )
    pcn_entries: list[PCNEntry] = Field(
        default_factory=list,
        description=(
            "One or more PCN strength entries. A runway may have multiple entries "
            "when it has mixed surfaces (e.g. part ASPH, part CONC). "
            "The pcn_value must use slash separators between all five components: "
            "'82/R/B/W/T', '70/R/B/W/T', '92/F/C/W/T'. Never omit the slashes."
        ),
    )
    true_bearing: str | None = Field(
        None,
        description="True bearing in hundredths of a degree (e.g. '102.3')",
    )
    magnetic_bearing: str | None = Field(
        None,
        description="Magnetic bearing in whole degrees (e.g. '112')",
    )
    thr_coordinates: str | None = Field(
        None,
        description=(
            "Geographical coordinates of THIS runway's threshold (THR) in DDMMSS format. "
            "CRITICAL — the AIP table lists TWO coordinate pairs per runway pair: "
            "the far-end coordinates appear FIRST in the raw text (above the designator line), "
            "and the THIS-end THR coordinates appear WITH or AFTER the designator. "
            "The THR of runway X is where an aircraft LANDS on runway X, i.e. the point "
            "the aircraft crosses when touching down. "
            "Cross-check using bearing: for a runway with magnetic bearing ~112° (heading east), "
            "its THR is at the WESTERN end — the coordinate with the LARGER west longitude value. "
            "For its reciprocal (~292°, heading west), the THR is at the EASTERN end — "
            "the coordinate with the SMALLER west longitude value. "
            "Never assign the same coordinates to both ends of a runway pair."
        ),
    )
    thr_elevation_m: float | None = Field(
        None,
        description=(
            "Elevation of THIS threshold (THR) in metres. "
            "This is a small number like 19 or 20. "
            "Do NOT confuse with GUND (geoid undulation) which is also nearby in the table."
        ),
    )
    thr_elevation_ft: float | None = Field(
        None,
        description="Elevation of THIS threshold (THR) in feet (e.g. 62 or 67).",
    )
    gund_m: float | None = Field(
        None,
        description=(
            "Geoid undulation (GUND) at the THR position, in metres. "
            "Appears after THR elevation in the table. Typically a value like 16.23."
        ),
    )
    slope: str | None = Field(
        None,
        description="Longitudinal slope of the runway (e.g. '+0.01%', '-0.01%')",
    )
    swy_dimensions_m: str | None = Field(
        None,
        description=(
            "Stopway (SWY) dimensions LxW in metres. "
            "This is column 8 of the second physical characteristics table. "
            "'No' means no stopway exists."
        ),
    )
    cwy_dimensions_m: str | None = Field(
        None,
        description=(
            "Clearway (CWY) dimensions LxW in metres. "
            "This is column 9 — AFTER SWY (col 8) in the table. "
            "'No' means no clearway exists."
        ),
    )
    strip_dimensions_m: str | None = Field(
        None,
        description="Runway strip dimensions LxW in metres (e.g. '3420x280')",
    )
    resa_dimensions_m: str | None = Field(
        None,
        description=(
            "Runway End Safety Area (RESA) dimensions LxW in metres. "
            "This is column 11 in the second table — AFTER SWY (col 8), CWY (col 9), "
            "and strip (col 10). Extract the LxW value here, not in swy or cwy."
        ),
    )
    arresting_system: str | None = Field(
        None,
        description=(
            "Location and description of arresting system, or 'No' if none. "
            "Column 12 of the second table."
        ),
    )
    ofz: bool | None = Field(
        None,
        description="Obstacle Free Zone present: true ('Sí'/'Yes'), false ('No')",
    )
    remarks: str | None = None


class RunwayPhysicalCharacteristicsSection(BaseModel):
    """AD 2.12 CARACTERÍSTICAS FÍSICAS DE LAS PISTAS."""

    runways: list[RunwayPhysicalCharacteristics] = Field(
        default_factory=list,
        description=(
            "One entry per runway direction (THR end). "
            "A runway pair (e.g. 11/29) produces two entries with mirrored bearings."
        ),
    )
    section_remarks: str | None = None
