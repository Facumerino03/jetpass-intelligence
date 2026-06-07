"""AD 2.19 — Navigational and landing aids."""

from pydantic import BaseModel, Field


class NavigationAid(BaseModel):
    """One row of AD 2.19 — maps 1:1 to the seven AIP table columns."""

    type_of_aid: str | None = Field(
        None,
        description=(
            "Column 1 — Type of aid (and MAG VAR / ILS CAT when stated in this column). "
            "Examples: VOR DME, ILS/LOC, GP/DME, NDB."
        ),
    )
    identification: str | None = Field(
        None,
        description="Column 2 — Aid ID (e.g. EZE, PC, EZ). Null if empty in the document.",
    )
    frequency_channel: str | None = Field(
        None,
        description="Column 3 — Frequency and channel (e.g. '116.5 MHz').",
    )
    hours_of_operation: str | None = Field(
        None,
        description="Column 4 — Hours of operation (e.g. 'H24'). Null if empty.",
    )
    coordinates: str | None = Field(
        None,
        description=(
            "Column 5 — Geographical coordinates of the transmitting antenna "
            "(DDMMSS format)."
        ),
    )
    elevation_m: float | None = Field(
        None,
        description=(
            "Column 6 — Elevation of the transmitting antenna in metres (numeric only). "
            "The document shows 'X.XX m YY ft'; extract only the metres value as a float. "
            "Example: '20.50 m 67 ft' → 20.5. Null if empty."
        ),
    )
    remarks: str | None = Field(
        None,
        description=(
            "Column 7 — Observaciones / Remarks, verbatim. "
            "Do NOT parse or split this field into other properties: "
            "CAT, runway, GP angle, DME channel, etc. stay here exactly as written."
        ),
    )


class NavigationAids(BaseModel):
    """AD 2.19 RADIOAYUDAS PARA LA NAVEGACIÓN Y EL ATERRIZAJE."""

    aids: list[NavigationAid] = Field(
        default_factory=list,
        description="One entry per table row (one aid per row).",
    )
