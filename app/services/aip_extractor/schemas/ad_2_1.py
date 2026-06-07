"""AD 2.1 — Aerodrome location indicator and name."""

from pydantic import BaseModel, Field


class LocationAndName(BaseModel):
    """AD 2.1 INDICADOR DE LUGAR Y NOMBRE DEL AERÓDROMO."""

    location_indicator: str | None = Field(
        None,
        description="ICAO location indicator (e.g. SAEZ)",
    )
    aerodrome_name: str | None = Field(
        None,
        description="Aerodrome name in Spanish and/or English",
    )
    airport_type: str | None = Field(
        None,
        description="Type of airport / traffic permitted description",
    )
    remarks: str | None = None
