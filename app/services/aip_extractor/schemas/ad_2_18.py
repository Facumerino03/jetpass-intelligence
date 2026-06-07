"""AD 2.18 — ATS communication facilities."""

from pydantic import BaseModel, Field


class FrequencyEntry(BaseModel):
    """One frequency/channel pair for an ATS service."""

    channel_type: str | None = Field(
        None,
        description=(
            "Channel role within the service. "
            "Standard values: 'CPPL' (primary), 'CAUX' (auxiliary), 'EMERG' (emergency). "
            "Null if not specified."
        ),
    )
    frequency: str | None = Field(
        None,
        description="Radio frequency including unit (e.g. '118.60 MHz', '121.50 MHz')",
    )


class ATSCommunicationFacility(BaseModel):
    """Single ATS communication service with all its frequencies."""

    service_designation: str | None = Field(
        None,
        description=(
            "ATS service type abbreviation. "
            "Examples: TWR (tower), APP (approach), ATIS, SMC (surface movement), "
            "CLRD (clearance delivery), ACC, FIC, AFIS."
        ),
    )
    call_sign: str | None = Field(
        None,
        description="Radio call sign (e.g. 'EZEIZA TORRE / EZEIZA TOWER')",
    )
    frequencies: list[FrequencyEntry] = Field(
        default_factory=list,
        description=(
            "All frequencies for this service. A single service (e.g. TWR) may have "
            "multiple frequencies (CPPL, CAUX, EMERG). Group them here instead of "
            "creating separate service records."
        ),
    )
    hours_of_operation: str | None = Field(
        None,
        description=(
            "VOICE service hours of operation (e.g. 'H24', '09:00-01:00 UTC'). "
            "IMPORTANT: in PDF-extracted tables the hours column value sometimes appears "
            "on a different row than the service designator due to multi-row table layout. "
            "If an H24 value appears on a nearby row within the same visual table block "
            "(e.g. H24 on the SMC row immediately below TWR), it applies to ALL services "
            "in that block — assign it to this service too. "
            "Only leave null when no hours value can be found anywhere in the service block. "
            "For CLRD: the voice hours (e.g. '09:00-01:00 UTC') go here; "
            "the DCL data-link hours go in data_link_hours."
        ),
    )
    data_link_hours: str | None = Field(
        None,
        description=(
            "Hours for the DCL (Data Link / Enlace de datos) system, when present. "
            "Only applicable to CLRD (clearance delivery) services that have a separate "
            "DCL automatic system operating on different hours than the voice service. "
            "In the PDF the DCL hours appear inside the remarks column of the CLRD row, "
            "as a second time value (e.g. 'H24') after the voice hours ('09:00-01:00 UTC'). "
            "Example: if the text shows '09:00-01:00 UTC' as voice and 'H24' as DCL, "
            "set hours_of_operation='09:00-01:00 UTC' and data_link_hours='H24'. "
            "Null for all services that do not have a DCL system."
        ),
    )
    remarks: str | None = None


class ATSCommunicationFrequencies(BaseModel):
    """AD 2.18 INSTALACIONES DE COMUNICACIONES DE LOS ATS."""

    facilities: list[ATSCommunicationFacility] = Field(
        default_factory=list,
        description=(
            "One entry per ATS service (TWR, APP, etc.). "
            "Do NOT create separate entries for each frequency — group all frequencies "
            "of the same service in the 'frequencies' list."
        ),
    )
