"""AD 2.2 — Aerodrome geographical and administrative data."""

from pydantic import BaseModel, Field


class GeographicAndAdminData(BaseModel):
    """AD 2.2 DATOS GEOGRÁFICOS Y ADMINISTRATIVOS."""

    arp_coordinates: str | None = Field(
        None,
        description=(
            "ARP geographical coordinates in DDMMSS format (e.g. '344920S 0583209W'). "
            "Extract only the coordinate string, not the location description."
        ),
    )
    arp_location_description: str | None = Field(
        None,
        description="Location of ARP within the AD (e.g. 'Centro geométrico de pista 11/29')",
    )
    direction_and_distance_from_city: str | None = Field(
        None,
        description="Direction and distance from nearest city (e.g. '22 km al SSW de Buenos Aires')",
    )
    elevation_m: float | None = Field(
        None,
        description="Aerodrome elevation in metres",
    )
    elevation_ft: float | None = Field(
        None,
        description="Aerodrome elevation in feet",
    )
    temperature_reference_and_min: str | None = Field(
        None,
        description=(
            "Reference temperature and mean minimum temperature from item 3, "
            "copied verbatim from the document (Spanish part only). "
            "The format varies between documents: '28.4°C - 8.4°C', '23.2°C, -1.6°C', etc. "
            "Do NOT interpret, split, or infer the sign — transcribí el valor exactamente "
            "como aparece en el texto."
        ),
    )
    gund_m: float | None = Field(
        None,
        description="Geoid undulation (GUND) at AD elevation position, in metres",
    )
    magnetic_variation: str | None = Field(
        None,
        description="Magnetic variation value (e.g. '10° W')",
    )
    magnetic_variation_annual_change: str | None = Field(
        None,
        description="Annual change of magnetic variation (e.g. \"8' W\")",
    )
    ad_administration: str | None = Field(
        None,
        description="AD head office: address, phone, fax, AFS, email, website",
    )
    ad_operator: str | None = Field(
        None,
        description="AD operator contact details: name, address, phone, fax, SITA, email, website",
    )
    ans_provider: str | None = Field(
        None,
        description="Air Navigation Service Provider (ANSP) contact details",
    )
    traffic_types_permitted: str | None = Field(
        None,
        description="Types of traffic permitted, e.g. 'IFR/VFR'",
    )
    remarks: str | None = None
