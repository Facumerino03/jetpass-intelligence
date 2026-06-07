"""AD 2.3 — Operational hours."""

from pydantic import BaseModel, Field


class OperationalServiceHours(BaseModel):
    """Single operational service entry from AD 2.3."""

    service_name: str | None = Field(
        None,
        description=(
            "Service name. May be bilingual (ES/EN). "
            "Examples: 'Explotador del AD / AD Operator', 'Aduanas / Customs', 'ATS'."
        ),
    )
    hours: str | None = Field(
        None,
        description=(
            "Operating hours. Common values: 'H24' (24h), specific UTC ranges "
            "(e.g. '11:00-20:00 UTC'), 'No' (not available)."
        ),
    )


class OperationalHours(BaseModel):
    """AD 2.3 HORAS DE FUNCIONAMIENTO."""

    services: list[OperationalServiceHours] = Field(
        default_factory=list,
        description=(
            "List of services and their hours. "
            "Do NOT rely on row numbers from the PDF — they are unreliable due to "
            "table column extraction. Extract service_name and hours only."
        ),
    )
    remarks: str | None = None
