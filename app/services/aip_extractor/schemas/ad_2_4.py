"""AD 2.4 — Handling services and facilities."""

from pydantic import BaseModel, Field


class HandlingFacilityItem(BaseModel):
    """Single facility entry from AD 2.4."""

    item_number: int | None = None
    description: str | None = None
    details: str | None = None


class HandlingServices(BaseModel):
    """AD 2.4 SERVICIOS E INSTALACIONES DE ESCALA."""

    facilities: list[HandlingFacilityItem] = Field(default_factory=list)
    remarks: str | None = None
