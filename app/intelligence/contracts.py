"""Shared input/output contracts for the intelligence layer."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.schemas.aerodrome import AerodromeResponse


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Alert(BaseModel):
    level: AlertLevel
    code: str
    message: str


# ---------------------------------------------------------------------------
# Orchestrator request
# ---------------------------------------------------------------------------


class AerodromeIntent(BaseModel):
    """Intent to fetch or refresh aerodrome intelligence for a given ICAO."""

    icao: str
    force_refresh: bool = False
    section_ids: list[str] | None = Field(
        default=None,
        description="Subset of AD 2.x section IDs to enrich. None means all.",
    )


class OrchestratorRequest(BaseModel):
    """Top-level request received by the orchestrator from the backend core."""

    aerodrome: AerodromeIntent | None = None
    # Future intents can be added here:
    # weather: WeatherIntent | None = None
    # notams: NotamIntent | None = None


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class AerodromeIntelResult(BaseModel):
    """Result produced by AerodromeIntelligenceService."""

    icao: str
    data: AerodromeResponse | None = None
    source: Literal["cache", "fresh_import"]
    airac_cycle: str | None = None
    alerts: list[Alert] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrchestratorResponse(BaseModel):
    """Consolidated response returned to the backend core."""

    intent: str
    aerodrome: AerodromeIntelResult | None = None
    alerts: list[Alert] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
