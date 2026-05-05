"""Shared input/output contracts for the intelligence layer."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.models.notam import RawNotam
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
    notam: NotamIntent | None = None
    # Future intents can be added here:
    # weather: WeatherIntent | None = None


class NotamIntent(BaseModel):
    """Intent to fetch or refresh NOTAM intelligence for a given ICAO."""

    icao: str
    force_refresh: bool = False


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
    notam: NotamIntelResult | None = None
    alerts: list[Alert] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class NotamIntelResult(BaseModel):
    """Result produced by NotamIntelligenceService."""

    icao: str
    aerodrome_name: str | None = None
    site_last_updated_at: datetime | None = None
    fetched_at: datetime | None = None
    aerodrome_notams: list[RawNotam] = Field(default_factory=list)
    fir_notams: list[RawNotam] = Field(default_factory=list)
    fir_notams_by_location: dict[str, list[RawNotam]] = Field(default_factory=dict)
    source: Literal["cache", "fresh_scrape"]
    alerts: list[Alert] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class NotamSyncStatusResponse(BaseModel):
    """Operational status of the NOTAM location sync scheduler."""

    enabled: bool
    scheduler_running: bool
    interval_hours: int | None = None
    startup_sync_enabled: bool | None = None
    headless: bool | None = None
    in_progress: bool = False
    next_run_at: datetime | None = None
    last_run_started_at: datetime | None = None
    last_run_finished_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_synced_count: int | None = None
    last_missing_count: int | None = None
    last_site_labels_count: int | None = None
    last_aerodromes_count: int | None = None
