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


class OrchestratorRequest(BaseModel):
    """Top-level request received by the orchestrator from the backend core."""

    aerodrome: AerodromeIntent | None = None
    notam: NotamIntent | None = None
    weather: WeatherIntent | None = None
    aerodrome_geo: AerodromeGeoIntent | None = None


class NotamIntent(BaseModel):
    """Intent to fetch or refresh NOTAM intelligence for a given ICAO."""

    icao: str
    force_refresh: bool = False


class AerodromeGeoIntent(BaseModel):
    """Lightweight intent to resolve aerodrome coordinates from OurAirports CSV."""

    icao: str | None = None
    icaos: list[str] | None = None
    force_refresh: bool = False


class WeatherIntent(BaseModel):
    """Intent to fetch or refresh aviation weather for a given ICAO."""

    icao: str
    force_refresh: bool = False
    metar_hours_back: float | None = None


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


class WeatherStation(BaseModel):
    icao: str
    name: str | None = None
    lat: float | None = None
    lon: float | None = None
    elev: float | None = None


class WeatherMetar(BaseModel):
    raw: str | None = None
    observed_at: datetime | None = None
    flight_category: str | None = None
    wind_dir_degrees: int | str | None = None
    wind_speed_kt: int | None = None
    wind_gust_kt: int | None = None
    visibility: float | str | None = None
    altimeter_hpa: float | None = None
    temperature_c: float | None = None
    dewpoint_c: float | None = None
    present_weather: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherTaf(BaseModel):
    raw: str | None = None
    issued_at: datetime | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    forecast_periods: list[dict[str, Any]] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherSigmet(BaseModel):
    raw: str | None = None
    hazard: str | None = None
    fir_id: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    geometry: dict[str, Any] | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherIntelResult(BaseModel):
    """Result produced by WeatherIntelligenceService."""

    icao: str
    station: WeatherStation | None = None
    metar: WeatherMetar | None = None
    taf: WeatherTaf | None = None
    sigmets: list[WeatherSigmet] = Field(default_factory=list)
    fetched_at: datetime | None = None
    source: Literal["cache", "fresh_fetch", "mixed"]
    alerts: list[Alert] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeoCoords(BaseModel):
    """Coordinates and elevation for a single aerodrome."""

    icao: str
    lat: float | None = None
    lon: float | None = None
    elev_ft: int | None = None
    elev_m: float | None = None
    source: str = "not_found"


class OrchestratorResponse(BaseModel):
    """Consolidated response returned to the backend core."""

    intent: str
    aerodrome: AerodromeIntelResult | None = None
    notam: NotamIntelResult | None = None
    weather: WeatherIntelResult | None = None
    aerodrome_geo: dict[str, GeoCoords] | None = None
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


class AirportsSyncStatusResponse(BaseModel):
    """Operational status of the OurAirports CSV sync scheduler."""

    enabled: bool
    scheduler_running: bool
    interval_hours: int | None = None
    startup_sync_enabled: bool | None = None
    in_progress: bool = False
    next_run_at: datetime | None = None
    last_run_started_at: datetime | None = None
    last_run_finished_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_downloaded_rows: int | None = None
