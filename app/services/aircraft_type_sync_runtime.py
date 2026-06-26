"""Runtime state for ICAO Doc 8643 validation observability."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from app.intelligence.contracts import AircraftTypeValidateSyncStatusResponse

_status_lock = Lock()
_status = AircraftTypeValidateSyncStatusResponse(enabled=True, scheduler_running=False, cache_size=0)


def get_aircraft_type_validation_status() -> AircraftTypeValidateSyncStatusResponse:
    with _status_lock:
        return _status.model_copy(deep=True)


def update_aircraft_type_validation_status(**fields: Any) -> AircraftTypeValidateSyncStatusResponse:
    global _status
    with _status_lock:
        _status = _status.model_copy(update=fields)
        return _status.model_copy(deep=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
