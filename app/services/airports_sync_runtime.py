"""Runtime state for OurAirports CSV synchronisation observability."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from app.intelligence.contracts import AirportsSyncStatusResponse

_status_lock = Lock()
_status = AirportsSyncStatusResponse(enabled=False, scheduler_running=False)


def get_airports_sync_status() -> AirportsSyncStatusResponse:
    with _status_lock:
        return _status.model_copy(deep=True)


def update_airports_sync_status(**fields: Any) -> AirportsSyncStatusResponse:
    global _status
    with _status_lock:
        _status = _status.model_copy(update=fields)
        return _status.model_copy(deep=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
