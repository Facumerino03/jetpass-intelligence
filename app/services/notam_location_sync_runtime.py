"""Runtime state for NOTAM location synchronization observability."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from app.intelligence.contracts import NotamSyncStatusResponse

_status_lock = Lock()
_status = NotamSyncStatusResponse(enabled=False, scheduler_running=False)


def get_notam_sync_status() -> NotamSyncStatusResponse:
    with _status_lock:
        return _status.model_copy(deep=True)


def update_notam_sync_status(**fields: Any) -> NotamSyncStatusResponse:
    global _status
    with _status_lock:
        _status = _status.model_copy(update=fields)
        return _status.model_copy(deep=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
