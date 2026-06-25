"""Runtime state for ANAC catalog synchronisation observability."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from app.intelligence.contracts import AnacCatalogSyncStatusResponse

_status_lock = Lock()
_status = AnacCatalogSyncStatusResponse(enabled=False, scheduler_running=False)


def get_anac_catalog_sync_status() -> AnacCatalogSyncStatusResponse:
    with _status_lock:
        return _status.model_copy(deep=True)


def update_anac_catalog_sync_status(**fields: Any) -> AnacCatalogSyncStatusResponse:
    global _status
    with _status_lock:
        _status = _status.model_copy(update=fields)
        return _status.model_copy(deep=True)


# Deprecated aliases — kept for backward compatibility.
get_airports_sync_status = get_anac_catalog_sync_status
update_airports_sync_status = update_anac_catalog_sync_status


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
