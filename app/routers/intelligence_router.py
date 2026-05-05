"""Intelligence router — entry point for the backend core."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from app.intelligence.contracts import (
    NotamSyncStatusResponse,
    OrchestratorRequest,
    OrchestratorResponse,
)
from app.intelligence.orchestrator import run
from app.services.notam_location_sync_runtime import get_notam_sync_status

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/intelligence", tags=["intelligence"])


@router.post(
    "/run",
    response_model=OrchestratorResponse,
    summary="Run intelligence pipeline",
    description=(
        "Accepts a structured intent from the backend core and returns "
        "consolidated aeronautical intelligence. The response always includes "
        "alerts and a source field indicating whether data came from cache or "
        "a fresh import."
    ),
)
async def run_intelligence(request: OrchestratorRequest) -> OrchestratorResponse:
    if request.aerodrome is None and request.notam is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one intent must be specified (e.g. 'aerodrome' or 'notam').",
        )
    logger.info("Intelligence request received: %s", request.model_dump(exclude_none=True))
    return await run(request)


@router.get(
    "/notam-sync/status",
    response_model=NotamSyncStatusResponse,
    summary="Get NOTAM location sync operational status",
    description=(
        "Returns scheduler and last-run metadata for the NOTAM location mapping "
        "synchronization process."
    ),
)
async def get_notam_sync_operational_status() -> NotamSyncStatusResponse:
    return get_notam_sync_status()
