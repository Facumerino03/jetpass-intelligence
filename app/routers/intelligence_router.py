"""Intelligence router — entry point for the backend core."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from app.intelligence.contracts import (
    AirportsSyncStatusResponse,
    NotamSyncStatusResponse,
    OrchestratorRequest,
    OrchestratorResponse,
)
from app.intelligence.orchestrator import run
from app.services.airports_sync_runtime import get_airports_sync_status
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
    if not any([
        request.aerodrome,
        request.notam,
        request.weather,
        request.aerodrome_geo,
    ]):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one intent must be specified (e.g. 'aerodrome', 'notam', 'weather', or 'aerodrome_geo').",
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


@router.get(
    "/airports-sync/status",
    response_model=AirportsSyncStatusResponse,
    summary="Get OurAirports CSV sync operational status",
    description=(
        "Returns scheduler and last-run metadata for the periodic synchronisation "
        "of the OurAirports airports.csv file."
    ),
)
async def get_airports_sync_operational_status() -> AirportsSyncStatusResponse:
    return get_airports_sync_status()
