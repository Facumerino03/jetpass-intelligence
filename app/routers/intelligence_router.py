"""Intelligence router — entry point for the backend core."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from app.intelligence.contracts import (
    AircraftTypeValidateSyncStatusResponse,
    AircraftTypeValidationResult,
    AnacCatalogSyncStatusResponse,
    AirportsSyncStatusResponse,
    NotamSyncStatusResponse,
    OrchestratorRequest,
    OrchestratorResponse,
)
from app.intelligence.aircraft_types.aircraft_type_service import validate_aircraft_type
from app.intelligence.orchestrator import run
from app.services.aircraft_type_sync_runtime import get_aircraft_type_validation_status
from app.services.airports_sync_runtime import (
    get_airports_sync_status,
    get_anac_catalog_sync_status,
)
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
        request.aerodrome_catalog_sync,
        request.fpl_field18,
        request.aircraft_type_validate,
    ]):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "At least one intent must be specified "
                "(e.g. 'aerodrome', 'notam', 'weather', 'aerodrome_geo', "
                "'aerodrome_catalog_sync', 'fpl_field18', or 'aircraft_type_validate')."
            ),
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
    "/anac-catalog-sync/status",
    response_model=AnacCatalogSyncStatusResponse,
    summary="Get ANAC catalog sync operational status",
    description=(
        "Returns scheduler and last-run metadata for the periodic synchronisation "
        "of the ANAC MADHEL aerodrome catalog."
    ),
)
async def get_anac_catalog_sync_operational_status() -> AnacCatalogSyncStatusResponse:
    return get_anac_catalog_sync_status()


@router.get(
    "/airports-sync/status",
    response_model=AirportsSyncStatusResponse,
    summary="Get ANAC catalog sync operational status (deprecated)",
    description=(
        "Deprecated alias for /anac-catalog-sync/status. "
        "Returns scheduler and last-run metadata for the ANAC MADHEL catalog sync."
    ),
    deprecated=True,
)
async def get_airports_sync_operational_status() -> AirportsSyncStatusResponse:
    return get_airports_sync_status()


@router.get(
    "/aircraft-types/{designator}",
    response_model=AircraftTypeValidationResult,
    summary="Validate an ICAO Doc 8643 aircraft type designator",
    description=(
        "Looks up the designator in the official ICAO Doc 8643 search UI and "
        "returns whether it is registered, along with minimal metadata when found."
    ),
)
async def validate_aircraft_type_designator(designator: str) -> AircraftTypeValidationResult:
    return await validate_aircraft_type(designator)


@router.get(
    "/aircraft-types-validation/status",
    response_model=AircraftTypeValidateSyncStatusResponse,
    summary="Get ICAO Doc 8643 validation cache status",
    description=(
        "Returns observability metadata for the in-memory Doc 8643 validation cache."
    ),
)
async def get_aircraft_type_validation_operational_status() -> AircraftTypeValidateSyncStatusResponse:
    return get_aircraft_type_validation_status()
