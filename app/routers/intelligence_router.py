"""Intelligence router — entry point for the backend core."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from app.intelligence.contracts import OrchestratorRequest, OrchestratorResponse
from app.intelligence.orchestrator import run

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
    if request.aerodrome is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one intent must be specified (e.g. 'aerodrome').",
        )
    logger.info("Intelligence request received: %s", request.model_dump(exclude_none=True))
    return await run(request)
