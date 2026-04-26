from fastapi import APIRouter, HTTPException, status

from app.core.dependencies import DbSession
from app.schemas.aerodrome import AerodromeResponse
from app.services.aerodrome_service import (
    AerodromeNotFoundError,
    get_aerodrome,
    list_aerodromes,
)

router = APIRouter(prefix="/api/v1/aerodromes", tags=["aerodromes"])


@router.get("", response_model=list[AerodromeResponse])
async def get_aerodromes(db: DbSession) -> list[AerodromeResponse]:
    return await list_aerodromes(db)


@router.get("/{icao}", response_model=AerodromeResponse)
async def get_aerodrome_by_icao(icao: str, db: DbSession) -> AerodromeResponse:
    try:
        return await get_aerodrome(db, icao)
    except AerodromeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
