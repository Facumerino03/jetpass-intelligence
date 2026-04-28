from fastapi import APIRouter, HTTPException, status

from app.schemas.aerodrome import AerodromeResponse, SectionResponse
from app.services.aerodrome_service import (
    AerodromeNotFoundError,
    AerodromeSectionNotFoundError,
    get_aerodrome,
    get_aerodrome_section,
    list_aerodromes,
)

router = APIRouter(prefix="/api/v1/aerodromes", tags=["aerodromes"])


@router.get("", response_model=list[AerodromeResponse])
async def get_aerodromes() -> list[AerodromeResponse]:
    return await list_aerodromes()


@router.get("/{icao}", response_model=AerodromeResponse)
async def get_aerodrome_by_icao(icao: str) -> AerodromeResponse:
    try:
        return await get_aerodrome(icao)
    except AerodromeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc


@router.get("/{icao}/sections/{section_id}", response_model=SectionResponse)
async def get_aerodrome_section_by_id(icao: str, section_id: str) -> SectionResponse:
    try:
        return await get_aerodrome_section(icao, section_id)
    except (AerodromeNotFoundError, AerodromeSectionNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
