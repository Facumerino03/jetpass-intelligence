from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse


class AerodromeNotFoundError(Exception):
    pass


async def list_aerodromes(db: AsyncSession) -> list[AerodromeResponse]:
    aerodromes = await aerodrome_repo.get_all(db)
    return [AerodromeResponse.model_validate(aerodrome) for aerodrome in aerodromes]


async def get_aerodrome(db: AsyncSession, icao: str) -> AerodromeResponse:
    aerodrome = await aerodrome_repo.get_by_icao(db, icao)
    if aerodrome is None:
        msg = f"Aerodrome '{icao.upper()}' was not found."
        raise AerodromeNotFoundError(msg)
    return AerodromeResponse.model_validate(aerodrome)
