"""Aerodrome use cases — no database details, no HTTP details."""

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse, SectionResponse


class AerodromeNotFoundError(Exception):
    pass


class AerodromeSectionNotFoundError(Exception):
    pass


async def list_aerodromes() -> list[AerodromeResponse]:
    aerodromes = await aerodrome_repo.get_all()
    return [AerodromeResponse.from_document(a) for a in aerodromes]


async def get_aerodrome(icao: str) -> AerodromeResponse:
    aerodrome = await aerodrome_repo.get_by_icao(icao)
    if aerodrome is None:
        msg = f"Aerodrome '{icao.upper()}' was not found."
        raise AerodromeNotFoundError(msg)
    return AerodromeResponse.from_document(aerodrome)


async def get_aerodrome_section(icao: str, section_id: str) -> SectionResponse:
    aerodrome = await aerodrome_repo.get_by_icao(icao)
    if aerodrome is None:
        msg = f"Aerodrome '{icao.upper()}' was not found."
        raise AerodromeNotFoundError(msg)
    section = await aerodrome_repo.get_section_by_icao(icao, section_id)
    if section is None:
        msg = f"Section '{section_id}' was not found for aerodrome '{icao.upper()}'."
        raise AerodromeSectionNotFoundError(msg)
    return SectionResponse.from_model(section)
