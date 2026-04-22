import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_maker
from app.repositories.aerodrome_repo import upsert
from app.schemas.aerodrome import AerodromeCreate, RunwayBase


async def seed_samr(db: AsyncSession) -> None:
    samr = AerodromeCreate(
        icao_code="SAMR",
        iata_code="RGL",
        name="Aeropuerto Internacional Piloto Civil Norberto Fernández",
        city="Río Gallegos",
        province="Santa Cruz",
        country="Argentina",
        latitude=-51.6089,
        longitude=-69.3126,
        elevation_ft=65,
        runways=[
            RunwayBase(designator="07/25", length_m=3550, width_m=45, surface_type="ASPH"),
            RunwayBase(designator="01/19", length_m=1750, width_m=30, surface_type="ASPH"),
        ],
    )
    await upsert(db, samr)


async def main() -> None:
    session_maker = get_session_maker()
    async with session_maker() as session:
        await seed_samr(session)
    print("Seed completed: SAMR upserted.")


if __name__ == "__main__":
    asyncio.run(main())
