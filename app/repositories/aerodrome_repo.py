from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.aerodrome import Aerodrome
from app.models.runway import Runway
from app.schemas.aerodrome import AerodromeCreate


async def get_by_icao(db: AsyncSession, icao: str) -> Aerodrome | None:
    normalized_icao = icao.strip().upper()
    stmt = (
        select(Aerodrome)
        .options(selectinload(Aerodrome.runways))
        .where(Aerodrome.icao_code == normalized_icao)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def get_all(db: AsyncSession) -> list[Aerodrome]:
    stmt = select(Aerodrome).options(selectinload(Aerodrome.runways)).order_by(Aerodrome.icao_code)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def upsert(db: AsyncSession, data: AerodromeCreate) -> Aerodrome:
    existing = await get_by_icao(db, data.icao_code)
    normalized_icao = data.icao_code.strip().upper()

    if existing is None:
        aerodrome = Aerodrome(
            icao_code=normalized_icao,
            iata_code=data.iata_code.strip().upper() if data.iata_code else None,
            name=data.name,
            city=data.city,
            province=data.province,
            country=data.country,
            latitude=data.latitude,
            longitude=data.longitude,
            elevation_ft=data.elevation_ft,
        )
        aerodrome.runways = [
            Runway(
                designator=runway.designator,
                length_m=runway.length_m,
                width_m=runway.width_m,
                surface_type=runway.surface_type,
            )
            for runway in data.runways
        ]
        db.add(aerodrome)
        await db.commit()
        await db.refresh(aerodrome)
        return aerodrome

    existing.iata_code = data.iata_code.strip().upper() if data.iata_code else None
    existing.name = data.name
    existing.city = data.city
    existing.province = data.province
    existing.country = data.country
    existing.latitude = data.latitude
    existing.longitude = data.longitude
    existing.elevation_ft = data.elevation_ft
    existing.runways = [
        Runway(
            designator=runway.designator,
            length_m=runway.length_m,
            width_m=runway.width_m,
            surface_type=runway.surface_type,
        )
        for runway in data.runways
    ]

    await db.commit()
    await db.refresh(existing)
    return existing
