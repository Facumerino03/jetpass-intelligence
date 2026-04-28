"""Seed sample aerodrome data into MongoDB."""

from __future__ import annotations

import asyncio

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.repositories.aerodrome_repo import upsert
from app.schemas.aerodrome import AerodromeCreate, SectionSchema


def _samr_payload() -> AerodromeCreate:
    sections = [
        SectionSchema(
            section_id=f"AD 2.{idx}",
            title=f"AD 2.{idx}",
            raw_text=f"Contenido literal AD 2.{idx}",
            data={"seed": True, "section": idx},
        )
        for idx in range(1, 26)
    ]
    return AerodromeCreate(
        icao_code="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        airac_cycle="2026-01",
        source_document="seed",
        downloaded_by="seed-script",
        ad_sections=sections,
    )


async def main() -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured.")
    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    await upsert(_samr_payload())
    print("Seed completed: SAMR upserted.")


if __name__ == "__main__":
    asyncio.run(main())
