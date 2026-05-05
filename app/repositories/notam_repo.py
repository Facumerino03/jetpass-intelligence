"""NOTAM repository — Beanie data access only."""

from __future__ import annotations

from app.models.notam import NotamDocument


async def get_by_icao(icao: str) -> NotamDocument | None:
    return await NotamDocument.get(icao.strip().upper())


async def upsert(doc: NotamDocument) -> NotamDocument:
    existing = await get_by_icao(doc.icao)
    if existing is None:
        await doc.insert()
        return doc

    existing.aerodrome_name = doc.aerodrome_name
    existing.site_last_updated_at = doc.site_last_updated_at
    existing.fetched_at = doc.fetched_at
    existing.aerodrome_notams = doc.aerodrome_notams
    existing.fir_notams = doc.fir_notams
    existing.fir_notams_by_location = doc.fir_notams_by_location
    await existing.save()
    return existing
