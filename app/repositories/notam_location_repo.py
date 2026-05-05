"""NOTAM location mapping repository."""

from __future__ import annotations

from datetime import datetime, timezone

from app.models.notam_location import NotamLocationDocument


async def get_by_icao(icao: str) -> NotamLocationDocument | None:
    return await NotamLocationDocument.get(icao.strip().upper())


async def upsert(icao: str, site_label_exact: str) -> NotamLocationDocument:
    normalized_icao = icao.strip().upper()
    existing = await get_by_icao(normalized_icao)
    if existing is None:
        doc = NotamLocationDocument(
            id=normalized_icao,
            icao=normalized_icao,
            site_label_exact=site_label_exact,
            updated_at=datetime.now(timezone.utc),
        )
        await doc.insert()
        return doc

    existing.site_label_exact = site_label_exact
    existing.updated_at = datetime.now(timezone.utc)
    await existing.save()
    return existing
