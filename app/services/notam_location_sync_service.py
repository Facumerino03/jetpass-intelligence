"""Service for syncing ICAO -> NOTAM site location labels."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from app.repositories import aerodrome_repo, notam_location_repo
from app.services.scraper.notam_scraper import list_notam_locations

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NotamLocationSyncStats:
    aerodromes_count: int
    site_labels_count: int
    synced_count: int
    missing_count: int
    missing_items: list[tuple[str, str]]


def _normalize_label(value: str) -> str:
    value = value.upper().strip()
    value = re.sub(r"\s*/\s*", "/", value)
    value = re.sub(r"\s+", " ", value)
    return value


async def sync_notam_locations(*, headless: bool = True) -> NotamLocationSyncStats:
    """Fetch website selector labels and persist exact mappings by ICAO."""
    site_labels = await list_notam_locations(headless=headless)
    normalized_site_index = {_normalize_label(label): label for label in site_labels}

    aerodromes = await aerodrome_repo.get_all()
    synced = 0
    missing: list[tuple[str, str]] = []

    for aerodrome in aerodromes:
        candidates = [
            _normalize_label(aerodrome.full_name or ""),
            _normalize_label(aerodrome.name or ""),
        ]

        match = None
        for candidate in candidates:
            if candidate and candidate in normalized_site_index:
                match = normalized_site_index[candidate]
                break

        if match is None:
            missing.append((aerodrome.icao, aerodrome.full_name or aerodrome.name))
            continue

        await notam_location_repo.upsert(aerodrome.icao, match)
        synced += 1

    stats = NotamLocationSyncStats(
        aerodromes_count=len(aerodromes),
        site_labels_count=len(site_labels),
        synced_count=synced,
        missing_count=len(missing),
        missing_items=missing,
    )
    logger.info(
        "NOTAM location sync completed (aerodromes=%d, labels=%d, synced=%d, missing=%d)",
        stats.aerodromes_count,
        stats.site_labels_count,
        stats.synced_count,
        stats.missing_count,
    )
    return stats
