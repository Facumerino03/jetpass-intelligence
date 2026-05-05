"""CLI script: sync ICAO -> exact NOTAM dropdown location labels.

Usage examples:
    uv run python -m scripts.sync_notam_locations
    uv run python -m scripts.sync_notam_locations --headful
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.services.scraper.notam_scraper import NotamScraperError
from app.services.notam_location_sync_service import sync_notam_locations


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NOTAM dropdown options and persist exact labels by ICAO."
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser in non-headless mode for debugging.",
    )
    return parser.parse_args()


async def _run(*, headless: bool) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    stats = await sync_notam_locations(headless=headless)

    print("\n✓ NOTAM locations sync complete")
    print(f"  Aerodromes in DB : {stats.aerodromes_count}")
    print(f"  Site labels read : {stats.site_labels_count}")
    print(f"  Synced mappings  : {stats.synced_count}")
    print(f"  Missing mappings : {stats.missing_count}")

    if stats.missing_items:
        print("\nMissing ICAOs (first 25):")
        for icao, name in stats.missing_items[:25]:
            print(f"  - {icao}: {name}")


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run(headless=not args.headful))
    except NotamScraperError as exc:
        raise SystemExit(f"Sync failed while reading NOTAM site: {exc}") from exc


if __name__ == "__main__":
    main()
