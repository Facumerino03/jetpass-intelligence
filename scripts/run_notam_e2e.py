"""CLI script: run NOTAM intelligence end-to-end for one ICAO.

Usage examples:
    uv run python -m scripts.run_notam_e2e --icao SAEZ
    uv run python -m scripts.run_notam_e2e --icao SAEZ --force-refresh
"""

from __future__ import annotations

import argparse
import asyncio
from pprint import pprint

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.intelligence.notam_intel_service import get_notam_intelligence


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run NOTAM cache/scrape flow for one aerodrome ICAO and print diagnostics."
    )
    parser.add_argument(
        "--icao",
        required=True,
        type=str.upper,
        metavar="ICAO",
        help="Four-letter ICAO aerodrome code (e.g. SAEZ).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Bypass cache and force a new scrape.",
    )
    return parser.parse_args()


async def _run(icao: str, *, force_refresh: bool) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    result = await get_notam_intelligence(icao, force_refresh=force_refresh)

    print(f"\n✓ NOTAM intelligence for {icao}")
    print(f"  Source      : {result.source}")
    print(f"  Aerodrome   : {result.aerodrome_name or '(unresolved)'}")
    print(f"  Site update : {result.site_last_updated_at or '(unknown)'}")
    print(f"  Fetched at  : {result.fetched_at or '(not persisted)'}")
    print(f"  AD NOTAMs   : {len(result.aerodrome_notams)}")
    print(f"  FIR NOTAMs  : {len(result.fir_notams)}")
    print(f"  FIR buckets : {len(result.fir_notams_by_location)}")

    if result.alerts:
        print("\nAlerts:")
        for alert in result.alerts:
            print(f"  - [{alert.level.value}] {alert.code}: {alert.message}")

    if result.messages:
        print("\nPipeline messages:")
        for message in result.messages:
            print(f"  - {message}")

    if result.metadata:
        print("\nMetadata:")
        pprint(result.metadata, sort_dicts=True)

    if result.aerodrome_notams:
        print("\nSample aerodrome NOTAM:")
        pprint(result.aerodrome_notams[0].model_dump(), sort_dicts=True)

    if result.fir_notams:
        print("\nSample FIR NOTAM:")
        pprint(result.fir_notams[0].model_dump(), sort_dicts=True)

    if result.fir_notams_by_location:
        print("\nFIR buckets:")
        for location, notams in sorted(result.fir_notams_by_location.items()):
            print(f"  - {location}: {len(notams)}")


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args.icao, force_refresh=args.force_refresh))


if __name__ == "__main__":
    main()
