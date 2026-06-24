#!/usr/bin/env python3
"""CLI to manually sync the OurAirports CSV from upstream.

Usage:

    uv run python -m scripts.sync_airports_csv

The file is downloaded from OurAirports and atomically replaces
``app/docs/airports.csv``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CSV_PATH = Path(__file__).resolve().parent.parent / "app" / "docs" / "airports.csv"


async def main() -> None:
    from app.intelligence.geo.airports_sync import download_airports_csv

    logger.info("Downloading airports.csv from OurAirports …")
    rows, error = await download_airports_csv(CSV_PATH)
    if error:
        logger.error("Sync failed: %s", error)
        sys.exit(1)
    logger.info("Sync complete — %d rows written to %s", rows, CSV_PATH)


if __name__ == "__main__":
    asyncio.run(main())
