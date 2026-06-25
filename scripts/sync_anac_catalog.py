#!/usr/bin/env python3
"""CLI to manually sync the ANAC MADHEL aerodrome catalog.

Usage:

    uv run python -m scripts.sync_anac_catalog
"""

from __future__ import annotations

import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    from app.intelligence.aerodromes_catalog_service import sync_anac_catalog

    logger.info("Syncing ANAC MADHEL aerodrome catalog …")
    result = await sync_anac_catalog(force_refresh=True)
    if result.alerts:
        for alert in result.alerts:
            logger.warning("%s: %s", alert.code, alert.message)
    logger.info(
        "Sync complete — %d aerodromes (%d helipuertos skipped, %d without ICAO)",
        result.total_aerodromes,
        result.total_helipuertos_skipped,
        result.total_without_icao,
    )
    if result.metadata.get("failed_locals"):
        logger.error("Failed locals: %s", result.metadata["failed_locals"])
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
