"""Tool: download AIP PDFs for a given ICAO aerodrome code."""

from __future__ import annotations

import logging
from pathlib import Path

from app.services.scraper.aip_scraper import AipScraperError, download_aip_pdfs

logger = logging.getLogger(__name__)


class ScrapeToolError(Exception):
    """Raised when the scrape tool fails to download AIP documents."""


async def scrape(icao: str, output_dir: Path | None = None) -> list[Path]:
    """Download AIP PDFs for *icao* and return the local file paths.

    Args:
        icao: Four-letter ICAO aerodrome code.
        output_dir: Override for the PDF download directory.

    Returns:
        List of ``Path`` objects pointing to the downloaded PDF files.

    Raises:
        ScrapeToolError: If the underlying scraper fails.
    """
    try:
        paths = await download_aip_pdfs(icao.strip().upper(), output_dir=output_dir)
    except AipScraperError as exc:
        raise ScrapeToolError(f"[{icao}] Scrape failed: {exc}") from exc
    logger.debug("[%s] Scrape tool: downloaded %d PDF(s)", icao, len(paths))
    return paths
