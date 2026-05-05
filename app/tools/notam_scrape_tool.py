"""Tool: scrape NOTAMs for one aerodrome name plus global FIR advisories."""

from __future__ import annotations

from app.services.scraper.notam_scraper import (
    NotamLocationNotFoundError,
    NotamScrapeResult,
    NotamScraperError,
    scrape_notams_for_aerodrome,
)


class NotamScrapeToolError(Exception):
    """Raised when the NOTAM scrape tool fails."""


async def scrape(aerodrome_name: str) -> NotamScrapeResult:
    try:
        return await scrape_notams_for_aerodrome(aerodrome_name.strip().upper())
    except (NotamScraperError, NotamLocationNotFoundError) as exc:
        raise NotamScrapeToolError(f"[{aerodrome_name}] NOTAM scrape failed: {exc}") from exc
