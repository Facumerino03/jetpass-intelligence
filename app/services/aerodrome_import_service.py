"""Orchestrates scraper + parser + repository to import aerodrome data from AIP."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse
from app.services.scraper.aip_parser import AipParserError, parse_aerodrome_from_documents
from app.services.scraper.aip_scraper import AipScraperError, download_aip_pdfs

logger = logging.getLogger(__name__)


class AipImportError(Exception):
    """Pipeline failure: scraper, parser or persistence raised an error."""


async def import_aerodrome_from_aip(
    icao: str,
    db: AsyncSession,
    output_dir: Path | None = None,
) -> AerodromeResponse:
    """Download AIP PDFs, parse them and upsert the aerodrome in the database.

    Args:
        icao: Four-letter ICAO aerodrome code (e.g. ``"SAMR"``).
        db: Active async SQLAlchemy session.
        output_dir: Override for the PDF download directory.

    Returns:
        :class:`~app.schemas.aerodrome.AerodromeResponse` with the persisted data.

    Raises:
        AipImportError: If scraping, parsing or persistence fails.
    """
    icao = icao.strip().upper()

    # 1 — Download PDFs
    try:
        pdf_paths = await download_aip_pdfs(icao, output_dir=output_dir)
    except AipScraperError as exc:
        raise AipImportError(f"[{icao}] Scraper failed: {exc}") from exc

    # 2 — Parse all downloaded AIP docs; model decides relevance.
    try:
        aerodrome_data = parse_aerodrome_from_documents(pdf_paths)
    except AipParserError as exc:
        raise AipImportError(f"[{icao}] Parser failed for downloaded AIP docs: {exc}") from exc

    # 3 — Persist
    try:
        aerodrome = await aerodrome_repo.upsert(db, aerodrome_data)
    except Exception as exc:
        raise AipImportError(f"[{icao}] Database upsert failed: {exc}") from exc

    logger.info(
        "[%s] Import complete — %d runway(s) persisted.",
        icao,
        len(aerodrome.runways),
    )
    return AerodromeResponse.model_validate(aerodrome)


