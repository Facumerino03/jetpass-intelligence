"""Orchestrates scraper + parser + repository to import aerodrome data from AIP."""

from __future__ import annotations

import logging
from pathlib import Path

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse
from app.services.enrichment import enrich_aerodrome_document
from app.services.scraper.aip_parser import AipParserError, parse_aerodrome_from_documents
from app.services.scraper.aip_scraper import AipScraperError, download_aip_pdfs

logger = logging.getLogger(__name__)


class AipImportError(Exception):
    """Pipeline failure: scraper, parser or persistence raised an error."""


def _select_ad20_documents(pdf_paths: list[Path], icao: str) -> list[Path]:
    ad20_paths = [path for path in pdf_paths if "AD-2.0" in path.name.upper()]
    if not ad20_paths:
        raise AipImportError(
            f"[{icao}] Scraper output does not include required AD-2.0 PDF."
        )
    return ad20_paths


async def import_aerodrome_from_aip(
    icao: str,
    output_dir: Path | None = None,
    enrich: bool = True,
) -> AerodromeResponse:
    """Download AIP PDFs, parse them and upsert the aerodrome in MongoDB.

    Args:
        icao: Four-letter ICAO aerodrome code (e.g. ``"SAMR"``).
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

    # 2 — Parse only AD-2.0 for aerodrome core sections.
    try:
        ad20_paths = _select_ad20_documents(pdf_paths, icao)
        aerodrome_data = parse_aerodrome_from_documents(ad20_paths, icao=icao)
    except AipParserError as exc:
        raise AipImportError(f"[{icao}] Parser failed for downloaded AIP docs: {exc}") from exc

    # 3 — Persist with Beanie (upsert + AIRAC versioning)
    try:
        aerodrome_doc = await aerodrome_repo.upsert(aerodrome_data)
    except Exception as exc:
        raise AipImportError(f"[{icao}] Database upsert failed: {exc}") from exc

    logger.info(
        "[%s] Import complete — %d section(s) persisted. AIRAC: %s",
        icao,
        len(aerodrome_doc.current.ad_sections),
        aerodrome_doc.current.meta.airac_cycle,
    )
    if enrich:
        try:
            aerodrome_doc = await enrich_aerodrome_document(aerodrome_doc)
        except Exception as exc:
            logger.warning("[%s] Enrichment failed: %s", icao, exc)

    return AerodromeResponse.from_document(aerodrome_doc)
