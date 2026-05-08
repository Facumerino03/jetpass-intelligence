"""Orchestrates scraper + Document AI + repository to import aerodrome data from AIP."""

from __future__ import annotations

import logging
from pathlib import Path

from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse
from app.services.documentai import DocumentAiAipError, parse_aerodrome_from_documentai
from app.services.scraper.aip_scraper import AipScraperError, download_aip_pdfs

logger = logging.getLogger(__name__)


class AipImportError(Exception):
    """Pipeline failure: scraper, Document AI or persistence raised an error."""


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
) -> AerodromeResponse:
    """Download AIP PDFs, extract AD-2.0 with Document AI, and upsert MongoDB."""
    icao = icao.strip().upper()

    try:
        pdf_paths = await download_aip_pdfs(icao, output_dir=output_dir)
    except AipScraperError as exc:
        raise AipImportError(f"[{icao}] Scraper failed: {exc}") from exc

    try:
        ad20_paths = _select_ad20_documents(pdf_paths, icao)
        aerodrome_data = parse_aerodrome_from_documentai(ad20_paths[0], icao=icao)
    except DocumentAiAipError as exc:
        raise AipImportError(f"[{icao}] Document AI extraction failed: {exc}") from exc

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
    return AerodromeResponse.from_document(aerodrome_doc)
