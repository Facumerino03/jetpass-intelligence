"""Tool: parse AIP PDF files into a structured AerodromeCreate DTO."""

from __future__ import annotations

import logging
from pathlib import Path

from app.schemas.aerodrome import AerodromeCreate
from app.services.scraper.aip_parser import AipParserError, parse_aerodrome_from_documents

logger = logging.getLogger(__name__)

_AD20_MARKER = "AD-2.0"


class ParseToolError(Exception):
    """Raised when the parse tool fails to process AIP documents."""


def parse(pdf_paths: list[Path], icao: str) -> AerodromeCreate:
    """Parse *pdf_paths* (filtered to AD-2.0) and return a structured DTO.

    Args:
        pdf_paths: Paths returned by the scrape tool.
        icao: Four-letter ICAO aerodrome code used for error context.

    Returns:
        :class:`~app.schemas.aerodrome.AerodromeCreate` ready for enrichment.

    Raises:
        ParseToolError: If no AD-2.0 document is found or parsing fails.
    """
    ad20_paths = [p for p in pdf_paths if _AD20_MARKER in p.name.upper()]
    if not ad20_paths:
        raise ParseToolError(
            f"[{icao}] No AD-2.0 PDF found in scraper output. "
            f"Available: {[p.name for p in pdf_paths]}"
        )
    try:
        data = parse_aerodrome_from_documents(ad20_paths, icao=icao)
    except AipParserError as exc:
        raise ParseToolError(f"[{icao}] Parse failed: {exc}") from exc
    logger.debug(
        "[%s] Parse tool: %d section(s) extracted", icao, len(data.ad_sections)
    )
    return data
