"""Aerodrome intelligence service — DB-first fetch with scrape/enrich fallback."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.intelligence.contracts import (
    Alert,
    AlertLevel,
    AerodromeIntelResult,
)
from app.models.aerodrome import AerodromeDocument
from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse
from app.tools.aip_enrich_tool import EnrichToolError, enrich
from app.tools.aip_parse_tool import ParseToolError, parse
from app.tools.aip_scrape_tool import ScrapeToolError, scrape

logger = logging.getLogger(__name__)

# Configurable TTL: documents older than this are considered stale.
_CACHE_TTL_DAYS = 30


def _is_stale(doc: AerodromeDocument) -> bool:
    updated_at = doc.current.meta.updated_at
    if updated_at is None:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(days=_CACHE_TTL_DAYS)
    return updated_at < cutoff


async def get_aerodrome_intelligence(
    icao: str,
    *,
    force_refresh: bool = False,
    section_ids: list[str] | None = None,
    output_dir: Path | None = None,
) -> AerodromeIntelResult:
    """Return aerodrome intelligence, hitting the DB cache when possible.

    Strategy:
    - If a fresh document exists in MongoDB and ``force_refresh`` is False,
      return it immediately (source="cache").
    - Otherwise run the full pipeline: scrape → parse → enrich → persist
      and return the result (source="fresh_import").

    Args:
        icao: Four-letter ICAO aerodrome code.
        force_refresh: Bypass the cache and always run the pipeline.
        section_ids: Restrict LLM enrichment to specific AD 2.x sections.
        output_dir: Override for the PDF download directory.

    Returns:
        :class:`~app.intelligence.contracts.AerodromeIntelResult`
    """
    icao = icao.strip().upper()
    alerts: list[Alert] = []

    # ------------------------------------------------------------------
    # 1. DB-first lookup
    # ------------------------------------------------------------------
    existing = await aerodrome_repo.get_by_icao(icao)
    if existing is not None and not force_refresh and not _is_stale(existing):
        logger.info("[%s] Serving aerodrome intelligence from cache.", icao)
        return AerodromeIntelResult(
            icao=icao,
            data=AerodromeResponse.from_document(existing),
            source="cache",
            airac_cycle=existing.current.meta.airac_cycle,
            messages=["Aerodrome data served from cache."],
        )

    if existing is not None and not force_refresh:
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="STALE_CACHE",
                message=f"Cached data for {icao} is older than {_CACHE_TTL_DAYS} days. Refreshing.",
            )
        )

    # ------------------------------------------------------------------
    # 2. Pipeline: scrape → parse → enrich → persist
    # ------------------------------------------------------------------
    logger.info("[%s] Running full import pipeline.", icao)

    try:
        pdf_paths = await scrape(icao, output_dir=output_dir)
    except ScrapeToolError as exc:
        alerts.append(Alert(level=AlertLevel.ERROR, code="SCRAPE_FAILED", message=str(exc)))
        logger.error("[%s] Scrape step failed: %s", icao, exc)
        return AerodromeIntelResult(
            icao=icao,
            data=None,
            source="fresh_import",
            airac_cycle=None,
            alerts=alerts,
        )

    try:
        aerodrome_data = parse(pdf_paths, icao)
    except ParseToolError as exc:
        alerts.append(Alert(level=AlertLevel.ERROR, code="PARSE_FAILED", message=str(exc)))
        logger.error("[%s] Parse step failed: %s", icao, exc)
        return AerodromeIntelResult(
            icao=icao,
            data=None,
            source="fresh_import",
            airac_cycle=None,
            alerts=alerts,
        )

    doc, is_new = await aerodrome_repo.prepare_document(aerodrome_data)

    try:
        doc = await enrich(doc, section_ids=section_ids, save=False)
    except EnrichToolError as exc:
        alerts.append(
            Alert(level=AlertLevel.WARNING, code="ENRICH_FAILED", message=str(exc))
        )
        logger.warning("[%s] Enrich step failed, persisting raw data: %s", icao, exc)

    doc = await aerodrome_repo.persist_document(doc, is_new=is_new)

    logger.info(
        "[%s] Import pipeline complete (is_new=%s, airac=%s).",
        icao,
        is_new,
        doc.current.meta.airac_cycle,
    )

    return AerodromeIntelResult(
        icao=icao,
        data=AerodromeResponse.from_document(doc),
        source="fresh_import",
        airac_cycle=doc.current.meta.airac_cycle,
        alerts=alerts,
        metadata={
            "is_new": is_new,
            "sections_count": len(doc.current.ad_sections),
        },
    )
