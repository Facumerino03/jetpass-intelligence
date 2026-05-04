"""Tool: LLM enrichment for AerodromeDocument sections."""

from __future__ import annotations

import logging

from app.models.aerodrome import AerodromeDocument
from app.services.enrichment.aerodrome_enricher import enrich_aerodrome_document

logger = logging.getLogger(__name__)


class EnrichToolError(Exception):
    """Raised when the enrich tool encounters an unrecoverable error."""


async def enrich(
    doc: AerodromeDocument,
    section_ids: list[str] | None = None,
    *,
    save: bool = True,
) -> AerodromeDocument:
    """Run LLM enrichment over *doc* sections and optionally persist.

    Args:
        doc: In-memory or persisted :class:`~app.models.aerodrome.AerodromeDocument`.
        section_ids: Subset of section IDs to enrich. ``None`` enriches all.
        save: Whether to call ``doc.save()`` after enrichment. Set to ``False``
            when the caller controls the final persistence step.

    Returns:
        The enriched document (same object, mutated in place).

    Raises:
        EnrichToolError: If enrichment raises an unexpected exception.
    """
    try:
        enriched = await enrich_aerodrome_document(doc, section_ids=section_ids, save=save)
    except Exception as exc:
        raise EnrichToolError(f"[{doc.icao}] Enrichment failed: {exc}") from exc
    logger.debug("[%s] Enrich tool: sections processed (save=%s)", doc.icao, save)
    return enriched
