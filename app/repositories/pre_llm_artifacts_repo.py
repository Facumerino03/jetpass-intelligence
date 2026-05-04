"""Persistence for marker raw extraction and pre-LLM payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.models.pre_llm_artifacts import PreLlmSectionsDocument, RawExtractionDocument


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _doc_id(icao: str, airac_cycle: str) -> str:
    return f"{icao.strip().upper()}:{airac_cycle.strip()}"


async def upsert_raw_extraction(
    *,
    icao: str,
    airac_cycle: str,
    source_filename: str,
    payload: dict[str, Any],
) -> None:
    icao_u = icao.strip().upper()
    cycle = airac_cycle.strip()
    doc_id = _doc_id(icao_u, cycle)
    now = _utcnow()
    existing = await RawExtractionDocument.get(doc_id)
    if existing:
        existing.source_filename = source_filename
        existing.payload = payload
        existing.updated_at = now
        await existing.save()
        return
    await RawExtractionDocument(
        id=doc_id,
        icao=icao_u,
        airac_cycle=cycle,
        source_filename=source_filename,
        payload=payload,
        updated_at=now,
    ).insert()


async def upsert_pre_llm_sections(
    *,
    icao: str,
    airac_cycle: str,
    source_filename: str,
    payload: dict[str, Any],
) -> None:
    icao_u = icao.strip().upper()
    cycle = airac_cycle.strip()
    doc_id = _doc_id(icao_u, cycle)
    now = _utcnow()
    existing = await PreLlmSectionsDocument.get(doc_id)
    if existing:
        existing.source_filename = source_filename
        existing.payload = payload
        existing.updated_at = now
        await existing.save()
        return
    await PreLlmSectionsDocument(
        id=doc_id,
        icao=icao_u,
        airac_cycle=cycle,
        source_filename=source_filename,
        payload=payload,
        updated_at=now,
    ).insert()
