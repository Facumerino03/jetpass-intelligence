"""MongoDB documents for marker raw extraction and pre-LLM section payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from beanie import Document
from pydantic import Field
from pymongo import ASCENDING, IndexModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RawExtractionDocument(Document):
    """Full marker JSON tree for an ICAO + AIRAC cycle."""

    id: str  # "{ICAO}:{airac_cycle}"
    icao: str
    airac_cycle: str
    source_filename: str
    payload: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=_utcnow)

    class Settings:
        name = "raw_extractions"
        indexes = [
            IndexModel([("icao", ASCENDING), ("airac_cycle", ASCENDING)], unique=True),
        ]


class PreLlmSectionsDocument(Document):
    """Per-section structured hints derived from marker (tables, blocks, quality)."""

    id: str  # "{ICAO}:{airac_cycle}"
    icao: str
    airac_cycle: str
    source_filename: str
    payload: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=_utcnow)

    class Settings:
        name = "pre_llm_sections"
        indexes = [
            IndexModel([("icao", ASCENDING), ("airac_cycle", ASCENDING)], unique=True),
        ]
