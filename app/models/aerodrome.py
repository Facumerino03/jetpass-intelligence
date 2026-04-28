"""Flexible aerodrome document model for AD 2.0 storage."""

from typing import Any

from beanie import Document
from pydantic import BaseModel, ConfigDict, Field
from pymongo import ASCENDING, IndexModel

from app.models.meta import DocumentMeta


class SectionMeta(BaseModel):
    """Optional metadata at section level (for mixed AIRAC pages)."""

    airac_cycle: str | None = None
    source_page: int | None = None


class AdSection(BaseModel):
    """One AD 2.x section with literal text and flexible structured data."""

    section_id: str
    title: str
    raw_text: str
    data: dict[str, Any] = Field(default_factory=dict)
    anchors: dict[str, Any] | None = None
    section_meta: SectionMeta | None = None


class AerodromeSnapshot(BaseModel):
    """Versioned payload for one AIRAC snapshot."""

    model_config = ConfigDict(populate_by_name=True)

    ad_sections: list[AdSection] = Field(default_factory=list)
    meta: DocumentMeta = Field(alias="_meta", default_factory=DocumentMeta)


class AerodromeDocument(Document):
    """Single-document AD 2.0 store with current + history snapshots."""

    model_config = ConfigDict(populate_by_name=True)

    id: str  # ICAO code -> _id in MongoDB
    icao: str
    name: str
    full_name: str | None = None

    current: AerodromeSnapshot
    history: list[AerodromeSnapshot] = Field(default_factory=list)

    class Settings:
        name = "aerodromes"
        indexes = [
            IndexModel([("icao", ASCENDING)], unique=True),
            IndexModel([("current._meta.airac_cycle", ASCENDING)]),
        ]
