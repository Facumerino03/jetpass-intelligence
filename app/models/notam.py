"""NOTAM storage model for cache-first intelligence responses."""

from __future__ import annotations

from datetime import datetime

from beanie import Document
from pydantic import BaseModel, Field
from pymongo import ASCENDING, IndexModel


class RawNotam(BaseModel):
    """Single NOTAM entry scraped from the ANAC NOTAM website."""

    notam_id: str
    location: str
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    raw_text: str
    english_text: str | None = None
    spanish_text: str | None = None


class NotamDocument(Document):
    """Cached NOTAM payload for one ICAO aerodrome."""

    id: str  # ICAO code -> _id in MongoDB
    icao: str
    aerodrome_name: str
    site_last_updated_at: datetime | None = None
    fetched_at: datetime
    aerodrome_notams: list[RawNotam]
    fir_notams: list[RawNotam]
    fir_notams_by_location: dict[str, list[RawNotam]] = Field(default_factory=dict)

    class Settings:
        name = "notams"
        indexes = [
            IndexModel([("icao", ASCENDING)], unique=True),
            IndexModel([("fetched_at", ASCENDING)]),
        ]
