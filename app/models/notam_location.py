"""NOTAM site location mapping by ICAO."""

from __future__ import annotations

from datetime import datetime

from beanie import Document
from pymongo import ASCENDING, IndexModel


class NotamLocationDocument(Document):
    """Persistent mapping between ICAO and exact NOTAM site dropdown label."""

    id: str  # ICAO code -> _id
    icao: str
    site_label_exact: str
    updated_at: datetime

    class Settings:
        name = "notam_locations"
        indexes = [
            IndexModel([("icao", ASCENDING)], unique=True),
        ]
