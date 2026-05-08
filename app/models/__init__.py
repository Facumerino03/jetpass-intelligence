"""Aeronautical domain document models (Beanie/MongoDB)."""

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import ChangeLogEntry, DocumentMeta, MetaSource
from app.models.notam_location import NotamLocationDocument
from app.models.notam import NotamDocument, RawNotam

__all__ = [
    "AerodromeDocument",
    "AerodromeSnapshot",
    "AdSection",
    "SectionMeta",
    "DocumentMeta",
    "MetaSource",
    "ChangeLogEntry",
    "RawNotam",
    "NotamDocument",
    "NotamLocationDocument",
]

ALL_DOCUMENTS = [
    AerodromeDocument,
    NotamDocument,
    NotamLocationDocument,
]
