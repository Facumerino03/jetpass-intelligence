"""Aeronautical domain document models (Beanie/MongoDB)."""

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import ChangeLogEntry, DocumentMeta, MetaSource

__all__ = [
    "AerodromeDocument",
    "AerodromeSnapshot",
    "AdSection",
    "SectionMeta",
    "DocumentMeta",
    "MetaSource",
    "ChangeLogEntry",
]

# Ordered list used by init_beanie to register all document classes.
ALL_DOCUMENTS = [
    AerodromeDocument,
]
