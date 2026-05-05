"""Aeronautical domain document models (Beanie/MongoDB)."""

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import ChangeLogEntry, DocumentMeta, MetaSource
from app.models.notam_location import NotamLocationDocument
from app.models.notam import NotamDocument, RawNotam
from app.models.pre_llm_artifacts import PreLlmSectionsDocument, RawExtractionDocument

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
    "RawExtractionDocument",
    "PreLlmSectionsDocument",
]

# Ordered list used by init_beanie to register all document classes.
ALL_DOCUMENTS = [
    AerodromeDocument,
    NotamDocument,
    NotamLocationDocument,
    RawExtractionDocument,
    PreLlmSectionsDocument,
]
