"""Aeronautical domain document models (Beanie/MongoDB)."""

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import ChangeLogEntry, DocumentMeta, MetaSource
from app.models.pre_llm_artifacts import PreLlmSectionsDocument, RawExtractionDocument

__all__ = [
    "AerodromeDocument",
    "AerodromeSnapshot",
    "AdSection",
    "SectionMeta",
    "DocumentMeta",
    "MetaSource",
    "ChangeLogEntry",
    "RawExtractionDocument",
    "PreLlmSectionsDocument",
]

# Ordered list used by init_beanie to register all document classes.
ALL_DOCUMENTS = [
    AerodromeDocument,
    RawExtractionDocument,
    PreLlmSectionsDocument,
]
