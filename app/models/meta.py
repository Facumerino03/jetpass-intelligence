"""Shared _meta document block used by all aeronautical collections."""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class MetaSource(BaseModel):
    type: Literal["AIP", "NOTAM", "MANUAL", "IMPORT"] = "IMPORT"
    document: str | None = None
    url: str | None = None
    downloaded_at: datetime | None = None
    downloaded_by: str | None = None


class ChangeLogEntry(BaseModel):
    airac_cycle: str
    date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    changed_fields: list[str] = Field(default_factory=list)
    changed_by: str = "system"
    notes: str | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DocumentMeta(BaseModel):
    airac_cycle: str = "unknown"
    airac_effective_date: datetime = Field(default_factory=_utcnow)
    airac_expiry_date: datetime = Field(default_factory=_utcnow)
    source: MetaSource = Field(default_factory=MetaSource)
    status: Literal["active", "superseded", "withdrawn"] = "active"
    version: int = 1
    replaces: str | None = None
    replaced_by: str | None = None
    valid_from: datetime = Field(default_factory=_utcnow)
    valid_to: datetime | None = None
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)
    change_log: list[ChangeLogEntry] = Field(default_factory=list)
