"""Schemas for flexible AD 2.0 aerodrome storage and API responses."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import DocumentMeta


class SectionMetaSchema(BaseModel):
    airac_cycle: str | None = None
    source_page: int | None = None


class SectionSchema(BaseModel):
    section_id: str
    title: str
    raw_text: str
    data: dict[str, Any] = Field(default_factory=dict)
    anchors: dict[str, Any] | None = None
    section_meta: SectionMetaSchema | None = None

    @field_validator("raw_text")
    @classmethod
    def raw_text_must_not_be_empty(cls, v: str) -> str:
        text = v.strip()
        if not text:
            raise ValueError("raw_text must not be empty")
        return text


class AerodromeCreate(BaseModel):
    """Parser output DTO consumed by repository upsert."""

    icao_code: str
    name: str
    full_name: str | None = None
    airac_cycle: str = "unknown"
    airac_effective_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    airac_expiry_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_document: str | None = None
    source_url: str | None = None
    downloaded_by: str | None = None
    ad_sections: list[SectionSchema] = Field(default_factory=list)

    @field_validator("icao_code")
    @classmethod
    def icao_must_be_argentine(cls, v: str) -> str:
        code = v.strip().upper()
        if not code.startswith("SA"):
            raise ValueError(f'icao_code must start with "SA" for Argentine aerodromes (got "{v}")')
        return code


class SnapshotResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ad_sections: list[SectionSchema] = Field(default_factory=list)
    meta: DocumentMeta = Field(alias="_meta")

    @classmethod
    def from_snapshot(cls, snapshot: AerodromeSnapshot) -> "SnapshotResponse":
        sections = [
            SectionSchema(
                section_id=s.section_id,
                title=s.title,
                raw_text=s.raw_text,
                data=s.data,
                anchors=s.anchors,
                section_meta=SectionMetaSchema(
                    airac_cycle=s.section_meta.airac_cycle if s.section_meta else None,
                    source_page=s.section_meta.source_page if s.section_meta else None,
                ) if s.section_meta else None,
            )
            for s in snapshot.ad_sections
        ]
        return cls(ad_sections=sections, _meta=snapshot.meta)


class AerodromeResponse(BaseModel):
    icao: str
    name: str
    full_name: str | None = None
    current: SnapshotResponse
    history: list[SnapshotResponse] = Field(default_factory=list)

    @classmethod
    def from_document(cls, doc: AerodromeDocument) -> "AerodromeResponse":
        return cls(
            icao=doc.icao,
            name=doc.name,
            full_name=doc.full_name,
            current=SnapshotResponse.from_snapshot(doc.current),
            history=[SnapshotResponse.from_snapshot(s) for s in doc.history],
        )


class SectionResponse(SectionSchema):
    @classmethod
    def from_model(cls, section: AdSection) -> "SectionResponse":
        return cls(
            section_id=section.section_id,
            title=section.title,
            raw_text=section.raw_text,
            data=section.data,
            anchors=section.anchors,
            section_meta=SectionMetaSchema(
                airac_cycle=section.section_meta.airac_cycle if section.section_meta else None,
                source_page=section.section_meta.source_page if section.section_meta else None,
            ) if section.section_meta else None,
        )
