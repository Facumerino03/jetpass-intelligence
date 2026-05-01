"""Aerodrome repository — data access via Beanie (no business logic here)."""

from __future__ import annotations

from datetime import datetime, timezone
from pydantic import ValidationError

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot, SectionMeta
from app.models.meta import ChangeLogEntry, DocumentMeta, MetaSource
from app.schemas.aerodrome import AerodromeCreate, SectionSchema


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_section_id(section_id: str) -> str:
    return " ".join(section_id.upper().split())


def _validate_sections(sections: list[SectionSchema]) -> None:
    if len(sections) != 25:
        raise ValueError(f"Expected 25 AD 2.x sections, got {len(sections)}")
    for section in sections:
        if not section.raw_text.strip():
            raise ValueError(f"Section '{section.section_id}' has empty raw_text")


def _to_model_section(section: SectionSchema) -> AdSection:
    return AdSection(
        section_id=section.section_id,
        title=section.title,
        section_title=section.section_title,
        raw_text=section.raw_text,
        data=section.data,
        anchors=section.anchors,
        section_meta=SectionMeta(
            airac_cycle=section.section_meta.airac_cycle,
            source_page=section.section_meta.source_page,
        ) if section.section_meta else None,
    )


def _build_meta(data: AerodromeCreate, version: int, replaces: str | None) -> DocumentMeta:
    now = _utcnow()
    return DocumentMeta(
        airac_cycle=data.airac_cycle,
        airac_effective_date=data.airac_effective_date,
        airac_expiry_date=data.airac_expiry_date,
        source=MetaSource(
            type="AIP",
            document=data.source_document,
            url=data.source_url,
            downloaded_at=now,
            downloaded_by=data.downloaded_by,
        ),
        status="active",
        version=version,
        replaces=replaces,
        replaced_by=None,
        valid_from=data.airac_effective_date,
        valid_to=None,
        created_at=now,
        updated_at=now,
        change_log=[
            ChangeLogEntry(
                airac_cycle=data.airac_cycle,
                changed_by=data.downloaded_by or "aip-parser",
                changed_fields=["current.ad_sections", "current._meta"],
                notes="Upsert AD 2.0 snapshot",
            )
        ],
    )


def _build_snapshot(data: AerodromeCreate, version: int, replaces: str | None) -> AerodromeSnapshot:
    return AerodromeSnapshot(
        ad_sections=[_to_model_section(section) for section in data.ad_sections],
        _meta=_build_meta(data, version=version, replaces=replaces),
    )


def _superseded_snapshot(snapshot: AerodromeSnapshot) -> AerodromeSnapshot:
    now = _utcnow()
    superseded_meta = snapshot.meta.model_copy(
        update={
            "status": "superseded",
            "valid_to": now,
            "updated_at": now,
        }
    )
    return snapshot.model_copy(update={"meta": superseded_meta})


async def get_by_icao(icao: str) -> AerodromeDocument | None:
    normalized = icao.strip().upper()
    try:
        return await AerodromeDocument.get(normalized)
    except ValidationError:
        # Legacy records might not match the current schema.
        return None


async def get_all() -> list[AerodromeDocument]:
    return await AerodromeDocument.find_all().sort("+icao").to_list()


async def get_section_by_icao(icao: str, section_id: str) -> AdSection | None:
    aerodrome = await get_by_icao(icao)
    if aerodrome is None:
        return None
    normalized = _normalize_section_id(section_id)
    for section in aerodrome.current.ad_sections:
        if _normalize_section_id(section.section_id) == normalized:
            return section
    return None


async def upsert(data: AerodromeCreate) -> AerodromeDocument:
    """Create or update aerodrome snapshot with internal AIRAC versioning."""
    _validate_sections(data.ad_sections)

    icao = data.icao_code.strip().upper()
    collection = AerodromeDocument.get_pymongo_collection()
    raw_existing = await collection.find_one({"_id": icao})

    if raw_existing is not None and "current" not in raw_existing:
        migrated = AerodromeDocument(
            id=icao,
            icao=icao,
            name=data.name,
            full_name=data.full_name,
            current=_build_snapshot(data, version=1, replaces=None),
            history=[],
        )
        await collection.replace_one(
            {"_id": icao},
            migrated.model_dump(by_alias=True),
            upsert=True,
        )
        return migrated

    existing = await get_by_icao(icao)

    if raw_existing is not None and existing is None:
        repaired = AerodromeDocument(
            id=icao,
            icao=icao,
            name=data.name,
            full_name=data.full_name,
            current=_build_snapshot(data, version=1, replaces=None),
            history=[],
        )
        await collection.replace_one(
            {"_id": icao},
            repaired.model_dump(by_alias=True),
            upsert=True,
        )
        return repaired

    if existing is None:
        doc = AerodromeDocument(
            id=icao,
            icao=icao,
            name=data.name,
            full_name=data.full_name,
            current=_build_snapshot(data, version=1, replaces=None),
            history=[],
        )
        await doc.insert()
        return doc

    current_meta = existing.current.meta
    next_version = current_meta.version + 1
    replaces = f"{icao}-v{current_meta.version}"
    new_snapshot = _build_snapshot(data, version=next_version, replaces=replaces)

    if current_meta.airac_cycle != data.airac_cycle:
        existing.history.append(_superseded_snapshot(existing.current))

    existing.name = data.name
    existing.full_name = data.full_name
    existing.current = new_snapshot
    await existing.save()
    return existing
