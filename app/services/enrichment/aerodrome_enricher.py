"""LLM enrichment tool for AD 2.0 section structured data."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

from ollama import Client
from pydantic import BaseModel, Field, ValidationError

from app.core.config import get_settings
from app.models.aerodrome import AerodromeDocument

logger = logging.getLogger(__name__)

PROMPT_VERSION = "ad2-v1"
TARGET_SECTION_IDS = ("AD 2.2", "AD 2.12", "AD 2.18", "AD 2.19")


class Coordinates(BaseModel):
    lat: str | None = None
    lon: str | None = None


class AdministrativeData(BaseModel):
    location_indicator: str | None = None
    aerodrome_name: str | None = None
    served_city: str | None = None
    province_state: str | None = None
    arp_coordinates: Coordinates | None = None
    elevation_ft: int | None = None
    magnetic_variation: str | None = None
    aerodrome_type: str | None = None
    customs_immigration: str | None = None
    remarks: str | None = None


class RunwayItem(BaseModel):
    designator: str
    length_m: int | None = None
    width_m: int | None = None
    surface: str | None = None
    pcn: str | None = None
    slope_pct: float | None = None
    threshold_elevation_ft: int | None = None
    lights: str | None = None
    remarks: str | None = None


class RunwaysData(BaseModel):
    runways: list[RunwayItem] = Field(default_factory=list)


class CommItem(BaseModel):
    service: str
    callsign: str | None = None
    frequency_mhz: str | None = None
    hours: str | None = None
    remarks: str | None = None


class CommsData(BaseModel):
    services: list[CommItem] = Field(default_factory=list)


class NavaidItem(BaseModel):
    type: str
    identifier: str | None = None
    frequency: str | None = None
    channel: str | None = None
    coordinates: Coordinates | None = None
    elevation_ft: int | None = None
    hours: str | None = None
    remarks: str | None = None


class NavaidsData(BaseModel):
    aids: list[NavaidItem] = Field(default_factory=list)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _get_client() -> Client:
    settings = get_settings()
    return Client(host=settings.ollama_host)


def _build_messages(icao: str, section_id: str, raw_text: str) -> list[dict[str, str]]:
    system = (
        "You extract structured aeronautical data from AD 2.0 sections. "
        "Return only JSON that matches the provided schema. "
        "Use null when unknown. Do not invent values."
    )
    user = (
        f"ICAO: {icao}\n"
        f"Section: {section_id}\n"
        "Extract fields from this raw text:\n\n"
        f"{raw_text}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _chat_structured(
    icao: str,
    section_id: str,
    raw_text: str,
    schema: dict[str, Any],
) -> str:
    settings = get_settings()
    client = _get_client()
    response = client.chat(
        model=settings.ollama_model,
        messages=_build_messages(icao, section_id, raw_text),
        format=schema,
        options={"temperature": settings.ollama_temperature},
    )
    return response.message.content


def _extract_for_section(icao: str, section_id: str, raw_text: str) -> tuple[str, dict[str, Any]]:
    if section_id == "AD 2.2":
        model: type[BaseModel] = AdministrativeData
        key = "administrative"
    elif section_id == "AD 2.12":
        model = RunwaysData
        key = "runways"
    elif section_id == "AD 2.18":
        model = CommsData
        key = "comms"
    elif section_id == "AD 2.19":
        model = NavaidsData
        key = "navaids"
    else:
        raise ValueError(f"Unsupported enrichment section: {section_id}")

    content = _chat_structured(icao, section_id, raw_text, model.model_json_schema())
    parsed = model.model_validate_json(content)
    return key, parsed.model_dump(mode="json")


async def enrich_aerodrome_document(
    aerodrome_doc: AerodromeDocument,
    section_ids: list[str] | None = None,
) -> AerodromeDocument:
    selected = section_ids or list(TARGET_SECTION_IDS)

    for section in aerodrome_doc.current.ad_sections:
        if section.section_id not in selected:
            continue

        source_hash = _sha256(section.raw_text)
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        if extraction.get("raw_text_sha256") == source_hash and extraction.get("status") == "ok":
            continue

        settings = get_settings()
        metadata = {
            "engine": "ollama",
            "model": settings.ollama_model,
            "prompt_version": PROMPT_VERSION,
            "raw_text_sha256": source_hash,
            "extracted_at": _utc_iso(),
            "status": "ok",
            "error": None,
        }

        try:
            key, payload = await asyncio.to_thread(
                _extract_for_section,
                aerodrome_doc.icao,
                section.section_id,
                section.raw_text,
            )
            section.data = {key: payload, "_extraction": metadata}
        except ValidationError as exc:
            metadata["status"] = "error"
            metadata["error"] = f"validation_error: {exc}"
            section.data = {"_extraction": metadata}
        except Exception as exc:
            metadata["status"] = "error"
            metadata["error"] = str(exc)
            section.data = {"_extraction": metadata}
            logger.warning(
                "aerodrome.enrichment.section_failed",
                extra={
                    "icao": aerodrome_doc.icao,
                    "section_id": section.section_id,
                    "error": str(exc),
                },
            )

    await aerodrome_doc.save()
    return aerodrome_doc


async def enrich_aerodrome(icao: str, section_ids: list[str] | None = None) -> AerodromeDocument | None:
    doc = await AerodromeDocument.get(icao.strip().upper())
    if doc is None:
        return None
    return await enrich_aerodrome_document(doc, section_ids=section_ids)
