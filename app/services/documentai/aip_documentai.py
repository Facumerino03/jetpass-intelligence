from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google.api_core.client_options import ClientOptions
from google.cloud import documentai_v1 as documentai
from google.protobuf.json_format import MessageToDict

from app.core.config import get_settings
from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS, validate_operational_section_ids
from app.schemas.aerodrome import AerodromeCreate, SectionSchema


DOCUMENTAI_TO_AD_SECTION_ID = {
    "ad_2_1": "AD 2.1",
    "ad_2_2": "AD 2.2",
    "ad_2_3": "AD 2.3",
    "ad_2_4": "AD 2.4",
    "ad_2_12": "AD 2.12",
    "ad_2_13": "AD 2.13",
    "ad_2_19": "AD 2.19",
}


class DocumentAiAipError(Exception):
    """Raised when Document AI output cannot be converted to AIP storage data."""


@dataclass(frozen=True)
class DocumentAiConfig:
    project_id: str
    location: str
    processor_id: str
    processor_version_id: str | None = None
    imageless_mode: bool = False


def _pb_or_none(message: Any) -> dict[str, Any] | None:
    pb = getattr(message, "_pb", None)
    if pb is None:
        return None
    data = MessageToDict(pb, preserving_proto_field_name=True)
    return data if data else None


def _nested_entity_payload(entity: documentai.Document.Entity) -> dict[str, Any]:
    confidence = round(entity.confidence, 6) if entity.confidence else None
    normalized = _pb_or_none(entity.normalized_value) if entity.normalized_value else None

    if entity.properties:
        children = collapse_schema_entities(entity.properties)
        if confidence is not None:
            children = {**children, "_confidence": confidence}
        return children

    output: dict[str, Any] = {}
    text = entity.mention_text.strip() if entity.mention_text else None
    if text:
        output["mention_text"] = text
    if normalized is not None:
        output["normalized_value"] = normalized
    if confidence is not None:
        output["confidence"] = confidence
    return output


def collapse_schema_entities(entities: Any) -> dict[str, Any]:
    output: dict[str, Any] = {}

    def put(key: str, value: dict[str, Any]) -> None:
        if key not in output:
            output[key] = value
            return
        existing = output[key]
        if isinstance(existing, list):
            existing.append(value)
            return
        output[key] = [existing, value]

    for entity in entities:
        put(entity.type_, _nested_entity_payload(entity))
    return output


def _make_client(location: str) -> documentai.DocumentProcessorServiceClient:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    return documentai.DocumentProcessorServiceClient(client_options=opts)


def _process_pdf(path: Path, config: DocumentAiConfig) -> documentai.Document:
    client = _make_client(config.location)
    if config.processor_version_id:
        name = client.processor_version_path(
            config.project_id,
            config.location,
            config.processor_id,
            config.processor_version_id,
        )
    else:
        name = client.processor_path(config.project_id, config.location, config.processor_id)

    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(
            content=path.read_bytes(),
            mime_type="application/pdf",
        ),
        imageless_mode=config.imageless_mode,
    )
    return client.process_document(request=request).document


def _field_text(section: Any, field: str) -> str | None:
    if not isinstance(section, dict):
        return None
    value = section.get(field)
    if not isinstance(value, dict):
        return None
    text = value.get("mention_text")
    return text.strip() if isinstance(text, str) and text.strip() else None


def _section_title(section_payload: Any) -> str | None:
    return _field_text(section_payload, "section_title")


def _extraction_meta(
    *,
    source_document: str,
    processor_id: str,
    processor_version_id: str | None,
) -> dict[str, Any]:
    return {
        "engine": "documentai",
        "processor_id": processor_id,
        "processor_version_id": processor_version_id,
        "source_document": source_document,
        "status": "ok",
    }


def _with_extraction(payload: Any, extraction: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise DocumentAiAipError("Document AI section payload must be an object")
    return {**payload, "_extraction": extraction}


def _validate_icao(schema_tree: dict[str, Any], requested_icao: str) -> None:
    extracted_icao = _field_text(schema_tree.get("ad_2_1"), "icao_code")
    if extracted_icao is None:
        return
    normalized = extracted_icao.upper().replace(" ", "")
    if normalized != requested_icao:
        raise DocumentAiAipError(
            f"Document AI ICAO mismatch: requested {requested_icao}, extracted {extracted_icao}"
        )


def build_aerodrome_from_schema_tree(
    schema_tree: dict[str, Any],
    *,
    icao: str,
    source_document: str,
    processor_id: str,
    processor_version_id: str | None,
) -> AerodromeCreate:
    requested_icao = icao.strip().upper()
    _validate_icao(schema_tree, requested_icao)

    extraction = _extraction_meta(
        source_document=source_document,
        processor_id=processor_id,
        processor_version_id=processor_version_id,
    )
    sections_by_id: dict[str, SectionSchema] = {}
    for documentai_key, section_id in DOCUMENTAI_TO_AD_SECTION_ID.items():
        payload = schema_tree.get(documentai_key)
        if payload is None:
            continue
        if isinstance(payload, list):
            raise DocumentAiAipError(
                f"Expected one payload for required section {section_id}, got list"
            )
        sections_by_id[section_id] = SectionSchema(
            section_id=section_id,
            title=section_id,
            section_title=_section_title(payload),
            data=_with_extraction(payload, extraction),
        )

    ordered_sections = [
        sections_by_id[section_id]
        for section_id in OPERATIONAL_AD_SECTION_IDS
        if section_id in sections_by_id
    ]
    try:
        validate_operational_section_ids(section.section_id for section in ordered_sections)
    except ValueError as exc:
        raise DocumentAiAipError(str(exc)) from exc

    ad_2_1 = schema_tree.get("ad_2_1")
    name = _field_text(ad_2_1, "ad_name") or requested_icao
    return AerodromeCreate(
        icao_code=requested_icao,
        name=name,
        full_name=name,
        source_document=source_document,
        downloaded_by="documentai",
        ad_sections=ordered_sections,
    )


def parse_aerodrome_from_documentai(
    pdf_path: Path,
    *,
    icao: str,
    config: DocumentAiConfig | None = None,
) -> AerodromeCreate:
    settings = get_settings()
    resolved_config = config or DocumentAiConfig(
        project_id=settings.documentai_project_id,
        location=settings.documentai_location,
        processor_id=settings.documentai_processor_id,
        processor_version_id=settings.documentai_processor_version_id,
        imageless_mode=settings.documentai_imageless_mode,
    )
    document = _process_pdf(pdf_path, resolved_config)
    return build_aerodrome_from_schema_tree(
        collapse_schema_entities(document.entities),
        icao=icao,
        source_document=pdf_path.name,
        processor_id=resolved_config.processor_id,
        processor_version_id=resolved_config.processor_version_id,
    )


def entity_types_histogram(document: documentai.Document) -> dict[str, int]:
    counts: Counter[str] = Counter()

    def walk(entities: Any) -> None:
        for entity in entities:
            counts[entity.type_] += 1
            walk(entity.properties)

    walk(document.entities)
    return dict(counts)
