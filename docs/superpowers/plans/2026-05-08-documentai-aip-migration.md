# Document AI AIP Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the AIP Docling/PyMuPDF parser and LLM enrichment flow with Google Cloud Document AI as the only AD-2.0 extraction source.

**Architecture:** Keep the ANAC PDF scraper and MongoDB repository, but replace parser/enrichment with a Document AI client service and adapter. The adapter converts Document AI `schema_tree` output into `AerodromeCreate` with exactly the seven operational AD sections and stores the Document AI payload directly in `section.data`.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, Beanie, MongoDB, pytest, `google-cloud-documentai`, Google Cloud Document AI v1.

---

## File Structure

- Create `app/services/documentai/__init__.py`: package exports for Document AI AIP extraction.
- Create `app/services/documentai/aip_documentai.py`: Document AI client, schema-tree collapse helpers, adapter, and public `parse_aerodrome_from_documentai` function.
- Modify `app/core/config.py`: add Document AI settings.
- Modify `app/schemas/aerodrome.py`: remove required `raw_text`; remove active `anchors` write dependency while tolerating old data.
- Modify `app/models/aerodrome.py`: make `raw_text` and `anchors` optional for legacy reads; new writes do not need them.
- Modify `app/repositories/aerodrome_repo.py`: remove non-empty `raw_text` validation and stop requiring `raw_text` in model conversion.
- Modify `app/services/aerodrome_import_service.py`: call Document AI parser, remove enrichment.
- Modify `scripts/import_aerodrome_from_aip.py`: remove `--skip-enrichment` and call the new import signature.
- Modify `scripts/documentai_aip_extractor.py`: make it a thin wrapper over `app/services/documentai/aip_documentai.py`.
- Modify `app/models/__init__.py`: remove pre-LLM artifact document registration.
- Delete legacy-only modules after tests pass: parser, segmenter, table repair, Docling parser, pre-LLM artifacts, AIP enrichment modules, and tools that only call removed code.
- Modify `pyproject.toml`: add `google-cloud-documentai`; remove `docling`; remove `pymupdf` if no imports remain.
- Create `tests/unit/test_aip_documentai_adapter.py`: adapter tests using `SAMR_AD-2.0_extracted.json`.
- Modify `tests/integration/test_aerodrome_import_service.py`: mock Document AI parser instead of legacy parser/enrichment.
- Modify `tests/unit/test_aerodrome_repo.py`: remove `raw_text` expectations.
- Modify or remove parser/enrichment tests that refer to deleted code.

## Task 1: Add Document AI Settings

**Files:**
- Modify: `app/core/config.py`
- Test: none for this small settings-only change; import validation happens in later tests.

- [ ] **Step 1: Add settings fields**

Modify `Settings` in `app/core/config.py` by adding these fields after `redis_url`:

```python
    documentai_project_id: str = Field(default="655673003934", alias="DOCUMENTAI_PROJECT_ID")
    documentai_location: str = Field(default="us", alias="DOCUMENTAI_LOCATION")
    documentai_processor_id: str = Field(
        default="c57cf2f52aedbbe3", alias="DOCUMENTAI_PROCESSOR_ID"
    )
    documentai_processor_version_id: str | None = Field(
        default=None, alias="DOCUMENTAI_PROCESSOR_VERSION_ID"
    )
    documentai_imageless_mode: bool = Field(
        default=False, alias="DOCUMENTAI_IMAGELESS_MODE"
    )
```

- [ ] **Step 2: Verify settings import**

Run: `uv run python -c "from app.core.config import get_settings; s=get_settings(); print(s.documentai_location)"`

Expected: prints `us` and exits with status 0.

- [ ] **Step 3: Commit**

```bash
git add app/core/config.py
git commit -m "config: add document ai settings"
```

## Task 2: Relax Aerodrome Section Shape

**Files:**
- Modify: `app/schemas/aerodrome.py`
- Modify: `app/models/aerodrome.py`
- Modify: `app/repositories/aerodrome_repo.py`
- Test: `tests/unit/test_aerodrome_repo.py`

- [ ] **Step 1: Update repository test fixtures first**

In `tests/unit/test_aerodrome_repo.py`, update `_sections` so `raw_text` is not provided:

```python
def _sections(cycle: str) -> list[SectionSchema]:
    return [
        SectionSchema(
            section_id=section_id,
            title=section_id,
            data={"value": section_id},
            section_meta=SectionMetaSchema(airac_cycle=cycle, source_page=idx),
        )
        for idx, section_id in enumerate(OPERATIONAL_AD_SECTION_IDS, start=1)
    ]
```

In `test_upsert_rejects_non_operational_section`, remove `raw_text`:

```python
    payload.ad_sections.append(
        SectionSchema(
            section_id="AD 2.24",
            title="AD 2.24",
        )
    )
```

- [ ] **Step 2: Run repository test and verify failure**

Run: `uv run pytest tests/unit/test_aerodrome_repo.py -q`

Expected: FAIL because `SectionSchema.raw_text` is still required.

- [ ] **Step 3: Make schema and model fields optional**

In `app/schemas/aerodrome.py`, remove `field_validator` from imports and replace `SectionSchema` with:

```python
class SectionSchema(BaseModel):
    section_id: str
    title: str
    section_title: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    raw_text: str | None = None
    anchors: dict[str, Any] | None = None
    section_meta: SectionMetaSchema | None = None
```

In the same file, update `SnapshotResponse.from_snapshot` and `SectionResponse.from_model` to pass optional `raw_text` after `data`:

```python
raw_text=s.raw_text,
anchors=s.anchors,
```

In `app/models/aerodrome.py`, replace `AdSection` with:

```python
class AdSection(BaseModel):
    """One AD 2.x section with flexible structured data."""

    section_id: str
    title: str
    section_title: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    raw_text: str | None = None
    anchors: dict[str, Any] | None = None
    section_meta: SectionMeta | None = None
```

In `app/repositories/aerodrome_repo.py`, replace `_validate_sections` with:

```python
def _validate_sections(sections: list[SectionSchema]) -> None:
    validate_operational_section_ids(section.section_id for section in sections)
```

Update `_to_model_section` so `data` comes before optional legacy fields:

```python
def _to_model_section(section: SectionSchema) -> AdSection:
    return AdSection(
        section_id=section.section_id,
        title=section.title,
        section_title=section.section_title,
        data=section.data,
        raw_text=section.raw_text,
        anchors=section.anchors,
        section_meta=SectionMeta(
            airac_cycle=section.section_meta.airac_cycle,
            source_page=section.section_meta.source_page,
        ) if section.section_meta else None,
    )
```

In `_build_meta`, change `changed_by` fallback:

```python
changed_by=data.downloaded_by or "documentai",
```

- [ ] **Step 4: Run repository test and verify pass**

Run: `uv run pytest tests/unit/test_aerodrome_repo.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/schemas/aerodrome.py app/models/aerodrome.py app/repositories/aerodrome_repo.py tests/unit/test_aerodrome_repo.py
git commit -m "refactor: store structured aip sections without raw text"
```

## Task 3: Add Document AI Adapter Tests

**Files:**
- Create: `tests/unit/test_aip_documentai_adapter.py`
- Create later: `app/services/documentai/aip_documentai.py`
- Create later: `app/services/documentai/__init__.py`

- [ ] **Step 1: Write failing adapter tests**

Create `tests/unit/test_aip_documentai_adapter.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.services.documentai.aip_documentai import (
    DocumentAiAipError,
    build_aerodrome_from_schema_tree,
)


FIXTURE_PATH = Path(__file__).resolve().parents[2] / "SAMR_AD-2.0_extracted.json"


def _schema_tree() -> dict:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return payload["schema_tree"]


def test_build_aerodrome_from_schema_tree_filters_operational_sections() -> None:
    result = build_aerodrome_from_schema_tree(
        _schema_tree(),
        icao="SAMR",
        source_document="SAMR_AD-2.0.pdf",
        processor_id="processor-1",
        processor_version_id="version-1",
    )

    assert result.icao_code == "SAMR"
    assert result.name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.full_name == "SAN RAFAEL / S. A. SANTIAGO GERMANO"
    assert result.source_document == "SAMR_AD-2.0.pdf"
    assert result.downloaded_by == "documentai"
    assert [section.section_id for section in result.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
    assert "runway" in result.ad_sections[4].data
    assert result.ad_sections[4].raw_text is None
    assert result.ad_sections[4].anchors is None


def test_build_aerodrome_from_schema_tree_preserves_documentai_payload() -> None:
    result = build_aerodrome_from_schema_tree(
        _schema_tree(),
        icao="SAMR",
        source_document="SAMR_AD-2.0.pdf",
        processor_id="processor-1",
        processor_version_id="version-1",
    )

    by_id = {section.section_id: section for section in result.ad_sections}
    ad_2_13 = by_id["AD 2.13"]

    assert ad_2_13.section_title == "DISTANCIAS DECLARADAS / DECLARED DISTANCES"
    assert ad_2_13.data["declared_distance"][0]["rwy_designator"]["mention_text"] == "11"
    assert ad_2_13.data["_extraction"] == {
        "engine": "documentai",
        "processor_id": "processor-1",
        "processor_version_id": "version-1",
        "source_document": "SAMR_AD-2.0.pdf",
        "status": "ok",
    }


def test_build_aerodrome_from_schema_tree_rejects_missing_required_section() -> None:
    schema_tree = dict(_schema_tree())
    schema_tree.pop("ad_2_19")

    with pytest.raises(DocumentAiAipError, match="Missing required operational AD 2.x sections"):
        build_aerodrome_from_schema_tree(
            schema_tree,
            icao="SAMR",
            source_document="SAMR_AD-2.0.pdf",
            processor_id="processor-1",
            processor_version_id=None,
        )


def test_build_aerodrome_from_schema_tree_rejects_icao_mismatch() -> None:
    with pytest.raises(DocumentAiAipError, match="Document AI ICAO mismatch"):
        build_aerodrome_from_schema_tree(
            _schema_tree(),
            icao="SAEZ",
            source_document="SAMR_AD-2.0.pdf",
            processor_id="processor-1",
            processor_version_id=None,
        )
```

- [ ] **Step 2: Run adapter tests and verify failure**

Run: `uv run pytest tests/unit/test_aip_documentai_adapter.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'app.services.documentai'`.

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/unit/test_aip_documentai_adapter.py
git commit -m "test: define document ai aip adapter behavior"
```

## Task 4: Implement Document AI Adapter

**Files:**
- Create: `app/services/documentai/__init__.py`
- Create: `app/services/documentai/aip_documentai.py`
- Test: `tests/unit/test_aip_documentai_adapter.py`

- [ ] **Step 1: Add package export**

Create `app/services/documentai/__init__.py`:

```python
"""Document AI services for AIP extraction."""

from app.services.documentai.aip_documentai import (
    DocumentAiAipError,
    build_aerodrome_from_schema_tree,
    parse_aerodrome_from_documentai,
)

__all__ = [
    "DocumentAiAipError",
    "build_aerodrome_from_schema_tree",
    "parse_aerodrome_from_documentai",
]
```

- [ ] **Step 2: Implement adapter and Document AI client**

Create `app/services/documentai/aip_documentai.py`:

```python
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
```

- [ ] **Step 3: Run adapter tests**

Run: `uv run pytest tests/unit/test_aip_documentai_adapter.py -q`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add app/services/documentai tests/unit/test_aip_documentai_adapter.py
git commit -m "feat: add document ai aip adapter"
```

## Task 5: Switch Import Service to Document AI

**Files:**
- Modify: `app/services/aerodrome_import_service.py`
- Modify: `tests/integration/test_aerodrome_import_service.py`

- [ ] **Step 1: Update import-service tests first**

Replace `tests/integration/test_aerodrome_import_service.py` with tests that mock Document AI. Use this full file:

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app.models.aerodrome import AdSection, AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta
from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
from app.schemas.aerodrome import AerodromeCreate, SectionSchema
from app.services.aerodrome_import_service import AipImportError, import_aerodrome_from_aip
from app.services.documentai.aip_documentai import DocumentAiAipError
from app.services.scraper.aip_scraper import AipScraperError


def _operational_sections() -> list[SectionSchema]:
    return [
        SectionSchema(section_id=section_id, title=section_id, data={"value": section_id})
        for section_id in OPERATIONAL_AD_SECTION_IDS
    ]


def _aerodrome_create() -> AerodromeCreate:
    return AerodromeCreate(
        icao_code="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        airac_cycle="unknown",
        source_document="SAMR_AD-2.0.pdf",
        downloaded_by="documentai",
        ad_sections=_operational_sections(),
    )


def _aerodrome_doc() -> AerodromeDocument:
    return AerodromeDocument(
        id="SAMR",
        icao="SAMR",
        name="San Rafael",
        full_name="S. A. Santiago Germano",
        current=AerodromeSnapshot(
            ad_sections=[
                AdSection(
                    section_id=s.section_id,
                    title=s.title,
                    data=s.data,
                )
                for s in _operational_sections()
            ],
            _meta=DocumentMeta(airac_cycle="unknown", version=1),
        ),
    )


@pytest.mark.asyncio
async def test_import_aerodrome_documentai_pipeline(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad2a_path = tmp_path / "SAMR_AD-2.A.pdf"
    ad20_path.touch()
    ad2a_path.touch()

    upsert_mock = AsyncMock(return_value=_aerodrome_doc())

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path, ad2a_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documentai",
            return_value=_aerodrome_create(),
        ) as parse_call,
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            upsert_mock,
        ),
    ):
        result = await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)

    assert result.icao == "SAMR"
    assert [section.section_id for section in result.current.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
    parse_call.assert_called_once_with(ad20_path, icao="SAMR")
    upsert_payload = upsert_mock.await_args.args[0]
    assert [section.section_id for section in upsert_payload.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)


@pytest.mark.asyncio
async def test_import_raises_when_scraper_fails(tmp_path: Path) -> None:
    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(side_effect=AipScraperError("browser crash")),
    ):
        with pytest.raises(AipImportError, match="Scraper failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_documentai_fails(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documentai",
            side_effect=DocumentAiAipError("missing AD 2.19"),
        ),
    ):
        with pytest.raises(AipImportError, match="Document AI extraction failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_db_fails(tmp_path: Path) -> None:
    ad20_path = tmp_path / "SAMR_AD-2.0.pdf"
    ad20_path.touch()

    with (
        patch(
            "app.services.aerodrome_import_service.download_aip_pdfs",
            AsyncMock(return_value=[ad20_path]),
        ),
        patch(
            "app.services.aerodrome_import_service.parse_aerodrome_from_documentai",
            return_value=_aerodrome_create(),
        ),
        patch(
            "app.services.aerodrome_import_service.aerodrome_repo.upsert",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ),
    ):
        with pytest.raises(AipImportError, match="Database upsert failed"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_import_raises_when_ad20_pdf_missing(tmp_path: Path) -> None:
    ad2a_path = tmp_path / "SAMR_AD-2.A.pdf"
    ad2a_path.touch()

    with patch(
        "app.services.aerodrome_import_service.download_aip_pdfs",
        AsyncMock(return_value=[ad2a_path]),
    ):
        with pytest.raises(AipImportError, match="does not include required AD-2.0"):
            await import_aerodrome_from_aip("SAMR", output_dir=tmp_path)
```

- [ ] **Step 2: Run import tests and verify failure**

Run: `uv run pytest tests/integration/test_aerodrome_import_service.py -q`

Expected: FAIL because `aerodrome_import_service` still imports legacy parser/enrichment.

- [ ] **Step 3: Replace import service implementation**

Edit `app/services/aerodrome_import_service.py` so imports become:

```python
from app.repositories import aerodrome_repo
from app.schemas.aerodrome import AerodromeResponse
from app.services.documentai import DocumentAiAipError, parse_aerodrome_from_documentai
from app.services.scraper.aip_scraper import AipScraperError, download_aip_pdfs
```

Replace `import_aerodrome_from_aip` with:

```python
async def import_aerodrome_from_aip(
    icao: str,
    output_dir: Path | None = None,
) -> AerodromeResponse:
    """Download AIP PDFs, extract AD-2.0 with Document AI, and upsert MongoDB."""
    icao = icao.strip().upper()

    try:
        pdf_paths = await download_aip_pdfs(icao, output_dir=output_dir)
    except AipScraperError as exc:
        raise AipImportError(f"[{icao}] Scraper failed: {exc}") from exc

    try:
        ad20_paths = _select_ad20_documents(pdf_paths, icao)
        aerodrome_data = parse_aerodrome_from_documentai(ad20_paths[0], icao=icao)
    except DocumentAiAipError as exc:
        raise AipImportError(f"[{icao}] Document AI extraction failed: {exc}") from exc

    try:
        aerodrome_doc = await aerodrome_repo.upsert(aerodrome_data)
    except Exception as exc:
        raise AipImportError(f"[{icao}] Database upsert failed: {exc}") from exc

    logger.info(
        "[%s] Import complete — %d section(s) persisted. AIRAC: %s",
        icao,
        len(aerodrome_doc.current.ad_sections),
        aerodrome_doc.current.meta.airac_cycle,
    )
    return AerodromeResponse.from_document(aerodrome_doc)
```

- [ ] **Step 4: Run import tests**

Run: `uv run pytest tests/integration/test_aerodrome_import_service.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/aerodrome_import_service.py tests/integration/test_aerodrome_import_service.py
git commit -m "feat: import aip with document ai"
```

## Task 6: Update CLI and Script Wrappers

**Files:**
- Modify: `scripts/import_aerodrome_from_aip.py`
- Modify: `scripts/documentai_aip_extractor.py`

- [ ] **Step 1: Remove skip-enrichment from import CLI**

In `scripts/import_aerodrome_from_aip.py`, remove the `--skip-enrichment` argument block. Change `_run` to:

```python
async def _run(icao: str, output_dir: Path | None) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit(
            "MONGODB_URL is not configured. Set it in your .env file."
        )
    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    aerodrome = await import_aerodrome_from_aip(icao, output_dir=output_dir)
    _print_summary(aerodrome)
```

Change `main` to:

```python
def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run(args.icao, args.output_dir))
    except AipImportError as exc:
        print(f"\n✗ Import failed: {exc}")
        raise SystemExit(1) from exc
```

- [ ] **Step 2: Replace Document AI script with app-service wrapper**

Replace `scripts/documentai_aip_extractor.py` with:

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import documentai_v1 as documentai
from google.protobuf.json_format import MessageToDict

from app.services.documentai.aip_documentai import (
    DocumentAiConfig,
    _process_pdf,
    collapse_schema_entities,
    entity_types_histogram,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract AIP entities with Document AI.")
    parser.add_argument("file_path", help="Local PDF path")
    parser.add_argument("--project-id", default="655673003934")
    parser.add_argument("--location", default="us")
    parser.add_argument("--processor-id", default="c57cf2f52aedbbe3")
    parser.add_argument("--processor-version-id", default=None)
    parser.add_argument("--imageless-mode", action="store_true")
    parser.add_argument("--dump", choices=("nested", "entities", "full"), default="nested")
    parser.add_argument("--include-text", action="store_true")
    parser.add_argument("--print-json", action="store_true")
    parser.add_argument("--debug-summary", action="store_true")
    return parser.parse_args()


def _export(document: documentai.Document, dump: str, include_text: bool) -> dict:
    if dump == "full":
        return MessageToDict(document._pb, preserving_proto_field_name=True)
    if dump == "entities":
        output = {
            "entities": [
                MessageToDict(entity._pb, preserving_proto_field_name=True)
                for entity in document.entities
            ]
        }
        if include_text:
            output["text"] = document.text
        return output
    output = {
        "schema_tree": collapse_schema_entities(document.entities),
        "entity_root_count": len(document.entities),
    }
    if include_text:
        output["text"] = document.text
    return output


def main() -> None:
    args = _parse_args()
    config = DocumentAiConfig(
        project_id=args.project_id,
        location=args.location,
        processor_id=args.processor_id,
        processor_version_id=args.processor_version_id,
        imageless_mode=args.imageless_mode,
    )
    document = _process_pdf(Path(args.file_path), config)
    if args.debug_summary:
        print(json.dumps(entity_types_histogram(document), ensure_ascii=False, indent=2))
    result = _export(document, args.dump, args.include_text)
    output_path = Path(args.file_path).stem + "_extracted.json"
    Path(output_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Resultado guardado en: {output_path}")
    if args.print_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Verify CLI help**

Run: `uv run python -m scripts.import_aerodrome_from_aip --help`

Expected: help output does not include `--skip-enrichment`.

Run: `uv run python -m scripts.documentai_aip_extractor --help`

Expected: help output exits with status 0.

- [ ] **Step 4: Commit**

```bash
git add scripts/import_aerodrome_from_aip.py scripts/documentai_aip_extractor.py
git commit -m "chore: update aip document ai scripts"
```

## Task 7: Remove Legacy AIP Parser and Enrichment Code

**Files:**
- Delete: `app/services/scraper/aip_parser.py`
- Delete: `app/services/scraper/aip_segmenter.py`
- Delete: `app/services/scraper/aip_table_repair.py`
- Delete: `app/services/scraper/aip_table_profiles.py`
- Delete: `app/services/scraper/docling_aip_parser.py`
- Delete: `app/services/scraper/pre_llm_pipeline.py`
- Delete: `app/models/pre_llm_artifacts.py`
- Delete: `app/repositories/pre_llm_artifacts_repo.py`
- Delete: `app/tools/aip_parse_tool.py`
- Delete: `app/tools/aip_enrich_tool.py`
- Delete or update: AIP enrichment modules under `app/services/enrichment/`
- Modify: `app/models/__init__.py`
- Modify: `app/tools/__init__.py` if it imports deleted tools.

- [ ] **Step 1: Search legacy references**

Run: `rg "aip_parser|aip_segmenter|aip_table_repair|docling_aip_parser|pre_llm|aerodrome_enricher|aip_enrich_tool|aip_parse_tool|parse_aerodrome_from_documents|enrich_aerodrome" app tests scripts docs -g '*.py' -g '*.md'`

Expected: references exist before deletion.

- [ ] **Step 2: Delete legacy modules**

Remove the files listed above. If `app/services/enrichment/` is only used for AIP enrichment, remove these files too:

```text
app/services/enrichment/aerodrome_enricher.py
app/services/enrichment/ad2_contracts.py
app/services/enrichment/aip_section_schemas.py
app/services/enrichment/llm_providers.py
```

Keep `app/services/enrichment/__init__.py` only if another module imports the package. If nothing imports it, delete the package.

- [ ] **Step 3: Update model registration**

In `app/models/__init__.py`, remove imports and exports for `RawExtractionDocument` and `PreLlmSectionsDocument`. The file should contain:

```python
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
```

- [ ] **Step 4: Remove legacy tests**

Delete tests that only validate removed parser/enrichment code:

```text
tests/unit/test_aip_parser.py
tests/unit/test_aip_table_repair.py
tests/unit/test_aerodrome_enricher.py
tests/unit/test_aip_section_schema_alignment.py
tests/unit/test_ad2_contracts.py
```

Keep `tests/unit/test_ad_sections.py`, repository tests, scraper tests, API tests, and new Document AI tests.

- [ ] **Step 5: Verify no legacy references remain**

Run: `rg "aip_parser|aip_segmenter|aip_table_repair|docling_aip_parser|pre_llm|aerodrome_enricher|aip_enrich_tool|aip_parse_tool|parse_aerodrome_from_documents|enrich_aerodrome" app tests scripts -g '*.py'`

Expected: no matches.

- [ ] **Step 6: Commit**

```bash
git add app tests scripts
git add -u
git commit -m "chore: remove legacy aip parser enrichment pipeline"
```

## Task 8: Update Dependencies

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` if present and updated by `uv sync`.

- [ ] **Step 1: Update dependency list**

In `pyproject.toml`, add:

```toml
    "google-cloud-documentai>=3.0.0",
```

Remove:

```toml
    "docling>=2.92.0",
```

Remove `pymupdf>=1.27.2` only if this command has no matches:

Run: `rg "fitz|pymupdf|PyMuPDF" app scripts tests -g '*.py'`

Expected before removal decision: no matches outside deleted legacy files.

- [ ] **Step 2: Sync dependencies**

Run: `uv sync`

Expected: dependency resolution succeeds.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "deps: use google cloud document ai"
```

## Task 9: Update Documentation

**Files:**
- Modify: `docs/aip_parser_to_enrichment_flow.md`

- [ ] **Step 1: Replace legacy documentation with Document AI flow**

Replace `docs/aip_parser_to_enrichment_flow.md` with a concise document:

````markdown
# AIP AD-2.0 Document AI Import Flow

The productive AIP pipeline now uses Google Cloud Document AI as the only extractor for ANAC AD-2.0 PDFs.

```text
ICAO
  -> scraper downloads ANAC PDFs
  -> import service selects AD-2.0
  -> Document AI extracts schema_tree
  -> adapter persists seven operational sections
  -> aerodrome_repo.upsert writes MongoDB snapshot
```

## Persisted Sections

Only these sections are persisted:

```python
FLIGHT_PLANNING_AD_SECTION_IDS = (
    "AD 2.1",
    "AD 2.2",
    "AD 2.3",
    "AD 2.4",
    "AD 2.12",
    "AD 2.13",
    "AD 2.19",
)
```

Other sections returned by Document AI are ignored in this version.

## Stored Shape

Each section stores Document AI structured data directly in `section.data`. `raw_text`, layout anchors, table repair metadata, and LLM enrichment metadata are no longer part of the new write path.

Example:

```json
{
  "section_id": "AD 2.13",
  "title": "AD 2.13",
  "section_title": "DISTANCIAS DECLARADAS / DECLARED DISTANCES",
  "data": {
    "declared_distance": [
      {
        "rwy_designator": {"mention_text": "11", "confidence": 0.999966},
        "tora_m": {"mention_text": "2.102", "confidence": 0.999981}
      }
    ],
    "_extraction": {
      "engine": "documentai",
      "processor_id": "c57cf2f52aedbbe3",
      "processor_version_id": null,
      "source_document": "SAMR_AD-2.0.pdf",
      "status": "ok"
    }
  }
}
```

## Commands

Import one aerodrome:

```bash
uv run python -m scripts.import_aerodrome_from_aip --icao SAMR
```

Inspect Document AI extraction locally:

```bash
uv run python -m scripts.documentai_aip_extractor path/to/SAMR_AD-2.0.pdf --print-json
```

Run focused tests:

```bash
uv run pytest tests/unit/test_aip_documentai_adapter.py -q
uv run pytest tests/integration/test_aerodrome_import_service.py -q
uv run pytest tests/unit/test_aerodrome_repo.py -q
```
````

- [ ] **Step 2: Commit**

```bash
git add docs/aip_parser_to_enrichment_flow.md
git commit -m "docs: document aip document ai flow"
```

## Task 10: Final Verification

**Files:**
- No code changes unless verification exposes failures.

- [ ] **Step 1: Run focused tests**

Run: `uv run pytest tests/unit/test_aip_documentai_adapter.py tests/integration/test_aerodrome_import_service.py tests/unit/test_aerodrome_repo.py -q`

Expected: PASS.

- [ ] **Step 2: Run full unit test suite**

Run: `uv run pytest tests/unit -q`

Expected: PASS.

- [ ] **Step 3: Run import checks**

Run: `uv run python -m scripts.import_aerodrome_from_aip --help`

Expected: exits 0 and shows no `--skip-enrichment` option.

Run: `uv run python -m scripts.documentai_aip_extractor --help`

Expected: exits 0.

- [ ] **Step 4: Check for deleted dependency references**

Run: `rg "docling|pymupdf|fitz|aerodrome_enricher|parse_aerodrome_from_documents|skip-enrichment" app tests scripts pyproject.toml docs -g '*.*'`

Expected: no matches except historical text in committed design/plan docs.

- [ ] **Step 5: Final status check**

Run: `git status --short`

Expected: clean worktree if all task commits were made.

## Self-Review

- Spec coverage: the plan covers Document AI service, adapter, filtered seven sections, raw-text removal, import service switch, script updates, code removal, dependency cleanup, documentation, and verification.
- Placeholder scan: no task depends on unspecified behavior; code snippets define the concrete functions, tests, commands, and expected outcomes.
- Type consistency: the plan uses `DocumentAiAipError`, `DocumentAiConfig`, `build_aerodrome_from_schema_tree`, and `parse_aerodrome_from_documentai` consistently across tests, service code, and import integration.
