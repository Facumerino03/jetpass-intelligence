# Operational AD Sections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist and enrich only operationally relevant AD 2.x sections while keeping the PyMuPDF parser as a full 25-section validator.

**Architecture:** The parser remains unchanged and continues producing all AD 2.1-AD 2.25 sections. A new domain helper defines the operational section scope and filters parser output before repository upsert. The repository validates the persisted snapshot against the operational scope instead of the old 25-section contract.

**Tech Stack:** Python 3.12, Pydantic v2, Beanie/MongoDB models, pytest, existing PyMuPDF parser.

---

## File Structure

- Create `app/schemas/ad_sections.py`
  - Owns canonical AD 2.x section constants and section-id normalization.
  - Exposes helpers for filtering section-like objects and validating operational ids.
- Modify `app/repositories/aerodrome_repo.py`
  - Replace exact 25-section validation with operational-section validation.
- Modify `app/services/aerodrome_import_service.py`
  - Filter parsed 25-section `AerodromeCreate` to operational sections before `aerodrome_repo.upsert`.
- Modify `tests/unit/test_aerodrome_repo.py`
  - Update persisted-section expectations from 25 to the operational subset.
  - Add missing/extra section validation coverage.
- Modify `tests/integration/test_aerodrome_import_service.py`
  - Assert parser still returns 25 but repository receives/persists only operational sections.
- Leave `app/services/scraper/aip_parser.py` and `app/services/scraper/aip_segmenter.py` unchanged.
  - They keep the strict 25-section parser contract.

Operational section ids:

```python
(
    "AD 2.1",
    "AD 2.2",
    "AD 2.3",
    "AD 2.4",
    "AD 2.6",
    "AD 2.8",
    "AD 2.9",
    "AD 2.12",
    "AD 2.13",
    "AD 2.14",
    "AD 2.18",
    "AD 2.19",
)
```

---

### Task 1: Add Operational Section Scope Helper

**Files:**
- Create: `app/schemas/ad_sections.py`
- Test: `tests/unit/test_ad_sections.py`

- [ ] **Step 1: Write failing tests for operational scope helpers**

Create `tests/unit/test_ad_sections.py`:

```python
from __future__ import annotations

import pytest

from app.schemas.ad_sections import (
    OPERATIONAL_AD_SECTION_IDS,
    filter_operational_sections,
    normalize_ad_section_id,
    validate_operational_section_ids,
)
from app.schemas.aerodrome import SectionSchema


def _section(section_id: str) -> SectionSchema:
    return SectionSchema(
        section_id=section_id,
        title=section_id,
        raw_text=f"{section_id} raw text",
    )


def test_operational_section_ids_are_product_scope() -> None:
    assert OPERATIONAL_AD_SECTION_IDS == (
        "AD 2.1",
        "AD 2.2",
        "AD 2.3",
        "AD 2.4",
        "AD 2.6",
        "AD 2.8",
        "AD 2.9",
        "AD 2.12",
        "AD 2.13",
        "AD 2.14",
        "AD 2.18",
        "AD 2.19",
    )


def test_normalize_ad_section_id_handles_case_and_spacing() -> None:
    assert normalize_ad_section_id(" ad   2.12 ") == "AD 2.12"
    assert normalize_ad_section_id("AD  2.18") == "AD 2.18"


def test_filter_operational_sections_preserves_product_order() -> None:
    sections = [_section(f"AD 2.{idx}") for idx in range(1, 26)]

    filtered = filter_operational_sections(sections)

    assert [section.section_id for section in filtered] == list(OPERATIONAL_AD_SECTION_IDS)


def test_filter_operational_sections_accepts_non_canonical_input_order() -> None:
    sections = [
        _section("AD 2.24"),
        _section("ad 2.18"),
        _section("AD 2.1"),
        _section("AD 2.12"),
    ]

    filtered = filter_operational_sections(sections)

    assert [section.section_id for section in filtered] == ["AD 2.1", "AD 2.12", "ad 2.18"]


def test_validate_operational_section_ids_reports_missing_and_unexpected() -> None:
    ids = [sid for sid in OPERATIONAL_AD_SECTION_IDS if sid != "AD 2.18"]
    ids.append("AD 2.24")

    with pytest.raises(
        ValueError,
        match=r"Missing required operational AD 2\.x sections: \['AD 2\.18'\].*Unexpected non-operational AD 2\.x sections: \['AD 2\.24'\]",
    ):
        validate_operational_section_ids(ids)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run python -m pytest tests/unit/test_ad_sections.py -v
```

Expected:

```text
ModuleNotFoundError: No module named 'app.schemas.ad_sections'
```

- [ ] **Step 3: Implement operational scope helper**

Create `app/schemas/ad_sections.py`:

```python
"""Canonical AD 2.x section scopes used by parser persistence."""

from __future__ import annotations

from typing import Iterable, TypeVar

FULL_AD2_SECTION_IDS = tuple(f"AD 2.{idx}" for idx in range(1, 26))

OPERATIONAL_AD_SECTION_IDS = (
    "AD 2.1",
    "AD 2.2",
    "AD 2.3",
    "AD 2.4",
    "AD 2.6",
    "AD 2.8",
    "AD 2.9",
    "AD 2.12",
    "AD 2.13",
    "AD 2.14",
    "AD 2.18",
    "AD 2.19",
)

_OPERATIONAL_SET = set(OPERATIONAL_AD_SECTION_IDS)

T = TypeVar("T")


def normalize_ad_section_id(section_id: str) -> str:
    return " ".join(section_id.strip().upper().split())


def filter_operational_sections(sections: Iterable[T]) -> list[T]:
    by_id: dict[str, T] = {}
    for section in sections:
        section_id = getattr(section, "section_id", None)
        if not isinstance(section_id, str):
            continue
        normalized = normalize_ad_section_id(section_id)
        if normalized in _OPERATIONAL_SET:
            by_id.setdefault(normalized, section)
    return [by_id[sid] for sid in OPERATIONAL_AD_SECTION_IDS if sid in by_id]


def validate_operational_section_ids(section_ids: Iterable[str]) -> None:
    normalized_ids = [normalize_ad_section_id(section_id) for section_id in section_ids]
    seen = set(normalized_ids)
    missing = [sid for sid in OPERATIONAL_AD_SECTION_IDS if sid not in seen]
    unexpected = [sid for sid in normalized_ids if sid not in _OPERATIONAL_SET]
    duplicates = sorted({sid for sid in normalized_ids if normalized_ids.count(sid) > 1})

    messages: list[str] = []
    if missing:
        messages.append(f"Missing required operational AD 2.x sections: {missing}")
    if unexpected:
        messages.append(f"Unexpected non-operational AD 2.x sections: {unexpected}")
    if duplicates:
        messages.append(f"Duplicate AD 2.x sections: {duplicates}")
    if messages:
        raise ValueError("; ".join(messages))
```

- [ ] **Step 4: Run scope helper tests**

Run:

```bash
uv run python -m pytest tests/unit/test_ad_sections.py -v
```

Expected:

```text
5 passed
```

- [ ] **Step 5: Commit**

```bash
git add app/schemas/ad_sections.py tests/unit/test_ad_sections.py
git commit -m "feat: define operational AD section scope"
```

---

### Task 2: Update Repository Validation To Operational Scope

**Files:**
- Modify: `app/repositories/aerodrome_repo.py`
- Modify: `tests/unit/test_aerodrome_repo.py`

- [ ] **Step 1: Update repo test helpers to use operational sections**

In `tests/unit/test_aerodrome_repo.py`, import the new constants:

```python
from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS
```

Replace `_sections` with:

```python
def _sections(cycle: str) -> list[SectionSchema]:
    return [
        SectionSchema(
            section_id=section_id,
            title=section_id,
            raw_text=f"Raw bilingual text {section_id}",
            data={"value": section_id},
            section_meta=SectionMetaSchema(airac_cycle=cycle, source_page=idx),
        )
        for idx, section_id in enumerate(OPERATIONAL_AD_SECTION_IDS, start=1)
    ]
```

Rename `test_upsert_creates_aerodrome_with_25_sections` to `test_upsert_creates_aerodrome_with_operational_sections` and update the assertion:

```python
assert len(doc.current.ad_sections) == len(OPERATIONAL_AD_SECTION_IDS)
assert [section.section_id for section in doc.current.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
```

Replace `test_upsert_validates_section_count` with:

```python
@pytest.mark.asyncio
async def test_upsert_validates_missing_operational_section() -> None:
    payload = _create_payload()
    payload.ad_sections = [
        section for section in payload.ad_sections
        if section.section_id != "AD 2.18"
    ]

    with pytest.raises(ValueError, match=r"Missing required operational AD 2\.x sections: \['AD 2\.18'\]"):
        await aerodrome_repo.upsert(payload)


@pytest.mark.asyncio
async def test_upsert_rejects_non_operational_section() -> None:
    payload = _create_payload()
    payload.ad_sections.append(
        SectionSchema(
            section_id="AD 2.24",
            title="AD 2.24",
            raw_text="AD 2.24 chart text",
        )
    )

    with pytest.raises(ValueError, match=r"Unexpected non-operational AD 2\.x sections: \['AD 2\.24'\]"):
        await aerodrome_repo.upsert(payload)
```

Update legacy replacement assertion:

```python
assert len(result.current.ad_sections) == len(OPERATIONAL_AD_SECTION_IDS)
```

- [ ] **Step 2: Run repo tests to verify failure**

Run:

```bash
uv run python -m pytest tests/unit/test_aerodrome_repo.py -v
```

Expected:

```text
ValueError: Expected 25 AD 2.x sections, got 12
```

- [ ] **Step 3: Update repository validation**

In `app/repositories/aerodrome_repo.py`, add import:

```python
from app.schemas.ad_sections import validate_operational_section_ids
```

Remove `_normalize_section_id` from this file if it is only used by `get_section_by_icao`; replace that function usage with `normalize_ad_section_id` from the helper:

```python
from app.schemas.ad_sections import normalize_ad_section_id, validate_operational_section_ids
```

Replace `_validate_sections` with:

```python
def _validate_sections(sections: list[SectionSchema]) -> None:
    validate_operational_section_ids(section.section_id for section in sections)
    for section in sections:
        if not section.raw_text.strip():
            raise ValueError(f"Section '{section.section_id}' has empty raw_text")
```

Replace `get_section_by_icao` normalization lines with:

```python
normalized = normalize_ad_section_id(section_id)
for section in aerodrome.current.ad_sections:
    if normalize_ad_section_id(section.section_id) == normalized:
        return section
```

- [ ] **Step 4: Run repo tests**

Run:

```bash
uv run python -m pytest tests/unit/test_aerodrome_repo.py -v
```

Expected:

```text
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add app/repositories/aerodrome_repo.py tests/unit/test_aerodrome_repo.py
git commit -m "feat: validate persisted operational AD sections"
```

---

### Task 3: Filter Parser Output Before Persistence

**Files:**
- Modify: `app/services/aerodrome_import_service.py`
- Modify: `tests/integration/test_aerodrome_import_service.py`

- [ ] **Step 1: Update import integration test helpers**

In `tests/integration/test_aerodrome_import_service.py`, import:

```python
from app.schemas.ad_sections import OPERATIONAL_AD_SECTION_IDS, filter_operational_sections
```

Keep `_sections()` returning all 25 parser sections.

Add helper:

```python
def _operational_sections() -> list[SectionSchema]:
    return filter_operational_sections(_sections())
```

Update `_aerodrome_doc()` to use operational sections:

```python
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
                    raw_text=s.raw_text,
                    data=s.data,
                )
                for s in _operational_sections()
            ],
            _meta=DocumentMeta(airac_cycle="2026-01", version=1),
        ),
    )
```

- [ ] **Step 2: Assert import filters before repository upsert**

In `test_import_aerodrome_full_pipeline`, capture the upsert mock:

```python
upsert_mock = AsyncMock(return_value=_aerodrome_doc())
```

Use it in the patch:

```python
patch(
    "app.services.aerodrome_import_service.aerodrome_repo.upsert",
    upsert_mock,
),
```

Update assertions:

```python
assert result.icao == "SAMR"
assert len(result.current.ad_sections) == len(OPERATIONAL_AD_SECTION_IDS)
assert [section.section_id for section in result.current.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
parse_call.assert_called_once_with([ad20_path], icao="SAMR")
upsert_payload = upsert_mock.await_args.args[0]
assert len(upsert_payload.ad_sections) == len(OPERATIONAL_AD_SECTION_IDS)
assert [section.section_id for section in upsert_payload.ad_sections] == list(OPERATIONAL_AD_SECTION_IDS)
enrich_call.assert_called_once()
```

In `test_import_can_skip_enrichment`, update result assertion:

```python
assert len(result.current.ad_sections) == len(OPERATIONAL_AD_SECTION_IDS)
```

- [ ] **Step 3: Run import service tests to verify failure**

Run:

```bash
uv run python -m pytest tests/integration/test_aerodrome_import_service.py -v
```

Expected:

```text
AssertionError: assert 25 == 12
```

- [ ] **Step 4: Implement filtering in import service**

In `app/services/aerodrome_import_service.py`, import:

```python
from app.schemas.ad_sections import filter_operational_sections
```

After parsing, replace:

```python
aerodrome_data = parse_aerodrome_from_documents(ad20_paths, icao=icao)
```

with:

```python
parsed_data = parse_aerodrome_from_documents(ad20_paths, icao=icao)
operational_sections = filter_operational_sections(parsed_data.ad_sections)
aerodrome_data = parsed_data.model_copy(update={"ad_sections": operational_sections})
```

Do not change `_select_ad20_documents`.

- [ ] **Step 5: Run import service tests**

Run:

```bash
uv run python -m pytest tests/integration/test_aerodrome_import_service.py -v
```

Expected:

```text
5 passed
```

- [ ] **Step 6: Commit**

```bash
git add app/services/aerodrome_import_service.py tests/integration/test_aerodrome_import_service.py
git commit -m "feat: persist only operational AD sections"
```

---

### Task 4: Update User-Facing Test Expectations

**Files:**
- Modify: `tests/unit/test_aip_parser.py`
- Modify: `scripts/import_aerodrome_from_aip.py` only if its wording implies all 25 sections are persisted.

- [ ] **Step 1: Confirm parser tests still expect 25 sections**

Open `tests/unit/test_aip_parser.py` and keep these expectations unchanged:

```python
assert len(result.sections) == 25
assert len(result.ad_sections) == 25
```

Parser tests must continue proving the PDF parser validates the full AD 2.0 document.

- [ ] **Step 2: Check import script output wording**

Open `scripts/import_aerodrome_from_aip.py`.

If the script only prints:

```python
print(f"  Sections  : {len(aerodrome.current.ad_sections)}")
```

do not change it.

If it says “25 sections” or “all AD 2.x sections”, replace that wording with:

```python
print(f"  Sections  : {len(aerodrome.current.ad_sections)} operational")
```

In the current repo snapshot, no script wording change is expected.

- [ ] **Step 3: Run parser and import tests together**

Run:

```bash
uv run python -m pytest tests/unit/test_aip_parser.py tests/integration/test_aerodrome_import_service.py -v
```

Expected:

```text
all selected tests passed
```

- [ ] **Step 4: Commit only if script or parser tests changed**

If no files changed in this task, skip this commit.

If files changed:

```bash
git add tests/unit/test_aip_parser.py scripts/import_aerodrome_from_aip.py
git commit -m "test: clarify parser and import section scopes"
```

---

### Task 5: Full Regression And Manual Acceptance

**Files:**
- No code changes expected.

- [ ] **Step 1: Run focused regression suite**

Run:

```bash
uv run python -m pytest \
  tests/unit/test_ad_sections.py \
  tests/unit/test_aerodrome_repo.py \
  tests/unit/test_aip_parser.py \
  tests/unit/test_aerodrome_enricher.py \
  tests/integration/test_aerodrome_import_service.py \
  -v
```

Expected:

```text
all selected tests passed
```

- [ ] **Step 2: Run broader tests if local services are not required**

Run:

```bash
uv run python -m pytest tests/unit tests/integration -v
```

Expected:

```text
all selected tests passed
```

If tests fail because a local MongoDB or external service is missing, record the exact failing test and error in the implementation summary.

- [ ] **Step 3: Manual parse remains full**

Run:

```bash
uv run python -m scripts.test_ad20_parser \
  --pdf-path tmp/aip/SAMR/SAMR_AD-2.0.pdf \
  --save-to tmp/samr-parser-full.json
```

Expected:

```text
Parser succeeded
saved_to: tmp/samr-parser-full.json
```

Then verify 25 parser sections:

```bash
uv run python - <<'PY'
import json
from pathlib import Path
data = json.loads(Path("tmp/samr-parser-full.json").read_text())
print(len(data["ad_sections"]))
print([s["section_id"] for s in data["ad_sections"]])
PY
```

Expected first line:

```text
25
```

- [ ] **Step 4: Manual import persists operational subset**

Run:

```bash
uv run python -m scripts.import_aerodrome_from_aip \
  --icao SAMR \
  --skip-enrichment
```

Expected output includes:

```text
Sections  : 12
IDs       : AD 2.1, AD 2.2, AD 2.3, AD 2.4, AD 2.6, AD 2.8, AD 2.9, AD 2.12, AD 2.13, AD 2.14, AD 2.18, AD 2.19
```

- [ ] **Step 5: Manual Mongo inspection shows only operational subset**

Run:

```bash
uv run python -m scripts.inspect_aerodrome_data --icao SAMR
```

Expected:

```text
section_id rows only for AD 2.1, AD 2.2, AD 2.3, AD 2.4, AD 2.6, AD 2.8, AD 2.9, AD 2.12, AD 2.13, AD 2.14, AD 2.18, AD 2.19
```

- [ ] **Step 6: Final commit if Task 5 exposed small test/doc fixes**

If no files changed, skip.

If files changed:

```bash
git add <changed-files>
git commit -m "test: verify operational AD section persistence"
```

---

## Self-Review Checklist

- Spec coverage:
  - Parser remains 25-section full validator: Tasks 3 and 4 preserve this.
  - Mongo stores only operational sections: Tasks 2 and 3 implement this.
  - Enrichment only processes stored sections: Task 3 ensures import enriches the reduced document.
  - No schema shape migration: repository still stores `ad_sections: list`.
- Placeholder scan:
  - No unfinished placeholder markers or unspecified test instructions remain.
- Type consistency:
  - `filter_operational_sections` accepts section-like objects with `section_id`.
  - `validate_operational_section_ids` accepts strings and is used by repository validation.
  - `AerodromeCreate.model_copy(update={"ad_sections": ...})` preserves aerodrome metadata.
