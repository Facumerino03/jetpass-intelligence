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
