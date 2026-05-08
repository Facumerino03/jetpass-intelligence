# Document AI AIP Migration Design

## Context

The current AIP import pipeline downloads ANAC AD-2.0 PDFs, parses them with a hybrid PyMuPDF/Docling parser, repairs section tables, persists seven operational AD sections, and optionally enriches those sections with an LLM.

That parser and enrichment flow will be replaced by Google Cloud Document AI. The current Document AI test script already produces a `schema_tree` where top-level keys map to AD 2.x sections, for example `ad_2_1`, `ad_2_2`, `ad_2_12`, and each section contains extracted fields with `mention_text` and `confidence`.

Document AI is now the only AIP extraction source. The legacy parser/enrichment pipeline should be removed rather than kept as a fallback.

## Goals

- Use Document AI for AD-2.0 extraction from now on.
- Persist only these AD sections: `AD 2.1`, `AD 2.2`, `AD 2.3`, `AD 2.4`, `AD 2.12`, `AD 2.13`, `AD 2.19`.
- Store Document AI structured data directly in MongoDB.
- Remove raw-text, layout, table-repair, and LLM-enrichment requirements from the productive AIP flow.
- Delete unused legacy parser/enrichment code and tests after the Document AI path is wired and verified.

## Non-Goals

- No fallback to the Docling/PyMuPDF parser.
- No LLM enrichment for AIP sections.
- No per-section OCR text reconstruction.
- No support for additional AD 2.x sections in this migration.
- No schema redesign for downstream aeronautical intelligence beyond removing obsolete extraction fields.

## New Flow

```text
ICAO
  -> scraper downloads ANAC AIP PDFs
  -> import service selects AD-2.0 PDF
  -> Document AI processes the PDF
  -> adapter builds AerodromeCreate from schema_tree
  -> adapter filters to the seven operational sections
  -> aerodrome_repo.upsert persists MongoDB snapshot
  -> API returns AerodromeResponse
```

The scraper remains in place because Document AI replaces extraction, not PDF discovery/download.

## Components

### Document AI Client Service

Create a production service from the current script logic. It will:

- read configuration from environment/default settings;
- create `DocumentProcessorServiceClient` with the location-specific endpoint;
- process a local PDF with `RawDocument` and `ProcessRequest`;
- support processor resource or processor version resource;
- return either the raw `Document` or the collapsed `schema_tree` representation.

The service keeps the useful parts of `scripts/documentai_aip_extractor.py`, but scripts should become thin wrappers over app code.

### Document AI Adapter

Add an adapter that converts Document AI output into the existing persistence DTO.

Section mapping:

```python
{
    "ad_2_1": "AD 2.1",
    "ad_2_2": "AD 2.2",
    "ad_2_3": "AD 2.3",
    "ad_2_4": "AD 2.4",
    "ad_2_12": "AD 2.12",
    "ad_2_13": "AD 2.13",
    "ad_2_19": "AD 2.19",
}
```

All other `schema_tree` keys, such as `ad_2_10`, `ad_2_14`, and `ad_2_18`, are ignored for this version.

The adapter will build each section with:

- `section_id`: canonical AD section ID;
- `title`: same as `section_id`;
- `section_title`: from `section_title.mention_text` when available;
- `data`: the Document AI section payload plus extraction metadata.

`data` should preserve Document AI confidence values. No flattening is required in this migration because the processor already emits semantic fields.

Example stored section data:

```json
{
  "ad_name": {
    "mention_text": "SAN RAFAEL / S. A. SANTIAGO GERMANO",
    "confidence": 0.915146
  },
  "icao_code": {
    "mention_text": "SAMR",
    "confidence": 0.987489
  },
  "_confidence": 1.0,
  "_extraction": {
    "engine": "documentai",
    "processor_id": "...",
    "processor_version_id": "...",
    "source_document": "SAMR_AD-2.0.pdf",
    "status": "ok"
  }
}
```

### Import Service

`import_aerodrome_from_aip` should call the Document AI service instead of `parse_aerodrome_from_documents`.

The `enrich` argument becomes obsolete. It should be removed from the public function and scripts instead of silently ignored.

The import service still handles:

- ICAO normalization;
- scraper errors;
- AD-2.0 PDF selection;
- Document AI extraction errors;
- database upsert errors;
- response conversion.

### Persistence Model

`raw_text` and `anchors` were useful for parser debugging and LLM enrichment. They are no longer required in the productive document shape.

Update schemas/models so AIP sections persist as structured data:

- remove the `raw_text` required validator;
- remove repository validation that rejects empty `raw_text`;
- remove `anchors` from the active write path;
- keep only fields needed by the current API and DB snapshot model.

If existing MongoDB documents contain legacy `raw_text` or `anchors`, Beanie/Pydantic should tolerate them only if needed for reading existing records. The new write path should not produce them.

### Metadata

Aerodrome-level metadata stays in `DocumentMeta`, but `changed_by` should no longer say `aip-parser`. Use a Document AI-oriented source such as `documentai`.

AIRAC remains unchanged for this migration. If no reliable AIRAC extraction exists, keep the existing `unknown` behavior.

### Error Handling

The adapter must fail clearly when any required operational section is missing after filtering. This preserves the current repository invariant: the persisted aerodrome snapshot must contain exactly the seven operational sections.

The adapter must validate that `ad_2_1.icao_code.mention_text`, when present, matches the requested ICAO. A mismatch should raise an import error.

Document AI API failures should be wrapped as `AipImportError` by the import service with the ICAO included in the message.

## Code Removal

After the Document AI flow is implemented and verified, remove obsolete modules that are only used by the legacy AIP extraction/enrichment path:

- `app/services/scraper/aip_parser.py`
- `app/services/scraper/aip_segmenter.py`
- `app/services/scraper/aip_table_repair.py`
- `app/services/scraper/aip_table_profiles.py`
- `app/services/scraper/docling_aip_parser.py`
- `app/services/scraper/pre_llm_pipeline.py`
- AIP-specific LLM enrichment modules under `app/services/enrichment/`
- `app/models/pre_llm_artifacts.py`
- `app/repositories/pre_llm_artifacts_repo.py`
- legacy parse/enrich tools and scripts that only call removed code
- parser/table-repair/enrichment tests
- `docling` dependency
- `pymupdf` dependency if no remaining code imports it

Remove these only after references are updated so the application imports cleanly.

## Tests

Add tests around the new boundary instead of preserving parser tests.

Required tests:

- adapter builds an `AerodromeCreate` from `SAMR_AD-2.0_extracted.json`;
- adapter persists exactly the seven operational section IDs in canonical order;
- adapter ignores extra sections returned by Document AI;
- adapter raises a clear error when a required operational section is missing;
- adapter raises on ICAO mismatch when Document AI provides a conflicting `icao_code`;
- import service test mocks scraper and Document AI and verifies `aerodrome_repo.upsert` receives the filtered payload.

Existing repository tests should be updated for the new section shape without required `raw_text`.

## Migration Sequence

1. Introduce Document AI service and adapter.
2. Update schemas/models/repository validation to remove required `raw_text` and active `anchors` usage.
3. Switch import service and CLI scripts to Document AI.
4. Add/update tests for adapter, import service, and repository.
5. Remove legacy parser/enrichment modules, tests, and dependencies.
6. Run the focused tests and full unit suite.

## Open Decisions

- Existing legacy documents can be tolerated for reads if current API tests require it, but new writes should not include legacy extraction fields.
- AIRAC extraction is intentionally deferred unless the Document AI processor already emits a reliable field for it.
