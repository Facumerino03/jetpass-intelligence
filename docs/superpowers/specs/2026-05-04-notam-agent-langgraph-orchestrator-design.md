# NOTAM Agent + LangGraph Orchestrator — Design Spec

**Date:** 2026-05-04  
**Status:** Approved  
**Scope:** NOTAM scraping agent + migration of the central orchestrator to LangGraph StateGraph

---

## 1. Context and Motivation

The project currently has one intelligence capability: the **aerodrome agent**, which scrapes AIP PDFs from ANAC, parses them, enriches them via LLM, and caches them in MongoDB. The orchestrator (`app/intelligence/orchestrator.py`) is a thin dispatcher function that already anticipates becoming a LangGraph graph ("potentially via LangGraph nodes").

This spec covers two tightly coupled changes:

1. **NOTAM agent** — scrapes active NOTAMs from the ANAC NOTAM consultation website for a given aerodrome, caches results in MongoDB, and interprets them via LLM (summary + classification by criticality/type).
2. **LangGraph orchestrator migration** — the current `run()` function becomes a `StateGraph` with parallel-capable agent nodes. This is the right moment to introduce LangGraph because the system is graduating from 1 to 2 independent capabilities.

---

## 2. Source Website

**ANAC Argentina NOTAM consultation system** — `https://ais.anac.gob.ar/notam` (to be confirmed at implementation time; the exact URL must be verified before building the Playwright scraper).

Key observations from the UI:
- A **searchable dropdown** lists aerodrome locations by full name (e.g. `AEROPARQUE J. NEWBERY`, `MINISTRO PISTARINI`), not ICAO code.
- The dropdown also includes FIR-wide entries: `AVISOS A TODAS LAS FIRS`, `AVISOS FIR COMODORO`, `AVISOS FIR CÓRDOBA`, etc.
- A prominent banner shows **"Última actualización: DD Mon YYYY HH:MM"** — the timestamp when the NOF last refreshed its data.
- The results table has columns: `Lugar / Location` and `Información / Information`. Each row contains the NOTAM ID, validity dates (Desde/Hasta), raw ICAO text, and an optional Spanish translation ("Versión en Español").
- The data comes from the EANA NOF (Oficina NOTAM Internacional).

The site requires JavaScript interaction (dropdown selection), so **Playwright** is the right scraping primitive — consistent with the existing `aip_scraper.py`.

---

## 3. Architecture

### 3.1 Layer responsibilities (unchanged convention)

```
tool → service → repo → orchestrator (LangGraph node)
```

Each layer:
- **Tool** — thin wrapper around a service call, translates exceptions into domain errors.
- **Service** — business logic (scraping, LLM interpretation, caching strategy).
- **Repo** — MongoDB read/write, no business logic.
- **Orchestrator node** — calls the service, updates LangGraph State.

### 3.2 LangGraph StateGraph

```
START
  ↓
route_intents
  ↓ (fan-out to whichever nodes are requested)
┌─────────────────────┬───────────────────────┐
aerodrome_node         notam_node
└─────────────────────┴───────────────────────┘
  ↓ (fan-in)
aggregate_results
  ↓
END
```

`aerodrome_node` and `notam_node` are conditionally included based on which intents are present in the request. When both are present they run independently (LangGraph supports parallel branches via `Send` or conditional edges).

### 3.3 Intelligence State

```python
class IntelligenceState(TypedDict):
    request: OrchestratorRequest
    aerodrome_result: AerodromeIntelResult | None
    notam_result: NotamIntelResult | None
    alerts: list[Alert]
```

The `StateGraph` is compiled once at startup and reused across requests.

### 3.4 NOTAM agent internal flow (sequential service, not a subgraph)

```
notam_intel_service.get_notam_intelligence(icao)
  1. Resolve aerodrome name
       aerodrome_repo.get_by_icao(icao) → AerodromeDocument.full_name or .name
  2. Check MongoDB cache
       notam_repo.get_by_icao(icao)
       → if fetched_at is fresh (within TTL): return source="cache"
       → if stale or missing: proceed to step 3
  3. Scrape
       notam_scrape_tool.scrape(aerodrome_name)
       → Playwright: extract site_last_updated_at from banner
       → Playwright: select "AVISOS A TODAS LAS FIRS" → scrape fir_notams
       → Playwright: select aerodrome name from dropdown → scrape aerodrome_notams
  4. Interpret (LLM)
       notam_interpret_tool.interpret(aerodrome_notams + fir_notams)
       → classify each NOTAM by category and criticality
       → generate overall_summary paragraph
  5. Persist
       notam_repo.upsert(NotamDocument)
  6. Return NotamIntelResult (source="fresh_scrape")
```

The internal service is kept as a sequential function (not a LangGraph subgraph) following YAGNI. If the NOTAM agent needs branching logic in the future (e.g. fallback sources), it can be promoted to a subgraph then.

---

## 4. Data Model

```python
# app/models/notam.py

class RawNotam(BaseModel):
    notam_id: str             # "A1472/2026"
    location: str             # "SAEZ" or "AVISOS A TODAS LAS FIRS"
    valid_from: datetime
    valid_to: datetime | None
    raw_text: str             # ICAO-format NOTAM text
    spanish_text: str | None  # "Versión en Español" if present on site

class NotamClassification(BaseModel):
    category: str             # RUNWAY | NAVAID | AIRSPACE | SERVICES | OTHER
    criticality: str          # HIGH | MEDIUM | LOW
    summary: str              # one-line plain-language description

class InterpretedNotam(BaseModel):
    notam_id: str
    classification: NotamClassification

class NotamInterpretation(BaseModel):
    overall_summary: str                      # paragraph for the pilot in plain language
    critical_count: int
    aerodrome_notams_count: int
    fir_notams_count: int
    classified_notams: list[InterpretedNotam] # full per-NOTAM classification
    by_category: dict[str, list[str]]         # {"RUNWAY": ["A1472/2026"], ...} for quick lookup
    interpreted_at: datetime

class NotamDocument(Document):
    id: str                   # ICAO code → MongoDB _id
    icao: str
    aerodrome_name: str       # name as used in the NOTAM site dropdown
    site_last_updated_at: datetime    # from the "Última actualización" banner
    fetched_at: datetime              # timestamp of our scraping run
    aerodrome_notams: list[RawNotam]
    fir_notams: list[RawNotam]
    interpretation: NotamInterpretation | None

    class Settings:
        name = "notams"
        indexes = [
            IndexModel([("icao", ASCENDING)], unique=True),
            IndexModel([("fetched_at", ASCENDING)]),
        ]
```

---

## 5. Contracts (additions to `app/intelligence/contracts.py`)

```python
class NotamIntent(BaseModel):
    """Intent to fetch or refresh NOTAM intelligence for a given ICAO."""
    icao: str
    force_refresh: bool = False

# OrchestratorRequest — add field:
notam: NotamIntent | None = None

class NotamIntelResult(BaseModel):
    icao: str
    aerodrome_name: str | None = None
    site_last_updated_at: datetime | None = None
    fetched_at: datetime | None = None
    aerodrome_notams: list[RawNotam] | None = None
    fir_notams: list[RawNotam] | None = None
    interpretation: NotamInterpretation | None = None
    source: Literal["cache", "fresh_scrape"]
    alerts: list[Alert] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)

# OrchestratorResponse — add field:
notam: NotamIntelResult | None = None
```

---

## 6. Cache Strategy

Mirrors the aerodrome cache pattern exactly:

```python
_CACHE_TTL_HOURS: int  # configurable via settings, default 4
```

Staleness check in `notam_intel_service`:

```python
def _is_stale(doc: NotamDocument) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.notam_cache_ttl_hours)
    return doc.fetched_at < cutoff
```

When stale: emit a `STALE_CACHE` warning alert and re-scrape.  
When fresh: return immediately with `source="cache"`.

The `site_last_updated_at` timestamp (from the ANAC banner) is always surfaced in the response so the pilot can see the freshness of the NOTAM source, independent of our cache TTL.

---

## 7. LLM Interpretation

The `notam_interpreter.py` service receives all NOTAMs (aerodrome + FIR) and produces a `NotamInterpretation`. It uses the same LLM provider pattern already in place (`app/services/enrichment/llm_providers.py`).

**Classification prompt guidance:**
- `category`: RUNWAY (runway/taxiway closures, surface conditions), NAVAID (navigation aids out of service), AIRSPACE (restricted/danger areas, CTR changes), SERVICES (ATC hours, fuel, handling), OTHER.
- `criticality`: HIGH (affects safe operation directly — closed runway, NAVAID failure), MEDIUM (operational impact — reduced capacity, restricted hours), LOW (informational).
- `overall_summary`: a brief paragraph in Spanish suitable for a pilot briefing. Mentions the most critical items first.

---

## 8. New Files

| File | Purpose |
|------|---------|
| `app/models/notam.py` | Beanie document model |
| `app/repositories/notam_repo.py` | MongoDB read/write for NOTAMs |
| `app/services/scraper/notam_scraper.py` | Playwright scraper for ANAC NOTAM site |
| `app/services/interpretation/notam_interpreter.py` | LLM classification + summary |
| `app/tools/notam_scrape_tool.py` | Tool wrapper for scraper |
| `app/tools/notam_interpret_tool.py` | Tool wrapper for interpreter |
| `app/intelligence/notam_intel_service.py` | Service: cache-first, scrape/interpret on miss |
| `app/intelligence/graph.py` | LangGraph StateGraph definition + compilation |

## 9. Modified Files

| File | Change |
|------|--------|
| `app/intelligence/contracts.py` | Add `NotamIntent`, `NotamIntelResult`, fields in request/response |
| `app/intelligence/orchestrator.py` | Replace `run()` with LangGraph graph invocation |
| `app/core/config.py` | Add `notam_cache_ttl_hours: int = 4` |
| `app/main.py` | Register `NotamDocument` in Beanie init |

---

## 10. Error Handling

Each step can fail independently and emits a typed `Alert`:

| Step | Error code | Level | Behavior |
|------|-----------|-------|---------|
| Name resolution (aerodrome not in DB) | `AERODROME_NOT_FOUND` | ERROR | Return early, no scrape |
| Scrape fails | `NOTAM_SCRAPE_FAILED` | ERROR | Return early with alert |
| Dropdown name not found on site | `NOTAM_LOCATION_NOT_FOUND` | WARNING | Return FIR NOTAMs only |
| LLM interpretation fails | `NOTAM_INTERPRET_FAILED` | WARNING | Persist raw NOTAMs without interpretation |

---

## 11. Testing

- **Unit**: `notam_scraper.py` (mock Playwright), `notam_interpreter.py` (mock LLM), `notam_intel_service.py` (mock repo + tools).
- **Integration**: `test_notam_repo.py` (mongomock), `test_langgraph_orchestrator.py` (mock both services, verify State transitions).
- **E2E script**: `scripts/run_notam_e2e.py` — takes an ICAO, runs full pipeline, prints result. Mirrors `scripts/run_aip_e2e.py`.

---

## 12. Out of Scope

- Series S (SNOWTAM) and Series V (ASHTAM) — the ANAC site note mentions these but they are separate and not part of this implementation.
- Weather agent — future capability, will be a third LangGraph node.
- Real-time push updates / WebSocket — out of scope for MVP.
- FIR-specific filtering (AVISOS FIR COMODORO for northern aerodromes vs AVISOS FIR CÓRDOBA for central) — all requests fetch AVISOS A TODAS LAS FIRS; region-specific FIR notices are a future enhancement.
