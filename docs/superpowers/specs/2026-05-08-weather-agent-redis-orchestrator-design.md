# Weather Agent + Redis Cache + Dynamic Orchestrator - Design Spec

**Date:** 2026-05-08
**Status:** Approved
**Scope:** Worldwide aviation weather intelligence agent integrated into the LangGraph orchestrator

---

## 1. Context and Motivation

JetPass Intelligence currently orchestrates aerodrome intelligence and NOTAM intelligence through a LangGraph graph. The next capability is a weather intelligence agent that receives an ICAO code from the backend core and returns operational aviation weather for pilot-facing use.

Weather data must be available with low latency and must not be persisted in MongoDB. The agent will use Redis as the required cache layer and AviationWeather.gov as the upstream source. The response will include both normalized fields for product/UI consumption and raw aviation text for operational traceability.

The existing `OrchestratorRequest` already anticipates a future `weather` intent. Adding weather also exposes a scaling problem in the current graph: the manual `both_node` pattern only handles two capabilities. This change includes refactoring the orchestrator to a dynamic fan-out/fan-in flow so future agents can be added without creating manual combination nodes.

---

## 2. Upstream API Scope

Source:

`https://aviationweather.gov/api/data`

OpenAPI contract:

`docs/openapi (1).yaml`

Worldwide weather products included:

| Product | Endpoint | Purpose |
|---------|----------|---------|
| METAR | `GET /api/data/metar` | Current decoded observation for the ICAO station |
| TAF | `GET /api/data/taf` | Terminal aerodrome forecast for the ICAO station |
| International SIGMET | `GET /api/data/isigmet` | Current international SIGMETs filtered locally by airport position |
| Station info | `GET /api/data/stationinfo` | Support lookup for decimal station coordinates and source coverage |

Products explicitly excluded from this iteration:

| Endpoint | Reason |
|----------|--------|
| `/api/data/airsigmet` | Domestic SIGMETs for contiguous United States only |
| `/api/data/gairmet` | Contiguous United States only |
| `/api/data/airmet` | Alaska only |
| `/api/data/cwa` | United States CWSU advisories |
| `/api/data/mis` | United States CWSU statements |
| `/api/data/tcf` | Traffic Flow Management convective forecast, not worldwide |
| `/api/data/areafcst` | Alaska area forecasts |
| `/api/data/fcstdisc` | United States forecast discussions |
| `/api/data/windtemp` | Regional legacy wind/temp product, not general worldwide coverage |
| `/api/data/pirep` | Operationally valuable, but documentation scope is primarily US and North Atlantic, so it is not a worldwide core product |

---

## 3. Request and Capability Rules

The backend core will request weather by ICAO through the orchestrator:

```json
{
  "weather": {
    "icao": "SAEZ",
    "force_refresh": false
  }
}
```

Rules:

- The ICAO must exist in the JetPass aerodrome Mongo collection before weather is fetched.
- Redis is required. If Redis is unavailable, the weather agent returns an error alert and does not query AviationWeather.
- `force_refresh=true` bypasses Redis product cache for the requested ICAO and refreshes upstream data.
- Weather is not persisted to MongoDB.
- The agent returns partial weather when possible. Missing METAR or TAF is an alert, not a total failure, as long as the agent can return other products.

---

## 4. Data Flow

```text
weather_intel_service.get_weather_intelligence(icao)
  1. Normalize ICAO to uppercase.
  2. Require aerodrome_repo.get_by_icao(icao).
     - Missing aerodrome => AERODROME_NOT_FOUND.
  3. Require Redis client.
     - Missing/unavailable Redis => WEATHER_CACHE_UNAVAILABLE.
  4. Resolve station info.
     - Redis key: weather:station:<ICAO>
     - Upstream: /api/data/stationinfo?ids=<ICAO>&format=json
  5. Fetch METAR.
     - Redis key: weather:metar:<ICAO>
     - Upstream: /api/data/metar?ids=<ICAO>&format=json&hours=2
  6. Fetch TAF.
     - Redis key: weather:taf:<ICAO>
     - Upstream: /api/data/taf?ids=<ICAO>&format=json
  7. Fetch active international SIGMET set.
     - Redis key: weather:isigmet:global
     - Upstream: /api/data/isigmet?format=geojson
  8. Filter SIGMET GeoJSON features by point-in-polygon using station coordinates.
  9. Return WeatherIntelResult with normalized and raw data.
```

Station coordinates:

- Primary source: AviationWeather `stationinfo`, because it returns decimal coordinates suitable for geometry filtering.
- Mongo remains the authoritative JetPass aerodrome existence check.
- Mongo coordinates are not the primary geometry source because AIP-derived coordinates are stored as official document text and may require DMS parsing.

---

## 5. Cache Strategy

Redis is the only persistence layer for weather data.

| Key | TTL | Notes |
|-----|-----|-------|
| `weather:station:<ICAO>` | 7 days | Station metadata and decimal coordinates change rarely |
| `weather:metar:<ICAO>` | 2 minutes | Query uses latest reports within the last 2 hours |
| `weather:taf:<ICAO>` | 10 minutes | TAFs update less frequently than METAR |
| `weather:isigmet:global` | 2 minutes | Global active international SIGMET set, reused across ICAOs |

Cache payloads should store:

- Normalized product data used by the service.
- Raw upstream response fragment needed for traceability.
- `fetched_at` timestamp.
- Upstream URL/path and query parameters.

`force_refresh=true` bypasses product reads for the requested weather flow and rewrites Redis values after successful fetches. It should not delete unrelated ICAO keys.

---

## 6. Contracts

Add to `app/intelligence/contracts.py`:

```python
class WeatherIntent(BaseModel):
    """Intent to fetch or refresh aviation weather for a given ICAO."""

    icao: str
    force_refresh: bool = False


class WeatherStation(BaseModel):
    icao: str
    name: str | None = None
    lat: float | None = None
    lon: float | None = None
    elev: float | None = None


class WeatherMetar(BaseModel):
    raw: str | None = None
    observed_at: datetime | None = None
    flight_category: str | None = None
    wind_dir_degrees: int | str | None = None
    wind_speed_kt: int | None = None
    wind_gust_kt: int | None = None
    visibility: float | str | None = None
    altimeter_hpa: float | None = None
    temperature_c: float | None = None
    dewpoint_c: float | None = None
    present_weather: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherTaf(BaseModel):
    raw: str | None = None
    issued_at: datetime | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    forecast_periods: list[dict[str, Any]] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherSigmet(BaseModel):
    raw: str | None = None
    hazard: str | None = None
    fir_id: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    geometry: dict[str, Any] | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class WeatherIntelResult(BaseModel):
    icao: str
    station: WeatherStation | None = None
    metar: WeatherMetar | None = None
    taf: WeatherTaf | None = None
    sigmets: list[WeatherSigmet] = Field(default_factory=list)
    fetched_at: datetime | None = None
    source: Literal["cache", "fresh_fetch", "mixed"]
    alerts: list[Alert] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
```

Update `OrchestratorRequest`:

```python
weather: WeatherIntent | None = None
```

Update `OrchestratorResponse`:

```python
weather: WeatherIntelResult | None = None
```

---

## 7. Orchestrator Refactor

Current graph behavior has explicit nodes for:

- `aerodrome_node`
- `notam_node`
- `both_node`

This does not scale to three or more agents. The graph should move to dynamic execution:

```text
START
  -> run_requested_capabilities
       - collect requested intents
       - execute independent service calls concurrently with asyncio.gather
       - store *_result values in state
  -> aggregate_results
  -> END
```

This keeps the graph simple while preserving fan-out/fan-in behavior. Each capability remains isolated in its own intelligence service.

`aggregate_results` must collect alerts from every present result and build the intent string from requested capabilities, for example:

- `weather_context`
- `notam_context+weather_context`
- `aerodrome_context+notam_context+weather_context`

The router validation must accept `weather` as a valid intent.

---

## 8. Implementation Files

New files:

| File | Purpose |
|------|---------|
| `app/intelligence/weather_intel_service.py` | Cache-first weather business flow |
| `app/tools/aviation_weather_tool.py` | Thin tool wrapper around upstream client errors |
| `app/services/weather/aviation_weather_client.py` | Async HTTP client for AviationWeather API |
| `app/services/weather/geometry.py` | Point-in-polygon helpers for SIGMET filtering |

Modified files:

| File | Change |
|------|--------|
| `app/intelligence/contracts.py` | Add weather intent/result contracts |
| `app/intelligence/graph.py` | Replace manual combination routing with dynamic fan-out/fan-in |
| `app/intelligence/orchestrator.py` | Include weather result in response |
| `app/routers/intelligence_router.py` | Accept weather-only requests and update description |
| `app/core/config.py` | Add AviationWeather base URL and weather TTL settings |
| `.env.example` | Document required Redis and weather config |

No Mongo model or repository is added for weather.

---

## 9. Configuration

Add settings:

```python
aviation_weather_base_url: str = "https://aviationweather.gov/api/data"
weather_station_cache_ttl_seconds: int = 604800
weather_metar_cache_ttl_seconds: int = 120
weather_taf_cache_ttl_seconds: int = 600
weather_sigmet_cache_ttl_seconds: int = 120
weather_metar_hours_back: float = 2.0
weather_http_timeout_seconds: float = 10.0
weather_user_agent: str = "jetpass-intelligence"
```

Redis remains configured through existing `REDIS_URL`, but weather treats it as mandatory.

---

## 10. Error Handling

Alerts:

| Code | Level | Behavior |
|------|-------|----------|
| `AERODROME_NOT_FOUND` | error | Stop weather flow; ICAO is not recognized by JetPass |
| `WEATHER_CACHE_UNAVAILABLE` | error | Stop weather flow; Redis is mandatory |
| `WEATHER_STATION_NOT_FOUND` | error | Stop SIGMET filtering; return no station-based weather result if coordinates are unavailable |
| `METAR_NOT_AVAILABLE` | warning | Continue with TAF/SIGMET if available |
| `TAF_NOT_AVAILABLE` | warning | Continue with METAR/SIGMET if available |
| `SIGMET_FETCH_FAILED` | warning | Continue with METAR/TAF |
| `WEATHER_UPSTREAM_ERROR` | warning/error | Warning for product-level failure; error only if no product can be returned |

A valid `204 No Content` from AviationWeather should be treated as product unavailable, not as an exception.

---

## 11. Testing Strategy

Unit tests:

- AviationWeather client builds correct URLs and handles JSON, empty responses, timeouts, and upstream errors.
- Redis cache reads/writes include TTL and bypass behavior for `force_refresh`.
- Weather service returns cache hits without upstream calls.
- Weather service fetches on cache miss and writes normalized payloads to Redis.
- Missing Mongo aerodrome returns `AERODROME_NOT_FOUND`.
- Missing Redis returns `WEATHER_CACHE_UNAVAILABLE`.
- Point-in-polygon filtering includes only SIGMETs containing the station point.

Orchestrator/API tests:

- Weather-only request is accepted.
- Combined `notam + weather` request returns both result blocks.
- Combined `aerodrome + notam + weather` request aggregates alerts and intent correctly.
- No-intent request still returns HTTP 422.

Verification commands:

```bash
uv run pytest tests/unit
uv run pytest tests/api
uv run pytest tests/integration
uv run ruff check .
```

---

## 12. Out of Scope

- LLM-generated weather briefing summaries.
- PIREP/AIREP integration.
- Domestic US AIRMET/SIGMET products.
- Persisting weather in MongoDB.
- Parsing AIP coordinate strings as the primary coordinate source.
- Frontend/UI rendering.
