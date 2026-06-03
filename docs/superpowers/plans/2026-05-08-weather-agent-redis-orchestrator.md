# Weather Agent Redis Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Redis-backed worldwide aviation weather agent using AviationWeather.gov and integrate it into the existing LangGraph intelligence orchestrator.

**Architecture:** Weather is an intelligence capability alongside aerodrome and NOTAM. The service requires the ICAO to exist in Mongo, requires Redis for cache, fetches METAR/TAF/stationinfo/international SIGMET from AviationWeather with `httpx`, filters SIGMETs by point-in-polygon, and returns normalized plus raw data. The orchestrator is refactored from manual capability combinations to dynamic concurrent execution.

**Tech Stack:** FastAPI, Pydantic v2, LangGraph, Beanie/Mongo, Redis asyncio, httpx, pytest, pytest-asyncio, ruff.

---

## File Structure

Create:

- `app/services/weather/__init__.py` - package marker.
- `app/services/weather/aviation_weather_client.py` - async AviationWeather HTTP client and upstream error type.
- `app/services/weather/geometry.py` - geometry helpers for SIGMET point-in-polygon filtering.
- `app/intelligence/weather_intel_service.py` - cache-first weather intelligence flow.
- `app/tools/aviation_weather_tool.py` - thin tool wrapper around upstream client calls.
- `tests/unit/test_weather_geometry.py` - geometry unit tests.
- `tests/unit/test_aviation_weather_client.py` - HTTP client unit tests.
- `tests/unit/test_weather_intel_service.py` - weather service unit tests.
- `tests/unit/test_intelligence_orchestrator_weather.py` - orchestrator unit tests with monkeypatched services.

Modify:

- `app/core/config.py` - add weather base URL, TTL, hours-back, timeout, and user-agent settings.
- `.env.example` - document Redis and weather settings.
- `app/intelligence/contracts.py` - add `WeatherIntent`, weather result models, and request/response fields.
- `app/intelligence/graph.py` - replace manual `both_node` routing with dynamic concurrent capability execution.
- `app/intelligence/orchestrator.py` - include weather result in `OrchestratorResponse`.
- `app/routers/intelligence_router.py` - accept weather-only requests and update description.

---

### Task 1: Weather Contracts and Configuration

**Files:**

- Modify: `app/core/config.py`
- Modify: `app/intelligence/contracts.py`
- Modify: `.env.example`
- Test: `tests/unit/test_weather_contracts.py`

- [ ] **Step 1: Write contract/config tests**

Create `tests/unit/test_weather_contracts.py`:

```python
from app.core.config import Settings
from app.intelligence.contracts import OrchestratorRequest, OrchestratorResponse, WeatherIntelResult


def test_weather_intent_is_accepted_by_orchestrator_request():
    request = OrchestratorRequest.model_validate(
        {"weather": {"icao": "saez", "force_refresh": True}}
    )

    assert request.weather is not None
    assert request.weather.icao == "saez"
    assert request.weather.force_refresh is True


def test_orchestrator_response_can_include_weather_result():
    weather = WeatherIntelResult(icao="SAEZ", source="cache")
    response = OrchestratorResponse(intent="weather_context", weather=weather)

    assert response.weather is not None
    assert response.weather.icao == "SAEZ"
    assert response.weather.source == "cache"


def test_weather_settings_defaults_are_operational():
    settings = Settings()

    assert settings.aviation_weather_base_url == "https://aviationweather.gov/api/data"
    assert settings.weather_station_cache_ttl_seconds == 604800
    assert settings.weather_metar_cache_ttl_seconds == 120
    assert settings.weather_taf_cache_ttl_seconds == 600
    assert settings.weather_sigmet_cache_ttl_seconds == 120
    assert settings.weather_metar_hours_back == 2.0
    assert settings.weather_http_timeout_seconds == 10.0
    assert settings.weather_user_agent == "jetpass-intelligence"
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
uv run pytest tests/unit/test_weather_contracts.py -v
```

Expected: FAIL because weather contracts and settings do not exist.

- [ ] **Step 3: Add weather settings**

In `app/core/config.py`, add these fields to `Settings` after `notam_cache_ttl_hours`:

```python
    aviation_weather_base_url: str = Field(
        default="https://aviationweather.gov/api/data",
        alias="AVIATION_WEATHER_BASE_URL",
    )
    weather_station_cache_ttl_seconds: int = Field(
        default=604800,
        alias="WEATHER_STATION_CACHE_TTL_SECONDS",
    )
    weather_metar_cache_ttl_seconds: int = Field(
        default=120,
        alias="WEATHER_METAR_CACHE_TTL_SECONDS",
    )
    weather_taf_cache_ttl_seconds: int = Field(
        default=600,
        alias="WEATHER_TAF_CACHE_TTL_SECONDS",
    )
    weather_sigmet_cache_ttl_seconds: int = Field(
        default=120,
        alias="WEATHER_SIGMET_CACHE_TTL_SECONDS",
    )
    weather_metar_hours_back: float = Field(
        default=2.0,
        alias="WEATHER_METAR_HOURS_BACK",
    )
    weather_http_timeout_seconds: float = Field(
        default=10.0,
        alias="WEATHER_HTTP_TIMEOUT_SECONDS",
    )
    weather_user_agent: str = Field(
        default="jetpass-intelligence",
        alias="WEATHER_USER_AGENT",
    )
```

- [ ] **Step 4: Add weather contracts**

In `app/intelligence/contracts.py`, keep existing imports and add these models before `OrchestratorResponse`:

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
    """Result produced by WeatherIntelligenceService."""

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
class OrchestratorRequest(BaseModel):
    """Top-level request received by the orchestrator from the backend core."""

    aerodrome: AerodromeIntent | None = None
    notam: NotamIntent | None = None
    weather: WeatherIntent | None = None
```

Update `OrchestratorResponse`:

```python
class OrchestratorResponse(BaseModel):
    """Consolidated response returned to the backend core."""

    intent: str
    aerodrome: AerodromeIntelResult | None = None
    notam: NotamIntelResult | None = None
    weather: WeatherIntelResult | None = None
    alerts: list[Alert] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
```

- [ ] **Step 5: Document environment variables**

In `.env.example`, add:

```bash
REDIS_URL=redis://localhost:6379/0
AVIATION_WEATHER_BASE_URL=https://aviationweather.gov/api/data
WEATHER_STATION_CACHE_TTL_SECONDS=604800
WEATHER_METAR_CACHE_TTL_SECONDS=120
WEATHER_TAF_CACHE_TTL_SECONDS=600
WEATHER_SIGMET_CACHE_TTL_SECONDS=120
WEATHER_METAR_HOURS_BACK=2.0
WEATHER_HTTP_TIMEOUT_SECONDS=10.0
WEATHER_USER_AGENT=jetpass-intelligence
```

- [ ] **Step 6: Run tests and confirm pass**

Run:

```bash
uv run pytest tests/unit/test_weather_contracts.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit if approved**

Run only if the user explicitly requested commits:

```bash
git add app/core/config.py app/intelligence/contracts.py .env.example tests/unit/test_weather_contracts.py
git commit -m "feat: add weather intelligence contracts"
```

---

### Task 2: AviationWeather Client and SIGMET Geometry

**Files:**

- Create: `app/services/weather/__init__.py`
- Create: `app/services/weather/aviation_weather_client.py`
- Create: `app/services/weather/geometry.py`
- Test: `tests/unit/test_aviation_weather_client.py`
- Test: `tests/unit/test_weather_geometry.py`

- [ ] **Step 1: Write geometry tests**

Create `tests/unit/test_weather_geometry.py`:

```python
from app.services.weather.geometry import feature_contains_point


def test_feature_contains_point_inside_polygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-34.5, lon=-58.5) is True


def test_feature_contains_point_outside_polygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-33.0, lon=-58.5) is False


def test_feature_contains_point_inside_multipolygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-10.0, -10.0], [-9.0, -10.0], [-9.0, -9.0], [-10.0, -9.0], [-10.0, -10.0]]],
                [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
            ],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-34.5, lon=-58.5) is True
```

- [ ] **Step 2: Write client tests**

Create `tests/unit/test_aviation_weather_client.py`:

```python
import httpx
import pytest

from app.services.weather.aviation_weather_client import AviationWeatherClient, AviationWeatherClientError


@pytest.mark.asyncio
async def test_fetch_metar_uses_ids_format_and_hours():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[{"icaoId": "SAEZ", "rawOb": "SAEZ METAR"}])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://example.test") as http:
        client = AviationWeatherClient(http=http, user_agent="jetpass-test")
        result = await client.fetch_metar("SAEZ", hours=2.0)

    assert result == [{"icaoId": "SAEZ", "rawOb": "SAEZ METAR"}]
    assert requests[0].url.path == "/metar"
    assert dict(requests[0].url.params) == {"ids": "SAEZ", "format": "json", "hours": "2.0"}
    assert requests[0].headers["user-agent"] == "jetpass-test"


@pytest.mark.asyncio
async def test_fetch_taf_204_returns_empty_list():
    transport = httpx.MockTransport(lambda request: httpx.Response(204))
    async with httpx.AsyncClient(transport=transport, base_url="https://example.test") as http:
        client = AviationWeatherClient(http=http, user_agent="jetpass-test")
        result = await client.fetch_taf("SAEZ")

    assert result == []


@pytest.mark.asyncio
async def test_upstream_error_raises_client_error():
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="boom"))
    async with httpx.AsyncClient(transport=transport, base_url="https://example.test") as http:
        client = AviationWeatherClient(http=http, user_agent="jetpass-test")

        with pytest.raises(AviationWeatherClientError):
            await client.fetch_station_info("SAEZ")
```

- [ ] **Step 3: Run tests and confirm failure**

Run:

```bash
uv run pytest tests/unit/test_weather_geometry.py tests/unit/test_aviation_weather_client.py -v
```

Expected: FAIL because the modules do not exist.

- [ ] **Step 4: Implement geometry helpers**

Create `app/services/weather/__init__.py` as an empty file.

Create `app/services/weather/geometry.py`:

```python
from __future__ import annotations

from typing import Any


def _ring_contains_point(ring: list[list[float]], *, lat: float, lon: float) -> bool:
    inside = False
    point_x = lon
    point_y = lat
    count = len(ring)
    if count < 4:
        return False

    previous_x, previous_y = ring[-1][0], ring[-1][1]
    for coordinate in ring:
        current_x, current_y = coordinate[0], coordinate[1]
        crosses = (current_y > point_y) != (previous_y > point_y)
        if crosses:
            slope_x = (previous_x - current_x) * (point_y - current_y) / (previous_y - current_y) + current_x
            if point_x < slope_x:
                inside = not inside
        previous_x, previous_y = current_x, current_y
    return inside


def _polygon_contains_point(polygon: list[list[list[float]]], *, lat: float, lon: float) -> bool:
    if not polygon:
        return False
    outer_ring = polygon[0]
    if not _ring_contains_point(outer_ring, lat=lat, lon=lon):
        return False
    for hole in polygon[1:]:
        if _ring_contains_point(hole, lat=lat, lon=lon):
            return False
    return True


def feature_contains_point(feature: dict[str, Any], *, lat: float, lon: float) -> bool:
    geometry = feature.get("geometry") or {}
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []

    if geometry_type == "Polygon":
        return _polygon_contains_point(coordinates, lat=lat, lon=lon)
    if geometry_type == "MultiPolygon":
        return any(_polygon_contains_point(polygon, lat=lat, lon=lon) for polygon in coordinates)
    return False
```

- [ ] **Step 5: Implement AviationWeather client**

Create `app/services/weather/aviation_weather_client.py`:

```python
from __future__ import annotations

from typing import Any

import httpx


class AviationWeatherClientError(RuntimeError):
    """Raised when AviationWeather cannot return a usable response."""


class AviationWeatherClient:
    def __init__(self, *, http: httpx.AsyncClient, user_agent: str) -> None:
        self._http = http
        self._user_agent = user_agent

    async def fetch_station_info(self, icao: str) -> list[dict[str, Any]]:
        return await self._get_json_list("/stationinfo", {"ids": icao, "format": "json"})

    async def fetch_metar(self, icao: str, *, hours: float) -> list[dict[str, Any]]:
        return await self._get_json_list(
            "/metar",
            {"ids": icao, "format": "json", "hours": str(hours)},
        )

    async def fetch_taf(self, icao: str) -> list[dict[str, Any]]:
        return await self._get_json_list("/taf", {"ids": icao, "format": "json"})

    async def fetch_isigmet_geojson(self) -> dict[str, Any]:
        payload = await self._get_json("/isigmet", {"format": "geojson"})
        return payload if isinstance(payload, dict) else {"type": "FeatureCollection", "features": []}

    async def _get_json_list(self, path: str, params: dict[str, str]) -> list[dict[str, Any]]:
        payload = await self._get_json(path, params)
        return payload if isinstance(payload, list) else []

    async def _get_json(self, path: str, params: dict[str, str]) -> Any:
        try:
            response = await self._http.get(
                path,
                params=params,
                headers={"User-Agent": self._user_agent},
            )
        except httpx.HTTPError as exc:
            raise AviationWeatherClientError(str(exc)) from exc

        if response.status_code == 204:
            return []
        if response.status_code >= 400:
            raise AviationWeatherClientError(
                f"AviationWeather returned HTTP {response.status_code}: {response.text[:200]}"
            )
        return response.json()
```

- [ ] **Step 6: Run tests and confirm pass**

Run:

```bash
uv run pytest tests/unit/test_weather_geometry.py tests/unit/test_aviation_weather_client.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit if approved**

Run only if the user explicitly requested commits:

```bash
git add app/services/weather tests/unit/test_weather_geometry.py tests/unit/test_aviation_weather_client.py
git commit -m "feat: add AviationWeather client"
```

---

### Task 3: Weather Intelligence Service With Redis Cache

**Files:**

- Create: `app/intelligence/weather_intel_service.py`
- Create: `app/tools/aviation_weather_tool.py`
- Test: `tests/unit/test_weather_intel_service.py`

- [ ] **Step 1: Write service tests**

Create `tests/unit/test_weather_intel_service.py`:

```python
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.intelligence.weather_intel_service import get_weather_intelligence
from app.models.aerodrome import AerodromeDocument, AerodromeSnapshot
from app.models.meta import DocumentMeta, MetaSource


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int):
        self.values[key] = value
        self.ttls[key] = ex


def _aerodrome(icao: str = "SAEZ") -> AerodromeDocument:
    now = datetime.now(timezone.utc)
    return AerodromeDocument(
        id=icao,
        icao=icao,
        name="TEST",
        full_name="TEST AERODROME",
        current=AerodromeSnapshot(
            ad_sections=[],
            _meta=DocumentMeta(
                airac_cycle="2026-05",
                airac_effective_date=now,
                airac_expiry_date=None,
                source=MetaSource(type="AIP", document="test.pdf", url=None, downloaded_at=now, downloaded_by="test"),
                status="active",
                version=1,
                replaces=None,
                replaced_by=None,
                valid_from=now,
                valid_to=None,
                created_at=now,
                updated_at=now,
                change_log=[],
            ),
        ),
        history=[],
    )


@pytest.mark.asyncio
async def test_weather_requires_aerodrome(monkeypatch):
    async def missing_aerodrome(icao: str):
        return None

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", missing_aerodrome)

    result = await get_weather_intelligence("SAEZ")

    assert result.icao == "SAEZ"
    assert result.source == "fresh_fetch"
    assert result.alerts[0].code == "AERODROME_NOT_FOUND"


@pytest.mark.asyncio
async def test_weather_requires_redis(monkeypatch):
    async def existing_aerodrome(icao: str):
        return _aerodrome(icao)

    async def missing_redis():
        return None

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", existing_aerodrome)
    monkeypatch.setattr("app.intelligence.weather_intel_service.get_redis", missing_redis)

    result = await get_weather_intelligence("SAEZ")

    assert result.alerts[0].code == "WEATHER_CACHE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_weather_cache_hit_returns_cached_products(monkeypatch):
    redis = FakeRedis()
    redis.values["weather:station:SAEZ"] = json.dumps({"icaoId": "SAEZ", "name": "EZEIZA", "lat": -34.8, "lon": -58.5})
    redis.values["weather:metar:SAEZ"] = json.dumps({"icaoId": "SAEZ", "rawOb": "SAEZ METAR"})
    redis.values["weather:taf:SAEZ"] = json.dumps({"icaoId": "SAEZ", "rawTAF": "SAEZ TAF"})
    redis.values["weather:isigmet:global"] = json.dumps({"type": "FeatureCollection", "features": []})

    async def existing_aerodrome(icao: str):
        return _aerodrome(icao)

    async def fake_redis():
        return redis

    monkeypatch.setattr("app.intelligence.weather_intel_service.aerodrome_repo.get_by_icao", existing_aerodrome)
    monkeypatch.setattr("app.intelligence.weather_intel_service.get_redis", fake_redis)

    result = await get_weather_intelligence("saez")

    assert result.icao == "SAEZ"
    assert result.source == "cache"
    assert result.station is not None
    assert result.metar is not None
    assert result.taf is not None
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
uv run pytest tests/unit/test_weather_intel_service.py -v
```

Expected: FAIL because service does not exist.

- [ ] **Step 3: Add tool wrapper**

Create `app/tools/aviation_weather_tool.py`:

```python
from __future__ import annotations

from app.services.weather.aviation_weather_client import AviationWeatherClientError


class AviationWeatherToolError(RuntimeError):
    """Raised when the AviationWeather tool cannot complete a request."""


def translate_client_error(exc: AviationWeatherClientError) -> AviationWeatherToolError:
    return AviationWeatherToolError(str(exc))
```

- [ ] **Step 4: Implement weather service**

Create `app/intelligence/weather_intel_service.py`:

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import get_settings
from app.core.redis import get_redis
from app.intelligence.contracts import (
    Alert,
    AlertLevel,
    WeatherIntelResult,
    WeatherMetar,
    WeatherSigmet,
    WeatherStation,
    WeatherTaf,
)
from app.repositories import aerodrome_repo
from app.services.weather.aviation_weather_client import AviationWeatherClient, AviationWeatherClientError
from app.services.weather.geometry import feature_contains_point


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt_from_epoch(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _parse_station(payload: dict[str, Any]) -> WeatherStation:
    return WeatherStation(
        icao=str(payload.get("icaoId") or payload.get("id") or ""),
        name=payload.get("name"),
        lat=float(payload["lat"]) if payload.get("lat") is not None else None,
        lon=float(payload["lon"]) if payload.get("lon") is not None else None,
        elev=float(payload["elev"]) if payload.get("elev") is not None else None,
    )


def _parse_metar(payload: dict[str, Any]) -> WeatherMetar:
    return WeatherMetar(
        raw=payload.get("rawOb"),
        observed_at=_dt_from_epoch(payload.get("obsTime")),
        flight_category=payload.get("fltCat"),
        wind_dir_degrees=payload.get("wdir"),
        wind_speed_kt=payload.get("wspd"),
        wind_gust_kt=payload.get("wgst"),
        visibility=payload.get("visib"),
        altimeter_hpa=payload.get("altim"),
        temperature_c=payload.get("temp"),
        dewpoint_c=payload.get("dewp"),
        present_weather=payload.get("wxString"),
        raw_payload=payload,
    )


def _parse_taf(payload: dict[str, Any]) -> WeatherTaf:
    return WeatherTaf(
        raw=payload.get("rawTAF"),
        issued_at=None,
        valid_from=_dt_from_epoch(payload.get("validTimeFrom")),
        valid_to=_dt_from_epoch(payload.get("validTimeTo")),
        forecast_periods=list(payload.get("fcsts") or []),
        raw_payload=payload,
    )


def _parse_sigmet(feature: dict[str, Any]) -> WeatherSigmet:
    properties = feature.get("properties") or {}
    return WeatherSigmet(
        raw=properties.get("rawSigmet") or properties.get("rawAirSigmet"),
        hazard=properties.get("hazard"),
        fir_id=properties.get("firId") or properties.get("fir"),
        valid_from=_dt_from_epoch(properties.get("validTimeFrom")),
        valid_to=_dt_from_epoch(properties.get("validTimeTo")),
        geometry=feature.get("geometry"),
        raw_payload=feature,
    )


async def _get_json(redis, key: str) -> Any | None:
    value = await redis.get(key)
    if value is None:
        return None
    return json.loads(value)


async def _set_json(redis, key: str, value: Any, ttl: int) -> None:
    await redis.set(key, json.dumps(value, default=str), ex=ttl)


async def get_weather_intelligence(icao: str, *, force_refresh: bool = False) -> WeatherIntelResult:
    settings = get_settings()
    normalized = icao.strip().upper()
    alerts: list[Alert] = []
    messages: list[str] = []
    fetched_at = _utcnow()

    aerodrome = await aerodrome_repo.get_by_icao(normalized)
    if aerodrome is None:
        return WeatherIntelResult(
            icao=normalized,
            source="fresh_fetch",
            fetched_at=fetched_at,
            alerts=[Alert(level=AlertLevel.ERROR, code="AERODROME_NOT_FOUND", message=f"Aerodrome {normalized} is not available in JetPass.")],
        )

    redis = await get_redis()
    if redis is None:
        return WeatherIntelResult(
            icao=normalized,
            source="fresh_fetch",
            fetched_at=fetched_at,
            alerts=[Alert(level=AlertLevel.ERROR, code="WEATHER_CACHE_UNAVAILABLE", message="Redis is required for weather intelligence.")],
        )

    station_key = f"weather:station:{normalized}"
    metar_key = f"weather:metar:{normalized}"
    taf_key = f"weather:taf:{normalized}"
    sigmet_key = "weather:isigmet:global"

    station_payload = None if force_refresh else await _get_json(redis, station_key)
    metar_payload = None if force_refresh else await _get_json(redis, metar_key)
    taf_payload = None if force_refresh else await _get_json(redis, taf_key)
    sigmet_payload = None if force_refresh else await _get_json(redis, sigmet_key)

    cache_hits = [station_payload is not None, metar_payload is not None, taf_payload is not None, sigmet_payload is not None]

    async with httpx.AsyncClient(
        base_url=settings.aviation_weather_base_url,
        timeout=settings.weather_http_timeout_seconds,
    ) as http:
        client = AviationWeatherClient(http=http, user_agent=settings.weather_user_agent)

        try:
            if station_payload is None:
                station_rows = await client.fetch_station_info(normalized)
                station_payload = station_rows[0] if station_rows else None
                if station_payload is not None:
                    await _set_json(redis, station_key, station_payload, settings.weather_station_cache_ttl_seconds)
            if metar_payload is None:
                metar_rows = await client.fetch_metar(normalized, hours=settings.weather_metar_hours_back)
                metar_payload = metar_rows[0] if metar_rows else None
                if metar_payload is not None:
                    await _set_json(redis, metar_key, metar_payload, settings.weather_metar_cache_ttl_seconds)
            if taf_payload is None:
                taf_rows = await client.fetch_taf(normalized)
                taf_payload = taf_rows[0] if taf_rows else None
                if taf_payload is not None:
                    await _set_json(redis, taf_key, taf_payload, settings.weather_taf_cache_ttl_seconds)
            if sigmet_payload is None:
                sigmet_payload = await client.fetch_isigmet_geojson()
                await _set_json(redis, sigmet_key, sigmet_payload, settings.weather_sigmet_cache_ttl_seconds)
        except AviationWeatherClientError as exc:
            alerts.append(Alert(level=AlertLevel.WARNING, code="WEATHER_UPSTREAM_ERROR", message=str(exc)))

    station = _parse_station(station_payload) if isinstance(station_payload, dict) else None
    if station is None or station.lat is None or station.lon is None:
        alerts.append(Alert(level=AlertLevel.ERROR, code="WEATHER_STATION_NOT_FOUND", message=f"AviationWeather station coordinates are unavailable for {normalized}."))

    metar = _parse_metar(metar_payload) if isinstance(metar_payload, dict) else None
    if metar is None:
        alerts.append(Alert(level=AlertLevel.WARNING, code="METAR_NOT_AVAILABLE", message=f"METAR is unavailable for {normalized}."))

    taf = _parse_taf(taf_payload) if isinstance(taf_payload, dict) else None
    if taf is None:
        alerts.append(Alert(level=AlertLevel.WARNING, code="TAF_NOT_AVAILABLE", message=f"TAF is unavailable for {normalized}."))

    sigmets: list[WeatherSigmet] = []
    if station is not None and station.lat is not None and station.lon is not None and isinstance(sigmet_payload, dict):
        for feature in sigmet_payload.get("features") or []:
            if feature_contains_point(feature, lat=station.lat, lon=station.lon):
                sigmets.append(_parse_sigmet(feature))

    source = "cache" if all(cache_hits) and not force_refresh else "mixed" if any(cache_hits) and not force_refresh else "fresh_fetch"
    messages.append(f"[{normalized}] Weather intelligence resolved with source={source}.")

    return WeatherIntelResult(
        icao=normalized,
        station=station,
        metar=metar,
        taf=taf,
        sigmets=sigmets,
        fetched_at=fetched_at,
        source=source,
        alerts=alerts,
        messages=messages,
        metadata={"cache_keys": [station_key, metar_key, taf_key, sigmet_key]},
    )
```

- [ ] **Step 5: Run service tests**

Run:

```bash
uv run pytest tests/unit/test_weather_intel_service.py -v
```

Expected: PASS. If Pydantic model construction for `AerodromeDocument` requires additional fields, replace `_aerodrome()` in the test with a `SimpleNamespace(icao=icao)` because the service only checks for `None` versus existing.

- [ ] **Step 6: Commit if approved**

Run only if the user explicitly requested commits:

```bash
git add app/intelligence/weather_intel_service.py app/tools/aviation_weather_tool.py tests/unit/test_weather_intel_service.py
git commit -m "feat: add Redis-backed weather service"
```

---

### Task 4: Dynamic Orchestrator Integration

**Files:**

- Modify: `app/intelligence/graph.py`
- Modify: `app/intelligence/orchestrator.py`
- Modify: `app/routers/intelligence_router.py`
- Test: `tests/unit/test_intelligence_orchestrator_weather.py`

- [ ] **Step 1: Write orchestrator tests**

Create `tests/unit/test_intelligence_orchestrator_weather.py`:

```python
import pytest

from app.intelligence.contracts import OrchestratorRequest, WeatherIntelResult
from app.intelligence.orchestrator import run


@pytest.mark.asyncio
async def test_weather_only_request_returns_weather(monkeypatch):
    async def fake_weather(icao: str, *, force_refresh: bool = False):
        return WeatherIntelResult(icao=icao, source="cache")

    monkeypatch.setattr("app.intelligence.graph.get_weather_intelligence", fake_weather)

    response = await run(OrchestratorRequest.model_validate({"weather": {"icao": "SAEZ"}}))

    assert response.intent == "weather_context"
    assert response.weather is not None
    assert response.weather.icao == "SAEZ"


@pytest.mark.asyncio
async def test_weather_and_notam_request_builds_combined_intent(monkeypatch):
    async def fake_weather(icao: str, *, force_refresh: bool = False):
        return WeatherIntelResult(icao=icao, source="cache")

    async def fake_notam(icao: str, *, force_refresh: bool = False):
        from app.intelligence.contracts import NotamIntelResult

        return NotamIntelResult(icao=icao, source="cache")

    monkeypatch.setattr("app.intelligence.graph.get_weather_intelligence", fake_weather)
    monkeypatch.setattr("app.intelligence.graph.get_notam_intelligence", fake_notam)

    response = await run(
        OrchestratorRequest.model_validate(
            {"notam": {"icao": "SAEZ"}, "weather": {"icao": "SAEZ"}}
        )
    )

    assert response.intent == "notam_context+weather_context"
    assert response.notam is not None
    assert response.weather is not None
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
uv run pytest tests/unit/test_intelligence_orchestrator_weather.py -v
```

Expected: FAIL because graph/orchestrator do not include weather.

- [ ] **Step 3: Refactor graph**

In `app/intelligence/graph.py`, replace the manual routing implementation with this shape:

```python
"""LangGraph definition for the intelligence orchestrator."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from app.intelligence.aerodrome_intel_service import get_aerodrome_intelligence
from app.intelligence.contracts import (
    AerodromeIntelResult,
    Alert,
    NotamIntelResult,
    OrchestratorRequest,
    WeatherIntelResult,
)
from app.intelligence.notam_intel_service import get_notam_intelligence
from app.intelligence.weather_intel_service import get_weather_intelligence

logger = logging.getLogger(__name__)


class IntelligenceState(TypedDict, total=False):
    request: OrchestratorRequest
    aerodrome_result: AerodromeIntelResult
    notam_result: NotamIntelResult
    weather_result: WeatherIntelResult
    alerts: list[Alert]
    intent: str


async def _run_requested_capabilities(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    calls: list[tuple[str, Any]] = []

    if request.aerodrome is not None:
        intent = request.aerodrome
        calls.append((
            "aerodrome_result",
            get_aerodrome_intelligence(
                intent.icao,
                force_refresh=intent.force_refresh,
                section_ids=intent.section_ids,
            ),
        ))
    if request.notam is not None:
        intent = request.notam
        calls.append(("notam_result", get_notam_intelligence(intent.icao, force_refresh=intent.force_refresh)))
    if request.weather is not None:
        intent = request.weather
        calls.append(("weather_result", get_weather_intelligence(intent.icao, force_refresh=intent.force_refresh)))

    if not calls:
        return {}

    results = await asyncio.gather(*(call for _, call in calls))
    return {key: result for (key, _), result in zip(calls, results, strict=True)}


def _aggregate_results(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    alerts: list[Alert] = []

    for key in ("aerodrome_result", "notam_result", "weather_result"):
        result = state.get(key)
        if result is not None:
            alerts.extend(result.alerts)

    parts = []
    if request.aerodrome is not None:
        parts.append("aerodrome_context")
    if request.notam is not None:
        parts.append("notam_context")
    if request.weather is not None:
        parts.append("weather_context")

    return {"alerts": alerts, "intent": "+".join(parts) if parts else "noop"}


def build_graph():
    graph = StateGraph(IntelligenceState)
    graph.add_node("run_requested_capabilities", _run_requested_capabilities)
    graph.add_node("aggregate_results", _aggregate_results)

    graph.add_edge(START, "run_requested_capabilities")
    graph.add_edge("run_requested_capabilities", "aggregate_results")
    graph.add_edge("aggregate_results", END)
    return graph.compile()


intelligence_graph = build_graph()
```

- [ ] **Step 4: Include weather in orchestrator response**

In `app/intelligence/orchestrator.py`, update `run()`:

```python
async def run(request: OrchestratorRequest) -> OrchestratorResponse:
    """Execute requested intents via the compiled LangGraph orchestrator."""
    state = await intelligence_graph.ainvoke({"request": request})
    return OrchestratorResponse(
        intent=state.get("intent", "noop"),
        aerodrome=state.get("aerodrome_result"),
        notam=state.get("notam_result"),
        weather=state.get("weather_result"),
        alerts=state.get("alerts", []),
    )
```

- [ ] **Step 5: Update router validation**

In `app/routers/intelligence_router.py`, update the validation:

```python
async def run_intelligence(request: OrchestratorRequest) -> OrchestratorResponse:
    if request.aerodrome is None and request.notam is None and request.weather is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one intent must be specified (e.g. 'aerodrome', 'notam', or 'weather').",
        )
    logger.info("Intelligence request received: %s", request.model_dump(exclude_none=True))
    return await run(request)
```

- [ ] **Step 6: Run orchestrator tests**

Run:

```bash
uv run pytest tests/unit/test_intelligence_orchestrator_weather.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit if approved**

Run only if the user explicitly requested commits:

```bash
git add app/intelligence/graph.py app/intelligence/orchestrator.py app/routers/intelligence_router.py tests/unit/test_intelligence_orchestrator_weather.py
git commit -m "feat: integrate weather into orchestrator"
```

---

### Task 5: API Coverage and Full Verification

**Files:**

- Modify: `tests/api/test_intelligence_router.py`

- [ ] **Step 1: Write API tests**

Create `tests/api/test_intelligence_router.py`:

```python
from app.intelligence.contracts import WeatherIntelResult


def test_intelligence_router_accepts_weather_only(client, monkeypatch):
    async def fake_run(request):
        from app.intelligence.contracts import OrchestratorResponse

        return OrchestratorResponse(
            intent="weather_context",
            weather=WeatherIntelResult(icao=request.weather.icao, source="cache"),
        )

    monkeypatch.setattr("app.routers.intelligence_router.run", fake_run)

    response = client.post("/intelligence/run", json={"weather": {"icao": "SAEZ"}})

    assert response.status_code == 200
    assert response.json()["weather"]["icao"] == "SAEZ"


def test_intelligence_router_rejects_empty_intent(client):
    response = client.post("/intelligence/run", json={})

    assert response.status_code == 422
    assert "At least one intent" in response.json()["detail"]
```

- [ ] **Step 2: Run API test**

Run:

```bash
uv run pytest tests/api/test_intelligence_router.py -v
```

Expected: PASS.

- [ ] **Step 3: Run weather unit tests**

Run:

```bash
uv run pytest tests/unit/test_weather_contracts.py tests/unit/test_weather_geometry.py tests/unit/test_aviation_weather_client.py tests/unit/test_weather_intel_service.py tests/unit/test_intelligence_orchestrator_weather.py -v
```

Expected: PASS.

- [ ] **Step 4: Run full test suite**

Run:

```bash
uv run pytest
```

Expected: PASS.

- [ ] **Step 5: Run lint**

Run:

```bash
uv run ruff check .
```

Expected: PASS.

- [ ] **Step 6: Commit if approved**

Run only if the user explicitly requested commits:

```bash
git add tests/api/test_intelligence_router.py
git commit -m "test: cover weather intelligence API"
```

---

## Self-Review

- Spec coverage: METAR, TAF, international SIGMET, stationinfo, Redis-only cache, Mongo aerodrome prerequisite, raw plus normalized response, dynamic orchestrator, and tests are covered by Tasks 1-5.
- Placeholder scan: no unfinished markers or unspecified implementation steps remain.
- Type consistency: `WeatherIntent`, `WeatherIntelResult`, `weather_result`, and Redis key names match across contracts, service, graph, and tests.
- Scope control: LLM summaries, PIREP/AIREP, domestic US products, Mongo weather persistence, frontend rendering, and AIP coordinate parsing remain out of scope.
