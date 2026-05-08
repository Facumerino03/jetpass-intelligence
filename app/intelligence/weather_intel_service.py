from __future__ import annotations

import json
import logging
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
from app.services.weather.aviation_weather_client import (
    AviationWeatherClient,
    AviationWeatherClientError,
)
from app.services.weather.geometry import feature_contains_point

logger = logging.getLogger(__name__)


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
    try:
        raw = json.dumps(value)
    except (TypeError, ValueError) as exc:
        logger.error("Failed to serialize value for key %s: %s", key, exc)
        return
    await redis.set(key, raw, ex=ttl)


async def get_weather_intelligence(
    icao: str, *, force_refresh: bool = False, metar_hours_back: float | None = None
) -> WeatherIntelResult:
    settings = get_settings()
    normalized = icao.strip().upper()
    alerts: list[Alert] = []
    messages: list[str] = []
    hours_back = metar_hours_back if metar_hours_back is not None else settings.weather_metar_hours_back

    logger.info("[%s] Fetching weather intelligence...", normalized)

    aerodrome = await aerodrome_repo.get_by_icao(normalized)
    if aerodrome is None:
        logger.warning("[%s] Aerodrome not found in MongoDB.", normalized)
        return WeatherIntelResult(
            icao=normalized,
            source="fresh_fetch",
            fetched_at=_utcnow(),
            alerts=[
                Alert(
                    level=AlertLevel.ERROR,
                    code="AERODROME_NOT_FOUND",
                    message=f"Aerodrome {normalized} is not available in JetPass.",
                )
            ],
        )

    redis = await get_redis()
    if redis is None:
        logger.error("[%s] Redis is not available for weather intelligence.", normalized)
        return WeatherIntelResult(
            icao=normalized,
            source="fresh_fetch",
            fetched_at=_utcnow(),
            alerts=[
                Alert(
                    level=AlertLevel.ERROR,
                    code="WEATHER_CACHE_UNAVAILABLE",
                    message="Redis is required for weather intelligence.",
                )
            ],
        )

    station_key = f"weather:station:{normalized}"
    metar_key = f"weather:metar:{normalized}"
    taf_key = f"weather:taf:{normalized}"
    sigmet_key = "weather:isigmet:global"

    station_payload = None if force_refresh else await _get_json(redis, station_key)
    metar_payload = None if force_refresh else await _get_json(redis, metar_key)
    taf_payload = None if force_refresh else await _get_json(redis, taf_key)
    sigmet_payload = None if force_refresh else await _get_json(redis, sigmet_key)

    if not force_refresh:
        cache_hits = [
            station_payload is not None,
            metar_payload is not None,
            taf_payload is not None,
            sigmet_payload is not None,
        ]
        logger.info(
            "[%s] Cache hits: station=%s, metar=%s, taf=%s, sigmet=%s",
            normalized,
            cache_hits[0],
            cache_hits[1],
            cache_hits[2],
            cache_hits[3],
        )

    async with httpx.AsyncClient(
        base_url=settings.aviation_weather_base_url,
        timeout=settings.weather_http_timeout_seconds,
    ) as http:
        client = AviationWeatherClient(
            http=http, user_agent=settings.weather_user_agent
        )

        if station_payload is None:
            try:
                station_rows = await client.fetch_station_info(normalized)
                station_payload = station_rows[0] if station_rows else None
                if station_payload is not None:
                    await _set_json(
                        redis,
                        station_key,
                        station_payload,
                        settings.weather_station_cache_ttl_seconds,
                    )
            except AviationWeatherClientError as exc:
                alerts.append(
                    Alert(
                        level=AlertLevel.WARNING,
                        code="WEATHER_UPSTREAM_ERROR",
                        message=f"station: {exc}",
                    )
                )
                logger.warning("[%s] AviationWeather station error: %s", normalized, exc)

        if metar_payload is None:
            try:
                metar_rows = await client.fetch_metar(
                    normalized, hours=hours_back
                )
                metar_payload = metar_rows[0] if metar_rows else None
                if metar_payload is not None:
                    await _set_json(
                        redis,
                        metar_key,
                        metar_payload,
                        settings.weather_metar_cache_ttl_seconds,
                    )
            except AviationWeatherClientError as exc:
                alerts.append(
                    Alert(
                        level=AlertLevel.WARNING,
                        code="WEATHER_UPSTREAM_ERROR",
                        message=f"metar: {exc}",
                    )
                )
                logger.warning("[%s] AviationWeather metar error: %s", normalized, exc)

        if taf_payload is None:
            try:
                taf_rows = await client.fetch_taf(normalized)
                taf_payload = taf_rows[0] if taf_rows else None
                if taf_payload is not None:
                    await _set_json(
                        redis,
                        taf_key,
                        taf_payload,
                        settings.weather_taf_cache_ttl_seconds,
                    )
            except AviationWeatherClientError as exc:
                alerts.append(
                    Alert(
                        level=AlertLevel.WARNING,
                        code="WEATHER_UPSTREAM_ERROR",
                        message=f"taf: {exc}",
                    )
                )
                logger.warning("[%s] AviationWeather taf error: %s", normalized, exc)

        if sigmet_payload is None:
            try:
                sigmet_payload = await client.fetch_isigmet_geojson()
                await _set_json(
                    redis,
                    sigmet_key,
                    sigmet_payload,
                    settings.weather_sigmet_cache_ttl_seconds,
                )
            except AviationWeatherClientError as exc:
                alerts.append(
                    Alert(
                        level=AlertLevel.WARNING,
                        code="WEATHER_UPSTREAM_ERROR",
                        message=f"sigmet: {exc}",
                    )
                )
                logger.warning("[%s] AviationWeather sigmet error: %s", normalized, exc)

    station = (
        _parse_station(station_payload)
        if isinstance(station_payload, dict)
        else None
    )
    if station is None or station.lat is None or station.lon is None:
        alerts.append(
            Alert(
                level=AlertLevel.ERROR,
                code="WEATHER_STATION_NOT_FOUND",
                message=f"AviationWeather station coordinates are unavailable for {normalized}.",
            )
        )

    metar = (
        _parse_metar(metar_payload) if isinstance(metar_payload, dict) else None
    )
    if metar is None:
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="METAR_NOT_AVAILABLE",
                message=f"METAR is unavailable for {normalized}.",
            )
        )

    taf = _parse_taf(taf_payload) if isinstance(taf_payload, dict) else None
    if taf is None:
        alerts.append(
            Alert(
                level=AlertLevel.WARNING,
                code="TAF_NOT_AVAILABLE",
                message=f"TAF is unavailable for {normalized}.",
            )
        )

    sigmets: list[WeatherSigmet] = []
    if (
        station is not None
        and station.lat is not None
        and station.lon is not None
        and isinstance(sigmet_payload, dict)
    ):
        for feature in sigmet_payload.get("features") or []:
            if feature_contains_point(feature, lat=station.lat, lon=station.lon):
                sigmets.append(_parse_sigmet(feature))

    if not force_refresh:
        if all(cache_hits):
            source = "cache"
        elif any(cache_hits):
            source = "mixed"
        else:
            source = "fresh_fetch"
    else:
        source = "fresh_fetch"

    messages.append(
        f"[{normalized}] Weather intelligence resolved with source={source}."
    )

    fetched_at = _utcnow()
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
        metadata={
            "cache_keys": [station_key, metar_key, taf_key, sigmet_key]
        },
    )
