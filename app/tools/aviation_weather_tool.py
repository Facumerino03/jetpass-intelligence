"""Tool: fetch aviation weather products from AviationWeather.gov."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.services.weather.aviation_weather_client import (
    AviationWeatherClient,
    AviationWeatherClientError,
)

logger = logging.getLogger(__name__)


class AviationWeatherToolError(RuntimeError):
    """Raised when the AviationWeather tool cannot complete a request."""


class AviationWeatherTool:
    """Tool that wraps AviationWeatherClient calls and translates domain errors.

    The tool receives an open httpx.AsyncClient so the caller controls the
    HTTP session lifecycle (single connection pool across multiple fetches).
    """

    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        user_agent: str,
    ) -> None:
        self._client = AviationWeatherClient(http=http, user_agent=user_agent)

    async def fetch_station_info(self, icao: str) -> list[dict[str, Any]]:
        try:
            return await self._client.fetch_station_info(icao)
        except AviationWeatherClientError as exc:
            raise AviationWeatherToolError(str(exc)) from exc

    async def fetch_metar(self, icao: str, *, hours: float) -> list[dict[str, Any]]:
        try:
            return await self._client.fetch_metar(icao, hours=hours)
        except AviationWeatherClientError as exc:
            raise AviationWeatherToolError(str(exc)) from exc

    async def fetch_taf(self, icao: str) -> list[dict[str, Any]]:
        try:
            return await self._client.fetch_taf(icao)
        except AviationWeatherClientError as exc:
            raise AviationWeatherToolError(str(exc)) from exc

    async def fetch_isigmet_geojson(self) -> dict[str, Any]:
        try:
            return await self._client.fetch_isigmet_geojson()
        except AviationWeatherClientError as exc:
            raise AviationWeatherToolError(str(exc)) from exc
