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
        try:
            return response.json()
        except ValueError as exc:
            raise AviationWeatherClientError(
                f"AviationWeather returned non-JSON body (status {response.status_code})"
            ) from exc
