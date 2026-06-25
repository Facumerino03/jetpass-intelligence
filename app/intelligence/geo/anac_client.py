"""HTTP client for the ANAC MADHEL aerodrome API."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class AnacClient:
    """Async client for list and detail endpoints of the MADHEL API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        settings = get_settings()
        self._base_url = (base_url or settings.anac_madhel_base_url).rstrip("/")
        self._timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    async def fetch_airports_list(
        self,
        client: httpx.AsyncClient | None = None,
    ) -> list[dict[str, Any]]:
        """Return all items from the airports list endpoint."""
        url = self._url("/airports/?format=json")
        if client is None:
            async with httpx.AsyncClient(timeout=self._timeout) as owned:
                response = await owned.get(url)
                response.raise_for_status()
                payload = response.json()
        else:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        return list(payload.get("results") or [])

    async def fetch_airport_detail(
        self,
        local_identifier: str,
        *,
        client: httpx.AsyncClient,
    ) -> dict[str, Any]:
        """Fetch full detail for a single aerodrome by local identifier."""
        local = local_identifier.strip().upper()
        url = self._url(f"/airports/{local}/?format=json")
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

    async def fetch_all_details(
        self,
        local_identifiers: list[str],
        *,
        concurrency: int = 20,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Fetch details for many locals in parallel.

        Returns ``(successful_details, failed_locals)``.
        """
        semaphore = asyncio.Semaphore(max(1, concurrency))
        results: list[dict[str, Any]] = []
        failed: list[str] = []

        async with httpx.AsyncClient(timeout=self._timeout) as client:

            async def _fetch_one(local: str) -> None:
                async with semaphore:
                    try:
                        detail = await self.fetch_airport_detail(local, client=client)
                        results.append(detail)
                    except Exception as exc:
                        logger.warning("ANAC detail fetch failed for %s: %s", local, exc)
                        failed.append(local)

            await asyncio.gather(*(_fetch_one(local) for local in local_identifiers))

        return results, failed
