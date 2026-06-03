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
