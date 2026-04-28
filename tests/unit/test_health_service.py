from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.services.health_service import health_status, readiness_status


def test_health_status_returns_ok_and_service_name() -> None:
    settings = Settings()
    result = health_status(settings)

    assert result["status"] == "ok"
    assert result["service"] == settings.app_name


@pytest.mark.asyncio
async def test_readiness_not_configured_when_no_urls() -> None:
    settings = Settings()
    settings.mongodb_url = None
    settings.redis_url = None

    result = await readiness_status(settings, redis=None)

    assert result["database"] == "not_configured"
    assert result["redis"] == "not_configured"
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_redis_ok() -> None:
    settings = Settings()
    settings.mongodb_url = None
    settings.redis_url = "redis://localhost:6379"

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=True)

    result = await readiness_status(settings, redis=mock_redis)

    assert result["redis"] == "ok"
    assert result["database"] == "not_configured"
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_degraded_when_redis_fails() -> None:
    from redis.exceptions import RedisError

    settings = Settings()
    settings.mongodb_url = None
    settings.redis_url = "redis://localhost:6379"

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(side_effect=RedisError("conn refused"))

    result = await readiness_status(settings, redis=mock_redis)

    assert result["redis"] == "error"
    assert result["status"] == "degraded"
