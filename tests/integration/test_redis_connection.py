import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings


@pytest.mark.asyncio
async def test_redis_connection() -> None:
    settings = get_settings()

    if not settings.redis_url:
        pytest.skip("REDIS_URL is not configured")

    client = Redis.from_url(settings.redis_url, decode_responses=True)

    try:
        pong = await client.ping()
        assert pong is True
    except RedisError as exc:
        pytest.fail(f"Redis connection failed: {exc}")
    finally:
        await client.aclose()
