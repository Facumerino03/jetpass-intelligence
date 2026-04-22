from functools import lru_cache
from typing import cast

from redis.asyncio import Redis

from app.core.config import get_settings


@lru_cache
def get_redis_client() -> Redis:
    settings = get_settings()
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required to initialize the Redis client.")
    return Redis.from_url(settings.redis_url, decode_responses=True)


async def get_redis() -> Redis | None:
    settings = get_settings()
    if not settings.redis_url:
        return None
    return get_redis_client()


async def close_redis_client() -> None:
    try:
        client = cast(Redis, get_redis_client())
    except RuntimeError:
        return
    await client.aclose()
