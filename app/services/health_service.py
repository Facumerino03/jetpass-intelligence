"""Health and readiness checks for MongoDB + Redis."""

from redis.asyncio import Redis
from redis.exceptions import RedisError
from pymongo import AsyncMongoClient
from app.core.config import Settings


def health_status(settings: Settings) -> dict[str, str]:
    """Build the health payload for the running service instance."""
    return {"status": "ok", "service": settings.app_name}


async def readiness_status(settings: Settings, redis: Redis | None) -> dict[str, str]:
    database_status = "not_configured"
    redis_status = "not_configured"

    if settings.mongodb_url:
        try:
            client = AsyncMongoClient(settings.mongodb_url)
            await client.admin.command("ping")
            database_status = "ok"
        except Exception:
            database_status = "error"

    if settings.redis_url and redis is not None:
        try:
            await redis.ping()
            redis_status = "ok"
        except RedisError:
            redis_status = "error"

    status = (
        "ok"
        if database_status in {"ok", "not_configured"}
        and redis_status in {"ok", "not_configured"}
        else "degraded"
    )

    return {"status": status, "database": database_status, "redis": redis_status}
