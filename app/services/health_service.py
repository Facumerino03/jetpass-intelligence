from app.core.config import Settings
from app.core.database import get_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from redis.asyncio import Redis
from redis.exceptions import RedisError


def health_status(settings: Settings) -> dict[str, str]:
    """Build the health payload for the running service instance."""
    return {"status": "ok", "service": settings.app_name}


async def readiness_status(settings: Settings, redis: Redis | None) -> dict[str, str]:
    database_status = "not_configured"
    redis_status = "not_configured"

    if settings.database_url:
        try:
            engine = get_engine()
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            database_status = "ok"
        except SQLAlchemyError:
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
