import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.core.redis import close_redis_client
from app.routers.aerodrome_router import router as aerodrome_router
from app.routers.health_router import router as health_router
from app.routers.intelligence_router import router as intelligence_router

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    if settings.mongodb_url:
        await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
        logger.info("MongoDB / Beanie initialised (db: %s)", settings.mongodb_db_name)
    else:
        logger.warning("MONGODB_URL not configured — database unavailable.")
    try:
        yield
    finally:
        await close_redis_client()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.include_router(health_router)
app.include_router(aerodrome_router)
app.include_router(intelligence_router)
