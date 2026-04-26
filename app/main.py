from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.config import get_settings
from app.core.redis import close_redis_client
from app.routers.aerodrome_router import router as aerodrome_router
from app.routers.health_router import router as health_router

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await close_redis_client()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.include_router(health_router)
app.include_router(aerodrome_router)
