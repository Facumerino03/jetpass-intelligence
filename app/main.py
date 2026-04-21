from fastapi import FastAPI

from app.core.config import get_settings
from app.routers.health_router import router as health_router

settings = get_settings()

app = FastAPI(title=settings.app_name, debug=settings.debug)
app.include_router(health_router)
