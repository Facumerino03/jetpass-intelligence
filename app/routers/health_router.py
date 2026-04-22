from fastapi import APIRouter

from app.core.dependencies import RedisDep, SettingsDep
from app.schemas.health_schema import HealthResponse, ReadinessResponse
from app.services.health_service import health_status, readiness_status

router = APIRouter(prefix="/api/v1/health", tags=["health"])


@router.get("/", response_model=HealthResponse)
def healthcheck(settings: SettingsDep) -> HealthResponse:
    return HealthResponse.model_validate(health_status(settings))


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check(settings: SettingsDep, redis: RedisDep) -> ReadinessResponse:
    return ReadinessResponse.model_validate(await readiness_status(settings, redis))
