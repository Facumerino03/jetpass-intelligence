from fastapi import APIRouter

from app.core.dependencies import SettingsDep
from app.schemas.health_schema import HealthResponse
from app.services.health_service import health_status

router = APIRouter(prefix="/api/v1/health", tags=["health"])


@router.get("/", response_model=HealthResponse)
def healthcheck(settings: SettingsDep) -> HealthResponse:
    return HealthResponse.model_validate(health_status(settings))
