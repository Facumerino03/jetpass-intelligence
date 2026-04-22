from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """HTTP response body for the health endpoint."""

    status: str = Field(..., examples=["ok"])
    service: str = Field(..., description="Logical service name from configuration.")


class ReadinessResponse(BaseModel):
    """Readiness result for infra dependencies."""

    status: str = Field(..., examples=["ok", "degraded"])
    database: str = Field(..., examples=["ok", "not_configured", "error"])
    redis: str = Field(..., examples=["ok", "not_configured", "error"])
