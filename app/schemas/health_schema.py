from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """HTTP response body for the health endpoint."""

    status: str = Field(..., examples=["ok"])
    service: str = Field(..., description="Logical service name from configuration.")
