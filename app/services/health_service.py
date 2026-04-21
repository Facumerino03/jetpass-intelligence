from app.core.config import Settings


def health_status(settings: Settings) -> dict[str, str]:
    """Build the health payload for the running service instance."""
    return {"status": "ok", "service": settings.app_name}
