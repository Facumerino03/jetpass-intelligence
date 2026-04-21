from app.core.config import Settings
from app.services.health_service import health_status


def test_health_status_returns_ok_and_service_name() -> None:
    settings = Settings()
    result = health_status(settings)

    assert result["status"] == "ok"
    assert result["service"] == settings.app_name
