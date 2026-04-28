import pytest

from app.core.config import get_settings


@pytest.mark.asyncio
async def test_mongodb_connection() -> None:
    """Verifica conectividad real a MongoDB si MONGODB_URL está configurada."""
    settings = get_settings()
    if not settings.mongodb_url:
        pytest.skip("MONGODB_URL is not configured")

    try:
        from motor.motor_asyncio import AsyncIOMotorClient

        client: AsyncIOMotorClient = AsyncIOMotorClient(
            settings.mongodb_url,
            serverSelectionTimeoutMS=3000,
        )
        result = await client.admin.command("ping")
        assert result.get("ok") == 1.0
    except Exception as exc:
        pytest.skip(f"MongoDB is not reachable: {exc}")
