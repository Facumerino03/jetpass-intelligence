import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings


@pytest.mark.asyncio
async def test_database_connection() -> None:
    settings = get_settings()
    if not settings.database_url:
        pytest.skip("DATABASE_URL is not configured")

    engine = create_async_engine(settings.database_url, future=True)

    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT 1"))
            assert result.scalar_one() == 1
    except SQLAlchemyError as exc:
        pytest.skip(f"Database is not reachable: {exc}")
    finally:
        await engine.dispose()
