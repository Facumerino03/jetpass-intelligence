"""Test configuration and shared fixtures.

Beanie tests use mongomock-motor as in-memory database.
A compatibility patch is applied at import time to handle kwargs that
Beanie 2.x sends to list_collection_names but mongomock doesn't support.
"""

import mongomock.database
import pytest
import pytest_asyncio
from beanie import init_beanie
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from app.main import app
from app.models import ALL_DOCUMENTS

# ── mongomock compat patch ────────────────────────────────────────────────────
# Beanie 2.x calls list_collection_names(authorizedCollections=True, nameOnly=True)
# which mongomock doesn't support. Drop the unsupported kwargs transparently.
_orig_list_collection_names = mongomock.database.Database.list_collection_names


def _compat_list_collection_names(self, session=None, **_kwargs):
    return _orig_list_collection_names(self, session=session)


mongomock.database.Database.list_collection_names = _compat_list_collection_names
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest_asyncio.fixture(autouse=True)
async def init_test_db() -> None:
    """Initialise Beanie with an in-memory mongomock database for every test."""
    client = AsyncMongoMockClient()
    await init_beanie(
        database=client["test_jetpass"],
        document_models=ALL_DOCUMENTS,  # type: ignore[arg-type]
    )
