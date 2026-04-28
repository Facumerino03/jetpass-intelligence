"""MongoDB connection and Beanie initialisation."""

from beanie import init_beanie
from pymongo import AsyncMongoClient
from app.models import ALL_DOCUMENTS


async def init_mongodb(mongodb_url: str, db_name: str) -> None:
    """Connect to MongoDB and register all Beanie Document classes."""
    client = AsyncMongoClient(mongodb_url)
    await init_beanie(
        database=client[db_name],
        document_models=ALL_DOCUMENTS,  # type: ignore[arg-type]
    )
