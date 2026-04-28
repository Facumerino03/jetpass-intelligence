"""FastAPI dependency injection helpers.

With Beanie as ODM, database access is performed directly through the Document
classes — no session or connection dependency is needed in routes.
"""

from typing import Annotated

from fastapi import Depends
from redis.asyncio import Redis

from app.core.config import Settings, get_settings
from app.core.redis import get_redis

SettingsDep = Annotated[Settings, Depends(get_settings)]
RedisDep = Annotated[Redis | None, Depends(get_redis)]
