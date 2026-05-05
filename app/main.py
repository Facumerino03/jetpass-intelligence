import logging
from asyncio import Lock
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.core.redis import close_redis_client
from app.routers.aerodrome_router import router as aerodrome_router
from app.routers.health_router import router as health_router
from app.routers.intelligence_router import router as intelligence_router
from app.services.notam_location_sync_runtime import (
    update_notam_sync_status,
    utcnow,
)
from app.services.notam_location_sync_service import sync_notam_locations

logger = logging.getLogger(__name__)
settings = get_settings()
_sync_lock = Lock()


async def _run_notam_location_sync_job() -> None:
    if _sync_lock.locked():
        logger.info("NOTAM location sync skipped: previous run still in progress.")
        return

    update_notam_sync_status(
        in_progress=True,
        last_run_started_at=utcnow(),
        last_error=None,
    )
    async with _sync_lock:
        try:
            stats = await sync_notam_locations(headless=settings.notam_location_sync_headless)
            update_notam_sync_status(
                in_progress=False,
                last_run_finished_at=utcnow(),
                last_success_at=utcnow(),
                last_synced_count=stats.synced_count,
                last_missing_count=stats.missing_count,
                last_site_labels_count=stats.site_labels_count,
                last_aerodromes_count=stats.aerodromes_count,
                last_error=None,
            )
            logger.info(
                "NOTAM location sync stats: synced=%d missing=%d",
                stats.synced_count,
                stats.missing_count,
            )
        except Exception as exc:
            update_notam_sync_status(
                in_progress=False,
                last_run_finished_at=utcnow(),
                last_error=str(exc),
            )
            logger.exception("NOTAM location sync failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    scheduler: AsyncIOScheduler | None = None
    if settings.mongodb_url:
        await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
        logger.info("MongoDB / Beanie initialised (db: %s)", settings.mongodb_db_name)

        if settings.notam_location_sync_enabled:
            scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(
                _run_notam_location_sync_job,
                IntervalTrigger(hours=settings.notam_location_sync_interval_hours),
                id="notam_location_sync",
                replace_existing=True,
                max_instances=1,
            )
            scheduler.start()
            job = scheduler.get_job("notam_location_sync")
            update_notam_sync_status(
                enabled=True,
                scheduler_running=True,
                interval_hours=settings.notam_location_sync_interval_hours,
                startup_sync_enabled=settings.notam_location_sync_on_startup,
                headless=settings.notam_location_sync_headless,
                next_run_at=job.next_run_time if job is not None else None,
            )
            logger.info(
                "NOTAM location scheduler started (interval=%dh, headless=%s).",
                settings.notam_location_sync_interval_hours,
                settings.notam_location_sync_headless,
            )
            if settings.notam_location_sync_on_startup:
                await _run_notam_location_sync_job()
                job = scheduler.get_job("notam_location_sync")
                update_notam_sync_status(next_run_at=job.next_run_time if job is not None else None)
        else:
            update_notam_sync_status(
                enabled=False,
                scheduler_running=False,
                interval_hours=settings.notam_location_sync_interval_hours,
                startup_sync_enabled=settings.notam_location_sync_on_startup,
                headless=settings.notam_location_sync_headless,
            )
    else:
        update_notam_sync_status(enabled=False, scheduler_running=False)
        logger.warning("MONGODB_URL not configured — database unavailable.")
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)
        update_notam_sync_status(scheduler_running=False, in_progress=False)
        await close_redis_client()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.include_router(health_router)
app.include_router(aerodrome_router)
app.include_router(intelligence_router)
