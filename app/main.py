import asyncio
import logging
import sys
from asyncio import Lock
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

# Playwright on Windows needs ProactorEventLoop for asyncio subprocess support.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.core.redis import close_redis_client
from app.intelligence.aerodromes_catalog_service import sync_anac_catalog
from app.intelligence.geo.anac_catalog_cache import (
    get_global_catalog_cache,
    reload_global_catalog_cache,
)
from app.routers.aerodrome_router import router as aerodrome_router
from app.routers.health_router import router as health_router
from app.routers.intelligence_router import router as intelligence_router
from app.services.airports_sync_runtime import (
    update_anac_catalog_sync_status,
    utcnow as catalog_utcnow,
)
from app.services.notam_location_sync_runtime import (
    update_notam_sync_status,
    utcnow,
)
from app.services.notam_location_sync_service import sync_notam_locations

logger = logging.getLogger(__name__)
settings = get_settings()
_notam_sync_lock = Lock()
_anac_sync_lock = Lock()


def _safe_next_run_time(job: Any | None) -> datetime | None:
    """Return the job's next run time when the scheduler has assigned one."""
    if job is None:
        return None
    return getattr(job, "next_run_time", None)


anac_catalog_cache = get_global_catalog_cache()


async def _run_notam_location_sync_job() -> None:
    if _notam_sync_lock.locked():
        logger.info("NOTAM location sync skipped: previous run still in progress.")
        return

    update_notam_sync_status(
        in_progress=True,
        last_run_started_at=utcnow(),
        last_error=None,
    )
    async with _notam_sync_lock:
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


async def _run_anac_catalog_sync_job() -> None:
    if _anac_sync_lock.locked():
        logger.info("ANAC catalog sync skipped: previous run still in progress.")
        return

    update_anac_catalog_sync_status(
        in_progress=True,
        last_run_started_at=catalog_utcnow(),
        last_error=None,
    )
    async with _anac_sync_lock:
        try:
            result = await sync_anac_catalog(
                force_refresh=True,
                cache=reload_global_catalog_cache(),
            )
            await anac_catalog_cache.ensure_loaded()
            update_anac_catalog_sync_status(
                in_progress=False,
                last_run_finished_at=catalog_utcnow(),
                last_success_at=catalog_utcnow(),
                last_listed_count=result.total_listed,
                last_aerodromes_count=result.total_aerodromes,
                last_helipuertos_skipped=result.total_helipuertos_skipped,
                last_error=None,
            )
            logger.info(
                "ANAC catalog sync complete — %d aerodromes (%d helipuertos skipped)",
                result.total_aerodromes,
                result.total_helipuertos_skipped,
            )
        except Exception as exc:
            update_anac_catalog_sync_status(
                in_progress=False,
                last_run_finished_at=catalog_utcnow(),
                last_error=str(exc),
            )
            logger.exception("ANAC catalog sync failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    scheduler: AsyncIOScheduler | None = None
    if settings.mongodb_url:
        await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
        logger.info("MongoDB / Beanie initialised (db: %s)", settings.mongodb_db_name)

        await anac_catalog_cache.ensure_loaded()

        if settings.notam_location_sync_enabled:
            if scheduler is None:
                scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(
                _run_notam_location_sync_job,
                IntervalTrigger(hours=settings.notam_location_sync_interval_hours),
                id="notam_location_sync",
                replace_existing=True,
                max_instances=1,
            )

        if settings.anac_catalog_sync_enabled:
            if scheduler is None:
                scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(
                _run_anac_catalog_sync_job,
                IntervalTrigger(hours=settings.anac_catalog_sync_interval_hours),
                id="anac_catalog_sync",
                replace_existing=True,
                max_instances=1,
            )

        if scheduler is not None and not scheduler.running:
            scheduler.start()

        if settings.notam_location_sync_enabled:
            job = scheduler.get_job("notam_location_sync") if scheduler else None
            update_notam_sync_status(
                enabled=True,
                scheduler_running=scheduler is not None and scheduler.running,
                interval_hours=settings.notam_location_sync_interval_hours,
                startup_sync_enabled=settings.notam_location_sync_on_startup,
                headless=settings.notam_location_sync_headless,
                next_run_at=_safe_next_run_time(job),
            )
            logger.info(
                "NOTAM location scheduler started (interval=%dh, headless=%s).",
                settings.notam_location_sync_interval_hours,
                settings.notam_location_sync_headless,
            )
            if settings.notam_location_sync_on_startup:
                await _run_notam_location_sync_job()
                job = scheduler.get_job("notam_location_sync") if scheduler else None
                update_notam_sync_status(next_run_at=_safe_next_run_time(job))
        else:
            update_notam_sync_status(
                enabled=False,
                scheduler_running=False,
                interval_hours=settings.notam_location_sync_interval_hours,
                startup_sync_enabled=settings.notam_location_sync_on_startup,
                headless=settings.notam_location_sync_headless,
            )

        if settings.anac_catalog_sync_enabled:
            job = scheduler.get_job("anac_catalog_sync") if scheduler else None
            update_anac_catalog_sync_status(
                enabled=True,
                scheduler_running=scheduler is not None and scheduler.running,
                interval_hours=settings.anac_catalog_sync_interval_hours,
                startup_sync_enabled=settings.anac_catalog_sync_on_startup,
                next_run_at=_safe_next_run_time(job),
            )
            logger.info(
                "ANAC catalog scheduler started (interval=%dh).",
                settings.anac_catalog_sync_interval_hours,
            )
            if settings.anac_catalog_sync_on_startup:
                await _run_anac_catalog_sync_job()
                job = scheduler.get_job("anac_catalog_sync") if scheduler else None
                update_anac_catalog_sync_status(
                    next_run_at=_safe_next_run_time(job),
                )
        else:
            update_anac_catalog_sync_status(enabled=False, scheduler_running=False)

    else:
        update_notam_sync_status(enabled=False, scheduler_running=False)
        update_anac_catalog_sync_status(enabled=False, scheduler_running=False)
        logger.warning("MONGODB_URL not configured — database unavailable.")
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)
        update_notam_sync_status(scheduler_running=False, in_progress=False)
        update_anac_catalog_sync_status(scheduler_running=False, in_progress=False)
        await close_redis_client()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.include_router(health_router)
app.include_router(aerodrome_router)
app.include_router(intelligence_router)
