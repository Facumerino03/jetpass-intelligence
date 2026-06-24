import logging
from asyncio import Lock
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.core.redis import close_redis_client
from app.routers.aerodrome_router import router as aerodrome_router
from app.routers.health_router import router as health_router
from app.routers.intelligence_router import router as intelligence_router
from app.intelligence.geo.airports_sync import download_airports_csv
from app.intelligence.geo.airports_index import (
    get_global_index,
    reload_global_index,
)
from app.services.airports_sync_runtime import (
    update_airports_sync_status,
    utcnow as airports_utcnow,
)
from app.services.notam_location_sync_runtime import (
    update_notam_sync_status,
    utcnow,
)
from app.services.notam_location_sync_service import sync_notam_locations

logger = logging.getLogger(__name__)
settings = get_settings()
_sync_lock = Lock()


def _safe_next_run_time(job: Any | None) -> datetime | None:
    """Return the job's next run time when the scheduler has assigned one."""
    if job is None:
        return None
    return getattr(job, "next_run_time", None)

# Global CSV index — shared with geo_service via get_global_index().
airports_index = get_global_index(
    csv_path=Path(__file__).resolve().parent / "docs" / "airports.csv",
)


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


async def _run_airports_csv_sync_job() -> None:
    if _sync_lock.locked():
        logger.info("Airports CSV sync skipped: previous run still in progress.")
        return

    update_airports_sync_status(
        in_progress=True,
        last_run_started_at=airports_utcnow(),
        last_error=None,
    )
    async with _sync_lock:
        try:
            rows, error = await download_airports_csv(airports_index.csv_path)
            if error:
                raise RuntimeError(error)
            await reload_global_index().ensure_loaded()
            update_airports_sync_status(
                in_progress=False,
                last_run_finished_at=airports_utcnow(),
                last_success_at=airports_utcnow(),
                last_downloaded_rows=rows,
                last_error=None,
            )
            logger.info("Airports CSV sync complete — %d rows", rows)
        except Exception as exc:
            update_airports_sync_status(
                in_progress=False,
                last_run_finished_at=airports_utcnow(),
                last_error=str(exc),
            )
            logger.exception("Airports CSV sync failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    scheduler: AsyncIOScheduler | None = None
    if settings.mongodb_url:
        await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
        logger.info("MongoDB / Beanie initialised (db: %s)", settings.mongodb_db_name)

        # ── Load airports index (always, even if sync is disabled) ──────────
        await airports_index.ensure_loaded()

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

        if settings.airports_csv_sync_enabled:
            if scheduler is None:
                scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(
                _run_airports_csv_sync_job,
                IntervalTrigger(hours=settings.airports_csv_sync_interval_hours),
                id="airports_csv_sync",
                replace_existing=True,
                max_instances=1,
            )

        if scheduler is not None and not scheduler.running:
            scheduler.start()

        # ── NOTAM location sync scheduler ───────────────────────────────────
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

        # ── Airports CSV sync scheduler ─────────────────────────────────────
        if settings.airports_csv_sync_enabled:
            job = scheduler.get_job("airports_csv_sync") if scheduler else None
            update_airports_sync_status(
                enabled=True,
                scheduler_running=scheduler is not None and scheduler.running,
                interval_hours=settings.airports_csv_sync_interval_hours,
                startup_sync_enabled=settings.airports_csv_sync_on_startup,
                next_run_at=_safe_next_run_time(job),
            )
            logger.info(
                "Airports CSV scheduler started (interval=%dh).",
                settings.airports_csv_sync_interval_hours,
            )
            if settings.airports_csv_sync_on_startup:
                await _run_airports_csv_sync_job()
                job = scheduler.get_job("airports_csv_sync") if scheduler else None
                update_airports_sync_status(
                    next_run_at=_safe_next_run_time(job),
                )
        else:
            update_airports_sync_status(enabled=False, scheduler_running=False)

    else:
        update_notam_sync_status(enabled=False, scheduler_running=False)
        update_airports_sync_status(enabled=False, scheduler_running=False)
        logger.warning("MONGODB_URL not configured — database unavailable.")
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)
        update_notam_sync_status(scheduler_running=False, in_progress=False)
        update_airports_sync_status(scheduler_running=False, in_progress=False)
        await close_redis_client()


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.include_router(health_router)
app.include_router(aerodrome_router)
app.include_router(intelligence_router)
