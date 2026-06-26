"""ICAO Doc 8643 aircraft type designator validation via the official search UI."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

from playwright.sync_api import Frame, Page, sync_playwright

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_NO_MATCH_TEXT = "no matching records found"
_DESIGNATOR_COLUMN_INDEX = 2


class IcaoDoc8643ScraperError(Exception):
    """Base error for ICAO Doc 8643 scraper failures."""


@dataclass(slots=True)
class IcaoDoc8643ScrapeResult:
    """Structured output from a single designator lookup."""

    designator: str
    is_valid: bool
    manufacturer: str | None = None
    model: str | None = None
    engine_count: int | None = None
    engine_type: str | None = None
    wtc: str | None = None
    aircraft_description: str | None = None


def _normalize_designator(value: str) -> str:
    return value.strip().upper()


def _dismiss_cookie_banner(page: Page) -> None:
    selectors = (
        "#klaro button.cm-btn-success",
        ".cm-btn-accept-all",
        "button:has-text('Accept')",
        "button:has-text('Accept all')",
    )
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        try:
            locator.first.click(timeout=3_000)
            page.wait_for_timeout(500)
            return
        except Exception:
            continue


def _get_designators_frame(page: Page, *, timeout_seconds: int) -> Frame:
    for _ in range(timeout_seconds):
        for frame in page.frames:
            if "designators.html" in frame.url:
                return frame
        page.wait_for_timeout(1_000)
    raise IcaoDoc8643ScraperError("Could not locate the Doc 8643 designators iframe.")


def _wait_for_datatable_loaded(frame: Frame, page: Page, *, timeout_seconds: int) -> None:
    for _ in range(timeout_seconds):
        rows = frame.locator("#atd-table tbody tr")
        if rows.count() == 0:
            page.wait_for_timeout(1_000)
            continue
        first_row = rows.first.inner_text().strip()
        if first_row and "loading" not in first_row.lower():
            return
        page.wait_for_timeout(1_000)
    raise IcaoDoc8643ScraperError("Doc 8643 DataTable did not finish loading in time.")


def _parse_engine_count(raw: str | None) -> int | None:
    if not raw:
        return None
    match = re.search(r"\d+", raw.strip())
    if match is None:
        return None
    return int(match.group(0))


def _read_row_cells(row_locator) -> list[str]:
    cells = row_locator.locator("td")
    return [cells.nth(index).inner_text().strip() for index in range(cells.count())]


def _row_to_result(designator: str, cells: list[str]) -> IcaoDoc8643ScrapeResult:
    manufacturer = cells[0] if len(cells) > 0 else None
    model = cells[1] if len(cells) > 1 else None
    parsed_designator = cells[2] if len(cells) > 2 else designator
    aircraft_description = cells[3] if len(cells) > 3 else None
    engine_type = cells[4] if len(cells) > 4 else None
    engine_count = _parse_engine_count(cells[5] if len(cells) > 5 else None)
    wtc = cells[6] if len(cells) > 6 else None
    return IcaoDoc8643ScrapeResult(
        designator=parsed_designator,
        is_valid=True,
        manufacturer=manufacturer,
        model=model,
        engine_count=engine_count,
        engine_type=engine_type,
        wtc=wtc,
        aircraft_description=aircraft_description,
    )


def _search_designator_in_frame(frame: Frame, designator: str) -> IcaoDoc8643ScrapeResult:
    search_input = frame.locator("#atd-table thead input").nth(_DESIGNATOR_COLUMN_INDEX)
    search_input.wait_for(state="visible", timeout=30_000)
    search_input.fill("")
    search_input.type(designator, delay=30)
    search_input.press("Enter")
    frame.wait_for_timeout(2_500)

    rows = frame.locator("#atd-table tbody tr")
    if rows.count() == 0:
        return IcaoDoc8643ScrapeResult(designator=designator, is_valid=False)

    first_row_text = rows.first.inner_text().strip()
    if _NO_MATCH_TEXT in first_row_text.lower():
        return IcaoDoc8643ScrapeResult(designator=designator, is_valid=False)

    for index in range(rows.count()):
        cells = _read_row_cells(rows.nth(index))
        if len(cells) <= _DESIGNATOR_COLUMN_INDEX:
            continue
        if cells[_DESIGNATOR_COLUMN_INDEX].strip().upper() == designator:
            return _row_to_result(designator, cells)

    return IcaoDoc8643ScrapeResult(designator=designator, is_valid=False)


def _validate_designator_sync(
    designator: str,
    *,
    headless: bool,
    base_url: str,
    load_timeout_seconds: int,
) -> IcaoDoc8643ScrapeResult:
    normalized = _normalize_designator(designator)
    if not normalized:
        raise IcaoDoc8643ScraperError("Designator must not be empty.")

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            page = browser.new_page()
            try:
                page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(2_000)
                _dismiss_cookie_banner(page)
                frame = _get_designators_frame(page, timeout_seconds=30)
                _wait_for_datatable_loaded(frame, page, timeout_seconds=load_timeout_seconds)
                return _search_designator_in_frame(frame, normalized)
            finally:
                browser.close()
    except IcaoDoc8643ScraperError:
        raise
    except Exception as exc:
        raise IcaoDoc8643ScraperError(f"Unexpected Doc 8643 scrape error: {exc}") from exc


async def validate_designator(
    designator: str,
    *,
    headless: bool | None = None,
    base_url: str | None = None,
    load_timeout_seconds: int | None = None,
) -> IcaoDoc8643ScrapeResult:
    """Validate a designator using the official ICAO Doc 8643 search UI."""
    settings = get_settings()
    return await asyncio.to_thread(
        _validate_designator_sync,
        designator,
        headless=settings.icao_doc8643_validation_headless if headless is None else headless,
        base_url=base_url or settings.icao_doc8643_base_url,
        load_timeout_seconds=(
            settings.icao_doc8643_load_timeout_seconds
            if load_timeout_seconds is None
            else load_timeout_seconds
        ),
    )
