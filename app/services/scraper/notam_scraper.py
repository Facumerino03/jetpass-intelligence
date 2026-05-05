"""ANAC NOTAM scraper using Playwright."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from playwright.async_api import Locator, Page, async_playwright

from app.models.notam import RawNotam

logger = logging.getLogger(__name__)

NOTAM_BASE_URL = "https://ais.anac.gob.ar/notam"
ALL_FIRS_OPTION = "AVISOS A TODAS LAS FIRS"


class NotamScraperError(Exception):
    """Base error for NOTAM scraper failures."""


class NotamLocationNotFoundError(NotamScraperError):
    """Raised when a location does not exist in the website selector."""


@dataclass(slots=True)
class NotamScrapeResult:
    """Structured output from one full NOTAM site scrape."""

    site_last_updated_at: datetime | None
    site_last_updated_text: str | None
    aerodrome_notams: list[RawNotam]
    fir_notams: list[RawNotam]
    fir_notams_by_location: dict[str, list[RawNotam]]


async def scrape_notams_for_aerodrome(
    aerodrome_name: str,
    *,
    headless: bool = True,
) -> NotamScrapeResult:
    """Scrape NOTAMs for one aerodrome plus the global FIR advisories."""
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(NOTAM_BASE_URL, wait_until="networkidle")
                site_last_updated_text = await _extract_site_last_updated_text(page)
                site_last_updated_at = _parse_last_updated_from_text(site_last_updated_text)
                fir_notams_by_location = await _scrape_fir_locations(page)
                fir_notams = [
                    notam
                    for location_notams in fir_notams_by_location.values()
                    for notam in location_notams
                ]
                aerodrome_notams = await _scrape_location(page, aerodrome_name)
                return NotamScrapeResult(
                    site_last_updated_at=site_last_updated_at,
                    site_last_updated_text=site_last_updated_text,
                    aerodrome_notams=aerodrome_notams,
                    fir_notams=fir_notams,
                    fir_notams_by_location=fir_notams_by_location,
                )
            finally:
                await browser.close()
    except NotamScraperError:
        raise
    except Exception as exc:  # pragma: no cover - defensive wrapper
        raise NotamScraperError(f"Unexpected NOTAM scrape error: {exc}") from exc


async def list_notam_locations(*, headless: bool = True) -> list[str]:
    """Return all visible location labels from the NOTAM selector."""
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(NOTAM_BASE_URL, wait_until="networkidle")
                select = await _find_location_select(page)
                options = await select.locator("option").all_inner_texts()
                return [opt.strip() for opt in options if opt.strip()]
            finally:
                await browser.close()
    except Exception as exc:
        raise NotamScraperError(f"Could not list NOTAM locations: {exc}") from exc


async def _scrape_location(page: Page, location_name: str) -> list[RawNotam]:
    select = await _find_location_select(page)
    try:
        await select.select_option(label=location_name)
    except Exception as exc:
        raise NotamLocationNotFoundError(
            f"Location '{location_name}' not found in NOTAM selector."
        ) from exc
    await page.wait_for_timeout(800)
    return await _extract_notam_rows(page, location_name)


async def _scrape_fir_locations(page: Page) -> dict[str, list[RawNotam]]:
    options = await _list_select_options(page)
    fir_labels = [label for label in options if label.startswith("AVISOS FIR")]

    # Always include global FIR advisories as baseline context.
    ordered_labels = [ALL_FIRS_OPTION, *sorted(fir_labels)]
    by_location: dict[str, list[RawNotam]] = {}
    for label in ordered_labels:
        by_location[label] = await _scrape_location(page, label)
    return by_location


async def _find_location_select(page: Page) -> Locator:
    selectors = [
        "select",
        "select.form-control",
        "div:has-text('Seleccione un lugar') select",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            await locator.wait_for(state="visible", timeout=4_000)
            return locator
        except Exception:
            continue
    raise NotamScraperError("Could not locate NOTAM location selector.")


async def _list_select_options(page: Page) -> list[str]:
    select = await _find_location_select(page)
    options = await select.locator("option").all_inner_texts()
    return [opt.strip() for opt in options if opt.strip()]


async def _extract_site_last_updated_text(page: Page) -> str | None:
    # Match footer banner ("Última actualización: 05 May 2026 17:27"), not the bullet
    # that says "...la última actualización..." (timezone disclaimer).
    locator = page.locator(r"text=/Última actualización:\s*\d{2}/i").first
    try:
        await locator.wait_for(state="visible", timeout=10_000)
    except Exception:
        return None
    text = await locator.inner_text()
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip()


def _parse_last_updated_from_text(text: str | None) -> datetime | None:
    if not text:
        return None
    match = re.search(
        r"(\d{2}\s+[A-Za-zÀ-ÿ]{3,}\s+\d{4}\s+\d{2}:\d{2})",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    for fmt in ("%d %b %Y %H:%M", "%d %B %Y %H:%M"):
        try:
            parsed = datetime.strptime(match.group(1), fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


async def _extract_notam_rows(page: Page, default_location: str) -> list[RawNotam]:
    rows = page.locator("table tbody tr")
    count = await rows.count()
    notams: list[RawNotam] = []

    for idx in range(count):
        row_text = (await rows.nth(idx).inner_text()).strip()
        if not row_text:
            continue
        parsed = _parse_row_text(row_text, default_location)
        if parsed is not None:
            notams.append(parsed)
    return notams


def _parse_row_text(row_text: str, default_location: str) -> RawNotam | None:
    lines = [line.strip() for line in row_text.splitlines() if line.strip()]
    notam_id = next((line for line in lines if re.match(r"^[A-Z]\d+/\d{4}$", line)), None)
    if not notam_id:
        return None

    valid_from: datetime | None = None
    valid_to: datetime | None = None
    english_fragments: list[str] = []
    spanish_fragments: list[str] = []
    marker_idx = next(
        (idx for idx, line in enumerate(lines) if "Versión en Español" in line),
        None,
    )
    english_lines = lines if marker_idx is None else lines[:marker_idx]
    spanish_lines = [] if marker_idx is None else lines[marker_idx + 1 :]

    for line in english_lines:
        if _is_metadata_line(line, notam_id, default_location):
            continue
        english_fragments.append(line)

    for line in spanish_lines:
        if _is_metadata_line(line, notam_id, default_location):
            continue
        spanish_fragments.append(line)

    for line in lines:
        if line.startswith("Desde:"):
            valid_from = _parse_site_datetime(line.replace("Desde:", "").strip())
        elif line.startswith("Hasta:"):
            valid_to = _parse_site_datetime(line.replace("Hasta:", "").strip())

    english_text = " ".join(english_fragments).strip() or None
    spanish_text = " ".join(spanish_fragments).strip() or None
    raw_text = english_text or spanish_text or ""
    return RawNotam(
        notam_id=notam_id,
        location=default_location,
        valid_from=valid_from,
        valid_to=valid_to,
        raw_text=raw_text,
        english_text=english_text,
        spanish_text=spanish_text,
    )


def _is_metadata_line(line: str, notam_id: str, default_location: str) -> bool:
    if line in {notam_id, "(---)"}:
        return True
    if line.startswith("Desde:") or line.startswith("Hasta:"):
        return True
    if "Versión en Español" in line:
        return True
    if line == default_location:
        return True
    # Rows include a repeated location block; avoid polluting NOTAM text.
    if line.startswith("AVISOS ") or line.startswith("("):
        return True
    return False


def _parse_site_datetime(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None
