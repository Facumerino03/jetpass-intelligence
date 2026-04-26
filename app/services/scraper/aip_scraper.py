"""AIP ANAC scraper — downloads PDFs for a given aerodrome ICAO using Playwright."""

from __future__ import annotations

import logging
from pathlib import Path

from playwright.async_api import Page, async_playwright

logger = logging.getLogger(__name__)

AIP_BASE_URL = "https://ais.anac.gob.ar/aip"
PDF_SECTIONS = ("AD-2.0", "AD-2.A", "AD-2.B", "AD-2.M")
REQUIRED_SECTIONS = {"AD-2.0"}
DEFAULT_OUTPUT_DIR = Path("tmp/aip")

# Candidate selectors for the aerodrome search box, tried in order.
_SEARCH_INPUT_SELECTORS = [
    'input[placeholder*="aerodrome" i]',
    'input[placeholder*="aeródromo" i]',
    'input[placeholder*="icao" i]',
    'input[placeholder*="search" i]',
    'input[placeholder*="buscar" i]',
    'input[aria-label*="search" i]',
    'input[aria-label*="buscar" i]',
    "input[type=search]",
    "input[type=text]",
]


class AipScraperError(Exception):
    """Base error for the AIP scraper module."""


class AerodromeNotFoundInAipError(AipScraperError):
    """ICAO not found in the AIP site search results."""


class PdfNotAvailableError(AipScraperError):
    """A required PDF section is not available for the aerodrome."""


async def download_aip_pdfs(
    icao: str,
    output_dir: Path | None = None,
    headless: bool = True,
) -> list[Path]:
    """Navigate the AIP ANAC site and download PDFs for the given ICAO.

    Args:
        icao: Four-letter ICAO aerodrome code (e.g. ``"SAMR"``).
        output_dir: Base directory for downloads; defaults to ``tmp/aip/{icao}/``.
        headless: Run Chromium headless. Set ``False`` for local debugging.

    Returns:
        Ordered list of local :class:`~pathlib.Path` for each downloaded PDF.

    Raises:
        AerodromeNotFoundInAipError: ICAO not found in the AIP search.
        PdfNotAvailableError: A required section (AD-2.0) could not be downloaded.
        AipScraperError: Any other unexpected scraping failure.
    """
    icao = icao.strip().upper()
    dest_dir = (output_dir or DEFAULT_OUTPUT_DIR) / icao
    debug_dir = dest_dir / "debug"
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            try:
                logger.info("Navigating to %s", AIP_BASE_URL)
                await page.goto(AIP_BASE_URL, wait_until="networkidle")
                await _select_ad_tab(page)
                await _search_aerodrome(page, icao)
                paths = await _download_sections(page, icao, dest_dir)
            except AipScraperError:
                await _save_debug_screenshot(page, debug_dir, icao)
                raise
            finally:
                await browser.close()

    except (AipScraperError, AerodromeNotFoundInAipError, PdfNotAvailableError):
        raise
    except Exception as exc:
        msg = f"Unexpected error scraping AIP for '{icao}': {exc}"
        raise AipScraperError(msg) from exc

    return paths


# ── private navigation helpers ─────────────────────────────────────────────────


async def _select_ad_tab(page: Page) -> None:
    """Click the 'Ad' tab and wait for its content panel to appear."""
    # The ANAC AIP site renders the tab as "Ad" (not "AD").
    _TAB_CANDIDATES = [
        "a:text-is('Ad')",
        "a:text-is('AD')",
        "text='Ad'",
        "text='AD'",
        "[role=tab]:has-text('Ad')",
        "[role=tab]:has-text('AD')",
    ]
    try:
        clicked = False
        for selector in _TAB_CANDIDATES:
            try:
                await page.click(selector, timeout=5_000)
                clicked = True
                logger.debug("AD tab clicked with selector: %s", selector)
                break
            except Exception:
                continue

        if not clicked:
            raise AipScraperError(
                "Could not find the 'Ad' tab. "
                "Try headless=False to inspect the page manually."
            )

        # Wait for the search box to appear inside the Ad tab panel
        # before attempting to fill it.
        await page.wait_for_selector(
            "input[type=text], input[type=search]",
            state="visible",
            timeout=15_000,
        )
        logger.debug("AD tab content loaded")
    except AipScraperError:
        raise
    except Exception as exc:
        raise AipScraperError(f"Could not select the 'Ad' tab: {exc}") from exc


async def _search_aerodrome(page: Page, icao: str) -> None:
    """Locate the search input and fill it with the ICAO code."""
    search = await _find_search_input(page, icao)
    try:
        await search.clear()
        await search.fill(icao)
        await search.press("Enter")
        # Give the SPA a moment to filter results
        await page.wait_for_load_state("networkidle", timeout=15_000)

        result_count = await page.locator(f"text={icao}").count()
        if result_count == 0:
            raise AerodromeNotFoundInAipError(
                f"Aerodrome '{icao}' not found in AIP search results."
            )
        logger.debug("Search returned %d result(s) for %s", result_count, icao)
    except AerodromeNotFoundInAipError:
        raise
    except Exception as exc:
        raise AipScraperError(f"Error filling search for '{icao}': {exc}") from exc


async def _find_search_input(page: Page, icao: str):
    """Try each candidate selector and return the first visible input found."""
    for selector in _SEARCH_INPUT_SELECTORS:
        try:
            locator = page.locator(selector).first
            await locator.wait_for(state="visible", timeout=3_000)
            logger.debug("Search input found with selector: %s", selector)
            return locator
        except Exception:
            continue

    raise AipScraperError(
        f"No search input found on the AD page for '{icao}'. "
        "The site layout may have changed. "
        "Try headless=False to inspect the page manually."
    )


async def _download_sections(page: Page, icao: str, dest_dir: Path) -> list[Path]:
    """Try to download each PDF section; raise only for required ones."""
    paths: list[Path] = []
    missing_required: list[str] = []

    for section in PDF_SECTIONS:
        try:
            file_path = dest_dir / f"{icao}_{section}.pdf"
            row = page.locator(f"text={section}").first
            latest_link = row.locator("..").locator("a").last

            async with page.expect_download(timeout=30_000) as dl_info:
                await latest_link.click()
            download = await dl_info.value
            await download.save_as(file_path)
            logger.info("Downloaded %s → %s", section, file_path)
            paths.append(file_path)

        except Exception as exc:
            logger.warning("Section %s not available for %s: %s", section, icao, exc)
            if section in REQUIRED_SECTIONS:
                missing_required.append(section)

    if missing_required:
        raise PdfNotAvailableError(
            f"Required PDF section(s) {missing_required} could not be downloaded for '{icao}'."
        )

    return paths


async def _save_debug_screenshot(page: Page, debug_dir: Path, icao: str) -> None:
    """Take a screenshot for post-mortem analysis; never raises."""
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        path = debug_dir / f"{icao}_failure.png"
        await page.screenshot(path=str(path), full_page=True)
        logger.info("Debug screenshot saved → %s", path)
    except Exception as exc:
        logger.debug("Could not save debug screenshot: %s", exc)
