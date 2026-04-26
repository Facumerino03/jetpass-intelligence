"""Unit tests for app.services.scraper.aip_scraper.

All Playwright calls are mocked — no real browser is launched.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.scraper.aip_scraper import (
    AerodromeNotFoundInAipError,
    AipScraperError,
    download_aip_pdfs,
)


# ── helpers ────────────────────────────────────────────────────────────────────


def _make_playwright_mock(icao: str, dest_dir: Path) -> MagicMock:
    """Build a fully-mocked async_playwright context that simulates one download per section."""

    # Download mock
    def make_download(section: str) -> MagicMock:
        dl = AsyncMock()
        dl.save_as = AsyncMock()
        return dl

    # page.expect_download() context manager
    def make_expect_download_ctx(section: str):
        ctx = MagicMock()
        dl_mock = make_download(section)
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.value = AsyncMock(return_value=dl_mock)
        return ctx

    page = AsyncMock()
    page.goto = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.click = AsyncMock()

    # locator chain for search box
    search_box = AsyncMock()
    search_box.fill = AsyncMock()
    page.locator.return_value = search_box
    search_box.first = search_box

    # After search, make ICAO visible (count > 0)
    icao_locator = MagicMock()
    icao_locator.count = AsyncMock(return_value=1)

    def side_effect_locator(selector: str):
        if icao in selector:
            return icao_locator
        inner = AsyncMock()
        inner.first = inner
        inner.last = inner
        inner.locator = MagicMock(return_value=inner)
        inner.click = AsyncMock()
        return inner

    page.locator.side_effect = side_effect_locator
    page.expect_download = MagicMock(
        side_effect=lambda: make_expect_download_ctx("section")
    )

    context = AsyncMock()
    context.new_page = AsyncMock(return_value=page)

    browser = AsyncMock()
    browser.new_context = AsyncMock(return_value=context)
    browser.close = AsyncMock()

    chromium = AsyncMock()
    chromium.launch = AsyncMock(return_value=browser)

    pw = MagicMock()
    pw.chromium = chromium

    pw_ctx = MagicMock()
    pw_ctx.__aenter__ = AsyncMock(return_value=pw)
    pw_ctx.__aexit__ = AsyncMock(return_value=False)

    return pw_ctx


# ── tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_download_aip_pdfs_raises_on_playwright_failure(tmp_path: Path) -> None:
    with patch("app.services.scraper.aip_scraper.async_playwright") as mock_pw:
        mock_pw.return_value.__aenter__ = AsyncMock(
            side_effect=RuntimeError("browser crash")
        )
        mock_pw.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(AipScraperError):
            await download_aip_pdfs("SAMR", output_dir=tmp_path)


@pytest.mark.asyncio
async def test_aerodrome_not_found_raises_specific_error(tmp_path: Path) -> None:
    """When the ICAO search returns 0 results, AerodromeNotFoundInAipError is raised."""
    pw_ctx = MagicMock()
    pw = MagicMock()
    browser = AsyncMock()
    context = AsyncMock()
    page = AsyncMock()

    page.goto = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.click = AsyncMock()

    # Locator must be an AsyncMock so .fill() and .count() are awaitable
    search_locator = AsyncMock()
    search_locator.fill = AsyncMock()
    search_locator.first = search_locator
    search_locator.count = AsyncMock(return_value=0)  # 0 results → not found
    page.locator = MagicMock(return_value=search_locator)

    context.new_page = AsyncMock(return_value=page)
    browser.new_context = AsyncMock(return_value=context)
    browser.close = AsyncMock()
    pw.chromium.launch = AsyncMock(return_value=browser)

    pw_ctx.__aenter__ = AsyncMock(return_value=pw)
    pw_ctx.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "app.services.scraper.aip_scraper.async_playwright", return_value=pw_ctx
    ):
        with pytest.raises(AerodromeNotFoundInAipError):
            await download_aip_pdfs("XXXX", output_dir=tmp_path)

