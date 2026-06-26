from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.services.scraper.icao_doc8643_scraper import (
    IcaoDoc8643ScrapeResult,
    _search_designator_in_frame,
)


def _mock_row(cells: list[str]) -> MagicMock:
    row = MagicMock()
    cell_locator = MagicMock()

    def _nth(index: int) -> MagicMock:
        cell = MagicMock()
        cell.inner_text.return_value = cells[index]
        return cell

    cell_locator.count.return_value = len(cells)
    cell_locator.nth.side_effect = _nth
    row.locator.return_value = cell_locator
    row.inner_text.return_value = "\t".join(cells)
    return row


def _mock_frame(rows: list[list[str]], *, first_row_text: str | None = None) -> MagicMock:
    frame = MagicMock()
    search_input = MagicMock()
    frame.locator.return_value.nth.return_value = search_input

    rows_locator = MagicMock()
    rows_locator.count.return_value = len(rows)
    rows_locator.first.inner_text.return_value = first_row_text or (
        "\t".join(rows[0]) if rows else ""
    )

    def _row_nth(index: int) -> MagicMock:
        return _mock_row(rows[index])

    rows_locator.nth.side_effect = _row_nth
    frame.locator.side_effect = lambda selector: (
        MagicMock(nth=lambda _: search_input)
        if "thead input" in selector
        else rows_locator
    )
    return frame


def test_search_designator_in_frame_valid_match() -> None:
    frame = _mock_frame(
        [["CESSNA", "172", "C172", "LandPlane", "Piston", "1", "L"]],
    )

    result = _search_designator_in_frame(frame, "C172")

    assert result.is_valid is True
    assert result.designator == "C172"
    assert result.manufacturer == "CESSNA"
    assert result.wtc == "L"


def test_search_designator_in_frame_no_matching_records() -> None:
    frame = MagicMock()
    search_input = MagicMock()
    rows_locator = MagicMock()
    rows_locator.count.return_value = 1
    rows_locator.first.inner_text.return_value = "No matching records found"

    def locator_side_effect(selector: str) -> MagicMock:
        if "thead input" in selector:
            return MagicMock(nth=lambda _: search_input)
        return rows_locator

    frame.locator.side_effect = locator_side_effect

    result = _search_designator_in_frame(frame, "ZZZZINVALID")

    assert result.is_valid is False


def test_search_designator_in_frame_requires_exact_designator_match() -> None:
    frame = _mock_frame(
        [["CESSNA", "172", "C17", "LandPlane", "Piston", "1", "L"]],
    )

    result = _search_designator_in_frame(frame, "C172")

    assert result.is_valid is False
