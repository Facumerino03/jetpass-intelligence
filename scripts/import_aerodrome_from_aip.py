"""CLI script: import a single aerodrome from AIP ANAC into the database.

Usage::

    uv run python -m scripts.import_aerodrome_from_aip --icao SAMR
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.schemas.aerodrome import AerodromeResponse
from app.services.aerodrome_import_service import (
    AipImportError,
    import_aerodrome_from_aip,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import aerodrome data from AIP ANAC into MongoDB."
    )
    parser.add_argument(
        "--icao",
        required=True,
        type=str.upper,
        metavar="ICAO",
        help="Four-letter ICAO aerodrome code (e.g. SAMR).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where downloaded AIP PDFs will be stored.",
    )
    parser.add_argument(
        "--ocr-enabled",
        choices=["true", "false"],
        default=None,
        help="Override OCR fallback toggle for this run.",
    )
    parser.add_argument(
        "--ocr-mode",
        choices=["page", "document"],
        default=None,
        help="Override OCR mode for this run.",
    )
    parser.add_argument(
        "--quality-threshold",
        type=float,
        default=None,
        help="Override quality threshold that triggers OCR fallback.",
    )
    return parser.parse_args()


def _print_summary(aerodrome: AerodromeResponse) -> None:
    print(f"\n✓ Imported: {aerodrome.icao} — {aerodrome.name}")
    if aerodrome.full_name:
        print(f"  Full name : {aerodrome.full_name}")
    print(f"  AIRAC     : {aerodrome.current.meta.airac_cycle}")
    print(f"  Sections  : {len(aerodrome.current.ad_sections)}")
    if aerodrome.current.ad_sections:
        print("  IDs       : " + ", ".join(s.section_id for s in aerodrome.current.ad_sections))
    print(f"  History   : {len(aerodrome.history)} version(es)")


def _apply_parser_overrides(args: argparse.Namespace) -> None:
    if args.ocr_enabled is not None:
        os.environ["AIP_PARSER_OCR_ENABLED"] = args.ocr_enabled
    if args.ocr_mode is not None:
        os.environ["AIP_PARSER_OCR_MODE"] = args.ocr_mode
    if args.quality_threshold is not None:
        os.environ["AIP_PARSER_DOCLING_QUALITY_THRESHOLD"] = str(args.quality_threshold)
    get_settings.cache_clear()


async def _run(icao: str, output_dir: Path | None) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit(
            "MONGODB_URL is not configured. Set it in your .env file."
        )
    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    aerodrome = await import_aerodrome_from_aip(icao, output_dir=output_dir)
    _print_summary(aerodrome)


def main() -> None:
    args = _parse_args()
    _apply_parser_overrides(args)
    try:
        asyncio.run(_run(args.icao, args.output_dir))
    except AipImportError as exc:
        print(f"\n✗ Import failed: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
