"""CLI script: run AIP import + enrichment end-to-end for one ICAO.

Usage examples:
    uv run python -m scripts.run_aip_e2e --icao SAMR
    uv run python -m scripts.run_aip_e2e --icao SAMR --skip-import
    uv run python -m scripts.run_aip_e2e --icao SAMR --sections "AD 2.12" "AD 2.18"
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.models.aerodrome import AerodromeDocument
from app.services.aerodrome_import_service import AipImportError, import_aerodrome_from_aip
from app.services.enrichment import enrich_aerodrome


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run end-to-end AD 2.0 pipeline: import (raw_text) and enrich (data)."
    )
    parser.add_argument("--icao", required=True, type=str.upper, metavar="ICAO")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where downloaded AIP PDFs are stored during import.",
    )
    parser.add_argument(
        "--sections",
        nargs="*",
        default=None,
        help='Optional section IDs to enrich (e.g. "AD 2.12" "AD 2.18").',
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip import step and only run enrichment on existing DB data.",
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip enrichment and only run import.",
    )
    parser.add_argument("--ocr-enabled", choices=["true", "false"], default=None)
    parser.add_argument("--ocr-mode", choices=["page", "document"], default=None)
    parser.add_argument("--quality-threshold", type=float, default=None)
    return parser.parse_args()


def _apply_parser_overrides(args: argparse.Namespace) -> None:
    if args.ocr_enabled is not None:
        os.environ["AIP_PARSER_OCR_ENABLED"] = args.ocr_enabled
    if args.ocr_mode is not None:
        os.environ["AIP_PARSER_OCR_MODE"] = args.ocr_mode
    if args.quality_threshold is not None:
        os.environ["AIP_PARSER_DOCLING_QUALITY_THRESHOLD"] = str(args.quality_threshold)
    get_settings.cache_clear()


async def _print_status(icao: str) -> None:
    doc = await AerodromeDocument.get(icao)
    if doc is None:
        print(f"\nNo document found for ICAO {icao}.")
        return

    total = len(doc.current.ad_sections)
    enriched_ok = 0
    enriched_error = 0

    for section in doc.current.ad_sections:
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        if extraction.get("status") == "ok":
            enriched_ok += 1
        elif extraction.get("status") == "error":
            enriched_error += 1

    print(f"\n✓ ICAO: {doc.icao} - {doc.name}")
    print(f"  Sections total : {total}")
    print(f"  raw_text filled: {sum(1 for s in doc.current.ad_sections if bool(s.raw_text.strip()))}/{total}")
    print(f"  data status ok : {enriched_ok}")
    print(f"  data status err: {enriched_error}")


async def _run(args: argparse.Namespace) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)

    if not args.skip_import:
        try:
            await import_aerodrome_from_aip(args.icao, output_dir=args.output_dir, enrich=False)
            print("\nImport step completed (raw_text persisted).")
        except AipImportError as exc:
            raise SystemExit(f"Import failed: {exc}") from exc

    if not args.skip_enrich:
        enriched = await enrich_aerodrome(args.icao, section_ids=args.sections)
        if enriched is None:
            raise SystemExit(f"Aerodrome not found after import: {args.icao}")
        print("Enrichment step completed (data persisted).")

    await _print_status(args.icao)


def main() -> None:
    args = _parse_args()
    _apply_parser_overrides(args)
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
