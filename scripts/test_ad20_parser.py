"""CLI script: test AD-2.0 parser directly from a local PDF.

Usage examples:
    uv run python -m scripts.test_ad20_parser --pdf-path /tmp/SAMR_AD-2.0.pdf
    uv run python -m scripts.test_ad20_parser --pdf-path /tmp/SAMR_AD-2.0.pdf --ocr-mode page
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from app.core.config import get_settings
from app.services.scraper.aip_parser import (
    AipParserError,
    parse_aerodrome_from_ad20,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse a local AD-2.0 PDF and print structured aerodrome data."
    )
    parser.add_argument(
        "--pdf-path",
        required=True,
        type=Path,
        help="Path to local AD-2.0 PDF file.",
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
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Override max pages safety limit for this run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="Override timeout budget for this run.",
    )
    return parser.parse_args()


def _apply_overrides(args: argparse.Namespace) -> None:
    if args.ocr_enabled is not None:
        os.environ["AIP_PARSER_OCR_ENABLED"] = args.ocr_enabled
    if args.ocr_mode is not None:
        os.environ["AIP_PARSER_OCR_MODE"] = args.ocr_mode
    if args.quality_threshold is not None:
        os.environ["AIP_PARSER_DOCLING_QUALITY_THRESHOLD"] = str(args.quality_threshold)
    if args.max_pages is not None:
        os.environ["AIP_PARSER_MAX_PAGES"] = str(args.max_pages)
    if args.timeout_seconds is not None:
        os.environ["AIP_PARSER_TIMEOUT_SECONDS"] = str(args.timeout_seconds)
    get_settings.cache_clear()


def main() -> None:
    args = _parse_args()
    _apply_overrides(args)

    if not args.pdf_path.exists():
        raise SystemExit(f"PDF not found: {args.pdf_path}")

    started = time.monotonic()
    try:
        result = parse_aerodrome_from_ad20(args.pdf_path)
    except AipParserError as exc:
        print(f"\n✗ Parser failed: {exc}")
        raise SystemExit(1) from exc

    elapsed = time.monotonic() - started
    print("\n✓ Parser succeeded")
    print(f"  PDF     : {args.pdf_path}")
    print(f"  Elapsed : {elapsed:.2f}s")
    print("\nStructured output:")
    print(json.dumps(result.model_dump(mode="json"), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
