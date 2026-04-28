"""CLI script: extract raw text with Docling/OCR only (no AD 2.x segmentation).

Usage examples:
    uv run python -m scripts.test_docling_extraction --pdf-path tmp/aip/SAMR/SAMR_AD-2.0.pdf
    uv run python -m scripts.test_docling_extraction --pdf-path tmp/aip/SAMR/SAMR_AD-2.0.pdf --save-to tmp/samr_raw.txt
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from app.core.config import get_settings
from app.services.scraper.aip_parser import AipParserError, DoclingOcrParser, ParserConfig


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract raw text using Docling/OCR only (no section parsing)."
    )
    parser.add_argument("--pdf-path", required=True, type=Path, help="Path to local PDF file.")
    parser.add_argument("--save-to", type=Path, default=None, help="Optional text output file path.")
    parser.add_argument("--ocr-enabled", choices=["true", "false"], default=None)
    parser.add_argument("--ocr-mode", choices=["page", "document"], default=None)
    parser.add_argument("--quality-threshold", type=float, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=None)
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


def _build_config() -> ParserConfig:
    settings = get_settings()
    return ParserConfig(
        quality_threshold=settings.aip_parser_docling_quality_threshold,
        ocr_enabled=settings.aip_parser_ocr_enabled,
        ocr_mode=settings.aip_parser_ocr_mode,
        timeout_seconds=settings.aip_parser_timeout_seconds,
        max_pages=settings.aip_parser_max_pages,
    )


def main() -> None:
    args = _parse_args()
    _apply_overrides(args)

    if not args.pdf_path.exists():
        raise SystemExit(f"PDF not found: {args.pdf_path}")

    parser = DoclingOcrParser(config=_build_config())

    started = time.monotonic()
    try:
        text, stats = parser.extract_text(args.pdf_path)
    except AipParserError as exc:
        print(f"\n✗ Extraction failed: {exc}")
        raise SystemExit(1) from exc

    elapsed = time.monotonic() - started
    print("\n✓ Extraction succeeded")
    print(f"  PDF               : {args.pdf_path}")
    print(f"  Elapsed           : {elapsed:.2f}s")
    print(f"  extraction_seconds: {stats.extraction_seconds:.2f}s")
    print(f"  ocr_seconds       : {stats.ocr_seconds:.2f}s")
    print(f"  ocr_triggered     : {stats.ocr_triggered}")
    print(f"  strategy          : {stats.parser_strategy}")

    if args.save_to is not None:
        args.save_to.parent.mkdir(parents=True, exist_ok=True)
        args.save_to.write_text(text, encoding="utf-8")
        print(f"  saved_to          : {args.save_to}")
        return

    print("\nRaw output:\n")
    print(text)


if __name__ == "__main__":
    main()
