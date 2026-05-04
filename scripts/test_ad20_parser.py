"""CLI script: test AD-2.0 parser directly from a local PDF.

Usage examples:
    uv run python -m scripts.test_ad20_parser --pdf-path /tmp/SAMR_AD-2.0.pdf
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

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
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

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
