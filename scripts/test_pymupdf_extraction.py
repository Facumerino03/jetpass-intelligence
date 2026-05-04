"""CLI script: extract PyMuPDF layout text from a local PDF."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from app.services.scraper.aip_parser import AipParserError, PyMuPdfAipParser


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract PyMuPDF layout artifact from a local AIP PDF."
    )
    parser.add_argument("--pdf-path", required=True, type=Path, help="Path to local PDF file.")
    parser.add_argument("--save-to", type=Path, default=None, help="Optional JSON output file path.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.pdf_path.exists():
        raise SystemExit(f"PDF not found: {args.pdf_path}")

    parser = PyMuPdfAipParser()
    started = time.monotonic()
    try:
        artifact, stats = parser.extract_layout(args.pdf_path)
    except AipParserError as exc:
        print(f"\nExtraction failed: {exc}")
        raise SystemExit(1) from exc

    elapsed = time.monotonic() - started
    print("\nExtraction succeeded")
    print(f"  PDF               : {args.pdf_path}")
    print(f"  Elapsed           : {elapsed:.2f}s")
    print(f"  extraction_seconds: {stats.extraction_seconds:.2f}s")
    print(f"  ocr_triggered     : {stats.ocr_triggered}")
    print(f"  strategy          : {stats.parser_strategy}")

    output = json.dumps(artifact, indent=2, ensure_ascii=False)
    if args.save_to is not None:
        args.save_to.parent.mkdir(parents=True, exist_ok=True)
        args.save_to.write_text(output, encoding="utf-8")
        print(f"  saved_to          : {args.save_to}")
        return

    print("\nLayout artifact:\n")
    print(output)


if __name__ == "__main__":
    main()
