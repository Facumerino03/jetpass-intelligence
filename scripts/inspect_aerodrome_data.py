"""CLI script: inspect raw_text/data status per AD 2.x section in MongoDB.

Usage examples:
    uv run python -m scripts.inspect_aerodrome_data --icao SAMR
    uv run python -m scripts.inspect_aerodrome_data --icao SAMR --show-errors-only
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.models.aerodrome import AerodromeDocument


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect per-section raw_text/data extraction status.")
    parser.add_argument("--icao", required=True, type=str.upper, metavar="ICAO")
    parser.add_argument("--show-errors-only", action="store_true")
    return parser.parse_args()


async def _run(icao: str, show_errors_only: bool) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    doc = await AerodromeDocument.get(icao)
    if doc is None:
        raise SystemExit(f"Aerodrome not found: {icao}")

    print(f"\nICAO: {doc.icao} - {doc.name}")
    print(f"AIRAC: {doc.current.meta.airac_cycle}")
    print("section_id | raw_len | data_keys | extraction_status | error")

    for section in doc.current.ad_sections:
        raw_len = len(section.raw_text or "")
        data_keys = sorted(k for k in section.data.keys() if k != "_extraction") if isinstance(section.data, dict) else []
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        status = extraction.get("status", "-")
        error = extraction.get("error") or "-"

        if show_errors_only and status != "error":
            continue

        print(
            f"{section.section_id:7} | {raw_len:7} | {','.join(data_keys) or '-':20} | "
            f"{status:16} | {str(error)[:120]}"
        )


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args.icao, args.show_errors_only))


if __name__ == "__main__":
    main()
