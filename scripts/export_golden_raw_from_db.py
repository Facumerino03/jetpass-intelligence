"""Export AD 2.x raw_text from MongoDB into golden raw fixtures.

Usage:
    uv run python -m scripts.export_golden_raw_from_db --icao SAMR
    uv run python -m scripts.export_golden_raw_from_db --icao SAMR --output-dir tests/golden
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.models.aerodrome import AerodromeDocument


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export AD 2.x raw_text from DB to golden fixtures")
    parser.add_argument("--icao", required=True, help="ICAO code, e.g. SAMR")
    parser.add_argument(
        "--output-dir",
        default="tests/golden",
        help="Base output directory (default: tests/golden)",
    )
    return parser.parse_args()


def _section_stem(section_id: str) -> str:
    return section_id.replace(" ", "_").replace(".", "_")


async def _run(icao: str, output_dir: Path) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise RuntimeError("MONGODB_URL is required to export raw fixtures from DB")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)

    doc = await AerodromeDocument.get(icao)
    if doc is None:
        raise RuntimeError(f"Aerodrome document not found for ICAO={icao}")

    destination = output_dir / icao
    destination.mkdir(parents=True, exist_ok=True)

    exported = 0
    for section in doc.current.ad_sections:
        if not section.section_id.startswith("AD 2."):
            continue
        file_path = destination / f"{_section_stem(section.section_id)}.raw.txt"
        file_path.write_text(section.raw_text, encoding="utf-8")
        exported += 1

    print(f"Exported {exported} AD 2.x raw fixture(s) to {destination}")


def main() -> None:
    args = _parse_args()
    icao = args.icao.strip().upper()
    asyncio.run(_run(icao, Path(args.output_dir)))


if __name__ == "__main__":
    main()
