"""CLI script: enrich aerodrome AD 2.0 structured data with Ollama.

Usage examples:
    uv run python -m scripts.enrich_aerodrome --icao SAMR
    uv run python -m scripts.enrich_aerodrome --icao SAMR --sections AD 2.12 AD 2.18
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.services.enrichment import enrich_aerodrome


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich AD 2.0 section data for a single aerodrome using Ollama."
    )
    parser.add_argument(
        "--icao",
        required=True,
        type=str.upper,
        metavar="ICAO",
        help="Four-letter ICAO aerodrome code (e.g. SAMR).",
    )
    parser.add_argument(
        "--sections",
        nargs="*",
        default=None,
        help='Optional section IDs (e.g. "AD 2.12" "AD 2.18").',
    )
    return parser.parse_args()


async def _run(icao: str, sections: list[str] | None) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)
    enriched = await enrich_aerodrome(icao, section_ids=sections)
    if enriched is None:
        raise SystemExit(f"Aerodrome not found: {icao}")

    print(f"\n✓ Enriched: {enriched.icao} — {enriched.name}")
    print(f"  Model    : {settings.ollama_model}")
    print(f"  Sections : {len(enriched.current.ad_sections)}")

    touched = []
    for section in enriched.current.ad_sections:
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        if extraction.get("engine") == "ollama":
            touched.append(f"{section.section_id} [{extraction.get('status', 'unknown')}]")

    if touched:
        print("  Enriched : " + ", ".join(touched))


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args.icao, args.sections))


if __name__ == "__main__":
    main()
