"""CLI script: enrich aerodrome AD 2.0 structured data with configured LLM provider.

Usage examples:
    uv run python -m scripts.enrich_aerodrome --icao SAMR
    uv run python -m scripts.enrich_aerodrome --icao SAMR -v
    uv run python -m scripts.enrich_aerodrome --icao SAMR --sections AD 2.12 AD 2.18
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from app.core.config import get_settings
from app.core.database import init_mongodb
from app.models.aerodrome import AerodromeDocument
from app.services.enrichment import enrich_aerodrome
from app.services.enrichment.llm_providers import get_llm_provider


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich AD 2.0 section data for a single aerodrome using the configured LLM_PROVIDER."
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
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Log more (-v INFO, -vv DEBUG incl. traceback hints).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-enrichment by clearing existing _extraction metadata for selected sections.",
    )
    return parser.parse_args()


async def _clear_extraction_metadata(icao: str, sections: list[str] | None) -> int:
    doc = await AerodromeDocument.get(icao.strip().upper())
    if doc is None:
        return 0

    selected = set(sections) if sections else None
    cleared = 0
    for section in doc.current.ad_sections:
        if selected is not None and section.section_id not in selected:
            continue
        if isinstance(section.data, dict) and "_extraction" in section.data:
            section.data.pop("_extraction", None)
            cleared += 1

    if cleared > 0:
        await doc.save()
    return cleared


async def _run(
    icao: str,
    sections: list[str] | None,
    *,
    verbose: int,
    force: bool,
) -> None:
    settings = get_settings()
    if not settings.mongodb_url:
        raise SystemExit("MONGODB_URL is not configured. Set it in your .env file.")

    await init_mongodb(settings.mongodb_url, settings.mongodb_db_name)

    if force:
        cleared = await _clear_extraction_metadata(icao, sections)
        if verbose >= 1:
            print(f"Force mode: cleared _extraction in {cleared} section(s)")

    provider = get_llm_provider()
    if verbose >= 1:
        print(f"Configured LLM_PROVIDER env: {settings.llm_provider}")
        print(f"Resolved provider.engine: {provider.engine_name}")
        print(f"Resolved provider.model : {provider.model_name}")

    enriched = await enrich_aerodrome(icao, section_ids=sections)
    if enriched is None:
        raise SystemExit(f"Aerodrome not found: {icao}")

    providers: set[str] = set()
    models: set[str] = set()
    touched = []
    for section in enriched.current.ad_sections:
        extraction = section.data.get("_extraction", {}) if isinstance(section.data, dict) else {}
        status = extraction.get("status")
        if not status:
            continue
        engine = extraction.get("engine")
        model = extraction.get("model")
        if isinstance(engine, str):
            providers.add(engine)
        if isinstance(model, str):
            models.add(model)
        touched.append(f"{section.section_id} [{status}]")

    print(f"\n✓ Enriched: {enriched.icao} — {enriched.name}")
    print(f"  Provider : {', '.join(sorted(providers)) or getattr(provider, 'engine_name', settings.llm_provider)}")
    print(f"  Model    : {', '.join(sorted(models)) or getattr(provider, 'model_name', settings.ollama_model)}")
    print(f"  Sections : {len(enriched.current.ad_sections)}")

    if touched:
        print("  Enriched : " + ", ".join(touched))

    failures: list[tuple[str, str | None]] = []
    for section in enriched.current.ad_sections:
        if not isinstance(section.data, dict):
            continue
        ext = section.data.get("_extraction", {})
        if not isinstance(ext, dict) or ext.get("status") != "error":
            continue
        failures.append((section.section_id, ext.get("error")))

    if failures:
        print(f"\nFailed sections ({len(failures)}); detail stored in MongoDB `_extraction.error`:")
        for sid, err in failures[: min(25, len(failures))]:
            msg = (err or "(no message)").strip()
            print(f"  {sid}: {msg[:400]}{'…' if len(msg) > 400 else ''}")


def main() -> None:
    args = _parse_args()
    level = logging.DEBUG if args.verbose >= 2 else (logging.INFO if args.verbose >= 1 else logging.WARNING)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s — %(message)s")
    asyncio.run(_run(args.icao, args.sections, verbose=args.verbose, force=args.force))


if __name__ == "__main__":
    main()
