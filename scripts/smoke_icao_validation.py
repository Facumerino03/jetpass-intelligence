"""Live smoke test for ICAO Doc 8643 validation (manual / verify)."""

import asyncio

from app.intelligence.aircraft_types.aircraft_type_service import (
    clear_validation_cache,
    validate_aircraft_type,
)


async def main() -> None:
    clear_validation_cache()
    valid = await validate_aircraft_type("C172")
    print("C172", valid.is_valid, valid.entry)
    invalid = await validate_aircraft_type("ZZZZINVALID")
    print("ZZZZINVALID", invalid.is_valid)


if __name__ == "__main__":
    asyncio.run(main())
