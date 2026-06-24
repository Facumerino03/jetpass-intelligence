"""Download and atomically replace the OurAirports CSV file."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx

logger = logging.getLogger(__name__)

OUR_AIRPORTS_CSV_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
EXPECTED_HEADER = '"id","ident","type","name","latitude_deg","longitude_deg","elevation_ft"'


async def download_airports_csv(dst: Path, *, http_timeout: float = 60.0) -> tuple[int, str | None]:
    """Download airports.csv from OurAirports and atomically replace *dst*.

    Returns ``(row_count, error_message)``.
    ``row_count`` is 0 and *error_message* is set when the download fails.
    """
    tmp = Path(NamedTemporaryFile(suffix=".csv", delete=False).name)
    try:
        async with httpx.AsyncClient(timeout=http_timeout) as client:
            response = await client.get(OUR_AIRPORTS_CSV_URL)
            response.raise_for_status()
            content = response.text

        if not content.startswith(EXPECTED_HEADER):
            return 0, f"Unexpected CSV header; expected '{EXPECTED_HEADER}'"

        tmp.write_text(content, encoding="utf-8")
        os.replace(str(tmp), str(dst))

        row_count = content.count("\n") - 1  # header lines removed
        if row_count < 0:
            row_count = 0
        logger.info(
            "Downloaded %s → %s (%d rows)",
            OUR_AIRPORTS_CSV_URL,
            dst,
            row_count,
        )
        return row_count, None

    except httpx.HTTPStatusError as exc:
        msg = f"HTTP {exc.response.status_code} from {OUR_AIRPORTS_CSV_URL}: {exc}"
        logger.warning(msg)
        return 0, msg
    except httpx.RequestError as exc:
        msg = f"Request failed: {exc}"
        logger.warning(msg)
        return 0, msg
    except OSError as exc:
        msg = f"File write error: {exc}"
        logger.warning(msg)
        return 0, msg
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
