"""In-memory index of OurAirports airport data keyed by ICAO code."""

from __future__ import annotations

import csv
import logging
from asyncio import Lock
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AirportRow:
    ident: str
    type: str
    name: str
    lat: float | None
    lon: float | None
    elev_ft: int | None
    iso_country: str
    municipality: str | None
    icao_code: str | None


CSV_FIELD_NAMES = [
    "id",
    "ident",
    "type",
    "name",
    "latitude_deg",
    "longitude_deg",
    "elevation_ft",
    "continent",
    "iso_country",
    "iso_region",
    "municipality",
    "scheduled_service",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
    "home_link",
    "wikipedia_link",
    "keywords",
]


def _parse_row(row: dict[str, str]) -> AirportRow | None:
    icao = (row.get("icao_code") or "").strip().upper()
    ident = (row.get("ident") or "").strip().upper()
    if not icao:
        # Some airports have the ICAO code in the ident column but not in
        # icao_code.  A valid ICAO is always exactly 4 alpha characters.
        if len(ident) == 4 and ident.isalpha():
            icao = ident
        else:
            return None

    lat_raw = row.get("latitude_deg")
    lon_raw = row.get("longitude_deg")
    elev_raw = row.get("elevation_ft")

    lat: float | None = None
    lon: float | None = None
    elev: int | None = None

    if lat_raw:
        try:
            lat = float(lat_raw)
        except ValueError:
            pass
    if lon_raw:
        try:
            lon = float(lon_raw)
        except ValueError:
            pass
    if elev_raw:
        try:
            elev = int(round(float(elev_raw)))
        except (ValueError, OverflowError):
            pass

    return AirportRow(
        ident=(row.get("ident") or "").strip(),
        type=(row.get("type") or "").strip(),
        name=(row.get("name") or "").strip(),
        lat=lat,
        lon=lon,
        elev_ft=elev,
        iso_country=(row.get("iso_country") or "").strip(),
        municipality=(row.get("municipality") or "").strip() or None,
        icao_code=icao,
    )


def _iter_rows(path: Path) -> Iterator[AirportRow]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=CSV_FIELD_NAMES)
        next(reader, None)  # skip header
        for raw in reader:
            row = _parse_row(raw)
            if row is not None:
                yield row


class AirportCsvIndex:
    """In-memory index built from app/docs/airports.csv.

    Only entries with a non-empty icao_code column are indexed.
    Thread-safe via an asyncio.Lock for the initial load.
    """

    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._lock = Lock()
        self._index: dict[str, AirportRow] = {}
        self._loaded = False

    async def ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            self._load()

    def _load(self) -> None:
        count = 0
        self._index.clear()
        for row in _iter_rows(self._csv_path):
            if row.icao_code:
                self._index[row.icao_code] = row
                count += 1
        self._loaded = True
        logger.info(
            "Loaded %d airports from %s",
            count,
            self._csv_path,
        )

    def reload(self) -> None:
        """Force re-load after the CSV file has been replaced."""
        self._loaded = False

    def lookup(self, icao: str) -> AirportRow | None:
        if not self._loaded:
            raise RuntimeError("AirportCsvIndex not loaded — call ensure_loaded() first")
        return self._index.get(icao.strip().upper())

    def lookup_batch(self, icaos: list[str]) -> dict[str, AirportRow | None]:
        return {icao: self.lookup(icao) for icao in icaos}

    @property
    def size(self) -> int:
        return len(self._index)

    @property
    def csv_path(self) -> Path:
        return self._csv_path


# ---------------------------------------------------------------------------
# Global singleton  (used by both geo_service and main.py)
# ---------------------------------------------------------------------------

_global_index: AirportCsvIndex | None = None
_global_index_path: Path | None = None


def get_global_index(csv_path: Path | None = None) -> AirportCsvIndex:
    """Return the process-wide AirportCsvIndex singleton.

    The index is created on first call (lazy).  *csv_path* is only used on the
    very first call; subsequent calls ignore it.
    """
    global _global_index, _global_index_path
    if _global_index is None:
        if csv_path is None:
            csv_path = (
                Path(__file__).resolve().parent.parent.parent / "docs" / "airports.csv"
            )
        _global_index_path = csv_path
        _global_index = AirportCsvIndex(csv_path)
    return _global_index


def reload_global_index() -> AirportCsvIndex:
    """Reload the global index (e.g. after the CSV file was replaced)."""
    idx = get_global_index()
    idx.reload()
    return idx
