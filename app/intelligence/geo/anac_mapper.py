"""Map ANAC MADHEL API payloads to intelligence catalog entries."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from app.intelligence.contracts import AerodromeCatalogEntry

_ICAO_IN_PARENS_RE = re.compile(
    r"\(\s*[A-Z0-9]{2,3}\s*/\s*([A-Z]{4})\s*\)",
    re.IGNORECASE,
)


def parse_display_name(human_readable_identifier: str) -> str:
    """Extract the display name from the ANAC human-readable identifier."""
    return human_readable_identifier.split(" - ", 1)[0].strip()


def parse_icao_from_identifier(human_readable_identifier: str) -> str | None:
    """Extract a 4-letter ICAO when present as ``(LOCAL / ICAO)`` in the identifier."""
    match = _ICAO_IN_PARENS_RE.search(human_readable_identifier.upper())
    if match is None:
        return None
    return match.group(1).upper()


def parse_control_from_identifier(
    human_readable_identifier: str,
) -> Literal["CONTROLLED", "NON-CONTROLLED"]:
    """Derive control status from the human-readable identifier text."""
    text = human_readable_identifier.upper()
    if "NO CONTROLADO" in text:
        return "NON-CONTROLLED"
    if "CONTROLADO" in text:
        return "CONTROLLED"
    return "NON-CONTROLLED"


def is_helipuerto_list_item(item: dict[str, Any]) -> bool:
    """Heuristic to exclude helipuertos when only the list endpoint is available."""
    human_readable = (item.get("human_readable_identifier") or "").upper()
    if "HELIPUERTO" in human_readable:
        return True
    if "HLP CERRADO" in human_readable:
        return True
    if "[** HLP" in human_readable:
        return True
    return False


def _parse_updated_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _extract_coords_from_geom(item: dict[str, Any]) -> tuple[float | None, float | None]:
    geom = (item.get("the_geom") or {}).get("geometry") or {}
    raw = geom.get("coordinates") or []
    if len(raw) >= 2:
        return float(raw[1]), float(raw[0])
    return None, None


def _extract_coords(detail: dict[str, Any]) -> tuple[float | None, float | None]:
    metadata = detail.get("metadata") or {}
    localization = metadata.get("localization") or {}
    coords = localization.get("coordinates") or {}
    lat = coords.get("lat")
    lng = coords.get("lng")
    if lat is not None and lng is not None:
        return float(lat), float(lng)

    return _extract_coords_from_geom(detail)


def _normalize_control(value: str | None) -> Literal["CONTROLLED", "NON-CONTROLLED"] | None:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized == "CONTROLLED":
        return "CONTROLLED"
    if normalized == "NON-CONTROLLED":
        return "NON-CONTROLLED"
    return None


def _is_active(detail: dict[str, Any]) -> bool:
    metadata = detail.get("metadata") or {}
    status = (metadata.get("status") or "").strip().upper()
    identifier = (detail.get("human_readable_identifier") or "").upper()
    if "CLSD" in identifier:
        return False
    return status in {"", "OK"}


def map_list_item_to_entry(item: dict[str, Any]) -> AerodromeCatalogEntry | None:
    """Map a MADHEL list item to a catalog entry, or None if excluded/unparseable."""
    if is_helipuerto_list_item(item):
        return None

    local_identifier = str(item.get("local_identifier") or "").strip().upper()
    if not local_identifier:
        return None

    human_readable = item.get("human_readable_identifier") or ""
    control_status = parse_control_from_identifier(human_readable)

    lat, lon = _extract_coords_from_geom(item)
    if lat is None or lon is None:
        return None

    return AerodromeCatalogEntry(
        local_identifier=local_identifier,
        icao_code=parse_icao_from_identifier(human_readable),
        name=parse_display_name(human_readable),
        latitude=lat,
        longitude=lon,
        is_controlled=control_status == "CONTROLLED",
        control_status=control_status,
        is_active=True,
        source_updated_at=_parse_updated_at(item.get("updated_at")),
        anac_uri=item.get("uri"),
    )


def map_detail_to_entry(detail: dict[str, Any]) -> AerodromeCatalogEntry | None:
    """Map a MADHEL detail payload to a catalog entry, or None if not an aerodrome."""
    if (detail.get("type") or "").strip().upper() != "AD":
        return None

    metadata = detail.get("metadata") or {}
    identifiers = metadata.get("identifiers") or {}
    localization = metadata.get("localization") or {}

    control_status = _normalize_control(metadata.get("control"))
    if control_status is None:
        return None

    lat, lon = _extract_coords(detail)
    if lat is None or lon is None:
        return None

    human_readable = detail.get("human_readable_identifier") or ""
    icao = identifiers.get("icao")
    icao_code = icao.strip().upper() if isinstance(icao, str) and icao.strip() else None

    iata = identifiers.get("iata")
    iata_code = iata.strip().upper() if isinstance(iata, str) and iata.strip() else None

    local = identifiers.get("local") or detail.get("local_identifier") or ""
    local_identifier = str(local).strip().upper()
    if not local_identifier:
        return None

    elevation = localization.get("elevation")
    elevation_m = float(elevation) if elevation is not None else None

    return AerodromeCatalogEntry(
        local_identifier=local_identifier,
        icao_code=icao_code,
        iata_code=iata_code,
        name=parse_display_name(human_readable),
        latitude=lat,
        longitude=lon,
        elevation_m=elevation_m,
        is_controlled=control_status == "CONTROLLED",
        control_status=control_status,
        is_active=_is_active(detail),
        traffic_type=metadata.get("traffic"),
        flight_rules=None,
        category=None,
        condition=metadata.get("condition"),
        fir=localization.get("fir"),
        state=localization.get("state"),
        sna=metadata.get("sna"),
        ansp=metadata.get("ansp"),
        source_updated_at=_parse_updated_at(detail.get("updated_at")),
        anac_uri=detail.get("uri"),
    )
