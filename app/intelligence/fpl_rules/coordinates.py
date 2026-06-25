"""OACI coordinate formatting for ICAO FPL Field 18 (DEP/, DEST/, ALTN/)."""

from __future__ import annotations


def _format_latitude(lat: float) -> str:
    lat_abs = abs(lat)
    degrees = int(lat_abs)
    minutes = round((lat_abs - degrees) * 60)
    if minutes == 60:
        degrees += 1
        minutes = 0
    hemisphere = "N" if lat >= 0 else "S"
    return f"{degrees:02d}{minutes:02d}{hemisphere}"


def _format_longitude(lon: float) -> str:
    lon_abs = abs(lon)
    degrees = int(lon_abs)
    minutes = round((lon_abs - degrees) * 60)
    if minutes == 60:
        degrees += 1
        minutes = 0
    hemisphere = "E" if lon >= 0 else "W"
    return f"{degrees:03d}{minutes:02d}{hemisphere}"


def format_oaci_coordinates(latitude: float, longitude: float) -> str:
    """Format decimal degrees as an 11-character OACI position (e.g. ``4620S06630W``)."""
    return f"{_format_latitude(latitude)}{_format_longitude(longitude)}"
