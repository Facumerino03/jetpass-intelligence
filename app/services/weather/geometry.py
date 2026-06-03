from __future__ import annotations

from typing import Any


def _ring_contains_point(ring: list[list[float]], *, lat: float, lon: float) -> bool:
    inside = False
    point_x = lon
    point_y = lat
    count = len(ring)
    if count < 4:
        return False

    previous_x, previous_y = ring[-1][0], ring[-1][1]
    for coordinate in ring:
        current_x, current_y = coordinate[0], coordinate[1]
        crosses = (current_y > point_y) != (previous_y > point_y)
        if crosses:
            slope_x = (previous_x - current_x) * (point_y - current_y) / (previous_y - current_y) + current_x
            if point_x < slope_x:
                inside = not inside
        previous_x, previous_y = current_x, current_y
    return inside


def _polygon_contains_point(polygon: list[list[list[float]]], *, lat: float, lon: float) -> bool:
    if not polygon:
        return False
    outer_ring = polygon[0]
    if not _ring_contains_point(outer_ring, lat=lat, lon=lon):
        return False
    for hole in polygon[1:]:
        if _ring_contains_point(hole, lat=lat, lon=lon):
            return False
    return True


def feature_contains_point(feature: dict[str, Any], *, lat: float, lon: float) -> bool:
    geometry = feature.get("geometry") or {}
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []

    if geometry_type == "Polygon":
        return _polygon_contains_point(coordinates, lat=lat, lon=lon)
    if geometry_type == "MultiPolygon":
        return any(_polygon_contains_point(polygon, lat=lat, lon=lon) for polygon in coordinates)
    return False
