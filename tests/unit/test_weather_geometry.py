from app.services.weather.geometry import feature_contains_point


def test_feature_contains_point_inside_polygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-34.5, lon=-58.5) is True


def test_feature_contains_point_outside_polygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-33.0, lon=-58.5) is False


def test_feature_contains_point_inside_multipolygon():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-10.0, -10.0], [-9.0, -10.0], [-9.0, -9.0], [-10.0, -9.0], [-10.0, -10.0]]],
                [[[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]]],
            ],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-34.5, lon=-58.5) is True


def test_feature_contains_point_in_hole_is_outside():
    """Point inside a polygon hole should be considered outside the feature."""
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-59.0, -35.0], [-58.0, -35.0], [-58.0, -34.0], [-59.0, -34.0], [-59.0, -35.0]],  # outer ring
                [[-58.8, -34.8], [-58.2, -34.8], [-58.2, -34.2], [-58.8, -34.2], [-58.8, -34.8]],  # hole
            ],
        },
        "properties": {},
    }

    assert feature_contains_point(feature, lat=-34.5, lon=-58.5) is False
