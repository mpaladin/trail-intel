from __future__ import annotations

import math
from bisect import bisect_left
from datetime import datetime, timedelta
from pathlib import Path

import gpxpy

from trailintel.forecast.errors import GPXParseError, InputValidationError
from trailintel.forecast.models import Bounds, RouteData, RoutePoint, SamplePoint

EARTH_RADIUS_M = 6_371_000


def parse_gpx(path: str | Path) -> RouteData:
    gpx_path = Path(path)
    if not gpx_path.exists():
        raise GPXParseError(f"GPX file not found: {gpx_path}")

    try:
        content = gpx_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise GPXParseError(f"Could not read GPX file: {gpx_path}") from exc

    try:
        gpx = gpxpy.parse(content)
    except Exception as exc:  # pragma: no cover - gpxpy raises mixed types
        raise GPXParseError(f"Could not parse GPX file: {gpx_path}") from exc

    raw_points: list[tuple[float, float, float | None]] = []

    if gpx.tracks:
        for segment in gpx.tracks[0].segments:
            for point in segment.points:
                raw_points.append((point.latitude, point.longitude, point.elevation))
    elif gpx.routes:
        for point in gpx.routes[0].points:
            raw_points.append((point.latitude, point.longitude, point.elevation))
    else:
        raise GPXParseError("GPX file has no tracks or routes.")

    if len(raw_points) < 2:
        raise GPXParseError("GPX file must contain at least two points.")

    route_points: list[RoutePoint] = []
    total_distance = 0.0
    total_ascent = 0.0
    prev_lat, prev_lon, prev_ele = raw_points[0]

    min_lat = max_lat = prev_lat
    min_lon = max_lon = prev_lon
    route_points.append(
        RoutePoint(
            lat=prev_lat,
            lon=prev_lon,
            elevation_m=prev_ele,
            distance_m=0.0,
        )
    )

    for lat, lon, ele in raw_points[1:]:
        segment_distance = haversine_m(prev_lat, prev_lon, lat, lon)
        total_distance += segment_distance
        if prev_ele is not None and ele is not None and ele > prev_ele:
            total_ascent += ele - prev_ele

        route_points.append(
            RoutePoint(
                lat=lat,
                lon=lon,
                elevation_m=ele,
                distance_m=total_distance,
            )
        )

        min_lat = min(min_lat, lat)
        max_lat = max(max_lat, lat)
        min_lon = min(min_lon, lon)
        max_lon = max(max_lon, lon)
        prev_lat, prev_lon, prev_ele = lat, lon, ele

    if total_distance <= 0:
        raise GPXParseError("GPX route distance is zero.")

    return RouteData(
        points=route_points,
        total_distance_m=total_distance,
        total_ascent_m=total_ascent,
        bounds=Bounds(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ),
    )


def sample_route(
    route: RouteData,
    start_time: datetime,
    duration: timedelta,
    sample_minutes: int = 10,
) -> list[SamplePoint]:
    if sample_minutes <= 0:
        raise InputValidationError("--sample-minutes must be greater than zero.")

    duration_minutes = duration.total_seconds() / 60
    requested_steps = math.ceil(duration_minutes / sample_minutes)
    sample_count = max(15, min(120, requested_steps + 1))

    return [
        interpolate_sample(
            route=route,
            fraction=(index / (sample_count - 1)) if sample_count > 1 else 0.0,
            index=index,
            start_time=start_time,
            duration=duration,
        )
        for index in range(sample_count)
    ]


def interpolate_sample(
    *,
    route: RouteData,
    fraction: float,
    index: int,
    start_time: datetime,
    duration: timedelta,
) -> SamplePoint:
    target_distance = route.total_distance_m * fraction
    point = interpolate_route_point(route, target_distance)
    elapsed = duration * fraction
    return SamplePoint(
        index=index,
        fraction=fraction,
        elapsed=elapsed,
        timestamp=start_time + elapsed,
        lat=point.lat,
        lon=point.lon,
        elevation_m=point.elevation_m,
        distance_m=target_distance,
    )


def interpolate_route_point(route: RouteData, target_distance_m: float) -> RoutePoint:
    distances = [point.distance_m for point in route.points]
    index = bisect_left(distances, target_distance_m)

    if index <= 0:
        return route.points[0]
    if index >= len(route.points):
        return route.points[-1]

    left = route.points[index - 1]
    right = route.points[index]
    span = right.distance_m - left.distance_m
    if span <= 0:
        return right

    ratio = (target_distance_m - left.distance_m) / span
    elevation = interpolate_optional(left.elevation_m, right.elevation_m, ratio)
    return RoutePoint(
        lat=lerp(left.lat, right.lat, ratio),
        lon=lerp(left.lon, right.lon, ratio),
        elevation_m=elevation,
        distance_m=target_distance_m,
    )


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_M * c


def lerp(start: float, end: float, ratio: float) -> float:
    return start + (end - start) * ratio


def interpolate_optional(
    start: float | None,
    end: float | None,
    ratio: float,
) -> float | None:
    if start is None or end is None:
        return end if ratio >= 0.5 else start
    return lerp(start, end, ratio)
