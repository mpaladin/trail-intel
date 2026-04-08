from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class Bounds:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


@dataclass(frozen=True)
class RoutePoint:
    lat: float
    lon: float
    elevation_m: float | None
    distance_m: float


@dataclass(frozen=True)
class RouteData:
    points: list[RoutePoint]
    total_distance_m: float
    total_ascent_m: float
    bounds: Bounds


@dataclass(frozen=True)
class SamplePoint:
    index: int
    fraction: float
    elapsed: timedelta
    timestamp: datetime
    lat: float
    lon: float
    elevation_m: float | None
    distance_m: float


@dataclass(frozen=True)
class HourlyForecast:
    times: list[datetime]
    temperature_c: list[float]
    apparent_temperature_c: list[float | None]
    wind_kph: list[float]
    wind_gust_kph: list[float | None]
    wind_direction_deg: list[float]
    cloud_cover_pct: list[float]
    precipitation_mm: list[float]
    precipitation_probability: list[float | None]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class SampleForecast:
    sample: SamplePoint
    temperature_c: float
    apparent_temperature_c: float | None
    wind_kph: float
    wind_gust_kph: float | None
    wind_direction_deg: float
    cloud_cover_pct: float
    precipitation_mm: float
    precipitation_probability: float | None


@dataclass(frozen=True)
class ForecastReport:
    provider_id: str
    route: RouteData
    samples: list[SampleForecast]
    start_time: datetime
    end_time: datetime
    duration: timedelta
    source_label: str
    notes: tuple[str, ...] = ()
