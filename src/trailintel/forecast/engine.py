from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx

from trailintel.forecast.align import align_forecasts
from trailintel.forecast.gpx_route import parse_gpx, sample_route
from trailintel.forecast.models import ForecastReport, SampleForecast
from trailintel.forecast.time_utils import (
    parse_duration,
    parse_start_time,
    validate_forecast_window,
)
from trailintel.forecast.weather import OpenMeteoClient

SOURCE_LABEL = "Open-Meteo Forecast API"


@dataclass(frozen=True)
class ForecastSummary:
    temperature_min_c: float
    temperature_max_c: float
    wind_max_kph: float
    precipitation_total_mm: float
    wettest_time: datetime
    wettest_precipitation_mm: float
    wettest_probability_pct: float


def build_report(
    *,
    gpx_path: str | Path,
    start: str,
    duration: str,
    timezone_name: str | None = None,
    sample_minutes: int = 10,
    http_client: httpx.Client | None = None,
    now: datetime | None = None,
) -> ForecastReport:
    start_time = parse_start_time(start, timezone_name)
    duration_delta = parse_duration(duration)
    validate_forecast_window(start_time, duration_delta, now=now)

    route = parse_gpx(gpx_path)
    samples = sample_route(route, start_time, duration_delta, sample_minutes)

    client = OpenMeteoClient(http_client=http_client)
    try:
        forecasts = client.fetch_hourly(samples)
    finally:
        client.close()

    aligned = align_forecasts(samples, forecasts)
    return ForecastReport(
        route=route,
        samples=aligned,
        start_time=start_time,
        end_time=start_time + duration_delta,
        duration=duration_delta,
        source_label=SOURCE_LABEL,
    )


def summarize_report(report: ForecastReport) -> ForecastSummary:
    if not report.samples:
        raise ValueError("Forecast report has no samples to summarize.")

    temperatures = [sample.temperature_c for sample in report.samples]
    winds = [sample.wind_kph for sample in report.samples]
    precipitation_total = integrate_precipitation(report.samples)
    wettest_sample = select_wettest_sample(report.samples)

    return ForecastSummary(
        temperature_min_c=min(temperatures),
        temperature_max_c=max(temperatures),
        wind_max_kph=max(winds),
        precipitation_total_mm=precipitation_total,
        wettest_time=wettest_sample.sample.timestamp,
        wettest_precipitation_mm=wettest_sample.precipitation_mm,
        wettest_probability_pct=wettest_sample.precipitation_probability,
    )


def select_wettest_sample(samples: list[SampleForecast]) -> SampleForecast:
    if not samples:
        raise ValueError("Forecast sample list is empty.")
    return min(
        samples,
        key=lambda sample: (
            -sample.precipitation_mm,
            -sample.precipitation_probability,
            sample.sample.timestamp,
        ),
    )


def integrate_precipitation(samples: list[SampleForecast]) -> float:
    total_mm = 0.0
    for current, following in zip(samples, samples[1:], strict=False):
        hours = (
            following.sample.timestamp - current.sample.timestamp
        ).total_seconds() / 3600
        total_mm += current.precipitation_mm * hours
    return total_mm
