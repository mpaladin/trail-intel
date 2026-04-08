from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import httpx

from trailintel.forecast.align import align_forecasts
from trailintel.forecast.errors import InputValidationError
from trailintel.forecast.gpx_route import parse_gpx, sample_route
from trailintel.forecast.models import ForecastReport, SampleForecast
from trailintel.forecast.time_utils import (
    parse_duration,
    parse_start_time,
    validate_forecast_window,
)
from trailintel.forecast.weather import create_forecast_client, provider_definition


@dataclass(frozen=True)
class ForecastSummary:
    temperature_min_c: float
    temperature_max_c: float
    wind_max_kph: float
    precipitation_total_mm: float
    wettest_time: datetime
    wettest_precipitation_mm: float
    wettest_probability_pct: float | None


def build_report(
    *,
    gpx_path: str | Path,
    start: str,
    duration: str,
    timezone_name: str | None = None,
    sample_minutes: int = 10,
    http_client: httpx.Client | None = None,
    provider: str = "open-meteo",
    weatherapi_key: str | None = None,
    now: datetime | None = None,
) -> ForecastReport:
    reports = build_reports(
        gpx_path=gpx_path,
        start=start,
        duration=duration,
        timezone_name=timezone_name,
        sample_minutes=sample_minutes,
        http_client=http_client,
        provider=provider,
        weatherapi_key=weatherapi_key,
        now=now,
    )
    return reports[0]


def build_reports(
    *,
    gpx_path: str | Path,
    start: str,
    duration: str,
    timezone_name: str | None = None,
    sample_minutes: int = 10,
    http_client: httpx.Client | None = None,
    provider: str = "open-meteo",
    compare_providers: Sequence[str] = (),
    weatherapi_key: str | None = None,
    now: datetime | None = None,
) -> list[ForecastReport]:
    start_time = parse_start_time(start, timezone_name)
    duration_delta = parse_duration(duration)
    provider_ids = normalize_provider_ids(provider, compare_providers)
    horizon_days = min(
        provider_definition(provider_id).horizon_days for provider_id in provider_ids
    )
    validate_forecast_window(
        start_time,
        duration_delta,
        now=now,
        horizon_days=horizon_days,
    )

    route = parse_gpx(gpx_path)
    samples = sample_route(route, start_time, duration_delta, sample_minutes)

    reports: list[ForecastReport] = []
    for provider_id in provider_ids:
        definition = provider_definition(provider_id)
        client = create_forecast_client(
            provider_id,
            http_client=http_client,
            weatherapi_key=weatherapi_key,
        )
        try:
            forecasts = client.fetch_hourly(samples)
        finally:
            client.close()

        aligned = align_forecasts(samples, forecasts)
        notes = tuple(
            dict.fromkeys(note for forecast in forecasts for note in forecast.notes)
        )
        reports.append(
            ForecastReport(
                provider_id=definition.provider_id,
                route=route,
                samples=aligned,
                start_time=start_time,
                end_time=start_time + duration_delta,
                duration=duration_delta,
                source_label=definition.source_label,
                notes=notes,
            )
        )
    return reports


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
            -(
                sample.precipitation_probability
                if sample.precipitation_probability is not None
                else -1.0
            ),
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


def normalize_provider_ids(
    provider: str,
    compare_providers: Sequence[str],
) -> list[str]:
    primary = provider_definition(provider).provider_id
    resolved = [primary]
    seen = {primary}
    for compare_provider in compare_providers:
        normalized = provider_definition(compare_provider).provider_id
        if normalized == primary:
            raise InputValidationError(
                f"--compare-provider cannot repeat the primary provider '{primary}'."
            )
        if normalized in seen:
            raise InputValidationError("--compare-provider values must be unique.")
        resolved.append(normalized)
        seen.add(normalized)
    return resolved
