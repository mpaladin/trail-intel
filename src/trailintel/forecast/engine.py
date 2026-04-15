from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence

import httpx

from trailintel.forecast.align import align_forecasts
from trailintel.forecast.errors import InputValidationError, WeatherAPIError
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


@dataclass(frozen=True)
class SkippedComparisonProvider:
    provider_id: str
    label: str
    source_label: str
    reason: str


@dataclass(frozen=True)
class ForecastBuildResult:
    reports: list[ForecastReport]
    skipped_comparisons: tuple[SkippedComparisonProvider, ...] = ()


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
    result = build_reports_with_metadata(
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
    return result.reports[0]


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
    return build_reports_with_metadata(
        gpx_path=gpx_path,
        start=start,
        duration=duration,
        timezone_name=timezone_name,
        sample_minutes=sample_minutes,
        http_client=http_client,
        provider=provider,
        compare_providers=compare_providers,
        weatherapi_key=weatherapi_key,
        now=now,
    ).reports


def build_reports_with_metadata(
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
) -> ForecastBuildResult:
    start_time = parse_start_time(start, timezone_name)
    duration_delta = parse_duration(duration)
    provider_ids = normalize_provider_ids(provider, compare_providers)
    primary_provider = provider_ids[0]
    horizon_days = provider_definition(primary_provider).horizon_days
    validate_forecast_window(
        start_time,
        duration_delta,
        now=now,
        horizon_days=horizon_days,
    )
    active_compare_providers, skipped_comparisons = resolve_comparison_providers(
        provider_ids[1:],
        start_time=start_time,
        duration=duration_delta,
        now=now,
    )

    route = parse_gpx(gpx_path)
    samples = sample_route(route, start_time, duration_delta, sample_minutes)

    reports = [
        build_provider_report(
            provider_id=primary_provider,
            route=route,
            samples=samples,
            start_time=start_time,
            duration=duration_delta,
            http_client=http_client,
            weatherapi_key=weatherapi_key,
        )
    ]
    skipped = list(skipped_comparisons)
    for provider_id in active_compare_providers:
        definition = provider_definition(provider_id)
        try:
            report = build_provider_report(
                provider_id=provider_id,
                route=route,
                samples=samples,
                start_time=start_time,
                duration=duration_delta,
                http_client=http_client,
                weatherapi_key=weatherapi_key,
            )
        except (InputValidationError, WeatherAPIError) as exc:
            skipped.append(
                SkippedComparisonProvider(
                    provider_id=definition.provider_id,
                    label=definition.label,
                    source_label=definition.source_label,
                    reason=str(exc),
                )
            )
            continue
        reports.append(report)
    return ForecastBuildResult(
        reports=reports,
        skipped_comparisons=tuple(skipped),
    )


def resolve_comparison_providers(
    provider_ids: Sequence[str],
    *,
    start_time: datetime,
    duration: timedelta,
    now: datetime | None = None,
) -> tuple[list[str], tuple[SkippedComparisonProvider, ...]]:
    now_utc = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    ride_end_utc = (start_time + duration).astimezone(UTC)

    active: list[str] = []
    skipped: list[SkippedComparisonProvider] = []
    for provider_id in provider_ids:
        definition = provider_definition(provider_id)
        horizon_utc = now_utc + timedelta(days=definition.horizon_days)
        if ride_end_utc > horizon_utc:
            skipped.append(
                SkippedComparisonProvider(
                    provider_id=definition.provider_id,
                    label=definition.label,
                    source_label=definition.source_label,
                    reason=(
                        "Ride end exceeds this provider's "
                        f"{definition.horizon_days}-day forecast horizon."
                    ),
                )
            )
            continue
        active.append(definition.provider_id)

    return active, tuple(skipped)


def build_provider_report(
    *,
    provider_id: str,
    route,
    samples,
    start_time: datetime,
    duration: timedelta,
    http_client: httpx.Client | None = None,
    weatherapi_key: str | None = None,
) -> ForecastReport:
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
    notes = tuple(dict.fromkeys(note for forecast in forecasts for note in forecast.notes))
    return ForecastReport(
        provider_id=definition.provider_id,
        route=route,
        samples=aligned,
        start_time=start_time,
        end_time=start_time + duration,
        duration=duration,
        source_label=definition.source_label,
        notes=notes,
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
