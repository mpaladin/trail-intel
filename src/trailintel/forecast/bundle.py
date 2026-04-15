from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

import httpx

from trailintel.forecast.engine import (
    ForecastSummary,
    SkippedComparisonProvider,
    build_reports_with_metadata,
    summarize_report,
)
from trailintel.forecast.errors import InputValidationError
from trailintel.forecast.models import ForecastReport
from trailintel.forecast.render import render_report
from trailintel.forecast.site import build_forecast_snapshot, export_forecast_site


@dataclass(frozen=True)
class ForecastBundleResult:
    report: ForecastReport
    summary: ForecastSummary
    image_path: Path
    site_dir: Path | None
    snapshot: dict[str, object] | None
    comparison_reports: tuple[ForecastReport, ...] = ()
    comparison_warnings: tuple[str, ...] = ()


def _default_title(gpx_path: str | Path) -> str:
    stem = Path(gpx_path).stem
    cleaned = re.sub(r"[_-]+", " ", stem).strip()
    return cleaned or "Route Forecast"


def resolve_forecast_title(gpx_path: str | Path, title: str | None) -> str:
    explicit = (title or "").strip()
    return explicit or _default_title(gpx_path)


def generate_forecast_assets(
    *,
    gpx_path: str | Path,
    start: str,
    duration: str,
    output_path: str | Path,
    site_dir: str | Path | None = None,
    title: str | None = None,
    timezone_name: str | None = None,
    sample_minutes: int = 10,
    http_client: httpx.Client | None = None,
    provider: str = "open-meteo",
    compare_providers: Sequence[str] = (),
    weatherapi_key: str | None = None,
    now: datetime | None = None,
    generated_at: datetime | None = None,
) -> ForecastBundleResult:
    if compare_providers and site_dir is None:
        raise InputValidationError(
            "--site-dir is required when using --compare-provider."
        )

    resolved_title = resolve_forecast_title(gpx_path, title)
    build_result = build_reports_with_metadata(
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
    )
    reports = build_result.reports
    report = reports[0]
    comparison_reports = tuple(reports[1:])
    image_path = render_report(report, output_path, title=resolved_title)
    summary = summarize_report(report)
    comparison_warnings = tuple(
        format_comparison_warning(item) for item in build_result.skipped_comparisons
    )

    exported_site_dir: Path | None = None
    snapshot: dict[str, object] | None = None
    if site_dir is not None:
        snapshot = build_forecast_snapshot(
            title=resolved_title,
            report=report,
            summary=summary,
            comparison_reports=comparison_reports,
            comparison_warnings=comparison_warnings,
            generated_at=generated_at or datetime.now(UTC),
        )
        exported_site_dir = export_forecast_site(
            snapshot=snapshot,
            image_path=image_path,
            gpx_path=gpx_path,
            destination=site_dir,
        )

    return ForecastBundleResult(
        report=report,
        summary=summary,
        image_path=Path(image_path),
        site_dir=exported_site_dir,
        snapshot=snapshot,
        comparison_reports=comparison_reports,
        comparison_warnings=comparison_warnings,
    )


def format_comparison_warning(build_warning: SkippedComparisonProvider) -> str:
    label = build_warning.label or build_warning.provider_id
    reason = build_warning.reason or "Unavailable for this run."
    return f"Skipped comparison provider {label}: {reason}"
