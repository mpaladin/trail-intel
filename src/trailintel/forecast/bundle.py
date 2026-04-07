from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from trailintel.forecast.engine import ForecastSummary, build_report, summarize_report
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
    now: datetime | None = None,
    generated_at: datetime | None = None,
) -> ForecastBundleResult:
    resolved_title = resolve_forecast_title(gpx_path, title)
    report = build_report(
        gpx_path=gpx_path,
        start=start,
        duration=duration,
        timezone_name=timezone_name,
        sample_minutes=sample_minutes,
        http_client=http_client,
        now=now,
    )
    image_path = render_report(report, output_path, title=resolved_title)
    summary = summarize_report(report)

    exported_site_dir: Path | None = None
    snapshot: dict[str, object] | None = None
    if site_dir is not None:
        snapshot = build_forecast_snapshot(
            title=resolved_title,
            report=report,
            summary=summary,
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
    )
