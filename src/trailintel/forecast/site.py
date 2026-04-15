from __future__ import annotations

import html
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from trailintel.forecast.engine import select_wettest_sample, summarize_report
from trailintel.forecast.weather import provider_definition
from trailintel.github_pipeline import normalize_slug_text
from trailintel.site import (
    FORECAST_REPORT_KIND,
    REPORT_HTML_FILENAME,
    REPORT_META_FILENAME,
    _format_compact_timestamp,
    _format_display_datetime,
    _format_duration_label,
    _render_action_link,
    _render_document,
    _render_meta_row,
    _render_metric_cards,
    copy_bundle_to_targets,
)

if TYPE_CHECKING:
    from trailintel.forecast.engine import ForecastSummary
    from trailintel.forecast.models import ForecastReport

FORECAST_PNG_FILENAME = "forecast.png"
FORECAST_GPX_FILENAME = "route.gpx"
FORECAST_JSON_FILENAME = "snapshot.json"
FORECAST_SECTION_DIR = "forecasts"


def _format_duration(value: timedelta) -> str:
    total_seconds = int(value.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if seconds:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}"


def _timezone_label(value: datetime) -> str:
    tz = value.tzinfo
    if tz is None:
        return "UTC"
    return getattr(tz, "key", None) or value.tzname() or "UTC"


def _round_optional(value: float | None, digits: int) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _display_cell(value: object) -> str:
    if value is None or value == "":
        return "n/a"
    return str(value)


def _format_probability_label(value: object) -> str:
    if value is None or value == "":
        return "chance unavailable"
    return f"{float(value):.0f}% chance"


def _optional_metric_label(value: float | None, unit: str, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{unit}"


def _build_sample_rows(report: ForecastReport) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for sample in report.samples:
        rows.append(
            {
                "index": sample.sample.index,
                "timestamp": sample.sample.timestamp.isoformat(),
                "distance_km": round(sample.sample.distance_m / 1000.0, 2),
                "elevation_m": sample.sample.elevation_m,
                "temperature_c": round(sample.temperature_c, 1),
                "apparent_temperature_c": _round_optional(
                    sample.apparent_temperature_c, 1
                ),
                "wind_kph": round(sample.wind_kph, 1),
                "wind_gust_kph": _round_optional(sample.wind_gust_kph, 1),
                "wind_direction_deg": round(sample.wind_direction_deg, 1),
                "cloud_cover_pct": round(sample.cloud_cover_pct, 1),
                "precipitation_mm": round(sample.precipitation_mm, 2),
                "precipitation_probability": _round_optional(
                    sample.precipitation_probability, 1
                ),
            }
        )
    return rows


def _build_key_moments(report: ForecastReport) -> list[dict[str, object]]:
    if not report.samples:
        return []

    coldest = min(
        report.samples,
        key=lambda sample: (sample.temperature_c, sample.sample.timestamp),
    )
    windiest = min(
        report.samples,
        key=lambda sample: (-sample.wind_kph, sample.sample.timestamp),
    )
    key_samples = [
        ("start", "Start", report.samples[0]),
        ("coldest", "Coldest", coldest),
        ("windiest", "Windiest", windiest),
        ("wettest", "Wettest", select_wettest_sample(report.samples)),
        ("finish", "Finish", report.samples[-1]),
    ]
    return [
        {
            "kind": kind,
            "label": label,
            "timestamp": sample.sample.timestamp.isoformat(),
            "distance_km": round(sample.sample.distance_m / 1000.0, 2),
            "temperature_c": round(sample.temperature_c, 1),
            "wind_kph": round(sample.wind_kph, 1),
            "precipitation_mm": round(sample.precipitation_mm, 2),
            "precipitation_probability": _round_optional(
                sample.precipitation_probability, 1
            ),
        }
        for kind, label, sample in key_samples
    ]


def build_forecast_snapshot(
    *,
    title: str,
    report: ForecastReport,
    summary: ForecastSummary,
    comparison_reports: Sequence[ForecastReport] = (),
    comparison_warnings: Sequence[str] = (),
    generated_at: datetime | None = None,
) -> dict[str, object]:
    stamp = (generated_at or datetime.now(UTC)).astimezone(UTC)
    snapshot = {
        "report_kind": FORECAST_REPORT_KIND,
        "title": title,
        "generated_at": stamp.isoformat(),
        "start_time": report.start_time.isoformat(),
        "end_time": report.end_time.isoformat(),
        "timezone": _timezone_label(report.start_time),
        "duration": _format_duration(report.duration),
        "route_distance_km": round(report.route.total_distance_m / 1000.0, 2),
        "route_ascent_m": round(report.route.total_ascent_m, 1),
        "sample_count": len(report.samples),
        "source_label": report.source_label,
        "summary": {
            "temperature_min_c": round(summary.temperature_min_c, 1),
            "temperature_max_c": round(summary.temperature_max_c, 1),
            "wind_max_kph": round(summary.wind_max_kph, 1),
            "precipitation_total_mm": round(summary.precipitation_total_mm, 1),
            "wettest_time": summary.wettest_time.isoformat(),
            "wettest_precipitation_mm": round(summary.wettest_precipitation_mm, 2),
            "wettest_probability_pct": _round_optional(
                summary.wettest_probability_pct, 1
            ),
        },
        "key_moments": _build_key_moments(report),
        "sample_rows": _build_sample_rows(report),
    }
    if comparison_reports or comparison_warnings:
        snapshot["comparison"] = _build_comparison_snapshot(
            report,
            comparison_reports=comparison_reports,
            comparison_warnings=comparison_warnings,
        )
    return snapshot


def _build_comparison_snapshot(
    report: ForecastReport,
    *,
    comparison_reports: Sequence[ForecastReport],
    comparison_warnings: Sequence[str] = (),
) -> dict[str, object]:
    reports = [report, *comparison_reports]
    snapshot = {
        "primary_provider": report.provider_id,
        "providers": [_build_comparison_provider_entry(item) for item in reports],
    }
    if comparison_warnings:
        snapshot["warnings"] = list(comparison_warnings)
    return snapshot


def _build_comparison_provider_entry(report: ForecastReport) -> dict[str, object]:
    summary = summarize_report(report)
    coverage = {
        "has_apparent_temperature": any(
            sample.apparent_temperature_c is not None for sample in report.samples
        ),
        "has_wind_gust": any(
            sample.wind_gust_kph is not None for sample in report.samples
        ),
        "has_precipitation_probability": any(
            sample.precipitation_probability is not None for sample in report.samples
        ),
    }
    definition = provider_definition(report.provider_id)
    return {
        "provider_id": report.provider_id,
        "label": definition.label,
        "source_label": report.source_label,
        "summary": {
            "temperature_min_c": round(summary.temperature_min_c, 1),
            "temperature_max_c": round(summary.temperature_max_c, 1),
            "wind_max_kph": round(summary.wind_max_kph, 1),
            "precipitation_total_mm": round(summary.precipitation_total_mm, 1),
            "wettest_time": summary.wettest_time.isoformat(),
            "wettest_precipitation_mm": round(summary.wettest_precipitation_mm, 2),
            "wettest_probability_pct": _round_optional(
                summary.wettest_probability_pct, 1
            ),
        },
        "key_moments": _build_key_moments(report),
        "coverage": coverage,
        "notes": list(report.notes),
    }


def build_forecast_metadata(snapshot: dict[str, object]) -> dict[str, object]:
    summary = snapshot.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    return {
        "report_kind": FORECAST_REPORT_KIND,
        "title": snapshot.get("title", "Route Forecast"),
        "generated_at": snapshot.get("generated_at"),
        "start_time": snapshot.get("start_time"),
        "end_time": snapshot.get("end_time"),
        "timezone": snapshot.get("timezone", ""),
        "duration": snapshot.get("duration", ""),
        "route_distance_km": float(snapshot.get("route_distance_km", 0.0) or 0.0),
        "route_ascent_m": float(snapshot.get("route_ascent_m", 0.0) or 0.0),
        "sample_count": int(snapshot.get("sample_count", 0) or 0),
        "source_label": snapshot.get("source_label", ""),
        "temperature_min_c": summary.get("temperature_min_c"),
        "temperature_max_c": summary.get("temperature_max_c"),
        "wind_max_kph": summary.get("wind_max_kph"),
        "precipitation_total_mm": summary.get("precipitation_total_mm"),
        "wettest_time": summary.get("wettest_time"),
        "wettest_precipitation_mm": summary.get("wettest_precipitation_mm"),
        "wettest_probability_pct": summary.get("wettest_probability_pct"),
    }


def _sample_rows_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty-state">No forecast samples available.</div>'

    body_rows: list[str] = []
    for row in rows:
        timestamp = _format_compact_timestamp(
            row.get("timestamp"),
            default=str(row.get("timestamp", "")),
        )
        body_rows.append(
            "".join(
                [
                    "<tr>",
                    f'<td class="score-cell">{html.escape(timestamp)}</td>',
                    f"<td>{html.escape(_display_cell(row.get('distance_km')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('elevation_m')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('temperature_c')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('apparent_temperature_c')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('wind_kph')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('wind_gust_kph')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('wind_direction_deg')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('cloud_cover_pct')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('precipitation_mm')))}</td>",
                    f"<td>{html.escape(_display_cell(row.get('precipitation_probability')))}</td>",
                    "</tr>",
                ]
            )
        )
    return (
        '<div class="table-wrap"><table class="results-table">'
        "<thead><tr>"
        "<th>Timestamp</th><th>Distance (km)</th><th>Elevation (m)</th><th>Temp (C)</th>"
        "<th>Feels Like (C)</th><th>Wind (km/h)</th><th>Gust (km/h)</th><th>Direction</th>"
        "<th>Clouds (%)</th><th>Precip (mm)</th><th>Rain Chance (%)</th>"
        "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table></div>"
    )


def _key_moments_grid(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty-state">No forecast highlights available.</div>'

    items: list[tuple[str, str, str]] = []
    for row in rows:
        label = str(row.get("label") or row.get("kind") or "Moment")
        timestamp = _format_compact_timestamp(
            row.get("timestamp"),
            default=str(row.get("timestamp", "")),
        )
        details = " • ".join(
            [
                f"{float(row.get('distance_km', 0.0) or 0.0):.2f} km",
                f"{float(row.get('temperature_c', 0.0) or 0.0):.1f}C",
                f"{float(row.get('wind_kph', 0.0) or 0.0):.1f} km/h wind",
                f"{float(row.get('precipitation_mm', 0.0) or 0.0):.2f} mm rain",
                _format_probability_label(row.get("precipitation_probability")),
            ]
        )
        items.append((label, timestamp, details))
    return f'<div class="metric-grid">{_render_metric_cards(items)}</div>'


def _comparison_coverage_label(coverage: dict[str, object]) -> str:
    return " | ".join(
        [
            "Feels-like: yes"
            if coverage.get("has_apparent_temperature")
            else "Feels-like: no",
            "Gust: yes" if coverage.get("has_wind_gust") else "Gust: no",
            "Rain chance: yes"
            if coverage.get("has_precipitation_probability")
            else "Rain chance: no",
        ]
    )


def _comparison_summary_table(providers: list[dict[str, object]]) -> str:
    if not providers:
        return '<div class="empty-state">No provider comparison data available.</div>'

    body_rows: list[str] = []
    for provider in providers:
        summary = provider.get("summary")
        if not isinstance(summary, dict):
            summary = {}
        coverage = provider.get("coverage")
        if not isinstance(coverage, dict):
            coverage = {}
        notes = provider.get("notes")
        note_items = notes if isinstance(notes, list) else []
        wettest_label = _format_compact_timestamp(
            summary.get("wettest_time"),
            default="Unknown",
        )
        wettest_details = (
            f"{float(summary.get('wettest_precipitation_mm', 0.0) or 0.0):.2f} mm"
            f" • {_format_probability_label(summary.get('wettest_probability_pct'))}"
        )
        body_rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{html.escape(str(provider.get('label') or provider.get('provider_id') or 'Provider'))}</td>",
                    (
                        "<td>"
                        f"{float(summary.get('temperature_min_c', 0.0) or 0.0):.1f}C to "
                        f"{float(summary.get('temperature_max_c', 0.0) or 0.0):.1f}C"
                        "</td>"
                    ),
                    f"<td>{float(summary.get('wind_max_kph', 0.0) or 0.0):.1f} km/h</td>",
                    (
                        "<td>"
                        f"{float(summary.get('precipitation_total_mm', 0.0) or 0.0):.1f} mm"
                        "</td>"
                    ),
                    (
                        "<td>"
                        f"{html.escape(wettest_label)}"
                        f'<div class="card-meta">{html.escape(wettest_details)}</div>'
                        "</td>"
                    ),
                    f"<td>{html.escape(_comparison_coverage_label(coverage))}</td>",
                    (
                        "<td>"
                        f"{html.escape(' | '.join(str(item) for item in note_items) or 'n/a')}"
                        "</td>"
                    ),
                    "</tr>",
                ]
            )
        )
    return (
        '<div class="table-wrap"><table class="results-table">'
        "<thead><tr>"
        "<th>Provider</th><th>Temp Range</th><th>Max Wind</th>"
        "<th>Estimated Rain</th><th>Wettest Segment</th><th>Coverage</th><th>Notes</th>"
        "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table></div>"
    )


def _comparison_key_moment_cards(providers: list[dict[str, object]]) -> str:
    if not providers:
        return '<div class="empty-state">No provider comparison data available.</div>'

    cards: list[str] = []
    for provider in providers:
        key_moments = provider.get("key_moments")
        if not isinstance(key_moments, list):
            key_moments = []
        rows: list[str] = []
        for moment in key_moments:
            if not isinstance(moment, dict):
                continue
            timestamp = _format_compact_timestamp(
                moment.get("timestamp"),
                default=str(moment.get("timestamp", "")),
            )
            details = " • ".join(
                [
                    f"{float(moment.get('temperature_c', 0.0) or 0.0):.1f}C",
                    f"{float(moment.get('wind_kph', 0.0) or 0.0):.1f} km/h wind",
                    f"{float(moment.get('precipitation_mm', 0.0) or 0.0):.2f} mm rain",
                    _format_probability_label(moment.get("precipitation_probability")),
                ]
            )
            rows.append(
                "".join(
                    [
                        '<div class="metric-row">',
                        f"<strong>{html.escape(str(moment.get('label') or moment.get('kind') or 'Moment'))}</strong>",
                        f"<div>{html.escape(timestamp)}</div>",
                        f'<div class="card-meta">{html.escape(details)}</div>',
                        "</div>",
                    ]
                )
            )
        cards.append(
            f"""
            <article class="panel">
              <div class="panel-head">
                <h3>{html.escape(str(provider.get("label") or provider.get("provider_id") or "Provider"))}</h3>
                <span class="pill">{html.escape(str(provider.get("provider_id") or ""))}</span>
              </div>
              <div class="section-stack">
                {"".join(rows) or '<div class="empty-state">No key moments available.</div>'}
              </div>
            </article>
            """
        )
    return '<div class="section-stack">' + "".join(cards) + "</div>"


def _comparison_warning_notice(warnings: list[str]) -> str:
    if not warnings:
        return ""
    lines = "".join(
        f"<li>{html.escape(message)}</li>" for message in warnings if message.strip()
    )
    if not lines:
        return ""
    return (
        '<div class="empty-state"><strong>Skipped comparison sources</strong>'
        f"<ul>{lines}</ul></div>"
    )


def _comparison_section(snapshot: dict[str, object]) -> str:
    comparison = snapshot.get("comparison")
    if not isinstance(comparison, dict):
        return ""
    providers = comparison.get("providers")
    if not isinstance(providers, list) or not providers:
        providers = []
    typed_providers = [item for item in providers if isinstance(item, dict)]
    warnings = comparison.get("warnings")
    typed_warnings = (
        [str(item) for item in warnings if str(item).strip()]
        if isinstance(warnings, list)
        else []
    )
    available_comparisons = typed_providers if len(typed_providers) > 1 else []
    comparison_count = max(len(typed_providers) - 1, 0)
    if not available_comparisons and not typed_warnings:
        return ""
    warning_notice = _comparison_warning_notice(typed_warnings)
    if not available_comparisons:
        return f"""
      <section class="panel">
        <div class="panel-head">
          <h2>Provider Comparison</h2>
          <span class="pill">0 available</span>
        </div>
        <p class="section-caption">The selected comparison sources could not cover the requested ride window.</p>
        {warning_notice}
      </section>
    """
    return f"""
      <section class="panel">
        <div class="panel-head">
          <h2>Provider Comparison</h2>
          <span class="pill">{comparison_count} comparison source{"s" if comparison_count != 1 else ""}</span>
        </div>
        <p class="section-caption">Side-by-side forecast summaries across the selected providers for the same sampled route timeline.</p>
        {warning_notice}
        {_comparison_summary_table(available_comparisons)}
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Provider Key Moments</h2>
          <span class="pill">Cross-source checkpoints</span>
        </div>
        <p class="section-caption">Start, coldest, windiest, wettest, and finish checkpoints shown provider by provider for quick comparison.</p>
        {_comparison_key_moment_cards(available_comparisons)}
      </section>
    """


def render_forecast_html(
    snapshot: dict[str, object],
    *,
    png_filename: str = FORECAST_PNG_FILENAME,
    gpx_filename: str = FORECAST_GPX_FILENAME,
    json_filename: str = FORECAST_JSON_FILENAME,
) -> str:
    title = str(snapshot.get("title", "Route Forecast"))
    summary = snapshot.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    key_moments = snapshot.get("key_moments", [])
    if not isinstance(key_moments, list):
        key_moments = []
    sample_rows = snapshot.get("sample_rows", [])
    if not isinstance(sample_rows, list):
        sample_rows = []

    start_label = _format_display_datetime(
        snapshot.get("start_time"), default="Start time unavailable"
    )
    generated_label = _format_display_datetime(
        snapshot.get("generated_at"),
        convert_to_utc=True,
    )
    duration_label = _format_duration_label(snapshot.get("duration"))
    distance_label = f"{float(snapshot.get('route_distance_km', 0.0) or 0.0):.2f} km"
    ascent_label = f"{float(snapshot.get('route_ascent_m', 0.0) or 0.0):.0f} m"
    source_label = str(snapshot.get("source_label") or "").strip()
    timezone_name = str(snapshot.get("timezone") or "").strip()
    wettest_label = _format_compact_timestamp(
        summary.get("wettest_time"), default="Unknown"
    )
    wettest_probability = summary.get("wettest_probability_pct")
    cards = _render_metric_cards(
        [
            ("Start", start_label, timezone_name or None),
            (
                "Duration",
                duration_label or str(snapshot.get("duration", "")),
                "Planned route duration",
            ),
            ("Distance", distance_label, "Total route distance"),
            ("Ascent", ascent_label, "Estimated climbing"),
            (
                "Temperature",
                (
                    f"{float(summary.get('temperature_min_c', 0.0) or 0.0):.1f}C to "
                    f"{float(summary.get('temperature_max_c', 0.0) or 0.0):.1f}C"
                ),
                "Expected route-wide range",
            ),
            (
                "Max wind",
                f"{float(summary.get('wind_max_kph', 0.0) or 0.0):.1f} km/h",
                "Peak forecast wind speed",
            ),
            (
                "Estimated rain",
                f"{float(summary.get('precipitation_total_mm', 0.0) or 0.0):.1f} mm",
                "Total route precipitation",
            ),
            (
                "Wettest segment",
                wettest_label,
                (
                    f"{float(summary.get('wettest_precipitation_mm', 0.0) or 0.0):.2f} mm rain"
                    f" • {_format_probability_label(wettest_probability)}"
                ),
            ),
        ]
    )

    png_href = html.escape(png_filename, quote=True)
    gpx_href = html.escape(gpx_filename, quote=True)
    json_href = html.escape(json_filename, quote=True)
    lead = f"Weather outlook for a {distance_label} route starting {start_label}."
    meta_bits = []
    if generated_label:
        meta_bits.append(f"Published {html.escape(generated_label)}")
    if timezone_name:
        meta_bits.append(html.escape(timezone_name))
    if source_label:
        meta_bits.append(html.escape(source_label))
    comparison_html = _comparison_section(snapshot)
    body_html = f"""
    <section class="hero">
      <p class="eyebrow">Route Forecast</p>
      <h1>{html.escape(title)}</h1>
      <p class="hero-lead">{html.escape(lead)}</p>
      {_render_meta_row(meta_bits)}
      <div class="action-row">
        {_render_action_link(png_href, "Download PNG", primary=True)}
        {_render_action_link(gpx_href, "Download GPX")}
        {_render_action_link(json_href, "Download JSON")}
      </div>
      <div class="metric-grid">{cards}</div>
    </section>

      <section class="section-stack">
      {comparison_html}
      <section class="panel">
        <div class="panel-head">
          <h2>Forecast Overview</h2>
          <span class="pill">Hosted PNG summary</span>
        </div>
        <p class="section-caption">The full rendered route forecast chart preserved as a static image for quick sharing.</p>
        <div class="chart-frame"><img src="{png_href}" alt="Forecast chart for {html.escape(title)}"></div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Key Moments</h2>
          <span class="pill">Fast route scan</span>
        </div>
        <p class="section-caption">Five fixed checkpoints pulled from the aligned route samples so the main swings in temperature, wind, and rain are easy to scan.</p>
        {_key_moments_grid(key_moments)}
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Route Timeline</h2>
          <span class="pill">Per-sample weather values</span>
        </div>
        <p class="section-caption">Aligned weather samples across the route timeline, formatted for quick scanning on desktop and mobile.</p>
        {_sample_rows_table(sample_rows)}
      </section>
    </section>
    """
    return _render_document(
        title=title,
        theme="forecast",
        active_nav="forecasts",
        home_href="../../../index.html",
        reports_href="../../../reports/index.html",
        forecasts_href="../../index.html",
        body_html=body_html,
    )


def export_forecast_site(
    *,
    snapshot: dict[str, object],
    image_path: str | Path,
    gpx_path: str | Path,
    destination: str | Path,
) -> Path:
    path = Path(destination)
    path.mkdir(parents=True, exist_ok=True)

    target_png = path / FORECAST_PNG_FILENAME
    target_gpx = path / FORECAST_GPX_FILENAME
    source_png = Path(image_path)
    source_gpx = Path(gpx_path)

    if source_png.resolve() != target_png.resolve():
        shutil.copy2(source_png, target_png)
    if source_gpx.resolve() != target_gpx.resolve():
        shutil.copy2(source_gpx, target_gpx)

    (path / FORECAST_JSON_FILENAME).write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / REPORT_META_FILENAME).write_text(
        json.dumps(build_forecast_metadata(snapshot), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / REPORT_HTML_FILENAME).write_text(
        render_forecast_html(snapshot),
        encoding="utf-8",
    )
    return path


def build_forecast_slug(route_name: str) -> str:
    return normalize_slug_text(route_name)


def build_publish_paths(*, route_name: str, published_at: datetime) -> tuple[str, str]:
    slug = build_forecast_slug(route_name)
    stamp = published_at.astimezone(UTC).strftime("%Y%m%d-%H%M%S")
    return (
        f"{FORECAST_SECTION_DIR}/{slug}/{stamp}",
        f"{FORECAST_SECTION_DIR}/{slug}/latest",
    )


def publish_forecast_bundle_to_site(
    *,
    source_dir: str | Path,
    pages_root: str | Path,
    route_name: str,
    gpx_url: str,
    start_time: str,
    timezone_name: str,
    duration: str,
    notes: str,
    published_at: datetime,
) -> dict[str, str]:
    report_dir, latest_dir = build_publish_paths(
        route_name=route_name, published_at=published_at
    )
    copy_bundle_to_targets(
        source_dir=source_dir,
        site_root=pages_root,
        report_dir=report_dir,
        latest_dir=latest_dir,
        published_metadata={
            "published_at": published_at.astimezone(UTC).isoformat(),
            "title": route_name,
            "report_kind": FORECAST_REPORT_KIND,
            "gpx_url": gpx_url,
            "start_time": start_time,
            "timezone": timezone_name,
            "duration": duration,
            "notes": notes,
        },
        asset_paths={
            "report_path": REPORT_HTML_FILENAME,
            "png_path": FORECAST_PNG_FILENAME,
            "gpx_path": FORECAST_GPX_FILENAME,
            "json_path": FORECAST_JSON_FILENAME,
        },
    )
    return {
        "report_dir": report_dir,
        "latest_dir": latest_dir,
        "report_path": f"{report_dir}/{REPORT_HTML_FILENAME}",
        "latest_path": f"{latest_dir}/{REPORT_HTML_FILENAME}",
        "png_path": f"{report_dir}/{FORECAST_PNG_FILENAME}",
        "gpx_path": f"{report_dir}/{FORECAST_GPX_FILENAME}",
        "json_path": f"{report_dir}/{FORECAST_JSON_FILENAME}",
    }
