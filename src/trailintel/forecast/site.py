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
UPLOT_VERSION = "1.6.32"
UPLOT_JS_URL = (
    f"https://cdn.jsdelivr.net/npm/uplot@{UPLOT_VERSION}/dist/uPlot.iife.min.js"
)
UPLOT_CSS_URL = (
    f"https://cdn.jsdelivr.net/npm/uplot@{UPLOT_VERSION}/dist/uPlot.min.css"
)
UPLOT_JS_INTEGRITY = (
    "sha384-Gx3t0zdBAuQOuvvmaLZj7HKEiSgWTAs+VdtNY7wt19QDPTDQjFIwAuXDj0zeN00c"
)
UPLOT_CSS_INTEGRITY = (
    "sha384-IfV0B7MIOYuO95kO9G5ySKPz/85zqFNOAs8iy4tkK5zd9izhJAB8b7lHrwYqqmYE"
)
FORECAST_CHART_DATA_ID = "forecast-chart-data"
FORECAST_CHARTS_SECTION_ID = "forecast-charts"
FORECAST_CHARTS_FALLBACK_ID = "forecast-charts-fallback"
FORECAST_CHART_HOVER_ID = "forecast-chart-hover"
PROVIDER_COLORS = {
    "open-meteo": "#0e5b85",
    "met-no": "#1c7c63",
    "weatherapi": "#c45d1c",
}
FALLBACK_PROVIDER_COLORS = (
    "#7d4ec6",
    "#ac3b61",
    "#1f7a8c",
    "#5f6c2f",
)
CHART_SPECS = (
    (
        "temperature",
        "Temperature",
        "Ambient temperature across the sampled route, with all available providers overlaid.",
    ),
    (
        "feels-like",
        "Feels Like",
        "Apparent temperature comparison across providers when the metric is available.",
    ),
    (
        "precipitation",
        "Precipitation",
        "Estimated precipitation intensity along the route for every available provider.",
    ),
    (
        "cloud-cover",
        "Cloud Cover",
        "Cloud cover percentage along the same route timeline for each provider.",
    ),
    (
        "wind",
        "Wind",
        "Sustained wind comparison across providers, aligned to the sampled route timeline.",
    ),
    (
        "elevation",
        "Elevation",
        "Route profile aligned to the forecast timeline so hover details can tie weather back to terrain.",
    ),
)


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


def _provider_color(provider_id: str) -> str:
    if provider_id in PROVIDER_COLORS:
        return PROVIDER_COLORS[provider_id]
    fallback_index = sum(ord(char) for char in provider_id) % len(
        FALLBACK_PROVIDER_COLORS
    )
    return FALLBACK_PROVIDER_COLORS[fallback_index]


def _build_provider_coverage(report: ForecastReport) -> dict[str, object]:
    return {
        "has_apparent_temperature": any(
            sample.apparent_temperature_c is not None for sample in report.samples
        ),
        "has_wind_gust": any(sample.wind_gust_kph is not None for sample in report.samples),
        "has_precipitation_probability": any(
            sample.precipitation_probability is not None for sample in report.samples
        ),
    }


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


def _build_chart_provider_samples(report: ForecastReport) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for sample in report.samples:
        rows.append(
            {
                "timestamp": sample.sample.timestamp.isoformat(),
                "distance_km": round(sample.sample.distance_m / 1000.0, 2),
                "elevation_m": _round_optional(sample.sample.elevation_m, 1),
                "temperature_c": round(sample.temperature_c, 1),
                "apparent_temperature_c": _round_optional(
                    sample.apparent_temperature_c, 1
                ),
                "wind_kph": round(sample.wind_kph, 1),
                "cloud_cover_pct": round(sample.cloud_cover_pct, 1),
                "precipitation_mm": round(sample.precipitation_mm, 2),
            }
        )
    return rows


def _build_chart_route_profile(report: ForecastReport) -> list[dict[str, object]]:
    return [
        {
            "timestamp": sample.sample.timestamp.isoformat(),
            "distance_km": round(sample.sample.distance_m / 1000.0, 2),
            "elevation_m": _round_optional(sample.sample.elevation_m, 1),
        }
        for sample in report.samples
    ]


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
        "chart_data": _build_chart_data(
            report,
            comparison_reports=comparison_reports,
        ),
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
    coverage = _build_provider_coverage(report)
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


def _build_chart_data(
    report: ForecastReport,
    *,
    comparison_reports: Sequence[ForecastReport],
) -> dict[str, object]:
    reports = [report, *comparison_reports]
    providers: list[dict[str, object]] = []
    for index, item in enumerate(reports):
        definition = provider_definition(item.provider_id)
        providers.append(
            {
                "provider_id": definition.provider_id,
                "label": definition.label,
                "is_primary": index == 0,
                "color": _provider_color(definition.provider_id),
                "coverage": _build_provider_coverage(item),
                "samples": _build_chart_provider_samples(item),
            }
        )
    return {
        "x_axis": "time",
        "timezone": _timezone_label(report.start_time),
        "providers": providers,
        "route_profile": _build_chart_route_profile(report),
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


def _comparison_summary_table(
    providers: list[dict[str, object]],
    *,
    color_lookup: dict[str, str],
) -> str:
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
        provider_id = str(provider.get("provider_id") or "").strip()
        label = str(provider.get("label") or provider_id or "Provider")
        color = color_lookup.get(provider_id, _provider_color(provider_id))
        body_rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{_provider_label_markup(label, color)}</td>",
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


def _comparison_key_moment_cards(
    providers: list[dict[str, object]],
    *,
    color_lookup: dict[str, str],
) -> str:
    if not providers:
        return '<div class="empty-state">No provider comparison data available.</div>'

    cards: list[str] = []
    for provider in providers:
        provider_id = str(provider.get("provider_id") or "").strip()
        label = str(provider.get("label") or provider_id or "Provider")
        color = color_lookup.get(provider_id, _provider_color(provider_id))
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
                <h3>{_provider_label_markup(label, color)}</h3>
                <span class="pill">{html.escape(provider_id)}</span>
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


def _chart_color_lookup(chart_data: dict[str, object]) -> dict[str, str]:
    providers = chart_data.get("providers")
    if not isinstance(providers, list):
        return {}
    colors: dict[str, str] = {}
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        provider_id = str(provider.get("provider_id") or "").strip()
        color = str(provider.get("color") or "").strip()
        if provider_id and color:
            colors[provider_id] = color
    return colors


def _provider_label_markup(label: str, color: str) -> str:
    return (
        '<span class="provider-swatch" '
        f'style="--provider-color:{html.escape(color, quote=True)}"></span>'
        f"{html.escape(label)}"
    )


def _provider_legend(chart_data: dict[str, object]) -> str:
    providers = chart_data.get("providers")
    if not isinstance(providers, list) or not providers:
        return ""

    items: list[str] = []
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        label = str(provider.get("label") or provider.get("provider_id") or "Provider")
        provider_id = str(provider.get("provider_id") or "").strip()
        color = str(provider.get("color") or _provider_color(provider_id)).strip()
        primary_pill = (
            '<span class="pill provider-pill">Primary</span>'
            if provider.get("is_primary")
            else ""
        )
        items.append(
            (
                '<span class="provider-chip">'
                f"{_provider_label_markup(label, color)}"
                f"{primary_pill}</span>"
            )
        )
    if not items:
        return ""
    return '<div class="provider-legend">' + "".join(items) + "</div>"


def _render_forecast_chart_cards() -> str:
    cards: list[str] = []
    for chart_id, title, caption in CHART_SPECS:
        cards.append(
            f"""
            <article class="forecast-chart-card">
              <div class="panel-head">
                <h3>{html.escape(title)}</h3>
              </div>
              <p class="section-caption">{html.escape(caption)}</p>
              <p class="chart-note" id="chart-note-{html.escape(chart_id)}">Preparing interactive comparison...</p>
              <div
                class="forecast-chart-canvas"
                id="chart-{html.escape(chart_id)}"
                data-chart-id="{html.escape(chart_id, quote=True)}"
              ></div>
            </article>
            """
        )
    return "".join(cards)


def _render_chart_section(chart_data: dict[str, object]) -> str:
    chart_json = _serialize_script_json(chart_data)
    provider_legend = _provider_legend(chart_data)
    return f"""
      <section class="panel" id="{FORECAST_CHARTS_SECTION_ID}">
        <div class="panel-head">
          <h2>Forecast Charts</h2>
          <span class="pill">uPlot comparison view</span>
        </div>
        <p class="section-caption">Interactive provider overlays across the sampled route timeline. Hover any chart to compare the same moment in time.</p>
        {provider_legend}
        <div id="{FORECAST_CHART_HOVER_ID}" class="forecast-chart-hover" aria-live="polite">
          Hover a chart to inspect the same route moment across providers.
        </div>
        <noscript>
          <div class="empty-state">Interactive charts need JavaScript enabled. The PNG summary and route timeline below still contain the forecast details.</div>
        </noscript>
        <div id="{FORECAST_CHARTS_FALLBACK_ID}" class="empty-state" hidden>
          Interactive charts could not load. The PNG summary and route timeline below still contain the forecast details.
        </div>
        <div class="forecast-chart-grid" data-forecast-chart-grid>
          {_render_forecast_chart_cards()}
        </div>
        <script id="{FORECAST_CHART_DATA_ID}" type="application/json">{chart_json}</script>
      </section>
    """


def _render_forecast_head_extras() -> str:
    return f"""
  <link rel="stylesheet" href="{UPLOT_CSS_URL}" integrity="{UPLOT_CSS_INTEGRITY}" crossorigin="anonymous">
  <style>
    .provider-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }}
    .provider-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.74);
      font-size: 0.92rem;
      font-weight: 600;
    }}
    .provider-swatch {{
      width: 0.8rem;
      height: 0.8rem;
      border-radius: 999px;
      background: var(--provider-color);
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.65);
    }}
    .provider-pill {{
      min-height: 28px;
      padding: 0 10px;
      font-size: 0.74rem;
    }}
    .forecast-chart-hover {{
      margin-top: 18px;
      padding: 14px 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.7);
      color: var(--muted);
      line-height: 1.55;
    }}
    .forecast-chart-hover strong {{
      color: var(--text);
    }}
    .forecast-chart-hover-grid {{
      display: grid;
      gap: 8px;
      margin-top: 10px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    }}
    .forecast-chart-hover-row {{
      display: grid;
      gap: 4px;
      padding: 10px 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.65);
    }}
    .forecast-chart-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
      margin-top: 18px;
    }}
    .forecast-chart-card {{
      padding: 18px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: var(--panel-strong);
    }}
    .forecast-chart-card h3 {{
      margin: 0;
      font-size: 1.16rem;
    }}
    .chart-note {{
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
    }}
    .forecast-chart-canvas {{
      margin-top: 14px;
      min-height: 320px;
      padding: 12px;
      border-radius: 20px;
      border: 1px solid var(--line);
      background: var(--panel-soft);
      overflow: hidden;
    }}
    .forecast-chart-canvas .uplot {{
      width: 100%;
    }}
    @media (max-width: 640px) {{
      .forecast-chart-grid {{
        grid-template-columns: 1fr;
      }}
      .forecast-chart-canvas {{
        min-height: 280px;
      }}
    }}
  </style>
"""


def _serialize_script_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _render_forecast_chart_bootstrap(chart_data: dict[str, object]) -> str:
    provider_lookup = _chart_color_lookup(chart_data)
    chart_specs_json = _serialize_script_json(
        [
            {"id": chart_id, "title": title, "caption": caption}
            for chart_id, title, caption in CHART_SPECS
        ]
    )
    return f"""
  <script src="{UPLOT_JS_URL}" integrity="{UPLOT_JS_INTEGRITY}" crossorigin="anonymous"></script>
  <script>
    (() => {{
      const chartSection = document.getElementById("{FORECAST_CHARTS_SECTION_ID}");
      const fallback = document.getElementById("{FORECAST_CHARTS_FALLBACK_ID}");
      const hover = document.getElementById("{FORECAST_CHART_HOVER_ID}");
      const payloadEl = document.getElementById("{FORECAST_CHART_DATA_ID}");
      const chartSpecs = {chart_specs_json};
      const colorLookup = {json.dumps(provider_lookup, ensure_ascii=False)};
      const chartEntries = [];
      let activeHoverKey = "";

      function setFallback(message) {{
        const grid = chartSection?.querySelector("[data-forecast-chart-grid]");
        if (grid) {{
          grid.hidden = true;
        }}
        if (fallback) {{
          fallback.hidden = false;
          fallback.textContent = message;
        }}
      }}

      function parsePayload() {{
        if (!payloadEl) {{
          setFallback("Interactive chart data is missing from this report.");
          return null;
        }}
        try {{
          return JSON.parse(payloadEl.textContent || "{{}}");
        }} catch (_error) {{
          setFallback("Interactive chart data could not be parsed.");
          return null;
        }}
      }}

      function buildDateFormatter(timezoneName) {{
        const options = {{
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        }};
        if (timezoneName) {{
          try {{
            return new Intl.DateTimeFormat("en-US", {{...options, timeZone: timezoneName}});
          }} catch (_error) {{
            return new Intl.DateTimeFormat("en-US", options);
          }}
        }}
        return new Intl.DateTimeFormat("en-US", options);
      }}

      function formatNumber(value, digits) {{
        if (value === null || value === undefined || Number.isNaN(value)) {{
          return "n/a";
        }}
        return Number(value).toFixed(digits);
      }}

      function hasMetricValue(metricId, coverage) {{
        if (metricId === "feels-like") {{
          return !!coverage?.has_apparent_temperature;
        }}
        return true;
      }}

      function getMetricValue(sample, metricId) {{
        if (!sample) {{
          return null;
        }}
        switch (metricId) {{
          case "temperature":
            return sample.temperature_c;
          case "feels-like":
            return sample.apparent_temperature_c;
          case "precipitation":
            return sample.precipitation_mm;
          case "cloud-cover":
            return sample.cloud_cover_pct;
          case "wind":
            return sample.wind_kph;
          default:
            return null;
        }}
      }}

      function metricUnit(metricId) {{
        switch (metricId) {{
          case "temperature":
          case "feels-like":
            return "C";
          case "precipitation":
            return "mm";
          case "cloud-cover":
            return "%";
          case "wind":
            return "km/h";
          case "elevation":
            return "m";
          default:
            return "";
        }}
      }}

      function metricDigits(metricId) {{
        switch (metricId) {{
          case "precipitation":
            return 2;
          case "cloud-cover":
          case "elevation":
            return 0;
          default:
            return 1;
        }}
      }}

      function renderHoverContent(metricId, metricTitle, idx, payload, providerSeries) {{
        if (!hover) {{
          return;
        }}
        const profile = payload.route_profile?.[idx];
        if (!profile) {{
          hover.textContent = "Hover a chart to inspect the same route moment across providers.";
          return;
        }}
        const unit = metricUnit(metricId);
        const digits = metricDigits(metricId);
        const rows = providerSeries
          .map((entry) => {{
            const value = entry.values[idx];
            return `
              <div class="forecast-chart-hover-row">
                <strong style="color:${{entry.color}}">${{entry.label}}</strong>
                <span>${{formatNumber(value, digits)}} ${{unit}}</span>
              </div>
            `;
          }})
          .join("");
        hover.innerHTML = `
          <strong>${{metricTitle}}</strong><br>
          ${{dateFormatter.format(new Date(Date.parse(profile.timestamp)))}} •
          ${{formatNumber(profile.distance_km, 2)}} km •
          ${{profile.elevation_m === null || profile.elevation_m === undefined ? "n/a" : `${{formatNumber(profile.elevation_m, 0)}} m`}}
          <div class="forecast-chart-hover-grid">${{rows}}</div>
        `;
      }}

      function initMetricChart(metricSpec, payload, dateFormatter) {{
        const note = document.getElementById(`chart-note-${{metricSpec.id}}`);
        const container = document.getElementById(`chart-${{metricSpec.id}}`);
        if (!container) {{
          return;
        }}

        if (metricSpec.id === "elevation") {{
          const xValues = payload.route_profile.map((point) => Date.parse(point.timestamp));
          const yValues = payload.route_profile.map((point) =>
            point.elevation_m === null || point.elevation_m === undefined
              ? null
              : Number(point.elevation_m),
          );
          if (!yValues.some((value) => value !== null)) {{
            container.innerHTML = '<div class="empty-state">Route elevation is unavailable for this forecast.</div>';
            if (note) {{
              note.textContent = "Elevation uses the route profile rather than provider data.";
            }}
            return;
          }}
          if (note) {{
            note.textContent = "Elevation comes from the GPX route profile and is aligned to the same time axis.";
          }}
          const opts = {{
            width: Math.max(container.clientWidth - 24, 280),
            height: 280,
            legend: {{ show: false }},
            cursor: {{
              sync: {{ key: "trailintel-forecast-sync" }},
              drag: {{ x: false, y: false }},
            }},
            scales: {{
              x: {{ time: true }},
            }},
            axes: [
              {{
                values: (_u, vals) => vals.map((value) => dateFormatter.format(new Date(value))),
              }},
              {{
                values: (_u, vals) => vals.map((value) => `${{formatNumber(value, 0)}} m`),
              }},
            ],
            series: [
              {{}},
              {{
                label: "Elevation",
                stroke: "#8a6a2f",
                width: 2,
                spanGaps: true,
                points: {{ show: false }},
              }},
            ],
            plugins: [
              {{
                hooks: {{
                  setCursor: [
                    (u) => {{
                      if (u.cursor.idx == null || u.cursor.idx < 0) {{
                        return;
                      }}
                      const hoverKey = `${{metricSpec.id}}:${{u.cursor.idx}}`;
                      if (hoverKey === activeHoverKey) {{
                        return;
                      }}
                      activeHoverKey = hoverKey;
                      renderHoverContent(
                        metricSpec.id,
                        metricSpec.title,
                        u.cursor.idx,
                        payload,
                        [{{
                          label: "Elevation",
                          color: "#8a6a2f",
                          values: yValues,
                        }}],
                      );
                    }},
                  ],
                }},
              }},
            ],
          }};
          const chart = new window.uPlot(opts, [xValues, yValues], container);
          chartEntries.push({{ chart, container }});
          return;
        }}

        const seriesEntries = [];
        const unavailableLabels = [];
        const xValues =
          payload.route_profile?.map((point) => Date.parse(point.timestamp)) ?? [];

        for (const provider of payload.providers ?? []) {{
          if (!hasMetricValue(metricSpec.id, provider.coverage || {{}})) {{
            unavailableLabels.push(provider.label);
            continue;
          }}
          const values = (provider.samples ?? []).map((sample) => {{
            const value = getMetricValue(sample, metricSpec.id);
            return value === null || value === undefined ? null : Number(value);
          }});
          if (!values.some((value) => value !== null)) {{
            unavailableLabels.push(provider.label);
            continue;
          }}
          seriesEntries.push({{
            providerId: provider.provider_id,
            label: provider.label,
            color: provider.color || colorLookup[provider.provider_id] || "#0e5b85",
            values,
          }});
        }}

        if (!seriesEntries.length) {{
          container.innerHTML = `<div class="empty-state">No ${{metricSpec.title.toLowerCase()}} comparison data is available for this forecast.</div>`;
          if (note) {{
            note.textContent = unavailableLabels.length
              ? `Unavailable from: ${{unavailableLabels.join(", ")}}.`
              : `No provider data is available for this metric.`;
          }}
          return;
        }}

        if (note) {{
          note.textContent = unavailableLabels.length
            ? `Unavailable from: ${{unavailableLabels.join(", ")}}.`
            : `All available providers are plotted on the same chart.`;
        }}

        const opts = {{
          width: Math.max(container.clientWidth - 24, 280),
          height: 280,
          legend: {{ show: false }},
          cursor: {{
            sync: {{ key: "trailintel-forecast-sync" }},
            drag: {{ x: false, y: false }},
          }},
          scales: {{
            x: {{ time: true }},
          }},
          axes: [
            {{
              values: (_u, vals) => vals.map((value) => dateFormatter.format(new Date(value))),
            }},
            {{
              values: (_u, vals) => vals.map((value) => `${{formatNumber(value, metricDigits(metricSpec.id))}} ${{metricUnit(metricSpec.id)}}`),
            }},
          ],
          series: [
            {{}},
            ...seriesEntries.map((entry) => ({{
              label: entry.label,
              stroke: entry.color,
              width: 2,
              spanGaps: true,
              points: {{ show: false }},
            }})),
          ],
          plugins: [
            {{
              hooks: {{
                setCursor: [
                  (u) => {{
                    if (u.cursor.idx == null || u.cursor.idx < 0) {{
                      return;
                    }}
                    const hoverKey = `${{metricSpec.id}}:${{u.cursor.idx}}`;
                    if (hoverKey === activeHoverKey) {{
                      return;
                    }}
                    activeHoverKey = hoverKey;
                    renderHoverContent(
                      metricSpec.id,
                      metricSpec.title,
                      u.cursor.idx,
                      payload,
                      seriesEntries,
                    );
                  }},
                ],
              }},
            }},
          ],
        }};

        const chart = new window.uPlot(
          opts,
          [xValues, ...seriesEntries.map((entry) => entry.values)],
          container,
        );
        chartEntries.push({{ chart, container }});
      }}

      const payload = parsePayload();
      if (!payload) {{
        return;
      }}
      if (typeof window.uPlot !== "function") {{
        setFallback("Interactive charts could not load from the CDN. The PNG summary and route timeline below still contain the forecast details.");
        return;
      }}
      if (!Array.isArray(payload.providers) || !payload.providers.length) {{
        setFallback("No interactive provider chart data is available for this report.");
        return;
      }}
      if (!Array.isArray(payload.route_profile) || !payload.route_profile.length) {{
        setFallback("The route profile is missing from the interactive chart data.");
        return;
      }}

      const dateFormatter = buildDateFormatter(payload.timezone || "");
      for (const metricSpec of chartSpecs) {{
        initMetricChart(metricSpec, payload, dateFormatter);
      }}

      const resizeCharts = () => {{
        for (const entry of chartEntries) {{
          entry.chart.setSize({{
            width: Math.max(entry.container.clientWidth - 24, 280),
            height: 280,
          }});
        }}
      }};

      if (typeof ResizeObserver === "function") {{
        const observer = new ResizeObserver(() => resizeCharts());
        for (const entry of chartEntries) {{
          observer.observe(entry.container);
        }}
      }} else {{
        window.addEventListener("resize", resizeCharts);
      }}
    }})();
  </script>
"""

def _comparison_section(snapshot: dict[str, object]) -> str:
    comparison = snapshot.get("comparison")
    if not isinstance(comparison, dict):
        return ""
    chart_data = snapshot.get("chart_data")
    color_lookup = (
        _chart_color_lookup(chart_data)
        if isinstance(chart_data, dict)
        else {}
    )
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
        {_comparison_summary_table(available_comparisons, color_lookup=color_lookup)}
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Provider Key Moments</h2>
          <span class="pill">Cross-source checkpoints</span>
        </div>
        <p class="section-caption">Start, coldest, windiest, wettest, and finish checkpoints shown provider by provider for quick comparison.</p>
        {_comparison_key_moment_cards(available_comparisons, color_lookup=color_lookup)}
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
    chart_data = snapshot.get("chart_data", {})
    if not isinstance(chart_data, dict):
        chart_data = {}

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
    chart_section_html = _render_chart_section(chart_data)
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
      {chart_section_html}
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
        head_extras=_render_forecast_head_extras(),
        body_end_html=_render_forecast_chart_bootstrap(chart_data),
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
