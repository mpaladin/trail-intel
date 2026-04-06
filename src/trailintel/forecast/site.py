from __future__ import annotations

from datetime import UTC, datetime, timedelta
import html
import json
from pathlib import Path
import shutil
from typing import TYPE_CHECKING

from trailintel.github_pipeline import normalize_slug_text
from trailintel.site import (
    FORECAST_REPORT_KIND,
    REPORT_HTML_FILENAME,
    REPORT_META_FILENAME,
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
                "apparent_temperature_c": round(sample.apparent_temperature_c, 1),
                "wind_kph": round(sample.wind_kph, 1),
                "wind_gust_kph": round(sample.wind_gust_kph, 1),
                "wind_direction_deg": round(sample.wind_direction_deg, 1),
                "cloud_cover_pct": round(sample.cloud_cover_pct, 1),
                "precipitation_mm": round(sample.precipitation_mm, 2),
                "precipitation_probability": round(sample.precipitation_probability, 1),
            }
        )
    return rows


def build_forecast_snapshot(
    *,
    title: str,
    report: ForecastReport,
    summary: ForecastSummary,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    stamp = (generated_at or datetime.now(UTC)).astimezone(UTC)
    return {
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
            "wettest_probability_pct": round(summary.wettest_probability_pct, 1),
        },
        "sample_rows": _build_sample_rows(report),
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
        "wettest_probability_pct": summary.get("wettest_probability_pct"),
    }


def _meta_line(snapshot: dict[str, object]) -> str:
    generated = str(snapshot.get("generated_at") or "").strip()
    timezone_name = str(snapshot.get("timezone") or "").strip()
    source_label = str(snapshot.get("source_label") or "").strip()
    bits = [bit for bit in (generated, timezone_name, source_label) if bit]
    if not bits:
        return ""
    return '<p class="meta-line">' + " | ".join(html.escape(bit) for bit in bits) + "</p>"


def _summary_card(label: str, value: str) -> str:
    return (
        '<div class="metric-card">'
        f'<div class="metric-label">{html.escape(label)}</div>'
        f'<div class="metric-value">{html.escape(value)}</div>'
        "</div>"
    )


def _sample_rows_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty-state">No forecast samples available.</div>'

    body_rows: list[str] = []
    for row in rows:
        body_rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{html.escape(str(row.get('timestamp', '')))}</td>",
                    f"<td>{html.escape(str(row.get('distance_km', '')))}</td>",
                    f"<td>{html.escape(str(row.get('elevation_m', '')))}</td>",
                    f"<td>{html.escape(str(row.get('temperature_c', '')))}</td>",
                    f"<td>{html.escape(str(row.get('apparent_temperature_c', '')))}</td>",
                    f"<td>{html.escape(str(row.get('wind_kph', '')))}</td>",
                    f"<td>{html.escape(str(row.get('wind_gust_kph', '')))}</td>",
                    f"<td>{html.escape(str(row.get('wind_direction_deg', '')))}</td>",
                    f"<td>{html.escape(str(row.get('cloud_cover_pct', '')))}</td>",
                    f"<td>{html.escape(str(row.get('precipitation_mm', '')))}</td>",
                    f"<td>{html.escape(str(row.get('precipitation_probability', '')))}</td>",
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
        "</tr></thead><tbody>"
        + "".join(body_rows)
        + "</tbody></table></div>"
    )


def render_forecast_html(
    snapshot: dict[str, object],
    *,
    png_filename: str = FORECAST_PNG_FILENAME,
    gpx_filename: str = FORECAST_GPX_FILENAME,
    json_filename: str = FORECAST_JSON_FILENAME,
) -> str:
    title = html.escape(str(snapshot.get("title", "Route Forecast")))
    summary = snapshot.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    sample_rows = snapshot.get("sample_rows", [])
    if not isinstance(sample_rows, list):
        sample_rows = []

    cards = "".join(
        [
            _summary_card(
                "Start",
                str(snapshot.get("start_time", "")),
            ),
            _summary_card("Duration", str(snapshot.get("duration", ""))),
            _summary_card("Distance", f"{float(snapshot.get('route_distance_km', 0.0) or 0.0):.2f} km"),
            _summary_card("Ascent", f"{float(snapshot.get('route_ascent_m', 0.0) or 0.0):.0f} m"),
            _summary_card(
                "Temperature",
                (
                    f"{float(summary.get('temperature_min_c', 0.0) or 0.0):.1f} to "
                    f"{float(summary.get('temperature_max_c', 0.0) or 0.0):.1f} C"
                ),
            ),
            _summary_card("Max Wind", f"{float(summary.get('wind_max_kph', 0.0) or 0.0):.1f} km/h"),
            _summary_card(
                "Estimated Rain",
                f"{float(summary.get('precipitation_total_mm', 0.0) or 0.0):.1f} mm",
            ),
            _summary_card(
                "Wettest Segment",
                (
                    f"{summary.get('wettest_time', '')} "
                    f"({float(summary.get('wettest_probability_pct', 0.0) or 0.0):.0f}%)"
                ).strip(),
            ),
        ]
    )

    png_href = html.escape(png_filename, quote=True)
    gpx_href = html.escape(gpx_filename, quote=True)
    json_href = html.escape(json_filename, quote=True)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f5f0e8;
      --panel: #fffdfa;
      --panel-soft: #f7f3ec;
      --text: #1c1f1a;
      --muted: #5d5e54;
      --accent: #0e5b85;
      --accent-soft: #d7ecf8;
      --border: #ddd3c5;
      --shadow: 0 16px 40px rgba(32, 26, 17, 0.08);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: var(--text); background: radial-gradient(circle at top, #fbf7ef 0%, var(--bg) 60%, #ebe1d4 100%); }}
    a {{ color: var(--accent); }}
    .page {{ max-width: 1280px; margin: 0 auto; padding: 30px 20px 44px; }}
    .hero {{ background: linear-gradient(135deg, rgba(14,91,133,0.12), rgba(19,38,57,0.1)); border: 1px solid rgba(14,91,133,0.12); border-radius: 24px; padding: 26px; box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(2rem, 4vw, 3rem); line-height: 1.08; }}
    .hero p {{ margin: 0; color: var(--muted); }}
    .meta-line {{ margin-top: 10px; color: var(--muted); }}
    .download-row {{ display: flex; flex-wrap: wrap; gap: 12px; margin-top: 18px; }}
    .download-link {{ display: inline-flex; align-items: center; justify-content: center; min-width: 180px; padding: 12px 16px; background: var(--panel); border: 1px solid var(--border); border-radius: 999px; text-decoration: none; font-weight: 700; box-shadow: 0 10px 24px rgba(0,0,0,0.05); }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 18px; }}
    .metric-card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 18px; padding: 16px; box-shadow: 0 10px 26px rgba(0,0,0,0.04); }}
    .metric-label {{ color: var(--muted); font-size: 0.9rem; }}
    .metric-value {{ margin-top: 6px; font-size: 1.2rem; font-weight: 700; line-height: 1.35; }}
    .panel {{ margin-top: 22px; background: var(--panel); border: 1px solid var(--border); border-radius: 24px; padding: 22px; box-shadow: var(--shadow); }}
    .panel h2 {{ margin: 0 0 14px; font-size: 1.35rem; }}
    .section-caption {{ margin-top: -4px; color: var(--muted); }}
    .chart-frame {{ background: var(--panel-soft); border: 1px solid var(--border); border-radius: 18px; padding: 12px; }}
    .chart-frame img {{ display: block; width: 100%; border-radius: 12px; }}
    .results-table {{ width: 100%; border-collapse: collapse; font-size: 0.93rem; }}
    .results-table th, .results-table td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    .results-table th {{ font-size: 0.82rem; letter-spacing: 0.02em; text-transform: uppercase; color: var(--muted); background: #faf6ef; position: sticky; top: 0; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 18px; background: var(--panel); }}
    .empty-state {{ padding: 16px; border: 1px dashed var(--border); border-radius: 16px; color: var(--muted); background: rgba(255,255,255,0.4); }}
    @media (max-width: 760px) {{
      .page {{ padding: 18px 14px 28px; }}
      .hero {{ padding: 22px; }}
      .results-table th, .results-table td {{ padding: 8px 10px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{title}</h1>
      <p>Static route forecast generated from the TrailIntel forecast pipeline.</p>
      {_meta_line(snapshot)}
      <div class="download-row">
        <a class="download-link" href="{png_href}">Download PNG</a>
        <a class="download-link" href="{gpx_href}">Download GPX</a>
        <a class="download-link" href="{json_href}">Download JSON</a>
      </div>
      <div class="metrics">{cards}</div>
    </section>

    <section class="panel">
      <h2>Forecast Overview</h2>
      <p class="section-caption">The PNG summary preserves the original chart layout in a TrailIntel-hosted report.</p>
      <div class="chart-frame"><img src="{png_href}" alt="Forecast chart for {title}"></div>
    </section>

    <section class="panel">
      <h2>Forecast Samples</h2>
      <p class="section-caption">Per-sample weather values aligned to the route timeline.</p>
      {_sample_rows_table(sample_rows)}
    </section>
  </main>
</body>
</html>
"""


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
    report_dir, latest_dir = build_publish_paths(route_name=route_name, published_at=published_at)
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
