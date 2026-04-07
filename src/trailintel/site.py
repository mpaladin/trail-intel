from __future__ import annotations

from datetime import UTC, datetime
import csv
import html
import json
import math
from pathlib import Path
import re
import shutil
from typing import Any

from trailintel.models import AthleteRecord
from trailintel.report import export_records, sort_records

REPORT_CSV_FILENAME = "report.csv"
REPORT_JSON_FILENAME = "report.json"
REPORT_HTML_FILENAME = "index.html"
REPORT_SNAPSHOT_FILENAME = "snapshot.json"
REPORT_META_FILENAME = "report-meta.json"
RACE_REPORT_KIND = "race"
FORECAST_REPORT_KIND = "forecast"
REPORTS_SECTION_DIR = "reports"
FORECASTS_SECTION_DIR = "forecasts"


def _score_fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def _normalize_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    cleaned = cleaned.strip(",.;:()[]{}")
    return cleaned


def _has_utmb_match(record: AthleteRecord) -> bool:
    return bool(
        record.utmb_match_name
        or record.utmb_profile_url
        or (record.utmb_index is not None)
    )


def _has_itra_match(record: AthleteRecord) -> bool:
    return bool(
        record.itra_match_name
        or record.itra_profile_url
        or (record.itra_score is not None)
    )


def _has_betrail_match(record: AthleteRecord) -> bool:
    return bool(
        record.betrail_match_name
        or record.betrail_profile_url
        or (record.betrail_score is not None)
    )


def compute_no_result_names(records: list[AthleteRecord]) -> list[str]:
    match_presence: dict[str, dict[str, bool]] = {}
    for record in records:
        name = record.input_name.strip()
        if not name:
            continue
        status = match_presence.setdefault(name, {"utmb": False, "itra": False, "betrail": False})
        status["utmb"] = status["utmb"] or _has_utmb_match(record)
        status["itra"] = status["itra"] or _has_itra_match(record)
        status["betrail"] = status["betrail"] or _has_betrail_match(record)

    return sorted(
        name
        for name, status in match_presence.items()
        if not status["utmb"] and not status["itra"] and not status["betrail"]
    )


def aggregate_scores_by_input(
    records: list[AthleteRecord],
) -> tuple[list[float], list[float], list[float], dict[str, int]]:
    by_input: dict[str, dict[str, float | None]] = {}
    for record in records:
        normalized = _normalize_name(record.input_name).casefold()
        key = normalized or record.input_name.strip().casefold()
        if not key:
            continue
        bucket = by_input.setdefault(key, {"utmb": None, "itra": None, "betrail": None})
        if record.utmb_index is not None:
            bucket["utmb"] = (
                record.utmb_index
                if bucket["utmb"] is None
                else max(bucket["utmb"], record.utmb_index)
            )
        if record.itra_score is not None:
            bucket["itra"] = (
                record.itra_score
                if bucket["itra"] is None
                else max(bucket["itra"], record.itra_score)
            )
        if record.betrail_score is not None:
            bucket["betrail"] = (
                record.betrail_score
                if bucket["betrail"] is None
                else max(bucket["betrail"], record.betrail_score)
            )

    utmb_scores = [bucket["utmb"] for bucket in by_input.values() if bucket["utmb"] is not None]
    itra_scores = [bucket["itra"] for bucket in by_input.values() if bucket["itra"] is not None]
    betrail_scores = [bucket["betrail"] for bucket in by_input.values() if bucket["betrail"] is not None]
    with_any = sum(
        1
        for bucket in by_input.values()
        if bucket["utmb"] is not None or bucket["itra"] is not None or bucket["betrail"] is not None
    )
    summary = {
        "participants": len(by_input),
        "with_utmb": len(utmb_scores),
        "with_itra": len(itra_scores),
        "with_betrail": len(betrail_scores),
        "with_any": with_any,
    }
    return utmb_scores, itra_scores, betrail_scores, summary


def build_score_histogram(scores: list[float], *, bin_size: int = 50) -> list[dict[str, int | str]]:
    if not scores:
        return []

    max_value = max(scores)
    max_edge = int(math.ceil(max_value / bin_size) * bin_size)
    if max_edge <= 0:
        max_edge = bin_size

    bins = list(range(0, max_edge, bin_size))
    counts = [0 for _ in bins]
    for score in scores:
        idx = int(score // bin_size)
        if idx >= len(counts):
            idx = len(counts) - 1
        if idx < 0:
            idx = 0
        counts[idx] += 1

    rows: list[dict[str, int | str]] = []
    for start, count in zip(bins, counts, strict=False):
        end = start + bin_size - 1
        rows.append({"range": f"{start}-{end}", "count": count})
    return rows


def records_to_rows(records: list[AthleteRecord], *, top: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, record in enumerate(records[:top], start=1):
        rows.append(
            {
                "Rank": idx,
                "Athlete": record.input_name,
                "UTMB": _score_fmt(record.utmb_index),
                "ITRA": _score_fmt(record.itra_score),
                "Betrail": _score_fmt(record.betrail_score),
                "Combined": f"{record.combined_score:.1f}",
                "UTMB Matched Name": record.utmb_match_name or "",
                "ITRA Matched Name": record.itra_match_name or "",
                "Betrail Matched Name": record.betrail_match_name or "",
                "UTMB Profile": record.utmb_profile_url or "",
                "ITRA Profile": record.itra_profile_url or "",
                "Betrail Profile": record.betrail_profile_url or "",
                "Notes": record.notes,
            }
        )
    return rows


def build_report_snapshot(
    *,
    title: str,
    all_records: list[AthleteRecord],
    qualified_records: list[AthleteRecord],
    participants_count: int,
    strategy: str,
    same_name_mode: str,
    top: int,
    sort_by: str,
    race_url: str = "",
    competition_name: str = "",
    score_threshold: float | None = None,
    stale_provider_fallback_used: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    ranked_all = sort_records(all_records, sort_by=sort_by)
    ranked_qualified = sort_records(qualified_records, sort_by=sort_by)
    no_result_names = compute_no_result_names(all_records)
    utmb_scores, itra_scores, betrail_scores, score_summary = aggregate_scores_by_input(all_records)
    score_summary["participants"] = participants_count
    stamp = generated_at or datetime.now(UTC)
    return {
        "report_kind": RACE_REPORT_KIND,
        "title": title,
        "participants_count": participants_count,
        "rows_evaluated": len(all_records),
        "qualified_count": len(qualified_records),
        "strategy": strategy,
        "same_name_mode": same_name_mode,
        "rows": records_to_rows(ranked_qualified, top=max(1, top)),
        "export_rows": records_to_rows(ranked_all, top=len(ranked_all)),
        "no_result_names": no_result_names,
        "utmb_scores": utmb_scores,
        "itra_scores": itra_scores,
        "betrail_scores": betrail_scores,
        "score_summary": score_summary,
        "stale_provider_fallback_used": stale_provider_fallback_used,
        "race_url": race_url,
        "competition_name": competition_name,
        "score_threshold": score_threshold,
        "sort_by": sort_by,
        "generated_at": stamp.astimezone(UTC).isoformat(),
    }


def _table_link(url: str, label: str) -> str:
    safe_url = html.escape(url, quote=True)
    safe_label = html.escape(label)
    return f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_label}</a>'


def _render_top_rows_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty-state">No athletes above the configured threshold were found.</div>'

    table_rows: list[str] = []
    for row in rows:
        utmb_profile = str(row.get("UTMB Profile") or "").strip()
        itra_profile = str(row.get("ITRA Profile") or "").strip()
        betrail_profile = str(row.get("Betrail Profile") or "").strip()
        utmb_link = _table_link(utmb_profile, "UTMB") if utmb_profile else ""
        itra_link = _table_link(itra_profile, "ITRA") if itra_profile else ""
        betrail_link = _table_link(betrail_profile, "Betrail") if betrail_profile else ""
        table_rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{int(row.get('Rank', 0))}</td>",
                    f"<td>{html.escape(str(row.get('Athlete', '')))}</td>",
                    f"<td>{html.escape(str(row.get('UTMB', '')))}</td>",
                    f"<td>{html.escape(str(row.get('ITRA', '')))}</td>",
                    f"<td>{html.escape(str(row.get('Betrail', '')))}</td>",
                    f"<td>{html.escape(str(row.get('Combined', '')))}</td>",
                    f"<td>{html.escape(str(row.get('UTMB Matched Name', '')))}</td>",
                    f"<td>{html.escape(str(row.get('ITRA Matched Name', '')))}</td>",
                    f"<td>{html.escape(str(row.get('Betrail Matched Name', '')))}</td>",
                    f"<td>{utmb_link}</td>",
                    f"<td>{itra_link}</td>",
                    f"<td>{betrail_link}</td>",
                    f"<td>{html.escape(str(row.get('Notes', '')))}</td>",
                    "</tr>",
                ]
            )
        )
    return "".join(
        [
            '<div class="table-wrap"><table class="results-table">',
            "<thead><tr>",
            "<th>Rank</th><th>Athlete</th><th>UTMB</th><th>ITRA</th><th>Betrail</th><th>Combined</th>",
            "<th>UTMB Matched Name</th><th>ITRA Matched Name</th><th>Betrail Matched Name</th>",
            "<th>UTMB Profile</th><th>ITRA Profile</th><th>Betrail Profile</th><th>Notes</th>",
            "</tr></thead><tbody>",
            "".join(table_rows),
            "</tbody></table></div>",
        ]
    )


def _render_histogram(title: str, scores: list[float]) -> str:
    histogram_rows = build_score_histogram([float(score) for score in scores if score is not None])
    if not histogram_rows:
        return (
            f'<section class="chart-card"><h3>{html.escape(title)}</h3>'
            '<div class="empty-state">No scores available.</div></section>'
        )

    max_count = max(int(row["count"]) for row in histogram_rows) or 1
    items: list[str] = []
    for row in histogram_rows:
        count = int(row["count"])
        width = max(6.0, (count / max_count) * 100.0) if count else 0.0
        items.append(
            "".join(
                [
                    '<div class="hist-row">',
                    f'<div class="hist-label">{html.escape(str(row["range"]))}</div>',
                    '<div class="hist-bar-track">',
                    (
                        f'<div class="hist-bar" style="width: {width:.2f}%">'
                        f'<span>{count}</span></div>'
                        if count
                        else '<div class="hist-bar hist-bar-empty"></div>'
                    ),
                    "</div>",
                    f'<div class="hist-count">{count}</div>',
                    "</div>",
                ]
            )
        )
    return (
        f'<section class="chart-card"><h3>{html.escape(title)}</h3>'
        + "".join(items)
        + "</section>"
    )


def _render_no_result_section(no_result_names: list[str]) -> str:
    if not no_result_names:
        return (
            '<section class="panel"><h2>No result on UTMB, ITRA, and Betrail</h2>'
            '<div class="empty-state">All participants matched at least one provider.</div></section>'
        )

    rows = "".join(
        f"<tr><td>{html.escape(name)}</td></tr>"
        for name in no_result_names
    )
    return (
        '<section class="panel"><h2>No result on UTMB, ITRA, and Betrail</h2>'
        f'<p class="section-caption">{len(no_result_names)} participant(s) had no match on any provider.</p>'
        '<div class="table-wrap compact-table"><table class="results-table">'
        '<thead><tr><th>Athlete</th></tr></thead><tbody>'
        f"{rows}</tbody></table></div></section>"
    )


def _parse_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _render_generated_meta(snapshot: dict[str, object]) -> str:
    generated_at = _parse_iso_timestamp(snapshot.get("generated_at"))
    race_url = str(snapshot.get("race_url") or "").strip()
    competition_name = str(snapshot.get("competition_name") or "").strip()
    bits: list[str] = []
    if generated_at is not None:
        bits.append(f"Generated {generated_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    if competition_name:
        bits.append(f"Competition: {competition_name}")
    if race_url:
        safe_url = html.escape(race_url, quote=True)
        bits.append(
            f'Race source: <a href="{safe_url}" target="_blank" rel="noopener noreferrer">open source page</a>'
        )
    return "<p class=\"meta-line\">" + " | ".join(bits) + "</p>" if bits else ""


def render_report_html(
    snapshot: dict[str, object],
    *,
    csv_filename: str = REPORT_CSV_FILENAME,
    json_filename: str = REPORT_JSON_FILENAME,
) -> str:
    title = html.escape(str(snapshot.get("title", "Trail Race Report")))
    rows = snapshot.get("rows", [])
    if not isinstance(rows, list):
        rows = []
    no_result_names = snapshot.get("no_result_names", [])
    if not isinstance(no_result_names, list):
        no_result_names = []

    score_summary = snapshot.get("score_summary", {})
    if not isinstance(score_summary, dict):
        score_summary = {}
    utmb_scores = snapshot.get("utmb_scores", [])
    if not isinstance(utmb_scores, list):
        utmb_scores = []
    itra_scores = snapshot.get("itra_scores", [])
    if not isinstance(itra_scores, list):
        itra_scores = []
    betrail_scores = snapshot.get("betrail_scores", [])
    if not isinstance(betrail_scores, list):
        betrail_scores = []

    metrics = {
        "Input participants": int(snapshot.get("participants_count", 0) or 0),
        "Rows evaluated": int(snapshot.get("rows_evaluated", 0) or 0),
        "Qualified (> threshold)": int(snapshot.get("qualified_count", 0) or 0),
        "Strategy": str(snapshot.get("strategy", "participant-first")),
        "Same-name mode": str(snapshot.get("same_name_mode", "highest")),
    }
    metric_cards = "".join(
        f'<div class="metric-card"><div class="metric-label">{html.escape(label)}</div>'
        f'<div class="metric-value">{html.escape(str(value))}</div></div>'
        for label, value in metrics.items()
    )

    score_cards = "".join(
        [
            f'<div class="metric-card"><div class="metric-label">Participants</div><div class="metric-value">{int(score_summary.get("participants", metrics["Input participants"]))}</div></div>',
            f'<div class="metric-card"><div class="metric-label">With UTMB</div><div class="metric-value">{int(score_summary.get("with_utmb", len(utmb_scores)))}</div></div>',
            f'<div class="metric-card"><div class="metric-label">With ITRA</div><div class="metric-value">{int(score_summary.get("with_itra", len(itra_scores)))}</div></div>',
            f'<div class="metric-card"><div class="metric-label">With Betrail</div><div class="metric-value">{int(score_summary.get("with_betrail", len(betrail_scores)))}</div></div>',
            f'<div class="metric-card"><div class="metric-label">With Any Score</div><div class="metric-value">{int(score_summary.get("with_any", max(len(utmb_scores), len(itra_scores), len(betrail_scores))))}</div></div>',
        ]
    )

    stale_markup = ""
    if snapshot.get("stale_provider_fallback_used"):
        stale_markup = (
            '<div class="warning">Some provider results were served from stale '
            "score-repo snapshots because live lookups failed.</div>"
        )

    csv_href = html.escape(csv_filename, quote=True)
    json_href = html.escape(json_filename, quote=True)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f1ea;
      --panel: #fffdfa;
      --panel-strong: #ffffff;
      --text: #1f1f1b;
      --muted: #625d53;
      --accent: #0b6b5f;
      --accent-soft: #d8efe7;
      --border: #ddd3c2;
      --warning-bg: #fff0cc;
      --warning-text: #724b00;
      --shadow: 0 16px 40px rgba(37, 28, 11, 0.08);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top, #fbf7ef 0%, var(--bg) 55%, #ebe3d4 100%); color: var(--text); }}
    a {{ color: var(--accent); }}
    .page {{ max-width: 1280px; margin: 0 auto; padding: 32px 20px 48px; }}
    .hero {{ background: linear-gradient(135deg, rgba(11,107,95,0.12), rgba(148,95,32,0.16)); border: 1px solid rgba(11,107,95,0.12); border-radius: 24px; padding: 28px; box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(1.9rem, 4vw, 3rem); line-height: 1.1; }}
    .hero p {{ margin: 8px 0 0; color: var(--muted); }}
    .meta-line {{ color: var(--muted); margin: 10px 0 0; }}
    .download-row {{ display: flex; flex-wrap: wrap; gap: 12px; margin-top: 20px; }}
    .download-link {{ display: inline-flex; align-items: center; justify-content: center; min-width: 180px; padding: 12px 16px; background: var(--panel-strong); border: 1px solid var(--border); border-radius: 999px; text-decoration: none; font-weight: 700; box-shadow: 0 10px 24px rgba(0,0,0,0.05); }}
    .metrics, .score-metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-top: 20px; }}
    .metric-card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 18px; padding: 16px; box-shadow: 0 10px 26px rgba(0,0,0,0.04); }}
    .metric-label {{ color: var(--muted); font-size: 0.9rem; }}
    .metric-value {{ margin-top: 6px; font-size: 1.45rem; font-weight: 700; }}
    .section-grid {{ display: grid; gap: 18px; margin-top: 24px; }}
    .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 24px; padding: 22px; box-shadow: var(--shadow); }}
    .panel h2 {{ margin: 0 0 14px; font-size: 1.4rem; }}
    .section-caption {{ margin-top: -4px; color: var(--muted); }}
    .warning {{ margin-top: 16px; padding: 14px 16px; border-radius: 16px; background: var(--warning-bg); color: var(--warning-text); border: 1px solid rgba(114,75,0,0.18); }}
    .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; margin-top: 18px; }}
    .chart-card {{ border: 1px solid var(--border); border-radius: 18px; background: var(--panel-strong); padding: 16px; }}
    .chart-card h3 {{ margin: 0 0 14px; font-size: 1.05rem; }}
    .hist-row {{ display: grid; grid-template-columns: 88px minmax(0, 1fr) 48px; gap: 10px; align-items: center; margin-top: 10px; }}
    .hist-label, .hist-count {{ color: var(--muted); font-size: 0.9rem; }}
    .hist-bar-track {{ width: 100%; min-height: 26px; background: #f1ece1; border-radius: 999px; overflow: hidden; }}
    .hist-bar {{ min-height: 26px; border-radius: 999px; background: linear-gradient(90deg, #0b6b5f, #1ea18f); color: white; display: flex; align-items: center; justify-content: flex-end; padding-right: 10px; font-weight: 700; font-size: 0.84rem; }}
    .hist-bar-empty {{ background: transparent; }}
    .results-table {{ width: 100%; border-collapse: collapse; font-size: 0.95rem; }}
    .results-table th, .results-table td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    .results-table th {{ font-size: 0.84rem; letter-spacing: 0.02em; text-transform: uppercase; color: var(--muted); background: #faf6ee; position: sticky; top: 0; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 18px; background: var(--panel-strong); }}
    .compact-table {{ max-width: 420px; }}
    .empty-state {{ padding: 16px; border: 1px dashed var(--border); border-radius: 16px; color: var(--muted); background: rgba(255,255,255,0.35); }}
    @media (max-width: 760px) {{
      .page {{ padding: 18px 14px 28px; }}
      .hero {{ padding: 22px; border-radius: 20px; }}
      .hist-row {{ grid-template-columns: 76px minmax(0, 1fr) 40px; }}
      .results-table {{ font-size: 0.9rem; }}
      .results-table th, .results-table td {{ padding: 8px 10px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{title}</h1>
      <p>Static race report generated from the TrailIntel CLI.</p>
      {_render_generated_meta(snapshot)}
      <div class="download-row">
        <a class="download-link" href="{csv_href}">Download CSV</a>
        <a class="download-link" href="{json_href}">Download JSON</a>
      </div>
      <div class="metrics">{metric_cards}</div>
      {stale_markup}
    </section>

    <section class="panel section-grid">
      <div>
        <h2>Score Distribution</h2>
        <div class="score-metrics">{score_cards}</div>
      </div>
      <div class="charts">
        {_render_histogram('UTMB Index', [float(value) for value in utmb_scores if value is not None])}
        {_render_histogram('ITRA Score', [float(value) for value in itra_scores if value is not None])}
        {_render_histogram('Betrail Score', [float(value) for value in betrail_scores if value is not None])}
      </div>
    </section>

    <section class="panel">
      <h2>Top Athletes</h2>
      <p class="section-caption">Showing up to the configured top rows for athletes above the threshold.</p>
      {_render_top_rows_table(rows)}
    </section>

    {_render_no_result_section([str(name) for name in no_result_names if str(name).strip()])}
  </main>
</body>
</html>
"""


def build_report_metadata(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "report_kind": snapshot.get("report_kind", RACE_REPORT_KIND),
        "title": snapshot.get("title", "Trail Race Report"),
        "generated_at": snapshot.get("generated_at"),
        "participants_count": int(snapshot.get("participants_count", 0) or 0),
        "rows_evaluated": int(snapshot.get("rows_evaluated", 0) or 0),
        "qualified_count": int(snapshot.get("qualified_count", 0) or 0),
        "strategy": snapshot.get("strategy", "participant-first"),
        "same_name_mode": snapshot.get("same_name_mode", "highest"),
        "race_url": snapshot.get("race_url", ""),
        "competition_name": snapshot.get("competition_name", ""),
        "score_threshold": snapshot.get("score_threshold"),
    }


def _export_rows_from_snapshot(snapshot: dict[str, object], destination: Path) -> None:
    rows = snapshot.get("export_rows", [])
    if not isinstance(rows, list):
        rows = []
    if destination.suffix.lower() == ".json":
        destination.write_text(
            json.dumps(rows, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return
    if not rows:
        destination.write_text("", encoding="utf-8")
        return
    headers = [str(key) for key in rows[0].keys()]
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            if isinstance(row, dict):
                writer.writerow({header: row.get(header, "") for header in headers})


def export_report_site(
    *,
    snapshot: dict[str, object],
    records: list[AthleteRecord] | None,
    destination: str | Path,
) -> Path:
    path = Path(destination)
    path.mkdir(parents=True, exist_ok=True)
    if records is None:
        _export_rows_from_snapshot(snapshot, path / REPORT_CSV_FILENAME)
        _export_rows_from_snapshot(snapshot, path / REPORT_JSON_FILENAME)
    else:
        export_records(records, path / REPORT_CSV_FILENAME)
        export_records(records, path / REPORT_JSON_FILENAME)
    (path / REPORT_SNAPSHOT_FILENAME).write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / REPORT_META_FILENAME).write_text(
        json.dumps(build_report_metadata(snapshot), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / REPORT_HTML_FILENAME).write_text(
        render_report_html(snapshot),
        encoding="utf-8",
    )
    return path


def _sorted_section_entries(
    site_root: Path,
    *,
    section: str,
    report_kind: str,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    section_root = site_root / section
    if not section_root.exists():
        return []

    for meta_path in section_root.rglob(REPORT_META_FILENAME):
        if "latest" in meta_path.parts:
            continue
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        relative_dir = meta_path.parent.relative_to(site_root)
        payload.setdefault("report_kind", report_kind)
        payload.setdefault("report_path", f"{relative_dir.as_posix()}/{REPORT_HTML_FILENAME}")
        if report_kind == RACE_REPORT_KIND:
            payload.setdefault("csv_path", f"{relative_dir.as_posix()}/{REPORT_CSV_FILENAME}")
            payload.setdefault("json_path", f"{relative_dir.as_posix()}/{REPORT_JSON_FILENAME}")
        else:
            payload.setdefault("json_path", f"{relative_dir.as_posix()}/{REPORT_SNAPSHOT_FILENAME}")
        entries.append(payload)

    def sort_key(item: dict[str, object]) -> tuple[str, str]:
        published = str(item.get("published_at") or item.get("generated_at") or "")
        return (published, str(item.get("title") or ""))

    entries.sort(key=sort_key, reverse=True)
    return entries


def _section_relative_path(path: object, *, section: str) -> str:
    text = str(path or "").strip()
    if not text:
        return "#"
    prefix = f"{section}/"
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def render_site_index(entries: list[dict[str, object]], *, site_title: str = "Trail Race Reports") -> str:
    cards: list[str] = []
    for entry in entries:
        title = html.escape(str(entry.get("title") or "Trail Race Report"))
        published_at = _parse_iso_timestamp(entry.get("published_at") or entry.get("generated_at"))
        published_text = (
            published_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
            if published_at is not None
            else "Unknown time"
        )
        participants = int(entry.get("participants_count", 0) or 0)
        qualified = int(entry.get("qualified_count", 0) or 0)
        strategy = html.escape(str(entry.get("strategy") or "participant-first"))
        report_path = html.escape(
            _section_relative_path(entry.get("report_path"), section=REPORTS_SECTION_DIR),
            quote=True,
        )
        csv_path = html.escape(
            _section_relative_path(entry.get("csv_path"), section=REPORTS_SECTION_DIR),
            quote=True,
        )
        json_path = html.escape(
            _section_relative_path(entry.get("json_path"), section=REPORTS_SECTION_DIR),
            quote=True,
        )
        cards.append(
            f"""
            <article class="report-card">
              <div class="report-card-head">
                <h2><a href="{report_path}">{title}</a></h2>
                <p>{published_text}</p>
              </div>
              <div class="report-card-metrics">
                <span>Participants: <strong>{participants}</strong></span>
                <span>Qualified: <strong>{qualified}</strong></span>
                <span>Strategy: <strong>{strategy}</strong></span>
              </div>
              <div class="report-card-links">
                <a href="{report_path}">Open report</a>
                <a href="{csv_path}">CSV</a>
                <a href="{json_path}">JSON</a>
              </div>
            </article>
            """
        )

    if not cards:
        cards.append('<div class="empty-state">No published reports yet.</div>')

    safe_title = html.escape(site_title)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: #fffdf9;
      --text: #1d1f1a;
      --muted: #645d52;
      --accent: #165d52;
      --border: #ddd1bc;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: linear-gradient(180deg, #faf7f0 0%, var(--bg) 100%); color: var(--text); }}
    .page {{ max-width: 980px; margin: 0 auto; padding: 28px 18px 42px; }}
    .hero {{ padding: 24px; background: var(--panel); border: 1px solid var(--border); border-radius: 24px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(1.8rem, 3.5vw, 2.7rem); }}
    .hero p {{ margin: 0; color: var(--muted); }}
    .report-list {{ display: grid; gap: 14px; margin-top: 20px; }}
    .report-card {{ padding: 18px; background: var(--panel); border: 1px solid var(--border); border-radius: 20px; }}
    .report-card-head h2 {{ margin: 0; font-size: 1.25rem; }}
    .report-card-head p {{ margin: 6px 0 0; color: var(--muted); }}
    .report-card-metrics {{ display: flex; flex-wrap: wrap; gap: 14px; margin-top: 12px; color: var(--muted); }}
    .report-card-links {{ display: flex; flex-wrap: wrap; gap: 14px; margin-top: 14px; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .empty-state {{ padding: 16px; border-radius: 16px; border: 1px dashed var(--border); color: var(--muted); background: rgba(255,255,255,0.5); }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{safe_title}</h1>
      <p>Static reports published by the TrailIntel GitHub Actions pipeline.</p>
    </section>
    <section class="report-list">
      {''.join(cards)}
    </section>
  </main>
</body>
</html>
"""


def render_forecast_index(
    entries: list[dict[str, object]],
    *,
    site_title: str = "Route Forecasts",
) -> str:
    cards: list[str] = []
    for entry in entries:
        title = html.escape(str(entry.get("title") or "Route Forecast"))
        published_at = _parse_iso_timestamp(entry.get("published_at") or entry.get("generated_at"))
        published_text = (
            published_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
            if published_at is not None
            else "Unknown time"
        )
        report_path = html.escape(
            _section_relative_path(entry.get("report_path"), section=FORECASTS_SECTION_DIR),
            quote=True,
        )
        png_path = html.escape(
            _section_relative_path(entry.get("png_path"), section=FORECASTS_SECTION_DIR),
            quote=True,
        )
        gpx_path = html.escape(
            _section_relative_path(entry.get("gpx_path"), section=FORECASTS_SECTION_DIR),
            quote=True,
        )
        json_path = html.escape(
            _section_relative_path(entry.get("json_path"), section=FORECASTS_SECTION_DIR),
            quote=True,
        )
        start_time = html.escape(str(entry.get("start_time") or ""))
        duration = html.escape(str(entry.get("duration") or ""))
        distance_km = float(entry.get("route_distance_km", 0.0) or 0.0)
        cards.append(
            f"""
            <article class="report-card">
              <div class="report-card-head">
                <h2><a href="{report_path}">{title}</a></h2>
                <p>{published_text}</p>
              </div>
              <div class="report-card-metrics">
                <span>Start: <strong>{start_time}</strong></span>
                <span>Duration: <strong>{duration}</strong></span>
                <span>Distance: <strong>{distance_km:.2f} km</strong></span>
              </div>
              <div class="report-card-links">
                <a href="{report_path}">Open report</a>
                <a href="{png_path}">PNG</a>
                <a href="{gpx_path}">GPX</a>
                <a href="{json_path}">JSON</a>
              </div>
            </article>
            """
        )

    if not cards:
        cards.append('<div class="empty-state">No published forecasts yet.</div>')

    safe_title = html.escape(site_title)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #f2efe8;
      --panel: #fffdf9;
      --text: #1d1f1a;
      --muted: #645d52;
      --accent: #0e5b85;
      --border: #ddd1bc;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: linear-gradient(180deg, #faf7f0 0%, var(--bg) 100%); color: var(--text); }}
    .page {{ max-width: 980px; margin: 0 auto; padding: 28px 18px 42px; }}
    .hero {{ padding: 24px; background: var(--panel); border: 1px solid var(--border); border-radius: 24px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(1.8rem, 3.5vw, 2.7rem); }}
    .hero p {{ margin: 0; color: var(--muted); }}
    .report-list {{ display: grid; gap: 14px; margin-top: 20px; }}
    .report-card {{ padding: 18px; background: var(--panel); border: 1px solid var(--border); border-radius: 20px; }}
    .report-card-head h2 {{ margin: 0; font-size: 1.25rem; }}
    .report-card-head p {{ margin: 6px 0 0; color: var(--muted); }}
    .report-card-metrics {{ display: flex; flex-wrap: wrap; gap: 14px; margin-top: 12px; color: var(--muted); }}
    .report-card-links {{ display: flex; flex-wrap: wrap; gap: 14px; margin-top: 14px; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .empty-state {{ padding: 16px; border-radius: 16px; border: 1px dashed var(--border); color: var(--muted); background: rgba(255,255,255,0.5); }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>{safe_title}</h1>
      <p>Static route forecasts published by the TrailIntel GitHub Actions pipeline.</p>
    </section>
    <section class="report-list">
      {''.join(cards)}
    </section>
  </main>
</body>
</html>
"""


def render_root_index(
    *,
    race_entries: list[dict[str, object]],
    forecast_entries: list[dict[str, object]],
) -> str:
    race_count = len(race_entries)
    forecast_count = len(forecast_entries)
    latest_race = html.escape(str(race_entries[0].get("title") or "")) if race_entries else "No published race reports yet"
    latest_forecast = (
        html.escape(str(forecast_entries[0].get("title") or ""))
        if forecast_entries
        else "No published forecasts yet"
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TrailIntel Pages</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffdf9;
      --text: #1f1e1a;
      --muted: #625d53;
      --border: #ddd1bc;
      --shadow: 0 18px 40px rgba(30, 24, 17, 0.08);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top, #fbf7ef 0%, var(--bg) 60%, #eae0d2 100%); color: var(--text); }}
    .page {{ max-width: 1040px; margin: 0 auto; padding: 30px 20px 46px; }}
    .hero {{ padding: 28px; background: var(--panel); border: 1px solid var(--border); border-radius: 26px; box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0 0 10px; font-size: clamp(2rem, 4vw, 3rem); }}
    .hero p {{ margin: 0; color: var(--muted); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 18px; margin-top: 24px; }}
    .card {{ padding: 22px; background: var(--panel); border: 1px solid var(--border); border-radius: 22px; box-shadow: var(--shadow); }}
    .card h2 {{ margin: 0 0 8px; }}
    .card p {{ margin: 0; color: var(--muted); }}
    .count {{ margin-top: 16px; font-size: 2.4rem; font-weight: 800; }}
    .links {{ display: flex; gap: 14px; margin-top: 16px; flex-wrap: wrap; }}
    a {{ color: #165d52; text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <h1>TrailIntel Pages</h1>
      <p>Published race reports and route forecasts generated by the TrailIntel workflows.</p>
    </section>
    <section class="grid">
      <article class="card">
        <h2>Race Reports</h2>
        <p>{latest_race}</p>
        <div class="count">{race_count}</div>
        <div class="links">
          <a href="{REPORTS_SECTION_DIR}/index.html">Open race reports</a>
        </div>
      </article>
      <article class="card">
        <h2>Forecast Reports</h2>
        <p>{latest_forecast}</p>
        <div class="count">{forecast_count}</div>
        <div class="links">
          <a href="{FORECASTS_SECTION_DIR}/index.html">Open forecasts</a>
        </div>
      </article>
    </section>
  </main>
</body>
</html>
"""


def refresh_site_index(site_root: str | Path) -> Path:
    root = Path(site_root)
    root.mkdir(parents=True, exist_ok=True)
    reports_root = root / REPORTS_SECTION_DIR
    forecasts_root = root / FORECASTS_SECTION_DIR
    reports_root.mkdir(parents=True, exist_ok=True)
    forecasts_root.mkdir(parents=True, exist_ok=True)

    race_entries = _sorted_section_entries(
        root,
        section=REPORTS_SECTION_DIR,
        report_kind=RACE_REPORT_KIND,
    )
    forecast_entries = _sorted_section_entries(
        root,
        section=FORECASTS_SECTION_DIR,
        report_kind=FORECAST_REPORT_KIND,
    )

    (reports_root / REPORT_HTML_FILENAME).write_text(
        render_site_index(race_entries),
        encoding="utf-8",
    )
    (forecasts_root / REPORT_HTML_FILENAME).write_text(
        render_forecast_index(forecast_entries),
        encoding="utf-8",
    )
    index_path = root / REPORT_HTML_FILENAME
    index_path.write_text(
        render_root_index(race_entries=race_entries, forecast_entries=forecast_entries),
        encoding="utf-8",
    )
    return index_path


def copy_bundle_to_targets(
    *,
    source_dir: str | Path,
    site_root: str | Path,
    report_dir: str,
    latest_dir: str,
    published_metadata: dict[str, object],
    asset_paths: dict[str, str],
) -> None:
    source = Path(source_dir)
    root = Path(site_root)
    root.mkdir(parents=True, exist_ok=True)
    timestamp_target = root / report_dir
    latest_target = root / latest_dir

    for target in (timestamp_target, latest_target):
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target)
        meta_path = target / REPORT_META_FILENAME
        existing: dict[str, Any] = {}
        if meta_path.exists():
            try:
                loaded = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    existing = loaded
            except (OSError, json.JSONDecodeError):
                existing = {}
        combined = {**existing, **published_metadata}
        relative_dir = target.relative_to(root).as_posix()
        for key, filename in asset_paths.items():
            combined[key] = f"{relative_dir}/{filename}"
        meta_path.write_text(json.dumps(combined, indent=2, ensure_ascii=False), encoding="utf-8")

    refresh_site_index(root)


def publish_bundle_to_site(
    *,
    source_dir: str | Path,
    site_root: str | Path,
    report_dir: str,
    latest_dir: str,
    published_metadata: dict[str, object],
) -> dict[str, str]:
    copy_bundle_to_targets(
        source_dir=source_dir,
        site_root=site_root,
        report_dir=report_dir,
        latest_dir=latest_dir,
        published_metadata=published_metadata,
        asset_paths={
            "report_path": REPORT_HTML_FILENAME,
            "csv_path": REPORT_CSV_FILENAME,
            "json_path": REPORT_JSON_FILENAME,
        },
    )
    return {
        "timestamp_report": f"{report_dir}/{REPORT_HTML_FILENAME}",
        "latest_report": f"{latest_dir}/{REPORT_HTML_FILENAME}",
        "timestamp_csv": f"{report_dir}/{REPORT_CSV_FILENAME}",
        "timestamp_json": f"{report_dir}/{REPORT_JSON_FILENAME}",
    }


def default_site_title_from_reports(site_root: str | Path) -> str:
    root = Path(site_root)
    entries = _sorted_section_entries(
        root,
        section=REPORTS_SECTION_DIR,
        report_kind=RACE_REPORT_KIND,
    )
    if not entries:
        return "Trail Race Reports"
    first_title = str(entries[0].get("title") or "").strip()
    if not first_title:
        return "Trail Race Reports"
    base = re.sub(r"\s+Report$", "", first_title).strip()
    return base or "Trail Race Reports"
