from __future__ import annotations

import csv
import html
import json
import math
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
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
HEADING_FONT_STACK = (
    '"Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif'
)
BODY_FONT_STACK = '"Avenir Next", "Segoe UI", sans-serif'

_THEME_TOKENS: dict[str, dict[str, str]] = {
    "hub": {
        "accent": "#8b5e34",
        "accent_strong": "#70461f",
        "accent_soft": "rgba(139, 94, 52, 0.16)",
        "hero_start": "rgba(139, 94, 52, 0.16)",
        "hero_end": "rgba(30, 107, 96, 0.16)",
        "pill_bg": "rgba(139, 94, 52, 0.12)",
        "pill_text": "#70461f",
    },
    "race": {
        "accent": "#0b6b5f",
        "accent_strong": "#084f46",
        "accent_soft": "rgba(11, 107, 95, 0.16)",
        "hero_start": "rgba(11, 107, 95, 0.14)",
        "hero_end": "rgba(155, 101, 40, 0.16)",
        "pill_bg": "rgba(11, 107, 95, 0.12)",
        "pill_text": "#084f46",
    },
    "forecast": {
        "accent": "#0e5b85",
        "accent_strong": "#0b4462",
        "accent_soft": "rgba(14, 91, 133, 0.16)",
        "hero_start": "rgba(14, 91, 133, 0.14)",
        "hero_end": "rgba(31, 59, 92, 0.16)",
        "pill_bg": "rgba(14, 91, 133, 0.12)",
        "pill_text": "#0b4462",
    },
}


def _month_label(month: int) -> str:
    labels = (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    )
    if 1 <= month <= len(labels):
        return labels[month - 1]
    return "Unknown"


def _format_display_datetime(
    value: object,
    *,
    convert_to_utc: bool = False,
    default: str = "",
) -> str:
    dt = value if isinstance(value, datetime) else _parse_iso_timestamp(value)
    if dt is None:
        return default
    if convert_to_utc:
        dt = dt.astimezone(UTC)
    zone = dt.tzname() or "UTC"
    return f"{_month_label(dt.month)} {dt.day}, {dt.year} at {dt:%H:%M} {zone}"


def _format_compact_timestamp(value: object, *, default: str = "") -> str:
    dt = value if isinstance(value, datetime) else _parse_iso_timestamp(value)
    if dt is None:
        return default
    return f"{_month_label(dt.month)} {dt.day}, {dt:%H:%M}"


def _format_duration_label(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.split(":")
    if len(parts) < 2 or not all(part.isdigit() for part in parts[:2]):
        return text
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    chunks: list[str] = []
    if hours:
        chunks.append(f"{hours}h")
    if minutes or not chunks:
        chunks.append(f"{minutes:02d}m")
    if seconds:
        chunks.append(f"{seconds:02d}s")
    return " ".join(chunks)


def _format_threshold_label(value: object) -> str:
    if value is None or value == "":
        return "Not set"
    try:
        number = float(value)
    except TypeError, ValueError:
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.1f}"


def _friendly_strategy_label(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "Participant First"
    return text.replace("-", " ").title()


def _render_meta_row(bits: list[str]) -> str:
    cleaned = [bit for bit in bits if bit]
    if not cleaned:
        return ""
    chips = "".join(f'<span class="meta-chip">{bit}</span>' for bit in cleaned)
    return f'<div class="meta-row">{chips}</div>'


def _render_action_link(href: str, label: str, *, primary: bool = False) -> str:
    class_name = "action-link primary" if primary else "action-link secondary"
    return f'<a class="{class_name}" href="{html.escape(href, quote=True)}">{html.escape(label)}</a>'


def _render_metric_cards(items: list[tuple[str, str, str | None]]) -> str:
    cards: list[str] = []
    for label, value, subvalue in items:
        sub_markup = (
            f'<div class="metric-subvalue">{html.escape(subvalue)}</div>'
            if subvalue
            else ""
        )
        cards.append(
            '<div class="metric-card">'
            f'<div class="metric-label">{html.escape(label)}</div>'
            f'<div class="metric-value">{html.escape(value)}</div>'
            f"{sub_markup}"
            "</div>"
        )
    return "".join(cards)


def _render_pills(items: list[str]) -> str:
    cleaned = [item for item in items if item]
    if not cleaned:
        return ""
    return "".join(f'<span class="pill">{html.escape(item)}</span>' for item in cleaned)


def _render_site_nav(
    *,
    home_href: str,
    reports_href: str,
    forecasts_href: str,
    active: str,
) -> str:
    items = [
        ("home", "Home", home_href),
        ("reports", "Race Reports", reports_href),
        ("forecasts", "Forecasts", forecasts_href),
    ]
    links: list[str] = []
    for key, label, href in items:
        class_name = "nav-link is-active" if key == active else "nav-link"
        current = ' aria-current="page"' if key == active else ""
        links.append(
            f'<a class="{class_name}" href="{html.escape(href, quote=True)}"{current}>{html.escape(label)}</a>'
        )
    return (
        '<nav class="site-nav" aria-label="Primary">'
        f'<a class="nav-brand" href="{html.escape(home_href, quote=True)}">TrailIntel Pages</a>'
        f'<div class="nav-links">{"".join(links)}</div>'
        "</nav>"
    )


def _shared_page_styles(theme: str) -> str:
    tokens = _THEME_TOKENS[theme]
    return f"""
    :root {{
      color-scheme: light;
      --page-bg: #f3ece2;
      --panel: rgba(255, 252, 247, 0.92);
      --panel-strong: #fffdf9;
      --panel-soft: #f8f2ea;
      --text: #1f1c18;
      --muted: #6a6056;
      --line: rgba(117, 98, 74, 0.20);
      --line-strong: rgba(117, 98, 74, 0.32);
      --shadow: 0 24px 64px rgba(35, 24, 9, 0.10);
      --accent: {tokens["accent"]};
      --accent-strong: {tokens["accent_strong"]};
      --accent-soft: {tokens["accent_soft"]};
      --hero-start: {tokens["hero_start"]};
      --hero-end: {tokens["hero_end"]};
      --pill-bg: {tokens["pill_bg"]};
      --pill-text: {tokens["pill_text"]};
      --warning-bg: #fff3d4;
      --warning-text: #734700;
      --heading-font: {HEADING_FONT_STACK};
      --body-font: {BODY_FONT_STACK};
      font-family: var(--body-font);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(255, 255, 255, 0.72), transparent 28%),
        radial-gradient(circle at top right, var(--accent-soft), transparent 32%),
        radial-gradient(circle at top, #fbf7f1 0%, var(--page-bg) 54%, #e7dccb 100%);
    }}
    a {{
      color: var(--accent-strong);
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
    .page {{
      max-width: 1260px;
      margin: 0 auto;
      padding: 22px 18px 56px;
    }}
    .site-nav {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .nav-brand {{
      font-family: var(--heading-font);
      font-size: 1.3rem;
      font-weight: 700;
      color: var(--text);
      letter-spacing: 0.01em;
    }}
    .nav-links {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .nav-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      padding: 0 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.62);
      color: var(--text);
      font-weight: 600;
    }}
    .nav-link:hover {{
      text-decoration: none;
      border-color: var(--accent);
    }}
    .nav-link.is-active {{
      color: #ffffff;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      border-color: transparent;
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.10);
    }}
    .hero {{
      position: relative;
      overflow: hidden;
      padding: 34px 32px;
      border-radius: 30px;
      border: 1px solid var(--line);
      background:
        linear-gradient(135deg, var(--hero-start), var(--hero-end)),
        var(--panel);
      box-shadow: var(--shadow);
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -48px -80px auto;
      width: 220px;
      height: 220px;
      background: radial-gradient(circle, var(--accent-soft) 0%, transparent 72%);
      opacity: 0.9;
      pointer-events: none;
    }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent-strong);
      font-size: 0.77rem;
      font-weight: 800;
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }}
    h1, h2, h3 {{
      font-family: var(--heading-font);
      letter-spacing: -0.01em;
    }}
    .hero h1 {{
      margin: 0;
      max-width: 14ch;
      font-size: clamp(2.2rem, 4vw, 4.1rem);
      line-height: 0.96;
    }}
    .hero-lead {{
      margin: 14px 0 0;
      max-width: 64ch;
      color: var(--muted);
      font-size: 1.04rem;
      line-height: 1.6;
    }}
    .meta-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 12px;
      margin-top: 18px;
    }}
    .meta-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 36px;
      padding: 7px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.60);
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.35;
    }}
    .action-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 22px;
    }}
    .action-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 46px;
      padding: 0 16px;
      border-radius: 999px;
      border: 1px solid var(--line-strong);
      background: var(--panel-strong);
      color: var(--text);
      font-weight: 700;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.06);
    }}
    .action-link:hover {{
      text-decoration: none;
      border-color: var(--accent);
    }}
    .action-link.primary {{
      color: #ffffff;
      border-color: transparent;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 14px;
      margin-top: 22px;
    }}
    .metric-card {{
      min-height: 122px;
      padding: 16px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.78);
      box-shadow: 0 14px 32px rgba(0, 0, 0, 0.05);
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 0.84rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    .metric-value {{
      margin-top: 10px;
      font-size: clamp(1.3rem, 2vw, 1.9rem);
      line-height: 1.1;
      font-weight: 800;
    }}
    .metric-subvalue {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.94rem;
      line-height: 1.45;
    }}
    .section-stack {{
      display: grid;
      gap: 20px;
      margin-top: 24px;
    }}
    .panel {{
      padding: 24px;
      border-radius: 28px;
      border: 1px solid var(--line);
      background: var(--panel);
      box-shadow: var(--shadow);
    }}
    .panel-head {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: baseline;
      gap: 10px 16px;
      margin-bottom: 14px;
    }}
    .panel h2 {{
      margin: 0;
      font-size: clamp(1.5rem, 2vw, 2.05rem);
      line-height: 1.02;
    }}
    .section-caption {{
      margin: 0;
      color: var(--muted);
      font-size: 0.98rem;
      line-height: 1.55;
    }}
    .collection-grid {{
      display: grid;
      gap: 16px;
    }}
    .collection-card {{
      padding: 22px;
      border-radius: 24px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.76);
      box-shadow: 0 14px 32px rgba(0, 0, 0, 0.05);
    }}
    .card-top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
    }}
    .card-title {{
      margin: 0;
      font-size: clamp(1.4rem, 2vw, 1.9rem);
      line-height: 1.04;
    }}
    .card-copy {{
      margin: 10px 0 0;
      color: var(--muted);
      line-height: 1.55;
    }}
    .card-meta {{
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.93rem;
    }}
    .pill-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 999px;
      background: var(--pill-bg);
      color: var(--pill-text);
      font-size: 0.88rem;
      font-weight: 700;
      line-height: 1.2;
    }}
    .warning {{
      margin-top: 18px;
      padding: 14px 16px;
      border-radius: 18px;
      border: 1px solid rgba(115, 71, 0, 0.20);
      background: var(--warning-bg);
      color: var(--warning-text);
      line-height: 1.55;
    }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
      margin-top: 18px;
    }}
    .chart-card {{
      padding: 18px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: var(--panel-strong);
    }}
    .chart-card h3 {{
      margin: 0 0 14px;
      font-size: 1.16rem;
    }}
    .hist-row {{
      display: grid;
      grid-template-columns: 88px minmax(0, 1fr) 52px;
      gap: 10px;
      align-items: center;
      margin-top: 10px;
    }}
    .hist-label,
    .hist-count {{
      color: var(--muted);
      font-size: 0.9rem;
      font-variant-numeric: tabular-nums;
    }}
    .hist-bar-track {{
      min-height: 28px;
      overflow: hidden;
      border-radius: 999px;
      background: var(--panel-soft);
    }}
    .hist-bar {{
      min-height: 28px;
      display: flex;
      align-items: center;
      justify-content: flex-end;
      padding-right: 10px;
      border-radius: 999px;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      color: #ffffff;
      font-size: 0.84rem;
      font-weight: 800;
      font-variant-numeric: tabular-nums;
    }}
    .hist-bar-empty {{
      background: transparent;
    }}
    .table-wrap {{
      overflow-x: auto;
      border-radius: 20px;
      border: 1px solid var(--line);
      background: var(--panel-strong);
    }}
    .results-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.94rem;
    }}
    .results-table th,
    .results-table td {{
      padding: 11px 12px;
      border-bottom: 1px solid rgba(117, 98, 74, 0.14);
      text-align: left;
      vertical-align: top;
    }}
    .results-table th {{
      position: sticky;
      top: 0;
      z-index: 1;
      background: #fbf6ef;
      color: var(--muted);
      font-size: 0.79rem;
      letter-spacing: 0.05em;
      text-transform: uppercase;
    }}
    .results-table tbody tr:nth-child(even) {{
      background: rgba(248, 242, 234, 0.65);
    }}
    .rank-cell,
    .score-cell {{
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .athlete-cell strong {{
      display: inline-block;
      font-size: 1rem;
    }}
    .supporting-cell,
    .link-cell,
    .notes-cell {{
      color: var(--muted);
      font-size: 0.88rem;
      line-height: 1.45;
    }}
    .notes-cell {{
      min-width: 180px;
    }}
    .compact-table {{
      max-width: 480px;
    }}
    .chart-frame {{
      padding: 14px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: var(--panel-soft);
    }}
    .chart-frame img {{
      display: block;
      width: 100%;
      border-radius: 14px;
    }}
    .empty-state {{
      padding: 16px;
      border-radius: 18px;
      border: 1px dashed var(--line-strong);
      background: rgba(255, 255, 255, 0.45);
      color: var(--muted);
      line-height: 1.5;
    }}
    @media (max-width: 900px) {{
      .page {{ padding: 16px 12px 34px; }}
      .site-nav {{ flex-direction: column; align-items: flex-start; }}
      .hero {{ padding: 26px 22px; }}
      .panel {{ padding: 20px; }}
      .card-top {{ flex-direction: column; }}
      .hist-row {{ grid-template-columns: 72px minmax(0, 1fr) 42px; }}
    }}
    @media (max-width: 640px) {{
      .hero h1 {{ max-width: none; }}
      .meta-chip {{ white-space: normal; }}
      .results-table th,
      .results-table td {{ padding: 9px 10px; }}
    }}
    """


def _render_document(
    *,
    title: str,
    theme: str,
    active_nav: str,
    home_href: str,
    reports_href: str,
    forecasts_href: str,
    body_html: str,
) -> str:
    safe_title = html.escape(title)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    {_shared_page_styles(theme)}
  </style>
</head>
<body>
  <main class="page">
    {
        _render_site_nav(
            home_href=home_href,
            reports_href=reports_href,
            forecasts_href=forecasts_href,
            active=active_nav,
        )
    }
    {body_html}
  </main>
</body>
</html>
"""


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
        status = match_presence.setdefault(
            name, {"utmb": False, "itra": False, "betrail": False}
        )
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

    utmb_scores = [
        bucket["utmb"] for bucket in by_input.values() if bucket["utmb"] is not None
    ]
    itra_scores = [
        bucket["itra"] for bucket in by_input.values() if bucket["itra"] is not None
    ]
    betrail_scores = [
        bucket["betrail"]
        for bucket in by_input.values()
        if bucket["betrail"] is not None
    ]
    with_any = sum(
        1
        for bucket in by_input.values()
        if bucket["utmb"] is not None
        or bucket["itra"] is not None
        or bucket["betrail"] is not None
    )
    summary = {
        "participants": len(by_input),
        "with_utmb": len(utmb_scores),
        "with_itra": len(itra_scores),
        "with_betrail": len(betrail_scores),
        "with_any": with_any,
    }
    return utmb_scores, itra_scores, betrail_scores, summary


def build_score_histogram(
    scores: list[float], *, bin_size: int = 50
) -> list[dict[str, int | str]]:
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


def records_to_rows(
    records: list[AthleteRecord], *, top: int
) -> list[dict[str, object]]:
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
    utmb_scores, itra_scores, betrail_scores, score_summary = aggregate_scores_by_input(
        all_records
    )
    score_summary["participants"] = participants_count
    stamp = generated_at or datetime.now(UTC)
    return {
        "report_kind": RACE_REPORT_KIND,
        "title": title,
        "participants_count": participants_count,
        "rows_evaluated": len(all_records),
        "qualified_count": len(qualified_records),
        "strategy": strategy,
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
        betrail_link = (
            _table_link(betrail_profile, "Betrail") if betrail_profile else ""
        )
        table_rows.append(
            "".join(
                [
                    "<tr>",
                    f'<td class="rank-cell">{int(row.get("Rank", 0))}</td>',
                    f'<td class="athlete-cell"><strong>{html.escape(str(row.get("Athlete", "")))}</strong></td>',
                    f'<td class="score-cell">{html.escape(str(row.get("UTMB", "")))}</td>',
                    f'<td class="score-cell">{html.escape(str(row.get("ITRA", "")))}</td>',
                    f'<td class="score-cell">{html.escape(str(row.get("Betrail", "")))}</td>',
                    f'<td class="score-cell">{html.escape(str(row.get("Combined", "")))}</td>',
                    f'<td class="supporting-cell">{html.escape(str(row.get("UTMB Matched Name", "")))}</td>',
                    f'<td class="supporting-cell">{html.escape(str(row.get("ITRA Matched Name", "")))}</td>',
                    f'<td class="supporting-cell">{html.escape(str(row.get("Betrail Matched Name", "")))}</td>',
                    f'<td class="link-cell">{utmb_link or "&mdash;"}</td>',
                    f'<td class="link-cell">{itra_link or "&mdash;"}</td>',
                    f'<td class="link-cell">{betrail_link or "&mdash;"}</td>',
                    f'<td class="notes-cell">{html.escape(str(row.get("Notes", ""))) or "&mdash;"}</td>',
                    "</tr>",
                ]
            )
        )
    return "".join(
        [
            '<div class="table-wrap"><table class="results-table">',
            "<thead><tr>",
            "<th>Rank</th><th>Athlete</th><th>UTMB</th><th>ITRA</th><th>Betrail</th><th>Combined</th>",
            "<th>UTMB Match</th><th>ITRA Match</th><th>Betrail Match</th>",
            "<th>UTMB Link</th><th>ITRA Link</th><th>Betrail Link</th><th>Notes</th>",
            "</tr></thead><tbody>",
            "".join(table_rows),
            "</tbody></table></div>",
        ]
    )


def _render_histogram(title: str, scores: list[float]) -> str:
    histogram_rows = build_score_histogram(
        [float(score) for score in scores if score is not None]
    )
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
                        f"<span>{count}</span></div>"
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
            '<section class="panel"><div class="panel-head"><h2>Unmatched Athletes</h2></div>'
            '<div class="empty-state">Every participant matched at least one provider.</div></section>'
        )

    rows = "".join(f"<tr><td>{html.escape(name)}</td></tr>" for name in no_result_names)
    return (
        '<section class="panel"><div class="panel-head"><h2>Unmatched Athletes</h2></div>'
        f'<p class="section-caption">{len(no_result_names)} participant(s) did not match UTMB, ITRA, or Betrail.</p>'
        '<div class="table-wrap compact-table"><table class="results-table">'
        "<thead><tr><th>Athlete</th></tr></thead><tbody>"
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
    race_url = str(snapshot.get("race_url") or "").strip()
    competition_name = str(snapshot.get("competition_name") or "").strip()
    bits: list[str] = []
    generated_text = _format_display_datetime(
        snapshot.get("generated_at"),
        convert_to_utc=True,
    )
    if generated_text:
        bits.append(f"Published {html.escape(generated_text)}")
    if competition_name:
        bits.append(f"Competition {html.escape(competition_name)}")
    if race_url:
        safe_url = html.escape(race_url, quote=True)
        bits.append(
            f'Race source <a href="{safe_url}" target="_blank" rel="noopener noreferrer">open source page</a>'
        )
    return _render_meta_row(bits)


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

    participants_count = int(snapshot.get("participants_count", 0) or 0)
    qualified_count = int(snapshot.get("qualified_count", 0) or 0)
    threshold_label = _format_threshold_label(snapshot.get("score_threshold"))
    score_cards = _render_metric_cards(
        [
            (
                "UTMB matches",
                str(int(score_summary.get("with_utmb", len(utmb_scores)))),
                "Athletes with a UTMB profile or index",
            ),
            (
                "ITRA matches",
                str(int(score_summary.get("with_itra", len(itra_scores)))),
                "Athletes with an ITRA profile or score",
            ),
            (
                "Betrail matches",
                str(int(score_summary.get("with_betrail", len(betrail_scores)))),
                "Athletes with a Betrail profile or score",
            ),
            (
                "Any provider",
                str(
                    int(
                        score_summary.get(
                            "with_any",
                            max(
                                len(utmb_scores), len(itra_scores), len(betrail_scores)
                            ),
                        )
                    )
                ),
                "Participants matched at least once",
            ),
            (
                "Unmatched",
                str(len([name for name in no_result_names if str(name).strip()])),
                "Participants with no provider match",
            ),
        ]
    )

    hero_metrics = _render_metric_cards(
        [
            (
                "Athletes screened",
                str(participants_count),
                f"{int(snapshot.get('rows_evaluated', participants_count) or participants_count)} rows evaluated",
            ),
            (
                "Above threshold",
                str(qualified_count),
                "Leaderboard entries surfaced",
            ),
            (
                "Score threshold",
                threshold_label,
                "Combined-score cutoff",
            ),
            (
                "Ranking strategy",
                _friendly_strategy_label(snapshot.get("strategy", "participant-first")),
                "Ordering rule used for the report",
            ),
            (
                "Coverage",
                str(int(score_summary.get("with_any", 0) or 0)),
                "Participants with at least one provider score",
            ),
        ]
    )

    stale_markup = ""
    if snapshot.get("stale_provider_fallback_used") or snapshot.get("stale_cache_used"):
        stale_markup = (
            '<div class="warning">Some provider results were served from stale '
            "score-repo snapshots because live lookups failed.</div>"
        )

    csv_href = html.escape(csv_filename, quote=True)
    json_href = html.escape(json_filename, quote=True)
    competition_name = str(snapshot.get("competition_name") or "").strip()
    lead = (
        f"Screened {participants_count} entrants"
        + (f" for {competition_name}" if competition_name else "")
        + (
            f" and surfaced {qualified_count} athletes above the {threshold_label} threshold."
            if threshold_label != "Not set"
            else f" and surfaced {qualified_count} athletes with standout combined scores."
        )
    )
    action_row = "".join(
        [
            _render_action_link(csv_href, "Download CSV", primary=True),
            _render_action_link(json_href, "Download JSON"),
        ]
    )

    body_html = f"""
    <section class="hero">
      <p class="eyebrow">Race Report</p>
      <h1>{title}</h1>
      <p class="hero-lead">{html.escape(lead)}</p>
      {_render_generated_meta(snapshot)}
      <div class="action-row">{action_row}</div>
      <div class="metric-grid">{hero_metrics}</div>
      {stale_markup}
    </section>

    <section class="section-stack">
      <section class="panel">
        <div class="panel-head">
          <h2>Field Snapshot</h2>
          <span class="pill">Coverage across UTMB, ITRA, and Betrail</span>
        </div>
        <p class="section-caption">Quick coverage stats plus score distributions for the full participant field.</p>
        <div class="metric-grid">{score_cards}</div>
        <div class="charts">
          {_render_histogram("UTMB Index", [float(value) for value in utmb_scores if value is not None])}
          {_render_histogram("ITRA Score", [float(value) for value in itra_scores if value is not None])}
          {_render_histogram("Betrail Score", [float(value) for value in betrail_scores if value is not None])}
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Leaderboard</h2>
          <span class="pill">Top combined-score athletes</span>
        </div>
        <p class="section-caption">Highest-ranked athletes above the configured threshold, including provider links and matching notes.</p>
        {_render_top_rows_table(rows)}
      </section>

      {_render_no_result_section([str(name) for name in no_result_names if str(name).strip()])}
    </section>
    """
    return _render_document(
        title=str(snapshot.get("title", "Trail Race Report")),
        theme="race",
        active_nav="reports",
        home_href="../../../index.html",
        reports_href="../../index.html",
        forecasts_href="../../../forecasts/index.html",
        body_html=body_html,
    )


def build_report_metadata(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "report_kind": snapshot.get("report_kind", RACE_REPORT_KIND),
        "title": snapshot.get("title", "Trail Race Report"),
        "generated_at": snapshot.get("generated_at"),
        "participants_count": int(snapshot.get("participants_count", 0) or 0),
        "rows_evaluated": int(snapshot.get("rows_evaluated", 0) or 0),
        "qualified_count": int(snapshot.get("qualified_count", 0) or 0),
        "strategy": snapshot.get("strategy", "participant-first"),
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
        except OSError, json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        relative_dir = meta_path.parent.relative_to(site_root)
        payload.setdefault("report_kind", report_kind)
        payload.setdefault(
            "report_path", f"{relative_dir.as_posix()}/{REPORT_HTML_FILENAME}"
        )
        if report_kind == RACE_REPORT_KIND:
            payload.setdefault(
                "csv_path", f"{relative_dir.as_posix()}/{REPORT_CSV_FILENAME}"
            )
            payload.setdefault(
                "json_path", f"{relative_dir.as_posix()}/{REPORT_JSON_FILENAME}"
            )
        else:
            payload.setdefault(
                "json_path", f"{relative_dir.as_posix()}/{REPORT_SNAPSHOT_FILENAME}"
            )
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
        return text[len(prefix) :]
    return text


def render_site_index(
    entries: list[dict[str, object]], *, site_title: str = "Trail Race Reports"
) -> str:
    cards: list[str] = []
    total_participants = 0
    total_qualified = 0
    for entry in entries:
        title = html.escape(str(entry.get("title") or "Trail Race Report"))
        participants = int(entry.get("participants_count", 0) or 0)
        qualified = int(entry.get("qualified_count", 0) or 0)
        total_participants += participants
        total_qualified += qualified
        strategy = _friendly_strategy_label(
            entry.get("strategy") or "participant-first"
        )
        published_text = _format_display_datetime(
            entry.get("published_at") or entry.get("generated_at"),
            convert_to_utc=True,
            default="Unknown time",
        )
        report_path = html.escape(
            _section_relative_path(
                entry.get("report_path"), section=REPORTS_SECTION_DIR
            ),
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
            <article class="collection-card">
              <div class="card-top">
                <div>
                  <p class="eyebrow">Race Report</p>
                  <h2 class="card-title"><a href="{report_path}">{title}</a></h2>
                  <p class="card-copy">Published {html.escape(published_text)}.</p>
                </div>
                <span class="pill">{participants} runners</span>
              </div>
              <div class="pill-row">
                {
                _render_pills(
                    [
                        f"{qualified} above threshold",
                        strategy,
                        "CSV + JSON export",
                    ]
                )
            }
              </div>
              <p class="card-meta">
                Latest published bundle for this race report archive entry.
              </p>
              <div class="action-row">
                {_render_action_link(report_path, "Open report", primary=True)}
                {_render_action_link(csv_path, "CSV")}
                {_render_action_link(json_path, "JSON")}
              </div>
            </article>
            """
        )

    if not cards:
        cards.append('<div class="empty-state">No published race reports yet.</div>')

    latest_text = (
        _format_display_datetime(
            entries[0].get("published_at") or entries[0].get("generated_at"),
            convert_to_utc=True,
            default="No published reports yet",
        )
        if entries
        else "No published reports yet"
    )
    body_html = f"""
    <section class="hero">
      <p class="eyebrow">Race Archive</p>
      <h1>{html.escape(site_title)}</h1>
      <p class="hero-lead">Published TrailIntel race reports with curated score coverage, provider links, and downloadable exports.</p>
      {
        _render_meta_row(
            [
                f"{len(entries)} published report(s)",
                f"Latest publish {html.escape(latest_text)}",
            ]
        )
    }
      <div class="action-row">
        {_render_action_link("../index.html", "Back to home")}
        {_render_action_link("../forecasts/index.html", "Browse forecasts")}
      </div>
      <div class="metric-grid">
        {
        _render_metric_cards(
            [
                (
                    "Published reports",
                    str(len(entries)),
                    "Timestamped race report bundles",
                ),
                (
                    "Participants screened",
                    str(total_participants),
                    "Across all published reports",
                ),
                (
                    "Above threshold",
                    str(total_qualified),
                    "Qualified leaderboard entries",
                ),
            ]
        )
    }
      </div>
    </section>

    <section class="section-stack">
      <section class="panel">
        <div class="panel-head">
          <h2>Published Reports</h2>
          <span class="pill">Newest first</span>
        </div>
        <p class="section-caption">Each card links to the published report plus the archived CSV and JSON exports for the same run.</p>
        <div class="collection-grid">
          {"".join(cards)}
        </div>
      </section>
    </section>
    """
    return _render_document(
        title=site_title,
        theme="race",
        active_nav="reports",
        home_href="../index.html",
        reports_href="index.html",
        forecasts_href="../forecasts/index.html",
        body_html=body_html,
    )


def render_forecast_index(
    entries: list[dict[str, object]],
    *,
    site_title: str = "Route Forecasts",
) -> str:
    cards: list[str] = []
    total_distance = 0.0
    for entry in entries:
        title = html.escape(str(entry.get("title") or "Route Forecast"))
        published_text = _format_display_datetime(
            entry.get("published_at") or entry.get("generated_at"),
            convert_to_utc=True,
            default="Unknown time",
        )
        report_path = html.escape(
            _section_relative_path(
                entry.get("report_path"), section=FORECASTS_SECTION_DIR
            ),
            quote=True,
        )
        png_path = html.escape(
            _section_relative_path(
                entry.get("png_path"), section=FORECASTS_SECTION_DIR
            ),
            quote=True,
        )
        gpx_path = html.escape(
            _section_relative_path(
                entry.get("gpx_path"), section=FORECASTS_SECTION_DIR
            ),
            quote=True,
        )
        json_path = html.escape(
            _section_relative_path(
                entry.get("json_path"), section=FORECASTS_SECTION_DIR
            ),
            quote=True,
        )
        start_time = _format_display_datetime(
            entry.get("start_time"), default="Start time unavailable"
        )
        duration = _format_duration_label(entry.get("duration"))
        distance_km = float(entry.get("route_distance_km", 0.0) or 0.0)
        total_distance += distance_km
        cards.append(
            f"""
            <article class="collection-card">
              <div class="card-top">
                <h2 class="card-title"><a href="{report_path}">{title}</a></h2>
                <span class="pill">{distance_km:.2f} km</span>
              </div>
              <p class="card-copy">Published {html.escape(published_text)}.</p>
              <div class="pill-row">
                {
                _render_pills(
                    [
                        start_time,
                        duration or "Duration unavailable",
                        f"{distance_km:.2f} km route",
                    ]
                )
            }
              </div>
              <p class="card-meta">Static route-weather forecasts with download links for the source GPX and published snapshot.</p>
              <div class="action-row">
                {_render_action_link(report_path, "Open report", primary=True)}
                {_render_action_link(png_path, "PNG")}
                {_render_action_link(gpx_path, "GPX")}
                {_render_action_link(json_path, "JSON")}
              </div>
            </article>
            """
        )

    if not cards:
        cards.append('<div class="empty-state">No published forecasts yet.</div>')

    latest_text = (
        _format_display_datetime(
            entries[0].get("published_at") or entries[0].get("generated_at"),
            convert_to_utc=True,
            default="No published forecasts yet",
        )
        if entries
        else "No published forecasts yet"
    )
    body_html = f"""
    <section class="hero">
      <p class="eyebrow">Forecast Archive</p>
      <h1>{html.escape(site_title)}</h1>
      <p class="hero-lead">Published route-weather forecasts with static PNG summaries, source GPX files, and archived JSON snapshots.</p>
      {
        _render_meta_row(
            [
                f"{len(entries)} published forecast(s)",
                f"Latest publish {html.escape(latest_text)}",
            ]
        )
    }
      <div class="action-row">
        {_render_action_link("../index.html", "Back to home")}
        {_render_action_link("../reports/index.html", "Browse race reports")}
      </div>
      <div class="metric-grid">
        {
        _render_metric_cards(
            [
                (
                    "Published forecasts",
                    str(len(entries)),
                    "Timestamped route forecast bundles",
                ),
                (
                    "Route distance",
                    f"{total_distance:.1f} km",
                    "Across all published forecasts",
                ),
                ("Latest publish", latest_text, "Most recent forecast bundle"),
            ]
        )
    }
      </div>
    </section>

    <section class="section-stack">
      <section class="panel">
        <div class="panel-head">
          <h2>Published Forecasts</h2>
          <span class="pill">Newest first</span>
        </div>
        <p class="section-caption">Each forecast card links to the hosted report plus its PNG, GPX, and JSON snapshot.</p>
        <div class="collection-grid">
          {"".join(cards)}
        </div>
      </section>
    </section>
    """
    return _render_document(
        title=site_title,
        theme="forecast",
        active_nav="forecasts",
        home_href="../index.html",
        reports_href="../reports/index.html",
        forecasts_href="index.html",
        body_html=body_html,
    )


def render_root_index(
    *,
    race_entries: list[dict[str, object]],
    forecast_entries: list[dict[str, object]],
) -> str:
    race_count = len(race_entries)
    forecast_count = len(forecast_entries)
    latest_race = race_entries[0] if race_entries else {}
    latest_forecast = forecast_entries[0] if forecast_entries else {}
    latest_publish = ""
    if race_entries or forecast_entries:
        merged = [*(race_entries[:1]), *(forecast_entries[:1])]
        merged.sort(
            key=lambda item: str(
                item.get("published_at") or item.get("generated_at") or ""
            ),
            reverse=True,
        )
        latest_publish = _format_display_datetime(
            merged[0].get("published_at") or merged[0].get("generated_at"),
            convert_to_utc=True,
        )

    race_report_path = str(
        latest_race.get("report_path") or f"{REPORTS_SECTION_DIR}/index.html"
    )
    forecast_report_path = str(
        latest_forecast.get("report_path") or f"{FORECASTS_SECTION_DIR}/index.html"
    )
    race_report_href = html.escape(race_report_path, quote=True)
    forecast_report_href = html.escape(forecast_report_path, quote=True)
    race_title = html.escape(
        str(latest_race.get("title") or "No published race reports yet")
    )
    forecast_title = html.escape(
        str(latest_forecast.get("title") or "No published forecasts yet")
    )
    race_published = (
        _format_display_datetime(
            latest_race.get("published_at") or latest_race.get("generated_at"),
            convert_to_utc=True,
            default="No race reports published yet",
        )
        if race_entries
        else "No race reports published yet"
    )
    forecast_published = (
        _format_display_datetime(
            latest_forecast.get("published_at") or latest_forecast.get("generated_at"),
            convert_to_utc=True,
            default="No forecasts published yet",
        )
        if forecast_entries
        else "No forecasts published yet"
    )
    body_html = f"""
    <section class="hero">
      <p class="eyebrow">TrailIntel Static Publishing</p>
      <h1>Trail race reports and forecast archives, in one editorial front door.</h1>
      <p class="hero-lead">Browse the public TrailIntel archive of race-score reports and route-weather forecasts published by the GitHub workflows.</p>
      {
        _render_meta_row(
            [
                f"{race_count} race report(s)",
                f"{forecast_count} forecast(s)",
                html.escape(f"Latest publish {latest_publish}")
                if latest_publish
                else "",
            ]
        )
    }
      <div class="action-row">
        {
        _render_action_link(
            f"{REPORTS_SECTION_DIR}/index.html", "Browse race reports", primary=True
        )
    }
        {_render_action_link(f"{FORECASTS_SECTION_DIR}/index.html", "Browse forecasts")}
      </div>
      <div class="metric-grid">
        {
        _render_metric_cards(
            [
                (
                    "Race reports",
                    str(race_count),
                    "Published score-driven race archives",
                ),
                ("Forecasts", str(forecast_count), "Published route-weather outlooks"),
                (
                    "Latest publish",
                    latest_publish or "Not yet published",
                    "Most recent site update",
                ),
            ]
        )
    }
      </div>
    </section>

    <section class="section-stack">
      <section class="panel">
        <div class="panel-head">
          <h2>Latest Highlights</h2>
          <span class="pill">Newest published outputs</span>
        </div>
        <p class="section-caption">Jump straight into the freshest race intelligence or forecast bundle from the public archive.</p>
        <div class="collection-grid">
          <article class="collection-card">
            <div class="card-top">
              <div>
                <p class="eyebrow">Race Reports</p>
                <h2 class="card-title"><a href="{race_report_href}">{
        race_title
    }</a></h2>
                <p class="card-copy">{html.escape(race_published)}.</p>
              </div>
              <span class="pill">{race_count} total</span>
            </div>
            <p class="card-meta">Score-based race analysis with leaderboard exports, provider links, and coverage summaries.</p>
            <div class="action-row">
              {
        _render_action_link(
            f"{REPORTS_SECTION_DIR}/index.html", "Open race archive", primary=True
        )
    }
              {_render_action_link(race_report_href, "Latest race report")}
            </div>
          </article>
          <article class="collection-card">
            <div class="card-top">
              <div>
                <p class="eyebrow">Forecasts</p>
                <h2 class="card-title"><a href="{forecast_report_href}">{
        forecast_title
    }</a></h2>
                <p class="card-copy">{html.escape(forecast_published)}.</p>
              </div>
              <span class="pill">{forecast_count} total</span>
            </div>
            <p class="card-meta">Static route-weather bundles with hosted PNG summaries, GPX files, and archived forecast snapshots.</p>
            <div class="action-row">
              {
        _render_action_link(
            f"{FORECASTS_SECTION_DIR}/index.html", "Open forecast archive", primary=True
        )
    }
              {_render_action_link(forecast_report_href, "Latest forecast")}
            </div>
          </article>
        </div>
      </section>
    </section>
    """
    return _render_document(
        title="TrailIntel Pages",
        theme="hub",
        active_nav="home",
        home_href="index.html",
        reports_href=f"{REPORTS_SECTION_DIR}/index.html",
        forecasts_href=f"{FORECASTS_SECTION_DIR}/index.html",
        body_html=body_html,
    )


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
            except OSError, json.JSONDecodeError:
                existing = {}
        combined = {**existing, **published_metadata}
        relative_dir = target.relative_to(root).as_posix()
        for key, filename in asset_paths.items():
            combined[key] = f"{relative_dir}/{filename}"
        meta_path.write_text(
            json.dumps(combined, indent=2, ensure_ascii=False), encoding="utf-8"
        )

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
