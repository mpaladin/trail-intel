from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path

from trailintel.models import AthleteRecord


def sort_records(records: list[AthleteRecord], sort_by: str = "combined") -> list[AthleteRecord]:
    if sort_by == "utmb":
        return sorted(records, key=lambda r: r.utmb_index if r.utmb_index is not None else -1, reverse=True)
    if sort_by == "itra":
        return sorted(records, key=lambda r: r.itra_score if r.itra_score is not None else -1, reverse=True)
    return sorted(records, key=lambda r: r.combined_score, reverse=True)


def _fmt_score(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def render_table(records: list[AthleteRecord], top: int = 20) -> str:
    shown = records[:top]
    headers = ["Rank", "Athlete", "UTMB", "ITRA", "Combined", "Notes"]
    rows = []
    for idx, athlete in enumerate(shown, start=1):
        rows.append(
            [
                str(idx),
                athlete.input_name,
                _fmt_score(athlete.utmb_index),
                _fmt_score(athlete.itra_score),
                f"{athlete.combined_score:.1f}",
                athlete.notes or "",
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for i, col in enumerate(row):
            widths[i] = max(widths[i], len(col))

    def format_row(columns: list[str]) -> str:
        return " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)


def export_records(records: list[AthleteRecord], destination: str | Path) -> Path:
    path = Path(destination)
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = []
        for record in records:
            item = asdict(record)
            item["combined_score"] = record.combined_score
            payload.append(item)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    headers = [
        "input_name",
        "utmb_index",
        "utmb_match_name",
        "utmb_match_score",
        "utmb_profile_url",
        "itra_score",
        "itra_match_name",
        "itra_match_score",
        "itra_profile_url",
        "combined_score",
        "notes",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "input_name": record.input_name,
                    "utmb_index": record.utmb_index,
                    "utmb_match_name": record.utmb_match_name,
                    "utmb_match_score": record.utmb_match_score,
                    "utmb_profile_url": record.utmb_profile_url,
                    "itra_score": record.itra_score,
                    "itra_match_name": record.itra_match_name,
                    "itra_match_score": record.itra_match_score,
                    "itra_profile_url": record.itra_profile_url,
                    "combined_score": record.combined_score,
                    "notes": record.notes,
                }
            )
    return path
