from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any

from trailintel.cli import _apply_betrail_catalog_match, _build_betrail_catalog, _is_above_threshold
from trailintel.matching import canonical_name
from trailintel.models import AthleteRecord
from trailintel.providers.betrail import BetrailCatalogEntry, BetrailClient
from trailintel.report import sort_records
from trailintel.site import (
    RACE_REPORT_KIND,
    REPORT_JSON_FILENAME,
    REPORT_META_FILENAME,
    REPORT_SNAPSHOT_FILENAME,
    REPORTS_SECTION_DIR,
    build_report_snapshot,
    build_report_metadata,
    export_report_site,
    refresh_site_index,
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = _read_json(path)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = _read_json(path)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _record_from_export_row(item: dict[str, Any]) -> AthleteRecord | None:
    input_name = str(item.get("input_name") or "").strip()
    if not input_name:
        return None
    return AthleteRecord(
        input_name=input_name,
        utmb_index=_float_or_none(item.get("utmb_index")),
        utmb_match_name=_text_or_none(item.get("utmb_match_name")),
        utmb_match_score=_float_or_none(item.get("utmb_match_score")),
        utmb_profile_url=_text_or_none(item.get("utmb_profile_url")),
        itra_score=_float_or_none(item.get("itra_score")),
        itra_match_name=_text_or_none(item.get("itra_match_name")),
        itra_match_score=_float_or_none(item.get("itra_match_score")),
        itra_profile_url=_text_or_none(item.get("itra_profile_url")),
        betrail_score=_float_or_none(item.get("betrail_score")),
        betrail_match_name=_text_or_none(item.get("betrail_match_name")),
        betrail_match_score=_float_or_none(item.get("betrail_match_score")),
        betrail_profile_url=_text_or_none(item.get("betrail_profile_url")),
        notes=str(item.get("notes") or "").strip(),
    )


def _load_records(bundle_dir: Path) -> list[AthleteRecord]:
    rows = _read_json_list(bundle_dir / REPORT_JSON_FILENAME)
    records: list[AthleteRecord] = []
    for row in rows:
        record = _record_from_export_row(row)
        if record is not None:
            records.append(record)
    return records


def _parse_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _derive_top(snapshot: dict[str, Any], meta: dict[str, Any], default: int) -> int:
    rows = snapshot.get("rows")
    if isinstance(rows, list) and rows:
        return len(rows)
    top = snapshot.get("top")
    if top not in (None, ""):
        try:
            parsed = int(top)
        except (TypeError, ValueError):
            parsed = 0
        if parsed > 0:
            return parsed
    try:
        qualified = int(meta.get("qualified_count", 0) or 0)
    except (TypeError, ValueError):
        qualified = 0
    if qualified > 0:
        return qualified
    return max(1, default)


def _catalog_payload(
    threshold: float,
    *,
    timeout: int,
    betrail_cookie: str | None,
    cache: dict[float, tuple[list[tuple[str, float, str | None]], dict[str, tuple[str, float, str | None]]]],
) -> tuple[list[tuple[str, float, str | None]], dict[str, tuple[str, float, str | None]]]:
    cached = cache.get(threshold)
    if cached is not None:
        return cached

    client = BetrailClient(timeout=timeout, cookie=betrail_cookie)
    catalog, issue = _build_betrail_catalog(betrail_client=client, threshold=threshold)
    if issue:
        raise RuntimeError(issue)
    entries = [(entry.name, entry.betrail_score, entry.profile_url) for entry in catalog]
    exact = {canonical_name(name): (name, score, profile) for name, score, profile in entries}
    cache[threshold] = (entries, exact)
    return entries, exact


def backfill_pages(
    *,
    pages_root: str | Path,
    timeout: int = 15,
    betrail_cookie: str | None = None,
) -> int:
    root = Path(pages_root)
    reports_root = root / REPORTS_SECTION_DIR
    if not reports_root.exists():
        raise FileNotFoundError(f"Reports root not found: {reports_root}")

    catalog_cache: dict[
        float,
        tuple[list[tuple[str, float, str | None]], dict[str, tuple[str, float, str | None]]],
    ] = {}
    processed = 0

    for meta_path in sorted(reports_root.rglob(REPORT_META_FILENAME)):
        existing_meta = _read_json_dict(meta_path)
        if existing_meta.get("report_kind", RACE_REPORT_KIND) != RACE_REPORT_KIND:
            continue

        bundle_dir = meta_path.parent
        records = _load_records(bundle_dir)
        if not records:
            continue

        existing_snapshot = _read_json_dict(bundle_dir / REPORT_SNAPSHOT_FILENAME)
        score_threshold = _float_or_none(
            existing_meta.get("score_threshold", existing_snapshot.get("score_threshold"))
        )
        if score_threshold is None:
            score_threshold = 680.0

        entries, exact = _catalog_payload(
            score_threshold,
            timeout=timeout,
            betrail_cookie=betrail_cookie,
            cache=catalog_cache,
        )
        for record in records:
            _apply_betrail_catalog_match(
                record,
                input_name=record.input_name,
                entries=entries,
                exact_lookup=exact,
                min_match_score=0.85,
                issue=None,
                note_missing=False,
            )

        sort_by = str(existing_snapshot.get("sort_by", "combined") or "combined")
        ranked_all = sort_records(records, sort_by=sort_by)
        qualified = [record for record in records if _is_above_threshold(record, score_threshold)]
        top = _derive_top(existing_snapshot, existing_meta, default=len(qualified) or 100)
        snapshot = build_report_snapshot(
            title=str(existing_meta.get("title", existing_snapshot.get("title", "Trail Race Report")) or "Trail Race Report"),
            all_records=records,
            qualified_records=qualified,
            participants_count=int(existing_meta.get("participants_count", existing_snapshot.get("participants_count", len(records))) or len(records)),
            strategy=str(existing_meta.get("strategy", existing_snapshot.get("strategy", "participant-first")) or "participant-first"),
            same_name_mode=str(existing_meta.get("same_name_mode", existing_snapshot.get("same_name_mode", "highest")) or "highest"),
            top=max(1, top),
            sort_by=sort_by,
            race_url=str(existing_meta.get("race_url", existing_snapshot.get("race_url", "")) or ""),
            competition_name=str(existing_meta.get("competition_name", existing_snapshot.get("competition_name", "")) or ""),
            score_threshold=score_threshold,
            cache_status=str(existing_snapshot.get("cache_status", "")),
            stale_cache_used=bool(existing_snapshot.get("stale_cache_used", False)),
            generated_at=_parse_iso_timestamp(existing_snapshot.get("generated_at", existing_meta.get("generated_at"))),
        )
        export_report_site(
            snapshot=snapshot,
            records=ranked_all,
            destination=bundle_dir,
        )

        generated_meta = build_report_metadata(snapshot)
        merged_meta = {**existing_meta, **generated_meta}
        meta_path.write_text(json.dumps(merged_meta, indent=2, ensure_ascii=False), encoding="utf-8")
        processed += 1

    refresh_site_index(root)
    return processed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trailintel-backfill-pages")
    parser.add_argument("--pages-root", required=True, help="Path to the cloned trail-intel-pages worktree.")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--betrail-cookie",
        help="Optional raw Cookie header value for Betrail requests. Can also be provided via BETRAIL_COOKIE.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    betrail_cookie = args.betrail_cookie or os.getenv("BETRAIL_COOKIE")

    try:
        processed = backfill_pages(
            pages_root=args.pages_root,
            timeout=int(args.timeout),
            betrail_cookie=betrail_cookie,
        )
    except Exception as exc:
        print(f"Backfill failed: {exc}", file=sys.stderr)
        return 1

    print(f"Updated {processed} published race report bundle(s).")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
