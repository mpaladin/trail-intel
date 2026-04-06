from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Iterable

import requests

from trailintel.cache_store import LookupCacheStore, default_cache_db_path
from trailintel.matching import canonical_name, is_strong_person_name_match, match_score
from trailintel.models import AthleteRecord
from trailintel.participants import (
    dedupe_names,
    fetch_participants_from_url,
    load_itra_overrides,
    load_participants_file,
    normalize_name,
)
from trailintel.providers.itra import ItraCatalogEntry, ItraClient, ItraLookupError
from trailintel.providers.utmb import UtmbCatalogEntry, UtmbClient
from trailintel.report import export_records, render_table, sort_records
from trailintel.site import build_report_snapshot, export_report_site


def _append_note(current: str, message: str) -> str:
    return message if not current else f"{current}; {message}"


def _collect_participants(args: argparse.Namespace) -> list[str]:
    names: list[str] = []
    if args.race_url:
        names.extend(
            fetch_participants_from_url(
                args.race_url,
                selector=args.name_selector,
                competition_name=args.competition_name,
                timeout=args.timeout,
            )
        )
    if args.participants_file:
        names.extend(load_participants_file(args.participants_file))
    if args.participant:
        names.extend(args.participant)

    normalized = [normalize_name(name) for name in names if name]
    return dedupe_names(name for name in normalized if name)


def _override_lookup(name: str, overrides: dict[str, float]) -> float | None:
    # Exact-match by normalized name first.
    direct = overrides.get(name)
    if direct is not None:
        return direct
    folded = name.casefold()
    for key, score in overrides.items():
        if key.casefold() == folded:
            return score
    return None


@dataclass(slots=True)
class CatalogMatch:
    confidence: float
    matched_name: str
    profile_url: str | None
    score: float


def _best_catalog_match(
    input_name: str,
    *,
    entries: list[tuple[str, float, str | None]],
    exact_lookup: dict[str, tuple[str, float, str | None]],
    min_match_score: float,
    enforce_strong_name_guard: bool = False,
) -> CatalogMatch | None:
    exact = exact_lookup.get(canonical_name(input_name))
    if exact:
        name, score, profile_url = exact
        return CatalogMatch(confidence=1.0, matched_name=name, profile_url=profile_url, score=score)

    best: tuple[str, float, str | None] | None = None
    best_confidence = 0.0
    for candidate_name, candidate_score, candidate_profile in entries:
        if enforce_strong_name_guard and not _is_strong_catalog_name_match(input_name, candidate_name):
            continue
        confidence = match_score(input_name, candidate_name)
        if confidence > best_confidence:
            best_confidence = confidence
            best = (candidate_name, candidate_score, candidate_profile)

    if not best or best_confidence < min_match_score:
        return None
    return CatalogMatch(
        confidence=best_confidence,
        matched_name=best[0],
        score=best[1],
        profile_url=best[2],
    )


def _is_strong_catalog_name_match(query_name: str, candidate_name: str) -> bool:
    return is_strong_person_name_match(query_name, candidate_name)


def _is_above_threshold(record: AthleteRecord, threshold: float) -> bool:
    return (
        (record.utmb_index is not None and record.utmb_index > threshold)
        or (record.itra_score is not None and record.itra_score > threshold)
    )


def _should_lookup_itra_after_utmb(*, utmb_index: float | None, threshold: float) -> bool:
    return utmb_index is None or utmb_index > threshold


def _itra_skipped_due_to_utmb_note(*, utmb_index: float | None, threshold: float) -> str:
    if utmb_index is None:
        return "ITRA skipped after UTMB pass"
    return f"ITRA skipped because UTMB {utmb_index:.1f} <= threshold {threshold:.1f}"


def _enrich_records(
    names: Iterable[str],
    *,
    min_match_score: float,
    score_threshold: float = 680.0,
    timeout: int,
    skip_itra: bool,
    itra_overrides: dict[str, float] | None,
    itra_cookie: str | None,
    cache_store: LookupCacheStore | None = None,
    use_cache: bool = True,
    force_refresh_cache: bool = False,
) -> list[AthleteRecord]:
    utmb_client = UtmbClient(
        timeout=timeout,
        cache_store=cache_store,
        use_cache=use_cache,
        force_refresh=force_refresh_cache,
    )
    itra_client = ItraClient(
        timeout=timeout,
        cookie=itra_cookie,
        cache_store=cache_store,
        use_cache=use_cache,
        force_refresh=force_refresh_cache,
    )

    records: list[AthleteRecord] = []
    itra_block_reason: str | None = None
    consecutive_itra_failures = 0
    max_consecutive_itra_failures = 8
    overrides = itra_overrides or {}

    for name in names:
        record = AthleteRecord(input_name=name)

        try:
            utmb_match = utmb_client.search(name)
            if utmb_match:
                record.utmb_index = utmb_match.utmb_index
                record.utmb_match_name = utmb_match.matched_name
                record.utmb_match_score = utmb_match.match_score
                record.utmb_profile_url = utmb_match.profile_url
                if utmb_match.match_score < min_match_score:
                    record.notes = _append_note(record.notes, "UTMB low-confidence match")
            else:
                record.notes = _append_note(record.notes, "UTMB not found")
            if utmb_client.last_lookup_stale_fallback:
                record.notes = _append_note(record.notes, "UTMB stale cache fallback used")
        except requests.RequestException as exc:
            record.notes = _append_note(record.notes, f"UTMB error: {exc.__class__.__name__}")

        override = _override_lookup(name, overrides)
        if override is not None:
            record.itra_score = override
            record.notes = _append_note(record.notes, "ITRA from override file")
            records.append(record)
            continue

        if skip_itra:
            record.notes = _append_note(record.notes, "ITRA skipped by flag")
            records.append(record)
            continue

        if not _should_lookup_itra_after_utmb(
            utmb_index=record.utmb_index,
            threshold=score_threshold,
        ):
            record.notes = _append_note(
                record.notes,
                _itra_skipped_due_to_utmb_note(
                    utmb_index=record.utmb_index,
                    threshold=score_threshold,
                ),
            )
            records.append(record)
            continue

        if itra_block_reason:
            record.notes = _append_note(record.notes, f"ITRA unavailable: {itra_block_reason}")
            records.append(record)
            continue

        try:
            itra_match = itra_client.search(name)
            if itra_client.last_lookup_used_cookie_fallback:
                record.notes = _append_note(record.notes, "ITRA cookie rejected, retried anonymously")
            if itra_match:
                record.itra_score = itra_match.itra_score
                record.itra_match_name = itra_match.matched_name
                record.itra_match_score = itra_match.match_score
                record.itra_profile_url = itra_match.profile_url
                if itra_match.match_score < min_match_score:
                    record.notes = _append_note(record.notes, "ITRA low-confidence match")
                if itra_match.itra_score is None:
                    record.notes = _append_note(record.notes, "ITRA score missing in response")
            else:
                record.notes = _append_note(record.notes, "ITRA not found")
            if itra_client.last_lookup_stale_fallback:
                record.notes = _append_note(record.notes, "ITRA stale cache fallback used")
            consecutive_itra_failures = 0
        except ItraLookupError as exc:
            consecutive_itra_failures += 1
            error_message = str(exc)
            record.notes = _append_note(record.notes, f"ITRA unavailable: {error_message}")
            if consecutive_itra_failures >= max_consecutive_itra_failures:
                itra_block_reason = (
                    f"{error_message} (stopped after {max_consecutive_itra_failures} consecutive failures)"
                )

        records.append(record)

    return records


def _build_utmb_catalog(
    utmb_client: UtmbClient,
    threshold: float,
    max_pages: int,
) -> tuple[list[UtmbCatalogEntry], str | None]:
    try:
        return utmb_client.fetch_catalog_above_threshold(
            threshold=threshold,
            page_size=100,
            max_pages=max_pages,
        ), None
    except requests.RequestException as exc:
        return [], f"UTMB catalog unavailable: {exc.__class__.__name__}"


def _build_itra_catalog(
    itra_client: ItraClient,
    *,
    threshold: float,
    skip_itra: bool,
) -> tuple[list[ItraCatalogEntry], str | None]:
    if skip_itra:
        return [], "ITRA skipped by flag"
    try:
        return itra_client.fetch_public_catalog_above_threshold(threshold=threshold), None
    except (ItraLookupError, requests.RequestException) as exc:
        return [], f"ITRA catalog unavailable: {exc}"


def _enrich_records_from_catalog(
    names: Iterable[str],
    *,
    timeout: int,
    skip_itra: bool,
    itra_overrides: dict[str, float] | None,
    itra_cookie: str | None,
    score_threshold: float,
    utmb_catalog_max_pages: int,
    catalog_min_match_score: float,
) -> list[AthleteRecord]:
    overrides = itra_overrides or {}
    utmb_client = UtmbClient(timeout=timeout)
    itra_client = ItraClient(timeout=timeout, cookie=itra_cookie)

    utmb_catalog, utmb_issue = _build_utmb_catalog(
        utmb_client=utmb_client,
        threshold=score_threshold,
        max_pages=utmb_catalog_max_pages,
    )
    itra_catalog, itra_issue = _build_itra_catalog(
        itra_client=itra_client,
        threshold=score_threshold,
        skip_itra=skip_itra,
    )

    utmb_entries = [(entry.name, entry.utmb_index, entry.profile_url) for entry in utmb_catalog]
    itra_entries = [(entry.name, entry.itra_score, entry.profile_url) for entry in itra_catalog]
    utmb_exact = {canonical_name(name): (name, score, profile) for name, score, profile in utmb_entries}
    itra_exact = {canonical_name(name): (name, score, profile) for name, score, profile in itra_entries}

    records: list[AthleteRecord] = []
    for name in names:
        record = AthleteRecord(input_name=name)

        if utmb_issue:
            record.notes = _append_note(record.notes, utmb_issue)
        else:
            utmb_match = _best_catalog_match(
                name,
                entries=utmb_entries,
                exact_lookup=utmb_exact,
                min_match_score=catalog_min_match_score,
                enforce_strong_name_guard=True,
            )
            if utmb_match:
                record.utmb_index = utmb_match.score
                record.utmb_match_name = utmb_match.matched_name
                record.utmb_match_score = utmb_match.confidence
                record.utmb_profile_url = utmb_match.profile_url
                if utmb_match.confidence < 1.0:
                    record.notes = _append_note(record.notes, "UTMB catalog fuzzy match")
            else:
                record.notes = _append_note(record.notes, "UTMB high-score catalog no match")

        override = _override_lookup(name, overrides)
        if override is not None:
            record.itra_score = override
            record.notes = _append_note(record.notes, "ITRA from override file")
            records.append(record)
            continue

        if itra_issue:
            record.notes = _append_note(record.notes, itra_issue)
            records.append(record)
            continue

        itra_match = _best_catalog_match(
            name,
            entries=itra_entries,
            exact_lookup=itra_exact,
            min_match_score=catalog_min_match_score,
            enforce_strong_name_guard=True,
        )
        if itra_match:
            record.itra_score = itra_match.score
            record.itra_match_name = itra_match.matched_name
            record.itra_match_score = itra_match.confidence
            record.itra_profile_url = itra_match.profile_url
            if itra_match.confidence < 1.0:
                record.notes = _append_note(record.notes, "ITRA catalog fuzzy match")
        else:
            record.notes = _append_note(record.notes, "ITRA high-score catalog no match")

        records.append(record)

    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trailintel",
        description=(
            "Build a TrailIntel top-athlete report from race participants enriched with "
            "UTMB index and ITRA score."
        ),
    )
    parser.add_argument("--race-name", help="Optional race name for report heading.")
    parser.add_argument(
        "--strategy",
        choices=("participant-first", "catalog-first"),
        default="participant-first",
        help=(
            "participant-first: enrich each participant via lookups; "
            "catalog-first: build high-score catalogs first, then match participants."
        ),
    )
    parser.add_argument(
        "--score-threshold",
        type=float,
        default=680.0,
        help="Only keep athletes with UTMB or ITRA score strictly greater than this value.",
    )
    parser.add_argument(
        "--include-below-threshold",
        action="store_true",
        help="Include participants below threshold in the report.",
    )
    parser.add_argument("--race-url", help="Race URL where participants can be fetched.")
    parser.add_argument(
        "--competition-name",
        help=(
            "Optional distance/competition filter for race URLs with multiple events "
            '(example: "Le 40 km").'
        ),
    )
    parser.add_argument(
        "--name-selector",
        help="CSS selector for extracting names from HTML race pages (optional).",
    )
    parser.add_argument(
        "--participants-file",
        help="Participants input file (.csv, .json, or .txt).",
    )
    parser.add_argument(
        "--participant",
        action="append",
        help='Add a participant by name (repeatable), e.g. --participant "Kilian Jornet".',
    )
    parser.add_argument("--itra-overrides", help="CSV/JSON file with manual name->ITRA score mappings.")
    parser.add_argument(
        "--itra-cookie",
        help=(
            "Optional raw Cookie header value for authenticated ITRA requests. "
            "Can also be provided via ITRA_COOKIE env var."
        ),
    )
    parser.add_argument("--skip-itra", action="store_true", help="Disable live ITRA lookups.")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable persistent lookup cache.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Bypass cache reads and refresh lookup entries from live providers.",
    )
    parser.add_argument(
        "--cache-db",
        help=(
            "Path to cache DuckDB file (default: TRAILINTEL_CACHE_DB env var, then "
            "TRAILINTEL_CONFIG_FILE/~/.config/trailintel/config.toml [cache].db_path, "
            "then ~/.cache/trailintel/trailintel_cache.duckdb)."
        ),
    )
    parser.add_argument("--top", type=int, default=100, help="Number of athletes to display.")
    parser.add_argument(
        "--sort-by",
        choices=("combined", "utmb", "itra"),
        default="combined",
        help="Sort mode for the ranking.",
    )
    parser.add_argument(
        "--min-match-score",
        type=float,
        default=0.6,
        help="Minimum name-match confidence before marking as low-confidence (0-1).",
    )
    parser.add_argument(
        "--catalog-min-match-score",
        type=float,
        default=0.85,
        help="Catalog-first only: minimum fuzzy match confidence for catalog matching (0-1).",
    )
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout (seconds).")
    parser.add_argument(
        "--utmb-catalog-max-pages",
        type=int,
        default=120,
        help="Catalog-first only: maximum UTMB pages to scan (100 athletes/page).",
    )
    parser.add_argument("--output", help="Optional output file (.csv or .json).")
    parser.add_argument(
        "--site-dir",
        help="Optional output directory for a static HTML report bundle (index.html, report.csv, report.json).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not any((args.race_url, args.participants_file, args.participant)):
        parser.error("Provide at least one input source: --race-url, --participants-file, or --participant.")

    overrides: dict[str, float] | None = None
    if args.itra_overrides:
        try:
            overrides = load_itra_overrides(args.itra_overrides)
        except Exception as exc:  # pragma: no cover
            print(f"Failed to parse --itra-overrides: {exc}", file=sys.stderr)
            return 2

    try:
        names = _collect_participants(args)
    except Exception as exc:
        print(f"Failed to load participants: {exc}", file=sys.stderr)
        return 2

    if not names:
        print("No participants found from the provided inputs.", file=sys.stderr)
        return 2

    itra_cookie = args.itra_cookie or os.getenv("ITRA_COOKIE")
    cache_store: LookupCacheStore | None = None
    use_cache = not args.no_cache
    cache_db_path = Path(args.cache_db).expanduser() if args.cache_db else default_cache_db_path()
    if use_cache:
        try:
            cache_store = LookupCacheStore(cache_db_path)
        except Exception as exc:
            use_cache = False
            print(f"Warning: cache disabled ({exc})", file=sys.stderr)

    try:
        if args.strategy == "catalog-first":
            records = _enrich_records_from_catalog(
                names,
                timeout=args.timeout,
                skip_itra=args.skip_itra,
                itra_overrides=overrides,
                itra_cookie=itra_cookie,
                score_threshold=args.score_threshold,
                utmb_catalog_max_pages=max(1, args.utmb_catalog_max_pages),
                catalog_min_match_score=max(0.0, min(1.0, args.catalog_min_match_score)),
            )
        else:
            records = _enrich_records(
                names,
                min_match_score=args.min_match_score,
                score_threshold=args.score_threshold,
                timeout=args.timeout,
                skip_itra=args.skip_itra,
                itra_overrides=overrides,
                itra_cookie=itra_cookie,
                cache_store=cache_store,
                use_cache=use_cache,
                force_refresh_cache=args.refresh_cache,
            )
    finally:
        if cache_store:
            cache_store.close()

    filtered = (
        records
        if args.include_below_threshold
        else [record for record in records if _is_above_threshold(record, args.score_threshold)]
    )
    ranked = sort_records(filtered, sort_by=args.sort_by)
    ranked_all = sort_records(records, sort_by=args.sort_by)

    heading = args.race_name or "Trail Race Report"
    print(heading)
    print(f"Participants: {len(names)}")
    print(f"Strategy: {args.strategy}")
    print(f"Threshold: > {args.score_threshold}")
    print(f"Qualified: {len(filtered)}")
    print(f"Sort: {args.sort_by}")
    print()
    if ranked:
        print(render_table(ranked, top=max(1, args.top)))
    else:
        print("No athletes above threshold were found.")

    if args.output:
        out_path = export_records(ranked, args.output)
        print()
        print(f"Saved full report to {out_path}")

    if args.site_dir:
        if use_cache:
            cache_status = (
                f"Cache: enabled (`{cache_db_path}`)"
                + (" - force refresh enabled" if args.refresh_cache else "")
            )
        else:
            cache_status = "Cache: disabled by flag or unavailable"
        snapshot = build_report_snapshot(
            title=heading,
            all_records=records,
            qualified_records=filtered,
            participants_count=len(names),
            strategy=args.strategy,
            same_name_mode="highest",
            top=max(1, args.top),
            sort_by=args.sort_by,
            race_url=args.race_url or "",
            competition_name=args.competition_name or "",
            score_threshold=args.score_threshold,
            cache_status=cache_status,
            stale_cache_used=any(
                "stale cache fallback used" in (record.notes or "")
                for record in records
            ),
        )
        site_path = export_report_site(
            snapshot=snapshot,
            records=ranked_all,
            destination=args.site_dir,
        )
        print()
        print(f"Saved static report site to {site_path}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
