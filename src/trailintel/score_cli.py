from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys

import duckdb
import requests

from trailintel.providers.betrail import BetrailClient, BetrailLookupError
from trailintel.providers.itra import ItraClient, ItraLookupError
from trailintel.providers.utmb import UtmbClient
from trailintel.score_repo import (
    AthleteScoreRepo,
    RepoProviderObservation,
    default_score_repo_path,
    provider_score_scale,
)

DEFAULT_MIN_MATCH_SCORE = 0.6
_PROVIDER_SCORE_KEYS = {
    "utmb": "utmb_index",
    "itra": "itra_score",
    "betrail": "betrail_score",
}


def _observation(
    *,
    provider: str,
    status: str,
    matched_name: str | None,
    profile_url: str | None,
    score: float | None,
    match_confidence: float | None,
    source_run_id: str,
    lookup_threshold: float | None = None,
    persist: bool = True,
) -> RepoProviderObservation:
    return RepoProviderObservation(
        provider=provider,
        status=status,
        matched_name=matched_name,
        profile_url=profile_url,
        score=score,
        score_scale=provider_score_scale(provider),
        match_confidence=match_confidence,
        source_run_id=source_run_id,
        lookup_threshold=lookup_threshold,
        persist=persist,
    )


def _empty_stats() -> dict[str, int]:
    return {
        "athletes_seen": 0,
        "athletes_created": 0,
        "athletes_updated": 0,
        "provider_updates": 0,
        "utmb_matches": 0,
        "utmb_misses": 0,
        "itra_matches": 0,
        "itra_misses": 0,
    }


def _normalize_dt(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _as_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _import_candidates_from_payload(
    *,
    provider: str,
    payload_json: str,
) -> list[dict[str, object]]:
    if not payload_json.strip():
        return []
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    score_key = _PROVIDER_SCORE_KEYS.get(provider, "score")
    imported: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        matched_name = str(item.get("matched_name", "")).strip()
        profile_url = str(item.get("profile_url", "")).strip() or None
        score = _as_float(item.get(score_key))
        if score is None:
            score = _as_float(item.get("score"))
        match_score = _as_float(item.get("match_score"))
        if not matched_name or score is None:
            continue
        imported.append(
            {
                "matched_name": matched_name,
                "profile_url": profile_url,
                "score": score,
                "match_score": match_score,
            }
        )
    return imported


def seed_betrail_repo(
    *,
    repo_path: str | Path,
    threshold: float,
    timeout: int = 15,
    fill_utmb: bool,
    fill_itra: bool,
    itra_cookie: str | None,
    betrail_cookie: str | None,
) -> tuple[int, dict[str, object]]:
    repo = AthleteScoreRepo(repo_path)
    repo.load()
    run_id = repo.generate_run_id()

    betrail_client = BetrailClient(timeout=timeout, cookie=betrail_cookie)
    utmb_client = UtmbClient(timeout=timeout)
    itra_client = ItraClient(timeout=timeout, cookie=itra_cookie)

    try:
        catalog = betrail_client.fetch_catalog_above_threshold(threshold)
    except (BetrailLookupError, requests.RequestException) as exc:
        raise RuntimeError(f"Betrail seed failed: {exc}") from exc

    stats = _empty_stats()
    provider_issues: dict[str, str | None] = {
        "betrail": None,
        "utmb": None,
        "itra": None,
    }
    consecutive_itra_failures = 0
    max_consecutive_itra_failures = 8
    itra_block_reason: str | None = None

    for entry in catalog:
        observations: list[RepoProviderObservation] = [
            _observation(
                provider="betrail",
                status="matched",
                matched_name=entry.name,
                profile_url=entry.profile_url,
                score=entry.betrail_score,
                match_confidence=1.0,
                source_run_id=run_id,
                lookup_threshold=threshold,
            )
        ]

        if fill_utmb:
            try:
                utmb_match = utmb_client.search(entry.name)
                if utmb_match:
                    observations.append(
                        _observation(
                            provider="utmb",
                            status="matched",
                            matched_name=utmb_match.matched_name,
                            profile_url=utmb_match.profile_url,
                            score=utmb_match.utmb_index,
                            match_confidence=utmb_match.match_score,
                            source_run_id=run_id,
                            persist=utmb_match.match_score >= DEFAULT_MIN_MATCH_SCORE,
                        )
                    )
                    if utmb_match.match_score >= DEFAULT_MIN_MATCH_SCORE and utmb_match.utmb_index is not None:
                        stats["utmb_matches"] += 1
                else:
                    observations.append(
                        _observation(
                            provider="utmb",
                            status="miss",
                            matched_name=None,
                            profile_url=None,
                            score=None,
                            match_confidence=None,
                            source_run_id=run_id,
                        )
                    )
                    stats["utmb_misses"] += 1
            except requests.RequestException as exc:
                provider_issues["utmb"] = exc.__class__.__name__

        if fill_itra:
            if itra_block_reason:
                provider_issues["itra"] = itra_block_reason
            else:
                try:
                    itra_match = itra_client.search(entry.name)
                    if itra_match:
                        observations.append(
                            _observation(
                                provider="itra",
                                status="matched",
                                matched_name=itra_match.matched_name,
                                profile_url=itra_match.profile_url,
                                score=itra_match.itra_score,
                                match_confidence=itra_match.match_score,
                                source_run_id=run_id,
                                persist=(
                                    itra_match.match_score >= DEFAULT_MIN_MATCH_SCORE
                                    and itra_match.itra_score is not None
                                ),
                            )
                        )
                        if itra_match.match_score >= DEFAULT_MIN_MATCH_SCORE and itra_match.itra_score is not None:
                            stats["itra_matches"] += 1
                    else:
                        observations.append(
                            _observation(
                                provider="itra",
                                status="miss",
                                matched_name=None,
                                profile_url=None,
                                score=None,
                                match_confidence=None,
                                source_run_id=run_id,
                            )
                        )
                        stats["itra_misses"] += 1
                    consecutive_itra_failures = 0
                except ItraLookupError as exc:
                    consecutive_itra_failures += 1
                    provider_issues["itra"] = str(exc)
                    if consecutive_itra_failures >= max_consecutive_itra_failures:
                        itra_block_reason = (
                            f"{exc} (stopped after {max_consecutive_itra_failures} consecutive failures)"
                        )
                        provider_issues["itra"] = itra_block_reason

        result = repo.write_athlete_observations(
            input_name=entry.name,
            observations=observations,
            source_run_id=run_id,
            source_kind="seed-betrail",
        )
        stats["athletes_seen"] += 1
        if result.created:
            stats["athletes_created"] += 1
        if result.updated:
            stats["athletes_updated"] += 1
        stats["provider_updates"] += result.provider_updates

    summary = {
        "betrail_threshold": threshold,
        "seeded_athletes": len(catalog),
        "fill_utmb": fill_utmb,
        "fill_itra": fill_itra,
        "score_repo": {
            "path": str(repo.root),
            **stats,
        },
        "provider_issues": provider_issues,
    }
    repo.write_run_summary(
        run_id=run_id,
        run_kind="seed-betrail",
        summary=summary,
    )
    return len(catalog), summary


def import_duckdb_cache(
    *,
    repo_path: str | Path,
    cache_db_path: str | Path,
    min_match_score: float = DEFAULT_MIN_MATCH_SCORE,
) -> tuple[int, dict[str, object]]:
    repo = AthleteScoreRepo(repo_path)
    repo.load()
    cache_path = Path(cache_db_path).expanduser()
    if not cache_path.exists():
        raise RuntimeError(f"DuckDB cache not found: {cache_path}")

    run_id = repo.generate_run_id()
    conn = duckdb.connect(str(cache_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT provider, query_key, status, payload_json, fetched_at, updated_at
            FROM athlete_lookup_cache
            WHERE status = 'success'
            ORDER BY COALESCE(updated_at, fetched_at) ASC, provider ASC, query_key ASC
            """
        ).fetchall()
    except Exception as exc:
        conn.close()
        raise RuntimeError(f"Failed to read athlete_lookup_cache from {cache_path}: {exc}") from exc

    stats = {
        "rows_scanned": len(rows),
        "rows_imported": 0,
        "athletes_created": 0,
        "athletes_updated": 0,
        "provider_updates": 0,
        "candidates_imported": 0,
        "candidates_skipped_low_confidence": 0,
        "candidates_skipped_invalid": 0,
    }
    provider_counts: dict[str, int] = {}

    try:
        for provider, query_key, _status, payload_json, fetched_at, _updated_at in rows:
            provider_name = str(provider).strip()
            candidates = _import_candidates_from_payload(
                provider=provider_name,
                payload_json=str(payload_json or ""),
            )
            if not candidates:
                stats["candidates_skipped_invalid"] += 1
                continue

            row_imported = False
            checked_at = _normalize_dt(fetched_at)
            for candidate in candidates:
                match_score = _as_float(candidate.get("match_score"))
                if match_score is not None and match_score < min_match_score:
                    stats["candidates_skipped_low_confidence"] += 1
                    continue

                result = repo.write_athlete_observations(
                    input_name=str(candidate["matched_name"]),
                    observations=[
                        _observation(
                            provider=provider_name,
                            status="matched",
                            matched_name=str(candidate["matched_name"]),
                            profile_url=str(candidate["profile_url"]) if candidate["profile_url"] else None,
                            score=float(candidate["score"]),
                            match_confidence=match_score,
                            source_run_id=run_id,
                            persist=True,
                        )
                    ],
                    source_run_id=run_id,
                    source_kind="import-duckdb",
                    observed_at=checked_at,
                )
                stats["candidates_imported"] += 1
                if result.created:
                    stats["athletes_created"] += 1
                if result.updated:
                    stats["athletes_updated"] += 1
                stats["provider_updates"] += result.provider_updates
                provider_counts[provider_name] = provider_counts.get(provider_name, 0) + 1
                row_imported = True

            if row_imported:
                stats["rows_imported"] += 1
    finally:
        conn.close()

    summary = {
        "cache_db_path": str(cache_path),
        "min_match_score": min_match_score,
        "score_repo": {
            "path": str(repo.root),
            **stats,
        },
        "providers": provider_counts,
        "import_mode": "success-only",
        "notes": "DuckDB misses are query-scoped and are not imported as athlete docs.",
    }
    repo.write_run_summary(
        run_id=run_id,
        run_kind="import-duckdb",
        summary=summary,
    )
    return stats["candidates_imported"], summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trailintel-score",
        description="Manage the TrailIntel Git-backed athlete score repo.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    seed_betrail = subparsers.add_parser(
        "seed-betrail",
        help="Seed the score repo from the Betrail catalog above a threshold.",
    )
    seed_betrail.add_argument(
        "--repo",
        help=(
            "Path to the local score repo checkout "
            "(default: TRAILINTEL_SCORE_REPO env var, then config [score_repo].path)."
        ),
    )
    seed_betrail.add_argument("--threshold", type=float, default=68.0)
    seed_betrail.add_argument("--timeout", type=int, default=15)
    seed_betrail.add_argument("--fill-utmb", action="store_true")
    seed_betrail.add_argument("--fill-itra", action="store_true")
    seed_betrail.add_argument("--itra-cookie")
    seed_betrail.add_argument("--betrail-cookie")

    import_duckdb = subparsers.add_parser(
        "import-duckdb",
        help="Import matched athlete entries from a TrailIntel DuckDB lookup cache.",
    )
    import_duckdb.add_argument(
        "--repo",
        help=(
            "Path to the local score repo checkout "
            "(default: TRAILINTEL_SCORE_REPO env var, then config [score_repo].path)."
        ),
    )
    import_duckdb.add_argument(
        "--cache-db",
        required=True,
        help="Path to the TrailIntel DuckDB cache file to import from.",
    )
    import_duckdb.add_argument("--min-match-score", type=float, default=DEFAULT_MIN_MATCH_SCORE)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    repo_path = Path(args.repo).expanduser() if args.repo else default_score_repo_path()
    if repo_path is None:
        parser.error("Provide --repo or configure TRAILINTEL_SCORE_REPO / [score_repo].path.")

    try:
        if args.command == "seed-betrail":
            imported_count, summary = seed_betrail_repo(
                repo_path=repo_path,
                threshold=float(args.threshold),
                timeout=args.timeout,
                fill_utmb=bool(args.fill_utmb),
                fill_itra=bool(args.fill_itra),
                itra_cookie=args.itra_cookie or os.getenv("ITRA_COOKIE"),
                betrail_cookie=args.betrail_cookie or os.getenv("BETRAIL_COOKIE"),
            )
        elif args.command == "import-duckdb":
            imported_count, summary = import_duckdb_cache(
                repo_path=repo_path,
                cache_db_path=args.cache_db,
                min_match_score=max(0.0, min(1.0, float(args.min_match_score))),
            )
        else:
            parser.error(f"Unsupported command: {args.command}")
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.command == "seed-betrail":
        print(f"Seeded {imported_count} Betrail athletes into {repo_path}")
        issues = summary.get("provider_issues", {})
        if isinstance(issues, dict):
            active_issues = {key: value for key, value in issues.items() if value}
            if active_issues:
                print(f"Provider issues: {active_issues}")
    else:
        print(f"Imported {imported_count} cached athlete entries into {repo_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
