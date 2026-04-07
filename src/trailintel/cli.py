from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Iterable

import requests

from trailintel.matching import canonical_name, is_strong_person_name_match, match_score
from trailintel.models import AthleteRecord
from trailintel.participants import (
    dedupe_names,
    fetch_participants_from_url,
    load_itra_overrides,
    load_participants_file,
    normalize_name,
)
from trailintel.providers.betrail import BetrailCatalogEntry, BetrailClient, BetrailLookupError
from trailintel.providers.itra import ItraCatalogEntry, ItraClient, ItraLookupError
from trailintel.providers.utmb import UtmbCatalogEntry, UtmbClient
from trailintel.report import export_records, render_table, sort_records
from trailintel.score_repo import (
    AthleteScoreRepo,
    RepoProviderLookup,
    RepoProviderObservation,
    default_score_repo_path,
    provider_score_scale,
)
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
    for key in _catalog_exact_lookup_keys(input_name):
        exact = exact_lookup.get(key)
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


def _catalog_exact_lookup_keys(input_name: str) -> list[str]:
    canonical = canonical_name(input_name)
    if not canonical:
        return []

    tokens = canonical.split()
    keys = [canonical]
    if len(tokens) >= 2:
        rotated_first = " ".join(tokens[1:] + tokens[:1])
        rotated_last = " ".join(tokens[-1:] + tokens[:-1])
        if rotated_first and rotated_first not in keys:
            keys.append(rotated_first)
        if rotated_last and rotated_last not in keys:
            keys.append(rotated_last)
    return keys


def _is_strong_catalog_name_match(query_name: str, candidate_name: str) -> bool:
    return is_strong_person_name_match(query_name, candidate_name)


def _betrail_threshold(score_threshold: float) -> float:
    return score_threshold / 10.0


def _is_above_threshold(record: AthleteRecord, threshold: float) -> bool:
    return (
        (record.utmb_index is not None and record.utmb_index > threshold)
        or (record.itra_score is not None and record.itra_score > threshold)
        or (record.betrail_score is not None and record.betrail_score > _betrail_threshold(threshold))
    )


def _should_lookup_itra_after_utmb(*, utmb_index: float | None, threshold: float) -> bool:
    return utmb_index is None or utmb_index > threshold


def _itra_skipped_due_to_utmb_note(*, utmb_index: float | None, threshold: float) -> str:
    if utmb_index is None:
        return "ITRA skipped after UTMB pass"
    return f"ITRA skipped because UTMB {utmb_index:.1f} <= threshold {threshold:.1f}"


def _provider_label(provider: str) -> str:
    if provider == "betrail":
        return "Betrail"
    return provider.upper()


def _provider_miss_note(provider: str, *, lookup_threshold: float | None) -> str:
    label = _provider_label(provider)
    if provider == "betrail" or lookup_threshold is not None:
        return f"{label} high-score catalog no match"
    return f"{label} not found"


def _apply_provider_snapshot(
    record: AthleteRecord,
    *,
    provider: str,
    lookup: RepoProviderLookup,
) -> None:
    if lookup.status == "miss":
        record.notes = _append_note(
            record.notes,
            _provider_miss_note(provider, lookup_threshold=lookup.lookup_threshold),
        )
        return

    if provider == "utmb":
        record.utmb_index = lookup.score
        record.utmb_match_name = lookup.matched_name
        record.utmb_match_score = lookup.match_confidence
        record.utmb_profile_url = lookup.profile_url
        return

    if provider == "itra":
        record.itra_score = lookup.score
        record.itra_match_name = lookup.matched_name
        record.itra_match_score = lookup.match_confidence
        record.itra_profile_url = lookup.profile_url
        return

    record.betrail_score = lookup.score
    record.betrail_match_name = lookup.matched_name
    record.betrail_match_score = lookup.match_confidence
    record.betrail_profile_url = lookup.profile_url


def _apply_stale_repo_fallback(
    record: AthleteRecord,
    *,
    provider: str,
    lookup: RepoProviderLookup,
) -> None:
    _apply_provider_snapshot(record, provider=provider, lookup=lookup)
    record.notes = _append_note(
        record.notes,
        f"{_provider_label(provider)} stale score repo fallback used",
    )


def _repo_provider_observation(
    *,
    provider: str,
    status: str,
    matched_name: str | None,
    profile_url: str | None,
    score: float | None,
    match_confidence: float | None,
    source_run_id: str | None,
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


def _new_score_repo_stats() -> dict[str, int]:
    return {
        "athletes_seen": 0,
        "athletes_created": 0,
        "athletes_updated": 0,
        "provider_updates": 0,
    }


def _write_score_repo_record(
    *,
    score_repo: AthleteScoreRepo | None,
    input_name: str,
    observations: list[RepoProviderObservation],
    source_run_id: str | None,
    score_repo_read_only: bool,
    score_repo_stats: dict[str, int] | None,
) -> None:
    if score_repo is None or score_repo_read_only or source_run_id is None:
        return

    result = score_repo.write_athlete_observations(
        input_name=input_name,
        observations=observations,
        source_run_id=source_run_id,
        source_kind="report-refresh",
    )
    if score_repo_stats is None:
        return
    score_repo_stats["athletes_seen"] += 1
    if result.created:
        score_repo_stats["athletes_created"] += 1
    if result.updated:
        score_repo_stats["athletes_updated"] += 1
    score_repo_stats["provider_updates"] += result.provider_updates


def _enrich_records(
    names: Iterable[str],
    *,
    min_match_score: float,
    score_threshold: float = 680.0,
    timeout: int,
    skip_itra: bool,
    itra_overrides: dict[str, float] | None,
    itra_cookie: str | None,
    score_repo: AthleteScoreRepo | None = None,
    score_repo_read_only: bool = False,
    score_repo_run_id: str | None = None,
    score_repo_stats: dict[str, int] | None = None,
    provider_issues: dict[str, str | None] | None = None,
) -> list[AthleteRecord]:
    names_list = list(names)
    utmb_client = UtmbClient(timeout=timeout)
    itra_client = ItraClient(timeout=timeout, cookie=itra_cookie)
    betrail_client = BetrailClient(timeout=timeout)

    betrail_lookup_threshold = _betrail_threshold(score_threshold)
    need_betrail_catalog = any(
        score_repo is None
        or (
            (lookup := score_repo.get_provider_snapshot(
                query_name=name,
                provider="betrail",
                lookup_threshold=betrail_lookup_threshold,
            ))
            is None
            or lookup.is_stale
        )
        for name in names_list
    )
    if need_betrail_catalog:
        betrail_catalog, betrail_issue = _build_betrail_catalog(
            betrail_client=betrail_client,
            threshold=score_threshold,
        )
    else:
        betrail_catalog, betrail_issue = [], None
    if provider_issues is not None:
        provider_issues["betrail"] = betrail_issue

    betrail_entries = [(entry.name, entry.betrail_score, entry.profile_url) for entry in betrail_catalog]
    betrail_exact = {canonical_name(name): (name, score, profile) for name, score, profile in betrail_entries}
    betrail_min_match_score = max(0.85, min_match_score)

    records: list[AthleteRecord] = []
    itra_block_reason: str | None = None
    consecutive_itra_failures = 0
    max_consecutive_itra_failures = 8
    overrides = itra_overrides or {}

    for name in names_list:
        record = AthleteRecord(input_name=name)
        observations: list[RepoProviderObservation] = []

        utmb_repo_lookup = (
            score_repo.get_provider_snapshot(query_name=name, provider="utmb")
            if score_repo is not None
            else None
        )
        if utmb_repo_lookup is not None and not utmb_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="utmb", lookup=utmb_repo_lookup)
        else:
            try:
                utmb_match = utmb_client.search(name)
                if utmb_match:
                    record.utmb_index = utmb_match.utmb_index
                    record.utmb_match_name = utmb_match.matched_name
                    record.utmb_match_score = utmb_match.match_score
                    record.utmb_profile_url = utmb_match.profile_url
                    if utmb_match.match_score < min_match_score:
                        record.notes = _append_note(record.notes, "UTMB low-confidence match")
                    observations.append(
                        _repo_provider_observation(
                            provider="utmb",
                            status="matched",
                            matched_name=utmb_match.matched_name,
                            profile_url=utmb_match.profile_url,
                            score=utmb_match.utmb_index,
                            match_confidence=utmb_match.match_score,
                            source_run_id=score_repo_run_id,
                            persist=utmb_match.match_score >= min_match_score,
                        )
                    )
                else:
                    record.notes = _append_note(record.notes, "UTMB not found")
                    observations.append(
                        _repo_provider_observation(
                            provider="utmb",
                            status="miss",
                            matched_name=None,
                            profile_url=None,
                            score=None,
                            match_confidence=None,
                            source_run_id=score_repo_run_id,
                        )
                    )
            except requests.RequestException as exc:
                if utmb_repo_lookup is not None and utmb_repo_lookup.is_stale:
                    _apply_stale_repo_fallback(record, provider="utmb", lookup=utmb_repo_lookup)
                else:
                    record.notes = _append_note(record.notes, f"UTMB error: {exc.__class__.__name__}")

        override = _override_lookup(name, overrides)
        if override is not None:
            record.itra_score = override
            record.notes = _append_note(record.notes, "ITRA from override file")
            betrail_repo_lookup = (
                score_repo.get_provider_snapshot(
                    query_name=name,
                    provider="betrail",
                    lookup_threshold=betrail_lookup_threshold,
                )
                if score_repo is not None
                else None
            )
            if betrail_repo_lookup is not None and not betrail_repo_lookup.is_stale:
                _apply_provider_snapshot(record, provider="betrail", lookup=betrail_repo_lookup)
            elif betrail_issue:
                if betrail_repo_lookup is not None and betrail_repo_lookup.is_stale:
                    _apply_stale_repo_fallback(record, provider="betrail", lookup=betrail_repo_lookup)
                else:
                    record.notes = _append_note(record.notes, betrail_issue)
            else:
                previous_score = record.betrail_score
                _apply_betrail_catalog_match(
                    record,
                    input_name=name,
                    entries=betrail_entries,
                    exact_lookup=betrail_exact,
                    min_match_score=betrail_min_match_score,
                    issue=betrail_issue,
                    note_missing=False,
                )
                if record.betrail_score is not None and record.betrail_score != previous_score:
                    observations.append(
                        _repo_provider_observation(
                            provider="betrail",
                            status="matched",
                            matched_name=record.betrail_match_name,
                            profile_url=record.betrail_profile_url,
                            score=record.betrail_score,
                            match_confidence=record.betrail_match_score,
                            source_run_id=score_repo_run_id,
                            lookup_threshold=betrail_lookup_threshold,
                        )
                    )
                else:
                    observations.append(
                        _repo_provider_observation(
                            provider="betrail",
                            status="miss",
                            matched_name=None,
                            profile_url=None,
                            score=None,
                            match_confidence=None,
                            source_run_id=score_repo_run_id,
                            lookup_threshold=betrail_lookup_threshold,
                        )
                    )
            _write_score_repo_record(
                score_repo=score_repo,
                input_name=name,
                observations=observations,
                source_run_id=score_repo_run_id,
                score_repo_read_only=score_repo_read_only,
                score_repo_stats=score_repo_stats,
            )
            records.append(record)
            continue

        itra_repo_lookup = (
            score_repo.get_provider_snapshot(query_name=name, provider="itra")
            if score_repo is not None
            else None
        )
        if itra_repo_lookup is not None and not itra_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="itra", lookup=itra_repo_lookup)
        elif skip_itra:
            if itra_repo_lookup is not None and itra_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="itra", lookup=itra_repo_lookup)
            elif itra_repo_lookup is None:
                record.notes = _append_note(record.notes, "ITRA skipped by flag")
        elif not _should_lookup_itra_after_utmb(
            utmb_index=record.utmb_index,
            threshold=score_threshold,
        ):
            if itra_repo_lookup is not None and itra_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="itra", lookup=itra_repo_lookup)
            else:
                record.notes = _append_note(
                    record.notes,
                    _itra_skipped_due_to_utmb_note(
                        utmb_index=record.utmb_index,
                        threshold=score_threshold,
                    ),
                )
        elif itra_block_reason:
            if itra_repo_lookup is not None and itra_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="itra", lookup=itra_repo_lookup)
            else:
                record.notes = _append_note(record.notes, f"ITRA unavailable: {itra_block_reason}")
        else:
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
                    observations.append(
                        _repo_provider_observation(
                            provider="itra",
                            status="matched",
                            matched_name=itra_match.matched_name,
                            profile_url=itra_match.profile_url,
                            score=itra_match.itra_score,
                            match_confidence=itra_match.match_score,
                            source_run_id=score_repo_run_id,
                            persist=(
                                itra_match.match_score >= min_match_score
                                and itra_match.itra_score is not None
                            ),
                        )
                    )
                else:
                    record.notes = _append_note(record.notes, "ITRA not found")
                    observations.append(
                        _repo_provider_observation(
                            provider="itra",
                            status="miss",
                            matched_name=None,
                            profile_url=None,
                            score=None,
                            match_confidence=None,
                            source_run_id=score_repo_run_id,
                        )
                    )
                consecutive_itra_failures = 0
            except ItraLookupError as exc:
                consecutive_itra_failures += 1
                error_message = str(exc)
                if itra_repo_lookup is not None and itra_repo_lookup.is_stale:
                    _apply_stale_repo_fallback(record, provider="itra", lookup=itra_repo_lookup)
                else:
                    record.notes = _append_note(record.notes, f"ITRA unavailable: {error_message}")
                if consecutive_itra_failures >= max_consecutive_itra_failures:
                    itra_block_reason = (
                        f"{error_message} (stopped after {max_consecutive_itra_failures} consecutive failures)"
                    )

        betrail_repo_lookup = (
            score_repo.get_provider_snapshot(
                query_name=name,
                provider="betrail",
                lookup_threshold=betrail_lookup_threshold,
            )
            if score_repo is not None
            else None
        )
        if betrail_repo_lookup is not None and not betrail_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="betrail", lookup=betrail_repo_lookup)
        elif betrail_issue:
            if betrail_repo_lookup is not None and betrail_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="betrail", lookup=betrail_repo_lookup)
            else:
                record.notes = _append_note(record.notes, betrail_issue)
        else:
            previous_score = record.betrail_score
            _apply_betrail_catalog_match(
                record,
                input_name=name,
                entries=betrail_entries,
                exact_lookup=betrail_exact,
                min_match_score=betrail_min_match_score,
                issue=betrail_issue,
                note_missing=False,
            )
            if record.betrail_score is not None and record.betrail_score != previous_score:
                observations.append(
                    _repo_provider_observation(
                        provider="betrail",
                        status="matched",
                        matched_name=record.betrail_match_name,
                        profile_url=record.betrail_profile_url,
                        score=record.betrail_score,
                        match_confidence=record.betrail_match_score,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=betrail_lookup_threshold,
                    )
                )
            else:
                observations.append(
                    _repo_provider_observation(
                        provider="betrail",
                        status="miss",
                        matched_name=None,
                        profile_url=None,
                        score=None,
                        match_confidence=None,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=betrail_lookup_threshold,
                    )
                )

        _write_score_repo_record(
            score_repo=score_repo,
            input_name=name,
            observations=observations,
            source_run_id=score_repo_run_id,
            score_repo_read_only=score_repo_read_only,
            score_repo_stats=score_repo_stats,
        )
        records.append(record)

    if provider_issues is not None and itra_block_reason:
        provider_issues["itra"] = itra_block_reason
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


def _build_betrail_catalog(
    betrail_client: BetrailClient,
    *,
    threshold: float,
) -> tuple[list[BetrailCatalogEntry], str | None]:
    try:
        return betrail_client.fetch_catalog_above_threshold(threshold=_betrail_threshold(threshold)), None
    except (BetrailLookupError, requests.RequestException) as exc:
        return [], f"Betrail catalog unavailable: {exc}"


def _apply_betrail_catalog_match(
    record: AthleteRecord,
    *,
    input_name: str,
    entries: list[tuple[str, float, str | None]],
    exact_lookup: dict[str, tuple[str, float, str | None]],
    min_match_score: float,
    issue: str | None,
    note_missing: bool,
) -> None:
    if issue:
        record.notes = _append_note(record.notes, issue)
        return

    betrail_match = _best_catalog_match(
        input_name,
        entries=entries,
        exact_lookup=exact_lookup,
        min_match_score=min_match_score,
        enforce_strong_name_guard=True,
    )
    if betrail_match:
        record.betrail_score = betrail_match.score
        record.betrail_match_name = betrail_match.matched_name
        record.betrail_match_score = betrail_match.confidence
        record.betrail_profile_url = betrail_match.profile_url
        if betrail_match.confidence < 1.0:
            record.notes = _append_note(record.notes, "Betrail catalog fuzzy match")
    elif note_missing:
        record.notes = _append_note(record.notes, "Betrail high-score catalog no match")


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
    score_repo: AthleteScoreRepo | None = None,
    score_repo_read_only: bool = False,
    score_repo_run_id: str | None = None,
    score_repo_stats: dict[str, int] | None = None,
    provider_issues: dict[str, str | None] | None = None,
) -> list[AthleteRecord]:
    names_list = list(names)
    overrides = itra_overrides or {}
    utmb_client = UtmbClient(timeout=timeout)
    itra_client = ItraClient(timeout=timeout, cookie=itra_cookie)
    betrail_client = BetrailClient(timeout=timeout)

    betrail_lookup_threshold = _betrail_threshold(score_threshold)
    need_utmb_catalog = any(
        score_repo is None
        or (
            (lookup := score_repo.get_provider_snapshot(
                query_name=name,
                provider="utmb",
                lookup_threshold=score_threshold,
            ))
            is None
            or lookup.is_stale
        )
        for name in names_list
    )
    need_itra_catalog = (not skip_itra) and any(
        score_repo is None
        or (
            (lookup := score_repo.get_provider_snapshot(
                query_name=name,
                provider="itra",
                lookup_threshold=score_threshold,
            ))
            is None
            or lookup.is_stale
        )
        for name in names_list
    )
    need_betrail_catalog = any(
        score_repo is None
        or (
            (lookup := score_repo.get_provider_snapshot(
                query_name=name,
                provider="betrail",
                lookup_threshold=betrail_lookup_threshold,
            ))
            is None
            or lookup.is_stale
        )
        for name in names_list
    )

    if need_utmb_catalog:
        utmb_catalog, utmb_issue = _build_utmb_catalog(
            utmb_client=utmb_client,
            threshold=score_threshold,
            max_pages=utmb_catalog_max_pages,
        )
    else:
        utmb_catalog, utmb_issue = [], None
    if need_itra_catalog:
        itra_catalog, itra_issue = _build_itra_catalog(
            itra_client=itra_client,
            threshold=score_threshold,
            skip_itra=skip_itra,
        )
    else:
        itra_catalog, itra_issue = [], ("ITRA skipped by flag" if skip_itra else None)
    if need_betrail_catalog:
        betrail_catalog, betrail_issue = _build_betrail_catalog(
            betrail_client=betrail_client,
            threshold=score_threshold,
        )
    else:
        betrail_catalog, betrail_issue = [], None
    if provider_issues is not None:
        provider_issues["utmb"] = utmb_issue
        provider_issues["itra"] = itra_issue
        provider_issues["betrail"] = betrail_issue

    utmb_entries = [(entry.name, entry.utmb_index, entry.profile_url) for entry in utmb_catalog]
    itra_entries = [(entry.name, entry.itra_score, entry.profile_url) for entry in itra_catalog]
    betrail_entries = [(entry.name, entry.betrail_score, entry.profile_url) for entry in betrail_catalog]
    utmb_exact = {canonical_name(name): (name, score, profile) for name, score, profile in utmb_entries}
    itra_exact = {canonical_name(name): (name, score, profile) for name, score, profile in itra_entries}
    betrail_exact = {canonical_name(name): (name, score, profile) for name, score, profile in betrail_entries}

    records: list[AthleteRecord] = []
    for name in names_list:
        record = AthleteRecord(input_name=name)
        observations: list[RepoProviderObservation] = []

        utmb_repo_lookup = (
            score_repo.get_provider_snapshot(
                query_name=name,
                provider="utmb",
                lookup_threshold=score_threshold,
            )
            if score_repo is not None
            else None
        )
        if utmb_repo_lookup is not None and not utmb_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="utmb", lookup=utmb_repo_lookup)
        elif utmb_issue:
            if utmb_repo_lookup is not None and utmb_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="utmb", lookup=utmb_repo_lookup)
            else:
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
                observations.append(
                    _repo_provider_observation(
                        provider="utmb",
                        status="matched",
                        matched_name=utmb_match.matched_name,
                        profile_url=utmb_match.profile_url,
                        score=utmb_match.score,
                        match_confidence=utmb_match.confidence,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=score_threshold,
                    )
                )
            else:
                record.notes = _append_note(record.notes, "UTMB high-score catalog no match")
                observations.append(
                    _repo_provider_observation(
                        provider="utmb",
                        status="miss",
                        matched_name=None,
                        profile_url=None,
                        score=None,
                        match_confidence=None,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=score_threshold,
                    )
                )

        override = _override_lookup(name, overrides)
        if override is not None:
            record.itra_score = override
            record.notes = _append_note(record.notes, "ITRA from override file")
            betrail_repo_lookup = (
                score_repo.get_provider_snapshot(
                    query_name=name,
                    provider="betrail",
                    lookup_threshold=betrail_lookup_threshold,
                )
                if score_repo is not None
                else None
            )
            if betrail_repo_lookup is not None and not betrail_repo_lookup.is_stale:
                _apply_provider_snapshot(record, provider="betrail", lookup=betrail_repo_lookup)
            elif betrail_issue:
                if betrail_repo_lookup is not None and betrail_repo_lookup.is_stale:
                    _apply_stale_repo_fallback(record, provider="betrail", lookup=betrail_repo_lookup)
                else:
                    record.notes = _append_note(record.notes, betrail_issue)
            else:
                previous_score = record.betrail_score
                _apply_betrail_catalog_match(
                    record,
                    input_name=name,
                    entries=betrail_entries,
                    exact_lookup=betrail_exact,
                    min_match_score=catalog_min_match_score,
                    issue=betrail_issue,
                    note_missing=True,
                )
                if record.betrail_score is not None and record.betrail_score != previous_score:
                    observations.append(
                        _repo_provider_observation(
                            provider="betrail",
                            status="matched",
                            matched_name=record.betrail_match_name,
                            profile_url=record.betrail_profile_url,
                            score=record.betrail_score,
                            match_confidence=record.betrail_match_score,
                            source_run_id=score_repo_run_id,
                            lookup_threshold=betrail_lookup_threshold,
                        )
                    )
                else:
                    observations.append(
                        _repo_provider_observation(
                            provider="betrail",
                            status="miss",
                            matched_name=None,
                            profile_url=None,
                            score=None,
                            match_confidence=None,
                            source_run_id=score_repo_run_id,
                            lookup_threshold=betrail_lookup_threshold,
                        )
                    )
            _write_score_repo_record(
                score_repo=score_repo,
                input_name=name,
                observations=observations,
                source_run_id=score_repo_run_id,
                score_repo_read_only=score_repo_read_only,
                score_repo_stats=score_repo_stats,
            )
            records.append(record)
            continue

        itra_repo_lookup = (
            score_repo.get_provider_snapshot(
                query_name=name,
                provider="itra",
                lookup_threshold=score_threshold,
            )
            if score_repo is not None
            else None
        )
        if itra_repo_lookup is not None and not itra_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="itra", lookup=itra_repo_lookup)
        elif itra_issue:
            if itra_repo_lookup is not None and itra_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="itra", lookup=itra_repo_lookup)
            else:
                record.notes = _append_note(record.notes, itra_issue)
        else:
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
                observations.append(
                    _repo_provider_observation(
                        provider="itra",
                        status="matched",
                        matched_name=itra_match.matched_name,
                        profile_url=itra_match.profile_url,
                        score=itra_match.score,
                        match_confidence=itra_match.confidence,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=score_threshold,
                    )
                )
            else:
                record.notes = _append_note(record.notes, "ITRA high-score catalog no match")
                observations.append(
                    _repo_provider_observation(
                        provider="itra",
                        status="miss",
                        matched_name=None,
                        profile_url=None,
                        score=None,
                        match_confidence=None,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=score_threshold,
                    )
                )

        betrail_repo_lookup = (
            score_repo.get_provider_snapshot(
                query_name=name,
                provider="betrail",
                lookup_threshold=betrail_lookup_threshold,
            )
            if score_repo is not None
            else None
        )
        if betrail_repo_lookup is not None and not betrail_repo_lookup.is_stale:
            _apply_provider_snapshot(record, provider="betrail", lookup=betrail_repo_lookup)
        elif betrail_issue:
            if betrail_repo_lookup is not None and betrail_repo_lookup.is_stale:
                _apply_stale_repo_fallback(record, provider="betrail", lookup=betrail_repo_lookup)
            else:
                record.notes = _append_note(record.notes, betrail_issue)
        else:
            previous_score = record.betrail_score
            _apply_betrail_catalog_match(
                record,
                input_name=name,
                entries=betrail_entries,
                exact_lookup=betrail_exact,
                min_match_score=catalog_min_match_score,
                issue=betrail_issue,
                note_missing=True,
            )
            if record.betrail_score is not None and record.betrail_score != previous_score:
                observations.append(
                    _repo_provider_observation(
                        provider="betrail",
                        status="matched",
                        matched_name=record.betrail_match_name,
                        profile_url=record.betrail_profile_url,
                        score=record.betrail_score,
                        match_confidence=record.betrail_match_score,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=betrail_lookup_threshold,
                    )
                )
            else:
                observations.append(
                    _repo_provider_observation(
                        provider="betrail",
                        status="miss",
                        matched_name=None,
                        profile_url=None,
                        score=None,
                        match_confidence=None,
                        source_run_id=score_repo_run_id,
                        lookup_threshold=betrail_lookup_threshold,
                    )
                )

        _write_score_repo_record(
            score_repo=score_repo,
            input_name=name,
            observations=observations,
            source_run_id=score_repo_run_id,
            score_repo_read_only=score_repo_read_only,
            score_repo_stats=score_repo_stats,
        )
        records.append(record)

    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trailintel",
        description=(
            "Build a TrailIntel top-athlete report from race participants enriched with "
            "UTMB index, ITRA score, and Betrail score."
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
        help="Only keep athletes with UTMB/ITRA score > threshold, or Betrail score > threshold/10.",
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
        "--score-repo",
        help=(
            "Path to a local checkout of the score repo "
            "(default: TRAILINTEL_SCORE_REPO env var, then config [score_repo].path)."
        ),
    )
    parser.add_argument(
        "--score-repo-read-only",
        action="store_true",
        help="Read from the score repo cache without writing refreshed athlete snapshots.",
    )
    parser.add_argument("--top", type=int, default=100, help="Number of athletes to display.")
    parser.add_argument(
        "--sort-by",
        choices=("combined", "utmb", "itra", "betrail"),
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
    score_repo: AthleteScoreRepo | None = None
    score_repo_path = Path(args.score_repo).expanduser() if args.score_repo else default_score_repo_path()
    if score_repo_path is not None:
        try:
            score_repo = AthleteScoreRepo(score_repo_path)
            score_repo.load()
        except Exception as exc:
            score_repo = None
            print(f"Warning: score repo disabled ({exc})", file=sys.stderr)

    score_repo_run_id = score_repo.generate_run_id() if score_repo is not None else None
    score_repo_stats = _new_score_repo_stats()
    provider_issues: dict[str, str | None] = {
        "utmb": None,
        "itra": None,
        "betrail": None,
    }

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
            score_repo=score_repo,
            score_repo_read_only=args.score_repo_read_only,
            score_repo_run_id=score_repo_run_id,
            score_repo_stats=score_repo_stats,
            provider_issues=provider_issues,
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
            score_repo=score_repo,
            score_repo_read_only=args.score_repo_read_only,
            score_repo_run_id=score_repo_run_id,
            score_repo_stats=score_repo_stats,
            provider_issues=provider_issues,
        )

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

    if score_repo is not None and not args.score_repo_read_only and score_repo_run_id is not None:
        score_repo.write_run_summary(
            run_id=score_repo_run_id,
            run_kind="report-refresh",
            summary={
                "participants": len(names),
                "qualified": len(filtered),
                "strategy": args.strategy,
                "score_threshold": args.score_threshold,
                "race_name": heading,
                "race_url": args.race_url or "",
                "competition_name": args.competition_name or "",
                "score_repo": {
                    "path": str(score_repo.root),
                    "athletes_seen": score_repo_stats["athletes_seen"],
                    "athletes_created": score_repo_stats["athletes_created"],
                    "athletes_updated": score_repo_stats["athletes_updated"],
                    "provider_updates": score_repo_stats["provider_updates"],
                },
                "provider_issues": provider_issues,
            },
        )

    if args.site_dir:
        snapshot = build_report_snapshot(
            title=heading,
            all_records=records,
            qualified_records=filtered,
            participants_count=len(names),
            strategy=args.strategy,
            top=max(1, args.top),
            sort_by=args.sort_by,
            race_url=args.race_url or "",
            competition_name=args.competition_name or "",
            score_threshold=args.score_threshold,
            stale_provider_fallback_used=any(
                "stale score repo fallback used" in (record.notes or "")
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
