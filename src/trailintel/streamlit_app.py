from __future__ import annotations

import csv
from datetime import UTC, datetime
import html
import io
import json
import os
import re
import math
from itertools import product
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
import streamlit as st

from trailintel.cache_store import (
    RACE_HISTORY_MAX_RUNS,
    LookupCacheStore,
    RaceRunHistoryEntry,
    SavedRaceEntry,
    default_cache_db_path,
)
from trailintel.matching import canonical_name
from trailintel.cli import (
    _apply_betrail_catalog_match,
    _build_betrail_catalog,
    _enrich_records,
    _enrich_records_from_catalog,
    _is_above_threshold,
    _itra_skipped_due_to_utmb_note,
    _should_lookup_itra_after_utmb,
)
from trailintel.models import AthleteRecord
from trailintel.participants import dedupe_names, fetch_participants_from_url, load_participants_file, normalize_name
from trailintel.providers.betrail import BetrailClient
from trailintel.providers.itra import ItraClient, ItraLookupError, ItraMatch
from trailintel.providers.utmb import UtmbClient, UtmbMatch
from trailintel.report import sort_records

RESULT_STATE_KEY = "racer_last_report"
RACE_NAME_INPUT_KEY = "racer_input_race_name"
RACE_URL_INPUT_KEY = "racer_input_race_url"
COMPETITION_INPUT_KEY = "racer_input_competition_name"
SAVED_RACE_SELECT_KEY = "racer_saved_race_select"
SAVED_RACE_RUN_SELECT_KEY = "racer_saved_race_run_select"
PENDING_SAVED_RACE_SELECT_KEY = "racer_pending_saved_race_select"
LAST_SELECTED_SAVED_RACE_KEY = "racer_last_selected_saved_race"
ITRA_COOKIE_INPUT_KEY = "racer_input_itra_cookie"
ITRA_COOKIE_SETTING_KEY = "itra_cookie"
GOOGLE_CLIENT_SETTING_KEY = "google_oauth_client_json"
GOOGLE_TOKEN_SETTING_KEY = "google_oauth_token_json"
GOOGLE_CLIENT_INPUT_KEY = "racer_google_client_json"
GOOGLE_SHEET_URL_KEY = "racer_google_sheet_url"
GOOGLE_SHEET_TAB_KEY = "racer_google_sheet_tab"
CUSTOM_RACE_OPTION = "__custom__"
DEFAULT_SAVED_RACES: list[tuple[str, str, str]] = [
    (
        "Trail du Sanglier 2026 - Le 40 km",
        "https://in.yaka-inscription.com/trail-du-sanglier-2026?currentPage=select-competition",
        "Le 40 km",
    ),
    (
        "Entrelacs Run and Trail 2026 - Maratrail des hauts du lac - 42km",
        "https://in.njuko.com/entrelacs-run-and-trail-2026?currentPage=select-competition",
        "Maratrail des hauts du lac - 42km",
    ),
]
_DEFAULT_RACE_NAME, _DEFAULT_RACE_URL, _DEFAULT_COMPETITION = DEFAULT_SAVED_RACES[0]


def _append_note(current: str, message: str) -> str:
    return message if not current else f"{current}; {message}"


def _score_fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def _should_render_saved_snapshot(run_clicked: bool, session_state: dict[str, object]) -> bool:
    return (not run_clicked) and (RESULT_STATE_KEY in session_state)


def _build_report_snapshot(
    *,
    title: str,
    participants_count: int,
    rows_evaluated: int,
    qualified_count: int,
    strategy: str,
    same_name_mode: str,
    rows: list[dict[str, object]],
    export_rows: list[dict[str, object]],
    no_result_names: list[str],
    utmb_scores: list[float],
    itra_scores: list[float],
    betrail_scores: list[float],
    score_summary: dict[str, int],
    cache_status: str,
    stale_cache_used: bool,
) -> dict[str, object]:
    return {
        "title": title,
        "participants_count": participants_count,
        "rows_evaluated": rows_evaluated,
        "qualified_count": qualified_count,
        "strategy": strategy,
        "same_name_mode": same_name_mode,
        "rows": rows,
        "export_rows": export_rows,
        "no_result_names": no_result_names,
        "utmb_scores": utmb_scores,
        "itra_scores": itra_scores,
        "betrail_scores": betrail_scores,
        "score_summary": score_summary,
        "cache_status": cache_status,
        "stale_cache_used": stale_cache_used,
    }


def _ensure_race_input_defaults(session_state: dict[str, object]) -> None:
    session_state.setdefault(RACE_NAME_INPUT_KEY, _DEFAULT_RACE_NAME)
    session_state.setdefault(RACE_URL_INPUT_KEY, _DEFAULT_RACE_URL)
    session_state.setdefault(COMPETITION_INPUT_KEY, _DEFAULT_COMPETITION)
    session_state.setdefault(SAVED_RACE_SELECT_KEY, CUSTOM_RACE_OPTION)


def _apply_pending_saved_race_selection(session_state: dict[str, object]) -> None:
    pending = session_state.pop(PENDING_SAVED_RACE_SELECT_KEY, None)
    if pending is None:
        return
    session_state[SAVED_RACE_SELECT_KEY] = str(pending)


def _queue_saved_race_selection(session_state: dict[str, object], race_key: str) -> None:
    session_state[PENDING_SAVED_RACE_SELECT_KEY] = race_key


def _should_auto_load_selected_race(
    selected_race_key: str,
    previous_selected_race_key: str | None,
) -> bool:
    if selected_race_key == CUSTOM_RACE_OPTION:
        return False
    return selected_race_key != (previous_selected_race_key or "")


def _initialize_itra_cookie_input(
    session_state: dict[str, object],
    *,
    shared_store: LookupCacheStore | None,
) -> None:
    if ITRA_COOKIE_INPUT_KEY in session_state:
        return

    persisted = shared_store.get_setting(ITRA_COOKIE_SETTING_KEY) if shared_store else None
    initial_value = persisted if persisted is not None else os.getenv("ITRA_COOKIE", "")
    session_state[ITRA_COOKIE_INPUT_KEY] = initial_value


def _initialize_google_client_input(
    session_state: dict[str, object],
    *,
    shared_store: LookupCacheStore | None,
) -> None:
    if GOOGLE_CLIENT_INPUT_KEY in session_state:
        return
    persisted = shared_store.get_setting(GOOGLE_CLIENT_SETTING_KEY) if shared_store else None
    session_state[GOOGLE_CLIENT_INPUT_KEY] = persisted or ""


def _load_google_client_config(session_state: dict[str, object]) -> dict[str, object] | None:
    raw = str(session_state.get(GOOGLE_CLIENT_INPUT_KEY, "") or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_google_sheet_id(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"[a-zA-Z0-9-_]+", text):
        return text
    return None


def _get_google_credentials(
    *,
    shared_store: LookupCacheStore | None,
    session_state: dict[str, object],
) -> object | None:
    token_json = ""
    if shared_store:
        token_json = shared_store.get_setting(GOOGLE_TOKEN_SETTING_KEY) or ""
    if not token_json:
        token_json = str(session_state.get(GOOGLE_TOKEN_SETTING_KEY, "") or "")
    if not token_json.strip():
        return None

    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
    except Exception:
        return None

    try:
        payload = json.loads(token_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_authorized_user_info(payload, scopes=scopes)
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            return creds
        updated = creds.to_json()
        session_state[GOOGLE_TOKEN_SETTING_KEY] = updated
        if shared_store:
            try:
                shared_store.put_setting(
                    setting_key=GOOGLE_TOKEN_SETTING_KEY,
                    setting_value=updated,
                )
            except Exception:
                pass
    return creds


def _authorize_google_sheets(
    *,
    shared_store: LookupCacheStore | None,
    session_state: dict[str, object],
    client_config: dict[str, object],
) -> str | None:
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except Exception:
        return "Google auth dependencies are missing. Install google-auth-oauthlib."

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        flow = InstalledAppFlow.from_client_config(client_config, scopes=scopes)
        creds = flow.run_local_server(port=0, prompt="consent")
    except Exception as exc:
        return f"Authorization failed: {exc}"

    token_json = creds.to_json()
    session_state[GOOGLE_TOKEN_SETTING_KEY] = token_json
    if shared_store:
        try:
            shared_store.put_setting(
                setting_key=GOOGLE_TOKEN_SETTING_KEY,
                setting_value=token_json,
            )
        except Exception as exc:
            return f"Authorized, but could not persist token: {exc}"
    return None


def _export_to_google_sheet(
    *,
    credentials: object,
    sheet_id: str,
    worksheet: str,
    rows: list[dict[str, object]],
) -> str | None:
    if not rows:
        return "No data available to export."

    try:
        from googleapiclient.discovery import build
    except Exception:
        return "Google Sheets client is missing. Install google-api-python-client."

    service = build("sheets", "v4", credentials=credentials)
    headers = list(rows[0].keys())
    values = [headers]
    for row in rows:
        values.append([row.get(header, "") for header in headers])

    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    except Exception as exc:
        return f"Failed to open spreadsheet: {exc}"

    existing_titles = {
        sheet.get("properties", {}).get("title", "")
        for sheet in spreadsheet.get("sheets", [])
        if isinstance(sheet, dict)
    }
    if worksheet not in existing_titles:
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    "requests": [
                        {"addSheet": {"properties": {"title": worksheet}}},
                    ]
                },
            ).execute()
        except Exception as exc:
            return f"Failed to create worksheet '{worksheet}': {exc}"

    try:
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{worksheet}!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    except Exception as exc:
        return f"Failed to write data: {exc}"

    return None


def _build_race_label(race_name: str, competition_name: str, race_url: str) -> str:
    name = race_name.strip()
    competition = competition_name.strip()
    url = race_url.strip()
    return name or competition or url


def _can_auto_save_race(race_url: str) -> bool:
    return bool(race_url.strip())


def _saved_race_option_label(race: SavedRaceEntry) -> str:
    if race.competition_name:
        return f"{race.race_label} ({race.competition_name})"
    return race.race_label


def _apply_saved_race_inputs(
    session_state: dict[str, object],
    race: SavedRaceEntry,
) -> None:
    session_state[RACE_NAME_INPUT_KEY] = race.race_label
    session_state[RACE_URL_INPUT_KEY] = race.race_url
    session_state[COMPETITION_INPUT_KEY] = race.competition_name


def _history_entry_label(entry: RaceRunHistoryEntry) -> str:
    ts = entry.run_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{ts} | qualified {entry.qualified_count}/{entry.rows_evaluated} | "
        f"{entry.strategy}/{entry.same_name_mode}"
    )


def _snapshot_from_history_payload(payload_json: str) -> dict[str, object] | None:
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _collect_input_names(
    *,
    race_url: str,
    competition_name: str,
    uploaded_file,
    pasted_names: str,
    timeout: int,
) -> tuple[list[str], list[str]]:
    names: list[str] = []
    warnings: list[str] = []

    if race_url.strip():
        try:
            names.extend(
                fetch_participants_from_url(
                    race_url.strip(),
                    competition_name=competition_name.strip() or None,
                    timeout=timeout,
                )
            )
        except Exception as exc:
            warnings.append(
                "Race URL fetch failed "
                f"({exc.__class__.__name__}: {exc}). "
                "You can still use uploaded/pasted participants."
            )

    if uploaded_file is not None:
        suffix = Path(uploaded_file.name).suffix or ".txt"
        with NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = Path(tmp.name)
        try:
            names.extend(load_participants_file(tmp_path))
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

    if pasted_names.strip():
        names.extend(line.strip() for line in pasted_names.splitlines() if line.strip())

    normalized = [normalize_name(name) for name in names if name]
    return dedupe_names(name for name in normalized if name), warnings


def _build_record(
    *,
    input_name: str,
    utmb_candidate: UtmbMatch | None,
    itra_candidate: ItraMatch | None,
    notes: str = "",
    min_match_score: float,
) -> AthleteRecord:
    record = AthleteRecord(input_name=input_name, notes=notes)

    if utmb_candidate:
        record.utmb_index = utmb_candidate.utmb_index
        record.utmb_match_name = utmb_candidate.matched_name
        record.utmb_match_score = utmb_candidate.match_score
        record.utmb_profile_url = utmb_candidate.profile_url
        if utmb_candidate.match_score < min_match_score:
            record.notes = _append_note(record.notes, "UTMB low-confidence match")
    else:
        record.notes = _append_note(record.notes, "UTMB not found")

    if itra_candidate:
        record.itra_score = itra_candidate.itra_score
        record.itra_match_name = itra_candidate.matched_name
        record.itra_match_score = itra_candidate.match_score
        record.itra_profile_url = itra_candidate.profile_url
        if itra_candidate.match_score < min_match_score:
            record.notes = _append_note(record.notes, "ITRA low-confidence match")
        if itra_candidate.itra_score is None:
            record.notes = _append_note(record.notes, "ITRA score missing in response")
    return record


def _enrich_records_keep_all(
    names: list[str],
    *,
    min_match_score: float,
    score_threshold: float = 680.0,
    timeout: int,
    skip_itra: bool,
    itra_cookie: str | None,
    cache_store: LookupCacheStore | None,
    use_cache: bool,
    force_refresh_cache: bool,
    max_cartesian: int = 16,
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
    betrail_client = BetrailClient(timeout=timeout)
    betrail_catalog, betrail_issue = _build_betrail_catalog(
        betrail_client=betrail_client,
        threshold=score_threshold,
    )
    betrail_entries = [(entry.name, entry.betrail_score, entry.profile_url) for entry in betrail_catalog]
    betrail_exact = {canonical_name(name): (name, score, profile) for name, score, profile in betrail_entries}
    betrail_min_match_score = max(0.85, min_match_score)

    records: list[AthleteRecord] = []
    itra_block_reason: str | None = None
    consecutive_itra_failures = 0
    max_consecutive_itra_failures = 8

    for name in names:
        utmb_candidates: list[UtmbMatch] = []
        itra_candidates: list[ItraMatch] = []
        note_prefix = ""
        best_utmb_index: float | None = None

        try:
            utmb_candidates = utmb_client.search_same_name_candidates(name)
            utmb_scores = [
                candidate.utmb_index
                for candidate in utmb_candidates
                if candidate.utmb_index is not None
            ]
            if utmb_scores:
                best_utmb_index = max(utmb_scores)
            if utmb_client.last_lookup_stale_fallback:
                note_prefix = _append_note(note_prefix, "UTMB stale cache fallback used")
        except requests.RequestException as exc:
            note_prefix = _append_note(note_prefix, f"UTMB error: {exc.__class__.__name__}")

        if skip_itra:
            note_prefix = _append_note(note_prefix, "ITRA skipped by flag")
        elif not _should_lookup_itra_after_utmb(
            utmb_index=best_utmb_index,
            threshold=score_threshold,
        ):
            note_prefix = _append_note(
                note_prefix,
                _itra_skipped_due_to_utmb_note(
                    utmb_index=best_utmb_index,
                    threshold=score_threshold,
                ),
            )
        elif itra_block_reason:
            note_prefix = _append_note(note_prefix, f"ITRA unavailable: {itra_block_reason}")
        else:
            try:
                itra_candidates = itra_client.search_same_name_candidates(name)
                if itra_client.last_lookup_used_cookie_fallback:
                    note_prefix = _append_note(
                        note_prefix,
                        "ITRA cookie rejected, retried anonymously",
                    )
                if itra_client.last_lookup_stale_fallback:
                    note_prefix = _append_note(note_prefix, "ITRA stale cache fallback used")
                consecutive_itra_failures = 0
            except ItraLookupError as exc:
                consecutive_itra_failures += 1
                err = str(exc)
                note_prefix = _append_note(note_prefix, f"ITRA unavailable: {err}")
                if consecutive_itra_failures >= max_consecutive_itra_failures:
                    itra_block_reason = (
                        f"{err} (stopped after {max_consecutive_itra_failures} consecutive failures)"
                    )

        if len(utmb_candidates) > 1:
            note_prefix = _append_note(
                note_prefix,
                f"UTMB same-name candidates kept: {len(utmb_candidates)}",
            )
        if len(itra_candidates) > 1:
            note_prefix = _append_note(
                note_prefix,
                f"ITRA same-name candidates kept: {len(itra_candidates)}",
            )

        utmb_options: list[UtmbMatch | None] = utmb_candidates if utmb_candidates else [None]
        itra_options: list[ItraMatch | None] = itra_candidates if itra_candidates else [None]
        combinations = list(product(utmb_options, itra_options))

        if len(combinations) > max_cartesian:
            combinations.sort(
                key=lambda pair: (
                    pair[0].utmb_index if pair[0] and pair[0].utmb_index is not None else -1.0
                )
                + (pair[1].itra_score if pair[1] and pair[1].itra_score is not None else -1.0),
                reverse=True,
            )
            combinations = combinations[:max_cartesian]
            note_prefix = _append_note(
                note_prefix,
                f"Candidate combinations limited to top {max_cartesian}",
            )

        total = len(combinations)
        for idx, (utmb_candidate, itra_candidate) in enumerate(combinations, start=1):
            notes = note_prefix
            if total > 1:
                notes = _append_note(notes, f"Combination {idx}/{total}")
            record = _build_record(
                input_name=name,
                utmb_candidate=utmb_candidate,
                itra_candidate=itra_candidate,
                notes=notes,
                min_match_score=min_match_score,
            )
            _apply_betrail_catalog_match(
                record,
                input_name=name,
                entries=betrail_entries,
                exact_lookup=betrail_exact,
                min_match_score=betrail_min_match_score,
                issue=betrail_issue,
                note_missing=False,
            )
            records.append(record)

    return records


def _records_to_rows(records: list[AthleteRecord], *, top: int) -> list[dict[str, object]]:
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


def _compute_no_result_names(records: list[AthleteRecord]) -> list[str]:
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


def _aggregate_scores_by_input(
    records: list[AthleteRecord],
) -> tuple[list[float], list[float], list[float], dict[str, int]]:
    by_input: dict[str, dict[str, float | None]] = {}
    for record in records:
        normalized = normalize_name(record.input_name).casefold()
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


def _build_score_histogram(scores: list[float], *, bin_size: int = 50) -> list[dict[str, object]]:
    if not scores:
        return []

    max_value = max(scores)
    max_edge = int(math.ceil(max_value / bin_size) * bin_size)
    if max_edge <= 0:
        max_edge = bin_size
    if max_edge == 0:
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

    rows: list[dict[str, object]] = []
    for start, count in zip(bins, counts, strict=False):
        end = start + bin_size - 1
        rows.append({"Range": f"{start}-{end}", "Count": count})
    return rows


def _rows_to_csv(rows: list[dict[str, object]]) -> str:
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _render_histogram_chart(container, *, title: str, rows: list[dict[str, object]]) -> None:
    container.markdown(f"**{title}**")
    if not rows:
        container.info(f"No {title.lower()} available.")
        return

    container.vega_lite_chart(
        {
            "data": {"values": rows},
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {
                    "field": "Range",
                    "type": "ordinal",
                    "sort": None,
                    "axis": {"labelAngle": -35, "title": "Score range"},
                },
                "y": {
                    "field": "Count",
                    "type": "quantitative",
                    "title": "Participants",
                },
            },
            "height": 220,
        },
        width="stretch",
    )


def _render_profile_link_list(rows: list[dict[str, object]]) -> None:
    st.markdown("**Profile Links (open in new tab)**")
    shown = 0
    for row in rows:
        utmb_url = str(row.get("UTMB Profile") or "").strip()
        itra_url = str(row.get("ITRA Profile") or "").strip()
        betrail_url = str(row.get("Betrail Profile") or "").strip()
        if not utmb_url and not itra_url and not betrail_url:
            continue
        athlete = html.escape(str(row.get("Athlete", "")))
        parts: list[str] = []
        if utmb_url:
            safe_utmb = html.escape(utmb_url, quote=True)
            parts.append(
                f'<a href="{safe_utmb}" target="_blank" rel="noopener noreferrer">UTMB</a>'
            )
        if itra_url:
            safe_itra = html.escape(itra_url, quote=True)
            parts.append(
                f'<a href="{safe_itra}" target="_blank" rel="noopener noreferrer">ITRA</a>'
            )
        if betrail_url:
            safe_betrail = html.escape(betrail_url, quote=True)
            parts.append(
                f'<a href="{safe_betrail}" target="_blank" rel="noopener noreferrer">Betrail</a>'
            )
        st.markdown(f"- {athlete}: {' | '.join(parts)}", unsafe_allow_html=True)
        shown += 1
        if shown >= 50:
            break


def _render_score_distribution(snapshot: dict[str, object]) -> None:
    participants_count = snapshot.get("participants_count", 0)
    has_score_payload = any(
        key in snapshot for key in ("utmb_scores", "itra_scores", "betrail_scores", "score_summary")
    )
    utmb_scores = snapshot.get("utmb_scores", [])
    itra_scores = snapshot.get("itra_scores", [])
    betrail_scores = snapshot.get("betrail_scores", [])
    score_summary = snapshot.get("score_summary", {})

    if not isinstance(utmb_scores, list):
        utmb_scores = []
    if not isinstance(itra_scores, list):
        itra_scores = []
    if not isinstance(betrail_scores, list):
        betrail_scores = []
    if not isinstance(score_summary, dict):
        score_summary = {}

    st.markdown("### Score Distribution")
    if not has_score_payload:
        summary_cols = st.columns(5)
        summary_cols[0].metric("Participants", int(participants_count))
        summary_cols[1].metric("With UTMB", "n/a")
        summary_cols[2].metric("With ITRA", "n/a")
        summary_cols[3].metric("With Betrail", "n/a")
        summary_cols[4].metric("With Any Score", "n/a")
        st.info("Score distribution was not stored for this saved run. Re-run to compute charts.")
        return
    summary_cols = st.columns(5)
    summary_cols[0].metric(
        "Participants",
        int(score_summary.get("participants", participants_count)),
    )
    summary_cols[1].metric("With UTMB", int(score_summary.get("with_utmb", len(utmb_scores))))
    summary_cols[2].metric("With ITRA", int(score_summary.get("with_itra", len(itra_scores))))
    summary_cols[3].metric("With Betrail", int(score_summary.get("with_betrail", len(betrail_scores))))
    fallback_any = max(len(utmb_scores), len(itra_scores), len(betrail_scores))
    summary_cols[4].metric(
        "With Any Score",
        int(score_summary.get("with_any", fallback_any)),
    )

    if not utmb_scores and not itra_scores and not betrail_scores:
        st.info("No UTMB/ITRA/Betrail scores available to chart.")
        return

    chart_cols = st.columns(3)
    if utmb_scores:
        utmb_rows = _build_score_histogram([float(v) for v in utmb_scores if v is not None])
        _render_histogram_chart(chart_cols[0], title="UTMB Index", rows=utmb_rows)
    else:
        chart_cols[0].markdown("**UTMB Index**")
        chart_cols[0].info("No UTMB scores available.")

    if itra_scores:
        itra_rows = _build_score_histogram([float(v) for v in itra_scores if v is not None])
        _render_histogram_chart(chart_cols[1], title="ITRA Score", rows=itra_rows)
    else:
        chart_cols[1].markdown("**ITRA Score**")
        chart_cols[1].info("No ITRA scores available.")

    if betrail_scores:
        betrail_rows = _build_score_histogram([float(v) for v in betrail_scores if v is not None])
        _render_histogram_chart(chart_cols[2], title="Betrail Score", rows=betrail_rows)
    else:
        chart_cols[2].markdown("**Betrail Score**")
        chart_cols[2].info("No Betrail scores available.")


def _render_no_result_section(no_result_names: list[str]) -> None:
    st.markdown("### No result on UTMB, ITRA, and Betrail")
    if no_result_names:
        st.caption(f"{len(no_result_names)} participant(s) had no match on any provider.")
        st.dataframe(
            [{"Athlete": name} for name in no_result_names],
            width="stretch",
            hide_index=True,
        )
        return
    st.info("All participants matched at least one provider.")


def _normalize_cookie_header(raw: str) -> str | None:
    text = raw.strip()
    if not text:
        return None

    # Accept common pasted forms:
    # - "Cookie: a=1; b=2"
    # - "-b 'a=1; b=2'"
    # - "--cookie \"a=1; b=2\""
    text = re.sub(r"^\s*cookie\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*-(?:b|cookie)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*--cookie(?:=|\s+)", "", text, flags=re.IGNORECASE)
    text = text.strip().strip("'").strip('"')
    text = " ".join(text.replace("\\\n", " ").replace("\n", " ").split())
    text = re.sub(r"\s*;\s*", "; ", text).strip()

    if "=" not in text:
        return None
    return text


def _render_report_snapshot(
    snapshot: dict[str, object],
    *,
    shared_store: LookupCacheStore | None,
) -> None:
    title = str(snapshot.get("title", "Trail Race Report"))
    participants_count = int(snapshot.get("participants_count", 0))
    rows_evaluated = int(snapshot.get("rows_evaluated", 0))
    qualified_count = int(snapshot.get("qualified_count", 0))
    strategy = str(snapshot.get("strategy", "participant-first"))
    same_name_mode = str(snapshot.get("same_name_mode", "highest"))
    rows = snapshot.get("rows", [])
    export_rows = snapshot.get("export_rows", rows)
    no_result_names = snapshot.get("no_result_names", [])
    cache_status = str(snapshot.get("cache_status", ""))
    stale_cache_used = bool(snapshot.get("stale_cache_used", False))

    st.subheader(title)
    metric_cols = st.columns(5)
    metric_cols[0].metric("Input participants", participants_count)
    metric_cols[1].metric("Rows evaluated", rows_evaluated)
    metric_cols[2].metric("Qualified (> threshold)", qualified_count)
    metric_cols[3].metric("Strategy", strategy)
    metric_cols[4].metric("Same-name mode", same_name_mode)

    if cache_status:
        st.caption(cache_status)
    if stale_cache_used:
        st.warning("Some provider results were served from stale cache due live lookup failures.")

    _render_score_distribution(snapshot)

    if not isinstance(rows, list) or not rows:
        st.warning("No athletes above threshold were found.")
    else:
        st.dataframe(
            rows,
            width="stretch",
            column_config={
                "UTMB Profile": st.column_config.LinkColumn(
                    "UTMB Profile",
                    help="UTMB profile URL",
                    display_text="Open UTMB",
                ),
                "ITRA Profile": st.column_config.LinkColumn(
                    "ITRA Profile",
                    help="ITRA profile URL",
                    display_text="Open ITRA",
                ),
            },
        )
        _render_profile_link_list(rows)

    if not isinstance(export_rows, list):
        export_rows = rows if isinstance(rows, list) else []
    if export_rows:
        csv_payload = _rows_to_csv(export_rows)
        json_payload = json.dumps(export_rows, indent=2, ensure_ascii=False)
        download_cols = st.columns(2)
        download_cols[0].download_button(
            "Download CSV (all athletes)",
            data=csv_payload,
            file_name="trailintel_report_all.csv",
            mime="text/csv",
            width="stretch",
        )
        download_cols[1].download_button(
            "Download JSON (all athletes)",
            data=json_payload,
            file_name="trailintel_report_all.json",
            mime="application/json",
            width="stretch",
        )

        with st.expander("Export to Google Sheets"):
            sheet_input = st.text_input(
                "Google Sheet URL or ID",
                key=GOOGLE_SHEET_URL_KEY,
                placeholder="https://docs.google.com/spreadsheets/d/...",
            )
            sheet_tab = st.text_input(
                "Worksheet name",
                key=GOOGLE_SHEET_TAB_KEY,
                placeholder="trailintel_export",
            )

            client_upload = st.file_uploader(
                "OAuth client JSON (Desktop app)",
                type=["json"],
            )
            if client_upload is not None:
                try:
                    raw_client = client_upload.getvalue().decode("utf-8")
                except Exception:
                    raw_client = ""
                if raw_client.strip():
                    st.session_state[GOOGLE_CLIENT_INPUT_KEY] = raw_client
                    if shared_store is not None:
                        try:
                            shared_store.put_setting(
                                setting_key=GOOGLE_CLIENT_SETTING_KEY,
                                setting_value=raw_client,
                            )
                        except Exception as exc:
                            st.warning(f"Could not persist Google client config: {exc}")

            client_config = _load_google_client_config(st.session_state)
            if client_config is None:
                st.caption("Upload your OAuth client JSON to enable Google Sheets export.")

            auth_cols = st.columns(2)
            if auth_cols[0].button("Authorize Google Sheets", width="stretch"):
                if client_config is None:
                    st.warning("OAuth client JSON is required before authorizing.")
                else:
                    with st.spinner("Authorizing with Google..."):
                        error = _authorize_google_sheets(
                            shared_store=shared_store,
                            session_state=st.session_state,
                            client_config=client_config,
                        )
                    if error:
                        st.error(error)
                    else:
                        st.success("Google authorization complete.")

            if auth_cols[1].button("Export to Google Sheets", width="stretch"):
                sheet_id = _extract_google_sheet_id(sheet_input)
                if sheet_id is None:
                    st.warning("Please enter a valid Google Sheet URL or ID.")
                else:
                    creds = _get_google_credentials(
                        shared_store=shared_store,
                        session_state=st.session_state,
                    )
                    if creds is None:
                        st.warning("Please authorize Google Sheets before exporting.")
                    else:
                        with st.spinner("Exporting to Google Sheets..."):
                            error = _export_to_google_sheet(
                                credentials=creds,
                                sheet_id=sheet_id,
                                worksheet=sheet_tab.strip() or "trailintel_export",
                                rows=export_rows,
                            )
                        if error:
                            st.error(error)
                        else:
                            st.success("Export completed.")

    if not isinstance(no_result_names, list):
        no_result_names = []
    _render_no_result_section(sorted(str(name) for name in no_result_names if str(name).strip()))


def run_app() -> None:
    st.set_page_config(page_title="TrailIntel", layout="wide")
    st.title("TrailIntel: Trail Top Athlete Finder")
    _ensure_race_input_defaults(st.session_state)
    _apply_pending_saved_race_selection(st.session_state)
    shared_store: LookupCacheStore | None = None

    try:
        with st.sidebar:
            cache_db_path = default_cache_db_path()
            store_init_error: str | None = None
            try:
                shared_store = LookupCacheStore(cache_db_path)
                shared_store.seed_default_races(DEFAULT_SAVED_RACES)
            except Exception as exc:
                shared_store = None
                store_init_error = str(exc)

            if store_init_error:
                st.warning(f"Persistent storage unavailable: {store_init_error}")
            persisted_itra_cookie = (
                shared_store.get_setting(ITRA_COOKIE_SETTING_KEY) if shared_store is not None else None
            )
            _initialize_itra_cookie_input(
                st.session_state,
                shared_store=shared_store,
            )

            st.subheader("Saved Races")
            saved_races: list[SavedRaceEntry] = shared_store.list_saved_races() if shared_store else []
            saved_race_by_key = {race.race_key: race for race in saved_races}
            saved_race_options = [CUSTOM_RACE_OPTION, *saved_race_by_key.keys()]
            if st.session_state.get(SAVED_RACE_SELECT_KEY) not in saved_race_options:
                st.session_state[SAVED_RACE_SELECT_KEY] = CUSTOM_RACE_OPTION

            selected_saved_race_key = st.selectbox(
                "Saved race",
                options=saved_race_options,
                key=SAVED_RACE_SELECT_KEY,
                format_func=lambda key: (
                    "Custom / manual"
                    if key == CUSTOM_RACE_OPTION
                    else _saved_race_option_label(saved_race_by_key[key])
                ),
                disabled=shared_store is None,
            )

            previous_selected_saved_race_key = st.session_state.get(LAST_SELECTED_SAVED_RACE_KEY)
            st.session_state[LAST_SELECTED_SAVED_RACE_KEY] = selected_saved_race_key
            if (
                shared_store is not None
                and _should_auto_load_selected_race(
                    selected_saved_race_key,
                    str(previous_selected_saved_race_key)
                    if previous_selected_saved_race_key is not None
                    else None,
                )
            ):
                selected_race = saved_race_by_key.get(selected_saved_race_key)
                if selected_race is not None:
                    _apply_saved_race_inputs(st.session_state, selected_race)
                    st.rerun()

            race_action_cols = st.columns(2)
            save_race_clicked = race_action_cols[0].button(
                "Save current race",
                width="stretch",
                disabled=shared_store is None,
            )
            delete_race_clicked = race_action_cols[1].button(
                "Delete race",
                width="stretch",
                disabled=shared_store is None or selected_saved_race_key == CUSTOM_RACE_OPTION,
            )

            if save_race_clicked and shared_store is not None:
                current_race_name = str(st.session_state.get(RACE_NAME_INPUT_KEY, ""))
                current_race_url = str(st.session_state.get(RACE_URL_INPUT_KEY, ""))
                current_competition = str(st.session_state.get(COMPETITION_INPUT_KEY, ""))
                if not _can_auto_save_race(current_race_url):
                    st.warning("Race URL is required to save a race preset.")
                else:
                    saved_entry = shared_store.upsert_saved_race(
                        race_label=_build_race_label(
                            current_race_name,
                            current_competition,
                            current_race_url,
                        ),
                        race_url=current_race_url,
                        competition_name=current_competition,
                    )
                    _queue_saved_race_selection(st.session_state, saved_entry.race_key)
                    st.rerun()

            if delete_race_clicked and shared_store is not None:
                if shared_store.delete_saved_race(selected_saved_race_key):
                    _queue_saved_race_selection(st.session_state, CUSTOM_RACE_OPTION)
                    st.session_state.pop(SAVED_RACE_RUN_SELECT_KEY, None)
                    st.rerun()

            st.subheader("Run History")
            selected_for_history = str(st.session_state.get(SAVED_RACE_SELECT_KEY, CUSTOM_RACE_OPTION))
            history_entries: list[RaceRunHistoryEntry] = []
            if shared_store and selected_for_history != CUSTOM_RACE_OPTION:
                history_entries = shared_store.list_race_runs(
                    selected_for_history,
                    limit=RACE_HISTORY_MAX_RUNS,
                )
            if shared_store is None:
                st.caption("Run history unavailable while persistent storage is disabled.")
            elif selected_for_history == CUSTOM_RACE_OPTION:
                st.caption("Select a saved race to view its recent runs.")
            elif not history_entries:
                st.caption("No saved runs for this race yet.")
            else:
                history_by_id = {entry.run_id: entry for entry in history_entries}
                history_run_ids = list(history_by_id.keys())
                if st.session_state.get(SAVED_RACE_RUN_SELECT_KEY) not in history_run_ids:
                    st.session_state[SAVED_RACE_RUN_SELECT_KEY] = history_run_ids[0]

                selected_run_id = st.selectbox(
                    "Recent runs",
                    options=history_run_ids,
                    key=SAVED_RACE_RUN_SELECT_KEY,
                    format_func=lambda run_id: _history_entry_label(history_by_id[run_id]),
                )
                if st.button("Load saved result", width="stretch"):
                    selected_run = history_by_id[selected_run_id]
                    snapshot = _snapshot_from_history_payload(selected_run.payload_json)
                    if snapshot is None:
                        st.warning("Saved run payload is invalid and could not be loaded.")
                    else:
                        selected_race = saved_race_by_key.get(selected_for_history)
                        if selected_race is not None:
                            _apply_saved_race_inputs(st.session_state, selected_race)
                        st.session_state[RESULT_STATE_KEY] = snapshot
                        st.rerun()

            st.subheader("Inputs")
            race_name = st.text_input("Race Name", key=RACE_NAME_INPUT_KEY)
            race_url = st.text_input("Race URL", key=RACE_URL_INPUT_KEY)
            competition_name = st.text_input("Competition / Distance", key=COMPETITION_INPUT_KEY)
            uploaded_file = st.file_uploader(
                "Participants file (.csv/.json/.txt)",
                type=["csv", "json", "txt"],
            )
            pasted_names = st.text_area(
                "Or paste participants (one per line)",
                value="",
                height=140,
            )

            st.subheader("Scoring")
            strategy = st.selectbox("Strategy", options=["participant-first", "catalog-first"], index=0)
            duplicate_policy = st.radio(
                "Same-name matches",
                options=["highest", "keep_all"],
                help=(
                    "highest: keep the top score among same-name candidates. "
                    "keep_all: keep every same-name candidate (participant-first only)."
                ),
            )
            score_threshold = st.number_input("Score threshold (strict >)", min_value=0.0, value=680.0)
            top = st.number_input("Top rows", min_value=1, max_value=500, value=100)
            sort_by = st.selectbox("Sort by", options=["combined", "utmb", "itra", "betrail"], index=0)
            run_clicked = st.button("Run report", type="primary", width="stretch")

            st.subheader("Advanced")
            min_match_score = st.slider(
                "Min match score",
                min_value=0.0,
                max_value=1.0,
                value=0.6,
                step=0.01,
            )
            catalog_min_match_score = st.slider(
                "Catalog min match score",
                min_value=0.0,
                max_value=1.0,
                value=0.85,
                step=0.01,
            )
            timeout = st.number_input("HTTP timeout (seconds)", min_value=5, max_value=120, value=15)
            utmb_catalog_max_pages = st.number_input(
                "UTMB catalog max pages",
                min_value=1,
                max_value=300,
                value=120,
            )
            use_persistent_cache = st.checkbox("Use persistent cache", value=True)
            force_refresh_cache = st.checkbox(
                "Force refresh cache",
                value=False,
                disabled=not use_persistent_cache,
            )
            skip_itra = st.checkbox("Skip ITRA", value=False)
            itra_cookie = st.text_area(
                "ITRA Cookie (optional)",
                key=ITRA_COOKIE_INPUT_KEY,
                height=100,
                placeholder=(
                    "_ga=...; .AspNetCore.Session=...; "
                    ".AspNetCore.Identity.Application=...; SessionToken=..."
                ),
                help=(
                    "Format: cookie pairs separated by '; ' (no URL, no headers). "
                    "Example: _ga=...; .AspNetCore.Session=...; "
                    ".AspNetCore.Identity.Application=...; SessionToken=... . "
                    "You can also paste 'Cookie: ...' or '--cookie \"...\"' and it will be normalized."
                ),
            )
        if shared_store is not None and itra_cookie != (persisted_itra_cookie or ""):
            try:
                shared_store.put_setting(
                    setting_key=ITRA_COOKIE_SETTING_KEY,
                    setting_value=itra_cookie,
                )
            except Exception as exc:
                st.warning(f"Could not persist ITRA cookie: {exc}")

        _initialize_google_client_input(
            st.session_state,
            shared_store=shared_store,
        )
        if st.session_state.get(GOOGLE_SHEET_TAB_KEY) is None:
            st.session_state[GOOGLE_SHEET_TAB_KEY] = "trailintel_export"

        if _should_render_saved_snapshot(run_clicked, st.session_state):
            snapshot = st.session_state.get(RESULT_STATE_KEY)
            if isinstance(snapshot, dict):
                _render_report_snapshot(snapshot, shared_store=shared_store)
                return

        if not run_clicked:
            st.info("Configure inputs and click 'Run report'.")
            return

        cache_store_for_lookup = shared_store if use_persistent_cache else None
        if use_persistent_cache and shared_store is None:
            cache_status = "Cache: disabled (persistent storage unavailable)"
        elif use_persistent_cache:
            cache_status = (
                f"Cache: enabled (`{cache_db_path}`)"
                + (" - force refresh enabled" if force_refresh_cache else "")
            )
        else:
            cache_status = "Cache: disabled by setting"
        effective_use_cache = use_persistent_cache and (cache_store_for_lookup is not None)

        normalized_itra_cookie = _normalize_cookie_header(itra_cookie)
        if itra_cookie.strip() and not normalized_itra_cookie:
            st.warning("ITRA cookie format looks invalid. Expected: name=value; name2=value2")

        with st.spinner("Loading participants..."):
            names, input_warnings = _collect_input_names(
                race_url=race_url,
                competition_name=competition_name,
                uploaded_file=uploaded_file,
                pasted_names=pasted_names,
                timeout=int(timeout),
            )
        for message in input_warnings:
            st.warning(message)
        if not names:
            st.error("No participants found from the provided inputs.")
            return

        try:
            with st.spinner("Enriching scores..."):
                if strategy == "catalog-first":
                    if duplicate_policy == "keep_all":
                        st.warning(
                            "`keep_all` applies to participant-first only. Using highest for catalog-first."
                        )
                    records = _enrich_records_from_catalog(
                        names,
                        timeout=int(timeout),
                        skip_itra=skip_itra,
                        itra_overrides=None,
                        itra_cookie=normalized_itra_cookie,
                        score_threshold=float(score_threshold),
                        utmb_catalog_max_pages=int(utmb_catalog_max_pages),
                        catalog_min_match_score=float(catalog_min_match_score),
                    )
                elif duplicate_policy == "keep_all":
                    records = _enrich_records_keep_all(
                        names,
                        min_match_score=float(min_match_score),
                        score_threshold=float(score_threshold),
                        timeout=int(timeout),
                        skip_itra=skip_itra,
                        itra_cookie=normalized_itra_cookie,
                        cache_store=cache_store_for_lookup,
                        use_cache=effective_use_cache,
                        force_refresh_cache=force_refresh_cache,
                    )
                else:
                    records = _enrich_records(
                        names,
                        min_match_score=float(min_match_score),
                        score_threshold=float(score_threshold),
                        timeout=int(timeout),
                        skip_itra=skip_itra,
                        itra_overrides=None,
                        itra_cookie=normalized_itra_cookie,
                        cache_store=cache_store_for_lookup,
                        use_cache=effective_use_cache,
                        force_refresh_cache=force_refresh_cache,
                    )
        except Exception as exc:
            st.error(f"Run failed: {exc}")
            snapshot = st.session_state.get(RESULT_STATE_KEY)
            if isinstance(snapshot, dict):
                st.info("Showing previous successful report.")
                _render_report_snapshot(snapshot, shared_store=shared_store)
            return

        filtered = [record for record in records if _is_above_threshold(record, float(score_threshold))]
        no_result_names = _compute_no_result_names(records)
        utmb_scores, itra_scores, betrail_scores, score_summary = _aggregate_scores_by_input(records)
        score_summary["participants"] = len(names)
        ranked = sort_records(filtered, sort_by=sort_by)
        rows = _records_to_rows(ranked, top=int(top))
        ranked_all = sort_records(records, sort_by=sort_by)
        export_rows = _records_to_rows(ranked_all, top=len(ranked_all))
        stale_cache_used = any("stale cache fallback used" in (record.notes or "") for record in records)

        snapshot = _build_report_snapshot(
            title=race_name.strip() or "Trail Race Report",
            participants_count=len(names),
            rows_evaluated=len(records),
            qualified_count=len(filtered),
            strategy=strategy,
            same_name_mode=duplicate_policy,
            rows=rows,
            export_rows=export_rows,
            no_result_names=no_result_names,
            utmb_scores=utmb_scores,
            itra_scores=itra_scores,
            betrail_scores=betrail_scores,
            score_summary=score_summary,
            cache_status=cache_status,
            stale_cache_used=stale_cache_used,
        )
        st.session_state[RESULT_STATE_KEY] = snapshot

        if shared_store is not None and _can_auto_save_race(race_url):
            try:
                saved_entry = shared_store.upsert_saved_race(
                    race_label=_build_race_label(race_name, competition_name, race_url),
                    race_url=race_url,
                    competition_name=competition_name,
                    last_run_at=datetime.now(UTC),
                )
                shared_store.append_race_run(
                    race_key=saved_entry.race_key,
                    payload_json=json.dumps(snapshot, ensure_ascii=False),
                    participants_count=len(names),
                    rows_evaluated=len(records),
                    qualified_count=len(filtered),
                    strategy=strategy,
                    same_name_mode=duplicate_policy,
                    max_runs=RACE_HISTORY_MAX_RUNS,
                )
                _queue_saved_race_selection(st.session_state, saved_entry.race_key)
            except Exception as exc:
                st.warning(f"Could not persist race preset/history: {exc}")
        elif not _can_auto_save_race(race_url):
            st.info("Race URL is empty; this run was not stored in saved races history.")

        _render_report_snapshot(snapshot, shared_store=shared_store)
    finally:
        if shared_store:
            shared_store.close()


if __name__ == "__main__":
    run_app()
