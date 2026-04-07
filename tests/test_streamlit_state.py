from __future__ import annotations

from datetime import UTC, datetime
import json
import tempfile
import unittest
from unittest.mock import patch

from trailintel.cache_store import LookupCacheStore, RaceRunHistoryEntry, SavedRaceEntry
from trailintel.models import AthleteRecord
from trailintel.streamlit_app import (
    RESULT_STATE_KEY,
    RACE_NAME_INPUT_KEY,
    RACE_URL_INPUT_KEY,
    COMPETITION_INPUT_KEY,
    _apply_saved_race_inputs,
    _build_report_snapshot,
    _build_race_label,
    _can_auto_save_race,
    _collect_input_names,
    _compute_no_result_names,
    _enrich_records_keep_all,
    _history_entry_label,
    _initialize_itra_cookie_input,
    _saved_race_option_label,
    _should_auto_load_selected_race,
    _should_render_saved_snapshot,
    _snapshot_from_history_payload,
    ITRA_COOKIE_INPUT_KEY,
    ITRA_COOKIE_SETTING_KEY,
)


class StreamlitStateTests(unittest.TestCase):
    def test_apply_saved_race_inputs_updates_sidebar_fields(self) -> None:
        race = SavedRaceEntry(
            race_key="race-key",
            race_label="Trail du Sanglier 2026 - Le 40 km",
            race_url="https://in.yaka-inscription.com/trail-du-sanglier-2026?currentPage=select-competition",
            competition_name="Le 40 km",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            last_run_at=None,
        )
        state: dict[str, object] = {}
        _apply_saved_race_inputs(state, race)
        self.assertEqual(state[RACE_NAME_INPUT_KEY], race.race_label)
        self.assertEqual(state[RACE_URL_INPUT_KEY], race.race_url)
        self.assertEqual(state[COMPETITION_INPUT_KEY], race.competition_name)

    def test_should_render_saved_snapshot_on_rerun(self) -> None:
        state = {RESULT_STATE_KEY: {"title": "Saved"}}
        self.assertTrue(_should_render_saved_snapshot(False, state))
        self.assertFalse(_should_render_saved_snapshot(True, state))
        self.assertFalse(_should_render_saved_snapshot(False, {}))

    def test_build_report_snapshot(self) -> None:
        snapshot = _build_report_snapshot(
            title="Trail Race",
            participants_count=10,
            rows_evaluated=12,
            qualified_count=4,
            strategy="participant-first",
            same_name_mode="highest",
            rows=[{"Athlete": "A"}],
            export_rows=[{"Athlete": "A"}, {"Athlete": "B"}],
            no_result_names=["B", "C"],
            utmb_scores=[700.0],
            itra_scores=[690.0],
            betrail_scores=[74.5],
            score_summary={"participants": 10, "with_utmb": 1, "with_itra": 1, "with_betrail": 1, "with_any": 1},
            cache_status="Cache: enabled",
            stale_cache_used=True,
        )
        self.assertEqual(snapshot["title"], "Trail Race")
        self.assertEqual(snapshot["participants_count"], 10)
        self.assertEqual(snapshot["qualified_count"], 4)
        self.assertEqual(snapshot["no_result_names"], ["B", "C"])
        self.assertEqual(snapshot["utmb_scores"], [700.0])
        self.assertEqual(snapshot["itra_scores"], [690.0])
        self.assertEqual(snapshot["betrail_scores"], [74.5])
        self.assertEqual(snapshot["score_summary"]["with_any"], 1)
        self.assertEqual(snapshot["export_rows"], [{"Athlete": "A"}, {"Athlete": "B"}])
        self.assertEqual(snapshot["cache_status"], "Cache: enabled")
        self.assertTrue(snapshot["stale_cache_used"])

    def test_compute_no_result_names_includes_no_match_on_both(self) -> None:
        records = [AthleteRecord(input_name="No Match")]
        self.assertEqual(_compute_no_result_names(records), ["No Match"])

    def test_compute_no_result_names_excludes_utmb_only_match(self) -> None:
        records = [AthleteRecord(input_name="UTMB Only", utmb_match_name="Runner")]
        self.assertEqual(_compute_no_result_names(records), [])

    def test_compute_no_result_names_excludes_itra_match_without_score(self) -> None:
        records = [AthleteRecord(input_name="ITRA Match", itra_match_name="Runner", itra_score=None)]
        self.assertEqual(_compute_no_result_names(records), [])

    def test_compute_no_result_names_excludes_betrail_match_without_other_scores(self) -> None:
        records = [AthleteRecord(input_name="Betrail Match", betrail_match_name="Runner", betrail_score=72.0)]
        self.assertEqual(_compute_no_result_names(records), [])

    def test_compute_no_result_names_aggregates_keep_all_candidates(self) -> None:
        records = [
            AthleteRecord(input_name="Dual Candidate"),
            AthleteRecord(input_name="Dual Candidate", utmb_profile_url="https://utmb.world/runner/123"),
            AthleteRecord(input_name="Truly Missing"),
        ]
        self.assertEqual(_compute_no_result_names(records), ["Truly Missing"])

    def test_compute_no_result_names_unique_and_sorted(self) -> None:
        records = [
            AthleteRecord(input_name="Zoe"),
            AthleteRecord(input_name="Anna"),
            AthleteRecord(input_name="Zoe"),
            AthleteRecord(input_name="Matched", itra_profile_url="https://itra.run/runner/1"),
        ]
        self.assertEqual(_compute_no_result_names(records), ["Anna", "Zoe"])

    def test_race_key_normalization(self) -> None:
        key_a = LookupCacheStore.build_race_key(
            race_url="HTTPS://EXAMPLE.COM/RACE",
            competition_name="Maratráil des Hauts du Lac - 42KM",
        )
        key_b = LookupCacheStore.build_race_key(
            race_url="https://example.com/race",
            competition_name="maratrail des hauts du lac 42 km",
        )
        self.assertEqual(key_a, key_b)

    def test_saved_race_label_formatting(self) -> None:
        race = SavedRaceEntry(
            race_key="key",
            race_label="Trail du Sanglier 2026 - Le 40 km",
            race_url="https://in.yaka-inscription.com/trail-du-sanglier-2026",
            competition_name="Le 40 km",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            last_run_at=None,
        )
        self.assertIn("Le 40 km", _saved_race_option_label(race))

    def test_auto_save_decision_from_race_url(self) -> None:
        self.assertTrue(_can_auto_save_race("https://in.yaka-inscription.com/race"))
        self.assertFalse(_can_auto_save_race("   "))

    def test_history_entry_label_contains_metrics(self) -> None:
        entry = RaceRunHistoryEntry(
            run_id="run-1",
            race_key="race-1",
            run_at=datetime(2026, 2, 21, 12, 30, tzinfo=UTC),
            payload_json="{}",
            participants_count=120,
            rows_evaluated=140,
            qualified_count=6,
            strategy="participant-first",
            same_name_mode="highest",
        )
        label = _history_entry_label(entry)
        self.assertIn("qualified 6/140", label)
        self.assertIn("participant-first/highest", label)

    def test_snapshot_payload_round_trip(self) -> None:
        snapshot = _build_report_snapshot(
            title="Trail Race",
            participants_count=10,
            rows_evaluated=12,
            qualified_count=4,
            strategy="participant-first",
            same_name_mode="highest",
            rows=[{"Athlete": "A"}],
            export_rows=[{"Athlete": "A"}],
            no_result_names=[],
            utmb_scores=[],
            itra_scores=[],
            betrail_scores=[],
            score_summary={"participants": 10, "with_utmb": 0, "with_itra": 0, "with_betrail": 0, "with_any": 0},
            cache_status="Cache: enabled",
            stale_cache_used=False,
        )
        loaded = _snapshot_from_history_payload(json.dumps(snapshot))
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["title"], "Trail Race")

    def test_snapshot_payload_invalid(self) -> None:
        self.assertIsNone(_snapshot_from_history_payload("not json"))
        self.assertIsNone(_snapshot_from_history_payload('["not", "a", "dict"]'))

    def test_build_race_label_prefers_name_then_competition_then_url(self) -> None:
        self.assertEqual(
            _build_race_label("Race Name", "Comp", "https://example.com"),
            "Race Name",
        )
        self.assertEqual(
            _build_race_label("", "Comp", "https://example.com"),
            "Comp",
        )
        self.assertEqual(
            _build_race_label("", "", "https://example.com"),
            "https://example.com",
        )

    @patch("trailintel.streamlit_app.fetch_participants_from_url")
    def test_collect_input_names_continues_when_url_fetch_fails(self, mock_fetch) -> None:
        mock_fetch.side_effect = RuntimeError("403 forbidden")

        names, warnings = _collect_input_names(
            race_url="https://in.njuko.com/event",
            competition_name="42 km",
            uploaded_file=None,
            pasted_names="Alice Martin\nBob Trail\nAlice Martin",
            timeout=15,
        )

        self.assertEqual(names, ["Alice Martin", "Bob Trail"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("Race URL fetch failed", warnings[0])

    @patch("trailintel.streamlit_app.fetch_participants_from_url")
    def test_collect_input_names_merges_and_dedupes_sources(self, mock_fetch) -> None:
        mock_fetch.return_value = ["Alice Martin", "Bob Trail"]

        names, warnings = _collect_input_names(
            race_url="https://in.yaka-inscription.com/event",
            competition_name="40 km",
            uploaded_file=None,
            pasted_names="Bob Trail\nCharlie Hill",
            timeout=15,
        )

        self.assertEqual(names, ["Alice Martin", "Bob Trail", "Charlie Hill"])
        self.assertEqual(warnings, [])

    def test_initialize_itra_cookie_from_persisted_setting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            store.put_setting(setting_key=ITRA_COOKIE_SETTING_KEY, setting_value="session=abc")
            state: dict[str, object] = {}
            _initialize_itra_cookie_input(state, shared_store=store)
            self.assertEqual(state.get(ITRA_COOKIE_INPUT_KEY), "session=abc")
            store.close()

    def test_should_auto_load_selected_race(self) -> None:
        self.assertTrue(_should_auto_load_selected_race("race-key-1", None))
        self.assertTrue(_should_auto_load_selected_race("race-key-2", "race-key-1"))
        self.assertFalse(_should_auto_load_selected_race("__custom__", "race-key-1"))
        self.assertFalse(_should_auto_load_selected_race("race-key-1", "race-key-1"))

    @patch("trailintel.streamlit_app.BetrailClient")
    @patch("trailintel.streamlit_app.ItraClient")
    @patch("trailintel.streamlit_app.UtmbClient")
    def test_keep_all_skips_itra_when_utmb_candidates_below_threshold(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search_same_name_candidates.return_value = [
            type(
                "UtmbCandidate",
                (),
                {
                    "utmb_index": 650.0,
                    "matched_name": "Alice Martin",
                    "match_score": 1.0,
                    "profile_url": "https://utmb.world/runner/alice",
                },
            )()
        ]
        mock_utmb.last_lookup_stale_fallback = False
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        records = _enrich_records_keep_all(
            ["Alice Martin"],
            min_match_score=0.6,
            score_threshold=700.0,
            timeout=15,
            skip_itra=False,
            itra_cookie=None,
            cache_store=None,
            use_cache=False,
            force_refresh_cache=False,
        )

        self.assertEqual(mock_itra_client.return_value.search_same_name_candidates.call_count, 0)
        self.assertEqual(len(records), 1)
        self.assertIn("ITRA skipped because UTMB 650.0 <= threshold 700.0", records[0].notes)


if __name__ == "__main__":
    unittest.main()
