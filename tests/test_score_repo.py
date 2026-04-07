from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from trailintel.score_repo import (
    AthleteScoreRepo,
    RepoProviderObservation,
    default_score_repo_path,
)


class ScoreRepoTests(unittest.TestCase):
    def test_default_score_repo_path_uses_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                '[score_repo]\npath = "/tmp/trail-score"\n', encoding="utf-8"
            )

            with patch.dict(
                os.environ,
                {
                    "TRAILINTEL_CONFIG_FILE": str(config_path),
                },
                clear=False,
            ):
                os.environ.pop("TRAILINTEL_SCORE_REPO", None)
                resolved = default_score_repo_path()

        self.assertEqual(resolved, Path("/tmp/trail-score"))

    def test_roundtrip_is_deterministic_and_writes_schema_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
            checked_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
            observation = RepoProviderObservation(
                provider="utmb",
                status="matched",
                matched_name="Alice Trail",
                profile_url="https://utmb.world/runner/123.alice-trail",
                score=745.0,
                score_scale="1000",
                match_confidence=1.0,
                source_run_id="run-1",
                checked_at=checked_at,
            )

            first = repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[observation],
                source_run_id="run-1",
                source_kind="test",
            )
            athlete_path = next((repo.root / "athletes").glob("*/*.json"))
            first_text = athlete_path.read_text(encoding="utf-8")

            second = repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[observation],
                source_run_id="run-1",
                source_kind="test",
            )
            second_text = athlete_path.read_text(encoding="utf-8")

            self.assertTrue(first.created)
            self.assertFalse(second.updated)
            self.assertEqual(first_text, second_text)
            self.assertTrue((repo.root / "schema" / "athlete-v1.schema.json").exists())
            self.assertTrue(
                (repo.root / "schema" / "run-summary-v1.schema.json").exists()
            )

    def test_matched_and_miss_ttls_match_existing_cache_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
            checked_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
            repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://utmb.world/runner/123.alice-trail",
                        score=745.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-1",
                        checked_at=checked_at,
                    ),
                    RepoProviderObservation(
                        provider="itra",
                        status="miss",
                        matched_name=None,
                        profile_url=None,
                        score=None,
                        score_scale="1000",
                        match_confidence=None,
                        source_run_id="run-1",
                        checked_at=checked_at,
                    ),
                ],
                source_run_id="run-1",
                source_kind="test",
            )

            utmb_lookup = repo.get_provider_snapshot(
                query_name="Alice Trail", provider="utmb"
            )
            itra_lookup = repo.get_provider_snapshot(
                query_name="Alice Trail", provider="itra"
            )

            assert utmb_lookup is not None
            assert itra_lookup is not None
            self.assertEqual(
                (utmb_lookup.expires_at - utmb_lookup.last_checked_at).days, 60
            )
            self.assertEqual(
                (itra_lookup.expires_at - itra_lookup.last_checked_at).days, 7
            )

    def test_exact_provider_uid_reuses_existing_athlete_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
            first = repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://utmb.world/runner/123.alice-trail",
                        score=730.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-1",
                    )
                ],
                source_run_id="run-1",
                source_kind="test",
            )
            second = repo.write_athlete_observations(
                input_name="A. Trail",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://utmb.world/runner/123.alice-trail",
                        score=735.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-2",
                    )
                ],
                source_run_id="run-2",
                source_kind="test",
            )

            athlete_files = list((repo.root / "athletes").glob("*/*.json"))
            self.assertEqual(first.athlete_id, second.athlete_id)
            self.assertEqual(len(athlete_files), 1)

    def test_exact_name_with_strong_guard_links_placeholder_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
            first = repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[],
                source_run_id="run-1",
                source_kind="test",
            )
            second = repo.write_athlete_observations(
                input_name="Alice Trail",
                observations=[
                    RepoProviderObservation(
                        provider="itra",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://itra.run/RunnerSpace/Trail.Alice/42",
                        score=710.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-2",
                    )
                ],
                source_run_id="run-2",
                source_kind="test",
            )

            self.assertEqual(first.athlete_id, second.athlete_id)
            self.assertEqual(len(list((repo.root / "athletes").glob("*/*.json"))), 1)

    def test_ambiguous_same_name_creates_separate_athlete_doc(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            repo = AthleteScoreRepo(repo_root)
            repo.ensure_layout()
            doc_template = {
                "schema_version": "athlete-v1",
                "identity": {
                    "primary_name": "John Runner",
                    "canonical_name": "john runner",
                    "aliases": ["John Runner"],
                },
                "providers": {},
                "provenance": {
                    "source_run_ids": ["run-0"],
                    "first_source_kind": "test",
                    "last_source_kind": "test",
                    "last_source_run_id": "run-0",
                },
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
            doc_a = dict(doc_template)
            doc_a["identity"] = dict(doc_template["identity"])
            doc_a["identity"]["athlete_id"] = "aa111111111111111111111111111111"
            doc_b = dict(doc_template)
            doc_b["identity"] = dict(doc_template["identity"])
            doc_b["identity"]["athlete_id"] = "bb222222222222222222222222222222"
            path_a = (
                repo_root / "athletes" / "aa" / "aa111111111111111111111111111111.json"
            )
            path_b = (
                repo_root / "athletes" / "bb" / "bb222222222222222222222222222222.json"
            )
            path_a.parent.mkdir(parents=True, exist_ok=True)
            path_b.parent.mkdir(parents=True, exist_ok=True)
            path_a.write_text(json.dumps(doc_a, indent=2) + "\n", encoding="utf-8")
            path_b.write_text(json.dumps(doc_b, indent=2) + "\n", encoding="utf-8")

            repo.load()
            repo.write_athlete_observations(
                input_name="John Runner",
                observations=[],
                source_run_id="run-1",
                source_kind="test",
            )

            self.assertEqual(len(list((repo.root / "athletes").glob("*/*.json"))), 3)

    def test_same_provider_different_uid_same_name_stays_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
            first = repo.write_athlete_observations(
                input_name="John Doe",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="John Doe",
                        profile_url="https://utmb.world/runner/111.john.doe",
                        score=710.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-1",
                    )
                ],
                source_run_id="run-1",
                source_kind="test",
            )
            second = repo.write_athlete_observations(
                input_name="John Doe",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="John Doe",
                        profile_url="https://utmb.world/runner/222.john.doe",
                        score=705.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-2",
                    )
                ],
                source_run_id="run-2",
                source_kind="test",
            )

            self.assertNotEqual(first.athlete_id, second.athlete_id)
            self.assertEqual(len(list((repo.root / "athletes").glob("*/*.json"))), 2)


if __name__ == "__main__":
    unittest.main()
