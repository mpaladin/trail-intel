from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from trailintel.cli import _enrich_records
from trailintel.score_repo import AthleteScoreRepo, RepoProviderObservation


def _score_repo_stats() -> dict[str, int]:
    return {
        "athletes_seen": 0,
        "athletes_created": 0,
        "athletes_updated": 0,
        "provider_updates": 0,
    }


class ScoreRepoIntegrationTests(unittest.TestCase):
    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_fresh_repo_hits_avoid_live_requests(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = AthleteScoreRepo(Path(tmp) / "repo")
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
                    ),
                    RepoProviderObservation(
                        provider="itra",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://itra.run/RunnerSpace/Trail.Alice/42",
                        score=730.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-1",
                    ),
                    RepoProviderObservation(
                        provider="betrail",
                        status="matched",
                        matched_name="Alice Trail",
                        profile_url="https://www.betrail.run/runner/alice.trail/overview",
                        score=74.5,
                        score_scale="100",
                        match_confidence=1.0,
                        source_run_id="run-1",
                        lookup_threshold=68.0,
                    ),
                ],
                source_run_id="run-1",
                source_kind="test",
            )

            records = _enrich_records(
                ["Alice Trail"],
                min_match_score=0.6,
                score_threshold=680.0,
                timeout=15,
                skip_itra=False,
                itra_overrides=None,
                itra_cookie=None,
                betrail_cookie=None,
                score_repo=repo,
                score_repo_run_id="run-2",
                score_repo_stats=_score_repo_stats(),
            )

        self.assertEqual(records[0].utmb_index, 745.0)
        self.assertEqual(records[0].itra_score, 730.0)
        self.assertEqual(records[0].betrail_score, 74.5)
        self.assertEqual(mock_utmb_client.return_value.search.call_count, 0)
        self.assertEqual(mock_itra_client.return_value.search.call_count, 0)
        self.assertEqual(mock_betrail_client.return_value.fetch_catalog_above_threshold.call_count, 0)

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_stale_repo_snapshot_triggers_live_refresh(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search.return_value = type(
            "UtmbResult",
            (),
            {
                "utmb_index": 755.0,
                "matched_name": "Bob Runner",
                "match_score": 1.0,
                "profile_url": "https://utmb.world/runner/999.bob-runner",
            },
        )()
        mock_utmb.last_lookup_stale_fallback = False
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        with tempfile.TemporaryDirectory() as tmp:
            repo_path = Path(tmp) / "repo"
            repo = AthleteScoreRepo(repo_path)
            repo.write_athlete_observations(
                input_name="Bob Runner",
                observations=[
                    RepoProviderObservation(
                        provider="utmb",
                        status="matched",
                        matched_name="Bob Runner",
                        profile_url="https://utmb.world/runner/999.bob-runner",
                        score=700.0,
                        score_scale="1000",
                        match_confidence=1.0,
                        source_run_id="run-1",
                        checked_at=datetime.now(UTC) - timedelta(days=90),
                    )
                ],
                source_run_id="run-1",
                source_kind="test",
            )

            _enrich_records(
                ["Bob Runner"],
                min_match_score=0.6,
                score_threshold=680.0,
                timeout=15,
                skip_itra=True,
                itra_overrides=None,
                itra_cookie=None,
                betrail_cookie=None,
                score_repo=repo,
                score_repo_run_id="run-2",
                score_repo_stats=_score_repo_stats(),
            )

            reloaded = AthleteScoreRepo(repo_path)
            reloaded.load()
            lookup = reloaded.get_provider_snapshot(query_name="Bob Runner", provider="utmb")

        assert lookup is not None
        self.assertEqual(lookup.score, 755.0)
        self.assertFalse(lookup.is_stale)
        self.assertEqual(mock_utmb.search.call_count, 1)

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_read_only_mode_skips_repo_writes(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search.return_value = type(
            "UtmbResult",
            (),
            {
                "utmb_index": 720.0,
                "matched_name": "Read Only",
                "match_score": 1.0,
                "profile_url": "https://utmb.world/runner/123.read-only",
            },
        )()
        mock_utmb.last_lookup_stale_fallback = False
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        with tempfile.TemporaryDirectory() as tmp:
            repo_path = Path(tmp) / "repo"
            repo = AthleteScoreRepo(repo_path)

            _enrich_records(
                ["Read Only"],
                min_match_score=0.6,
                score_threshold=680.0,
                timeout=15,
                skip_itra=True,
                itra_overrides=None,
                itra_cookie=None,
                betrail_cookie=None,
                score_repo=repo,
                score_repo_read_only=True,
                score_repo_run_id="run-1",
                score_repo_stats=_score_repo_stats(),
            )

            self.assertFalse((repo_path / "athletes").exists())

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_below_threshold_athletes_are_still_written_to_repo(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search.return_value = type(
            "UtmbResult",
            (),
            {
                "utmb_index": 650.0,
                "matched_name": "Low Runner",
                "match_score": 1.0,
                "profile_url": "https://utmb.world/runner/456.low-runner",
            },
        )()
        mock_utmb.last_lookup_stale_fallback = False
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        with tempfile.TemporaryDirectory() as tmp:
            repo_path = Path(tmp) / "repo"
            repo = AthleteScoreRepo(repo_path)
            records = _enrich_records(
                ["Low Runner"],
                min_match_score=0.6,
                score_threshold=680.0,
                timeout=15,
                skip_itra=True,
                itra_overrides=None,
                itra_cookie=None,
                betrail_cookie=None,
                score_repo=repo,
                score_repo_run_id="run-1",
                score_repo_stats=_score_repo_stats(),
            )

            reloaded = AthleteScoreRepo(repo_path)
            reloaded.load()
            lookup = reloaded.get_provider_snapshot(query_name="Low Runner", provider="utmb")

        self.assertEqual(records[0].utmb_index, 650.0)
        assert lookup is not None
        self.assertEqual(lookup.score, 650.0)


if __name__ == "__main__":
    unittest.main()
