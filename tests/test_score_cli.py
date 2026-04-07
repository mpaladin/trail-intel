from __future__ import annotations

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from trailintel.providers.itra import ItraLookupError
from trailintel.score_cli import seed_betrail_repo
from trailintel.score_repo import AthleteScoreRepo


class ScoreCliTests(unittest.TestCase):
    @patch("trailintel.score_cli.BetrailClient")
    @patch("trailintel.score_cli.ItraClient")
    @patch("trailintel.score_cli.UtmbClient")
    def test_seed_betrail_populates_repo_with_matches_and_misses(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = [
            SimpleNamespace(
                name="Alice Trail",
                betrail_score=74.5,
                profile_url="https://www.betrail.run/runner/alice.trail/overview",
            ),
            SimpleNamespace(
                name="Bob Runner",
                betrail_score=69.2,
                profile_url="https://www.betrail.run/runner/bob.runner/overview",
            ),
        ]
        mock_utmb_client.return_value.search.side_effect = [
            SimpleNamespace(
                utmb_index=745.0,
                matched_name="Alice Trail",
                match_score=1.0,
                profile_url="https://utmb.world/runner/123.alice-trail",
            ),
            None,
        ]
        mock_itra_client.return_value.search.side_effect = [
            SimpleNamespace(
                itra_score=730.0,
                matched_name="Alice Trail",
                match_score=1.0,
                profile_url="https://itra.run/RunnerSpace/Trail.Alice/42",
            ),
            None,
        ]

        with tempfile.TemporaryDirectory() as tmp:
            repo_path = Path(tmp) / "repo"
            seeded_count, summary = seed_betrail_repo(
                repo_path=repo_path,
                threshold=68.0,
                fill_utmb=True,
                fill_itra=True,
                timeout=15,
                itra_cookie=None,
            )

            repo = AthleteScoreRepo(repo_path)
            repo.load()
            alice_utmb = repo.get_provider_snapshot(query_name="Alice Trail", provider="utmb")
            bob_utmb = repo.get_provider_snapshot(query_name="Bob Runner", provider="utmb")
            bob_itra = repo.get_provider_snapshot(query_name="Bob Runner", provider="itra")
            alice_betrail = repo.get_provider_snapshot(query_name="Alice Trail", provider="betrail")

        self.assertEqual(seeded_count, 2)
        self.assertEqual(summary["score_repo"]["athletes_seen"], 2)
        assert alice_utmb is not None
        assert alice_betrail is not None
        assert bob_utmb is not None
        assert bob_itra is not None
        self.assertEqual(alice_utmb.score, 745.0)
        self.assertEqual(alice_betrail.score, 74.5)
        self.assertEqual(bob_utmb.status, "miss")
        self.assertEqual(bob_itra.status, "miss")

    @patch("trailintel.score_cli.BetrailClient")
    @patch("trailintel.score_cli.ItraClient")
    @patch("trailintel.score_cli.UtmbClient")
    def test_seed_betrail_stops_itra_after_eight_consecutive_failures(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = [
            SimpleNamespace(
                name=f"Runner {index} Name",
                betrail_score=70.0 + index,
                profile_url=f"https://www.betrail.run/runner/runner-{index}/overview",
            )
            for index in range(9)
        ]
        mock_utmb_client.return_value.search.return_value = None
        mock_itra_client.return_value.search.side_effect = [ItraLookupError("blocked")] * 8

        with tempfile.TemporaryDirectory() as tmp:
            _, summary = seed_betrail_repo(
                repo_path=Path(tmp) / "repo",
                threshold=68.0,
                fill_utmb=False,
                fill_itra=True,
                timeout=15,
                itra_cookie=None,
            )

        self.assertEqual(mock_itra_client.return_value.search.call_count, 8)
        self.assertIn("stopped after 8 consecutive failures", summary["provider_issues"]["itra"])


if __name__ == "__main__":
    unittest.main()
