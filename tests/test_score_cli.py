from __future__ import annotations

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb

from trailintel.providers.itra import ItraLookupError
from trailintel.score_cli import import_duckdb_cache, seed_betrail_repo
from trailintel.score_repo import AthleteScoreRepo


class ScoreCliTests(unittest.TestCase):
    def test_import_duckdb_cache_imports_success_rows_and_keeps_same_name_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.duckdb"
            conn = duckdb.connect(str(cache_path))
            conn.execute(
                """
                CREATE TABLE athlete_lookup_cache (
                    provider TEXT NOT NULL,
                    query_key TEXT NOT NULL,
                    auth_scope TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (provider, query_key, auth_scope)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO athlete_lookup_cache (
                    provider, query_key, auth_scope, status, payload_json, fetched_at, expires_at, updated_at
                ) VALUES
                (
                    'utmb',
                    'john doe',
                    'public',
                    'success',
                    '[{\"matched_name\":\"John Doe\",\"utmb_index\":710,\"profile_url\":\"https://utmb.world/runner/111.john.doe\",\"match_score\":1.0},{\"matched_name\":\"John Doe\",\"utmb_index\":705,\"profile_url\":\"https://utmb.world/runner/222.john.doe\",\"match_score\":1.0}]',
                    TIMESTAMP '2026-01-01 00:00:00',
                    TIMESTAMP '2026-03-02 00:00:00',
                    TIMESTAMP '2026-01-01 00:00:00'
                ),
                (
                    'itra',
                    'john doe',
                    'public',
                    'success',
                    '[{\"matched_name\":\"John Doe\",\"itra_score\":700,\"profile_url\":\"https://itra.run/RunnerSpace/Doe.John/9\",\"match_score\":1.0}]',
                    TIMESTAMP '2026-01-02 00:00:00',
                    TIMESTAMP '2026-03-03 00:00:00',
                    TIMESTAMP '2026-01-02 00:00:00'
                ),
                (
                    'utmb',
                    'low confidence',
                    'public',
                    'success',
                    '[{\"matched_name\":\"Low Confidence\",\"utmb_index\":690,\"profile_url\":\"https://utmb.world/runner/333.low.confidence\",\"match_score\":0.4}]',
                    TIMESTAMP '2026-01-03 00:00:00',
                    TIMESTAMP '2026-03-04 00:00:00',
                    TIMESTAMP '2026-01-03 00:00:00'
                ),
                (
                    'itra',
                    'missed runner',
                    'public',
                    'miss',
                    '[]',
                    TIMESTAMP '2026-01-04 00:00:00',
                    TIMESTAMP '2026-01-11 00:00:00',
                    TIMESTAMP '2026-01-04 00:00:00'
                )
                """
            )
            conn.close()

            repo_path = Path(tmp) / "repo"
            imported_count, summary = import_duckdb_cache(
                repo_path=repo_path,
                cache_db_path=cache_path,
                min_match_score=0.6,
            )

            repo = AthleteScoreRepo(repo_path)
            repo.load()

            self.assertEqual(imported_count, 3)
            self.assertEqual(summary["score_repo"]["rows_scanned"], 3)
            self.assertEqual(summary["score_repo"]["candidates_skipped_low_confidence"], 1)
            self.assertEqual(len(list((repo.root / "athletes").glob("*/*.json"))), 3)
            utmb_docs = []
            itra_docs = []
            for athlete_path in (repo.root / "athletes").glob("*/*.json"):
                text = athlete_path.read_text(encoding="utf-8")
                if "111.john.doe" in text or "222.john.doe" in text:
                    utmb_docs.append(text)
                if "RunnerSpace/Doe.John/9" in text:
                    itra_docs.append(text)
            self.assertEqual(len(utmb_docs), 2)
            self.assertEqual(len(itra_docs), 1)

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
                betrail_cookie=None,
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
                betrail_cookie=None,
            )

        self.assertEqual(mock_itra_client.return_value.search.call_count, 8)
        self.assertIn("stopped after 8 consecutive failures", summary["provider_issues"]["itra"])


if __name__ == "__main__":
    unittest.main()
