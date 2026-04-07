from __future__ import annotations

import unittest
from unittest.mock import patch

from trailintel.cli import _enrich_records
from trailintel.providers.itra import ItraLookupError


class ItraResilienceTests(unittest.TestCase):
    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_single_itra_failure_does_not_disable_remaining_lookups(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search.return_value = None
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        mock_itra = mock_itra_client.return_value
        mock_itra.search.side_effect = [
            ItraLookupError("temporary failure"),
            None,
        ]

        records = _enrich_records(
            ["Alice Martin", "Bob Durand"],
            min_match_score=0.6,
            score_threshold=700.0,
            timeout=15,
            skip_itra=False,
            itra_overrides=None,
        )

        self.assertIn("ITRA unavailable: temporary failure", records[0].notes)
        self.assertIn("ITRA not found", records[1].notes)
        self.assertEqual(mock_itra.search.call_count, 2)

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_consecutive_failures_eventually_disable_itra(
        self,
        mock_utmb_client,
        mock_itra_client,
        mock_betrail_client,
    ) -> None:
        mock_utmb = mock_utmb_client.return_value
        mock_utmb.search.return_value = None
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        mock_itra = mock_itra_client.return_value
        mock_itra.search.side_effect = [ItraLookupError("blocked")] * 8

        names = [f"Runner {i} Name" for i in range(9)]
        records = _enrich_records(
            names,
            min_match_score=0.6,
            score_threshold=700.0,
            timeout=15,
            skip_itra=False,
            itra_overrides=None,
        )

        self.assertEqual(mock_itra.search.call_count, 8)
        self.assertIn(
            "stopped after 8 consecutive failures",
            records[-1].notes,
        )

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_itra_lookup_skipped_when_utmb_below_threshold(
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
                "utmb_index": 655.0,
                "matched_name": "Alice Martin",
                "match_score": 1.0,
                "profile_url": "https://utmb.world/runner/alice",
            },
        )()
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        records = _enrich_records(
            ["Alice Martin"],
            min_match_score=0.6,
            score_threshold=700.0,
            timeout=15,
            skip_itra=False,
            itra_overrides=None,
        )

        self.assertEqual(mock_itra_client.return_value.search.call_count, 0)
        self.assertIn(
            "ITRA skipped because UTMB 655.0 <= threshold 700.0", records[0].notes
        )

    @patch("trailintel.cli.BetrailClient")
    @patch("trailintel.cli.ItraClient")
    @patch("trailintel.cli.UtmbClient")
    def test_itra_lookup_still_runs_when_utmb_above_threshold(
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
                "utmb_index": 711.0,
                "matched_name": "Alice Martin",
                "match_score": 1.0,
                "profile_url": "https://utmb.world/runner/alice",
            },
        )()

        mock_itra = mock_itra_client.return_value
        mock_itra.search.return_value = None
        mock_betrail_client.return_value.fetch_catalog_above_threshold.return_value = []

        records = _enrich_records(
            ["Alice Martin"],
            min_match_score=0.6,
            score_threshold=700.0,
            timeout=15,
            skip_itra=False,
            itra_overrides=None,
        )

        self.assertEqual(mock_itra.search.call_count, 1)
        self.assertIn("ITRA not found", records[0].notes)


if __name__ == "__main__":
    unittest.main()
