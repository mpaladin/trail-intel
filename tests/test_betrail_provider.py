from __future__ import annotations

import unittest
from unittest.mock import patch

from trailintel.providers.betrail import BetrailClient, BetrailLookupError


def _betrail_row(
    *,
    level: int,
    display_title: str | None,
    title: str | None,
    firstname: str | None,
    lastname: str | None,
    alias: str | None,
) -> dict[str, object]:
    return {
        "level": level,
        "runner": {
            "display_title": display_title,
            "title": title,
            "firstname": firstname,
            "lastname": lastname,
            "alias": alias,
        },
    }


class BetrailProviderTests(unittest.TestCase):
    def test_default_catalog_page_size_matches_public_endpoint(self) -> None:
        self.assertEqual(BetrailClient.PAGE_SIZE, 25)

    @patch("trailintel.providers.betrail.BetrailClient._fetch_page")
    def test_catalog_fetch_normalizes_scores_and_dedupes(self, mock_fetch_page) -> None:
        mock_fetch_page.side_effect = [
            [
                _betrail_row(
                    level=7450,
                    display_title="Alice Trail",
                    title="ALICE TRAIL",
                    firstname="Alice",
                    lastname="Trail",
                    alias="alice.trail",
                ),
                _betrail_row(
                    level=7400,
                    display_title="Alice Trail",
                    title="ALICE TRAIL",
                    firstname="Alice",
                    lastname="Trail",
                    alias="alice.trail.duplicate",
                ),
            ],
            [
                _betrail_row(
                    level=6799,
                    display_title=None,
                    title=None,
                    firstname="Below",
                    lastname="Runner",
                    alias="below.runner",
                )
            ],
        ]

        client = BetrailClient(timeout=5)
        client.PAGE_SIZE = 2
        entries = client.fetch_catalog_above_threshold(68.0)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].name, "Alice Trail")
        self.assertEqual(entries[0].betrail_score, 74.5)
        self.assertEqual(entries[0].profile_url, "https://www.betrail.run/runner/alice.trail/overview")
        self.assertEqual(mock_fetch_page.call_count, 2)

    @patch("trailintel.providers.betrail.BetrailClient._fetch_page")
    def test_catalog_fetch_propagates_public_failure(self, mock_fetch_page) -> None:
        mock_fetch_page.side_effect = BetrailLookupError("Betrail request blocked by Cloudflare.")

        client = BetrailClient(timeout=5)

        with self.assertRaises(BetrailLookupError):
            client.fetch_catalog_above_threshold(68.0)
