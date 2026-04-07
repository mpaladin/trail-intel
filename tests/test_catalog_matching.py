from __future__ import annotations

import unittest

from trailintel.cli import _best_catalog_match, _is_strong_catalog_name_match
from trailintel.matching import canonical_name


class CatalogMatchingTests(unittest.TestCase):
    def test_strong_guard_rejects_single_token_overlap(self) -> None:
        self.assertFalse(
            _is_strong_catalog_name_match("Alexandre Lambert", "Alexandre Baudat")
        )

    def test_strong_guard_accepts_first_name_and_close_surname(self) -> None:
        self.assertTrue(_is_strong_catalog_name_match("Thomas Serna", "Thomas Sernat"))

    def test_best_catalog_match_respects_strong_guard(self) -> None:
        match = _best_catalog_match(
            "Alexandre Lambert",
            entries=[("Alexandre Baudat", 830.0, None)],
            exact_lookup={},
            min_match_score=0.5,
            enforce_strong_name_guard=True,
        )
        self.assertIsNone(match)

    def test_best_catalog_match_exact_bypasses_guard(self) -> None:
        exact_lookup = {
            canonical_name("Alexandre Lambert"): ("Alexandre Lambert", 810.0, None)
        }
        match = _best_catalog_match(
            "Alexandre Lambert",
            entries=[],
            exact_lookup=exact_lookup,
            min_match_score=0.95,
            enforce_strong_name_guard=True,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.matched_name, "Alexandre Lambert")
        self.assertEqual(match.score, 810.0)

    def test_best_catalog_match_rotated_exact_bypasses_guard(self) -> None:
        exact_lookup = {
            canonical_name("Aurélien Dunand-Pallaz"): (
                "Aurélien Dunand-Pallaz",
                911.0,
                None,
            )
        }
        match = _best_catalog_match(
            "DUNAND-PALLAZ Aurélien",
            entries=[],
            exact_lookup=exact_lookup,
            min_match_score=0.95,
            enforce_strong_name_guard=True,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.matched_name, "Aurélien Dunand-Pallaz")
        self.assertEqual(match.score, 911.0)


if __name__ == "__main__":
    unittest.main()
