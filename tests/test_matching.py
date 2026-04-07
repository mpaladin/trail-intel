from __future__ import annotations

import unittest

from trailintel.matching import (
    canonical_name,
    is_strong_person_name_match,
    match_score,
    search_name_variants,
)


class MatchingTests(unittest.TestCase):
    def test_canonical_name_strips_marks(self) -> None:
        self.assertEqual(canonical_name("Nélie Clément"), "nelie clement")
        self.assertEqual(canonical_name("  Jim  WALMSLEY "), "jim walmsley")

    def test_match_score_prefers_similar_names(self) -> None:
        close = match_score("Kilian Jornet", "Kilian Jornet Burgada")
        far = match_score("Kilian Jornet", "Courtney Dauwalter")
        self.assertGreater(close, far)

    def test_search_name_variants_adds_deaccented_value(self) -> None:
        variants = search_name_variants("Aurélien Roche")
        self.assertIn("Aurélien Roche", variants)
        self.assertIn("Aurelien Roche", variants)

    def test_strong_name_match_rejects_wrong_surname(self) -> None:
        self.assertFalse(
            is_strong_person_name_match("Marianne Coquard", "Marianne Hogan")
        )
        self.assertTrue(
            is_strong_person_name_match("Marianne Coquard", "Marianne Coquard")
        )


if __name__ == "__main__":
    unittest.main()
