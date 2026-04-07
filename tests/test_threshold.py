from __future__ import annotations

import unittest

from trailintel.cli import _is_above_threshold
from trailintel.models import AthleteRecord


class ThresholdTests(unittest.TestCase):
    def test_threshold_is_strictly_greater(self) -> None:
        at = AthleteRecord(input_name="A", utmb_index=700, itra_score=700)
        above = AthleteRecord(input_name="B", utmb_index=701)
        self.assertFalse(_is_above_threshold(at, 700))
        self.assertTrue(_is_above_threshold(above, 700))

    def test_betrail_threshold_uses_native_100_scale(self) -> None:
        at = AthleteRecord(input_name="A", betrail_score=68.0)
        above = AthleteRecord(input_name="B", betrail_score=68.1)
        self.assertFalse(_is_above_threshold(at, 680))
        self.assertTrue(_is_above_threshold(above, 680))


if __name__ == "__main__":
    unittest.main()
