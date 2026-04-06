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


if __name__ == "__main__":
    unittest.main()
