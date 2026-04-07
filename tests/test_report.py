from __future__ import annotations

import unittest

from trailintel.models import AthleteRecord
from trailintel.report import sort_records


class ReportSortTests(unittest.TestCase):
    def test_sort_by_combined(self) -> None:
        rows = [
            AthleteRecord(input_name="A", utmb_index=800, itra_score=700),
            AthleteRecord(input_name="B", utmb_index=850, itra_score=650),
            AthleteRecord(input_name="C", utmb_index=700, itra_score=900),
        ]
        sorted_rows = sort_records(rows, sort_by="combined")
        self.assertEqual(sorted_rows[0].input_name, "C")

    def test_sort_by_utmb(self) -> None:
        rows = [
            AthleteRecord(input_name="A", utmb_index=800),
            AthleteRecord(input_name="B", utmb_index=900),
        ]
        sorted_rows = sort_records(rows, sort_by="utmb")
        self.assertEqual(sorted_rows[0].input_name, "B")

    def test_sort_by_betrail(self) -> None:
        rows = [
            AthleteRecord(input_name="A", betrail_score=75.0),
            AthleteRecord(input_name="B", betrail_score=82.5),
        ]
        sorted_rows = sort_records(rows, sort_by="betrail")
        self.assertEqual(sorted_rows[0].input_name, "B")

    def test_combined_falls_back_to_betrail_when_other_scores_missing(self) -> None:
        record = AthleteRecord(input_name="A", betrail_score=74.5)
        self.assertEqual(record.combined_score, 745.0)


if __name__ == "__main__":
    unittest.main()
