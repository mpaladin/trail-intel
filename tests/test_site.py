from __future__ import annotations

from datetime import UTC, datetime
import tempfile
import unittest

from trailintel.models import AthleteRecord
from trailintel.site import (
    REPORT_CSV_FILENAME,
    REPORT_HTML_FILENAME,
    REPORT_JSON_FILENAME,
    REPORT_META_FILENAME,
    REPORT_SNAPSHOT_FILENAME,
    build_report_snapshot,
    export_report_site,
)


class SiteExportTests(unittest.TestCase):
    def test_export_report_site_writes_bundle_and_renders_sections(self) -> None:
        records = [
            AthleteRecord(
                input_name="Alice Trail",
                utmb_index=745.0,
                utmb_match_name="Alice Trail",
                utmb_profile_url="https://utmb.world/runner/123.alice-trail",
                itra_score=730.0,
                itra_match_name="Alice Trail",
                itra_profile_url="https://itra.run/RunnerSpace/Trail.Alice/123",
            ),
            AthleteRecord(
                input_name="Bob Missing",
                notes="UTMB not found; ITRA not found",
            ),
        ]
        qualified = [records[0]]
        snapshot = build_report_snapshot(
            title="Trail du Test 2026",
            all_records=records,
            qualified_records=qualified,
            participants_count=2,
            strategy="participant-first",
            same_name_mode="highest",
            top=100,
            sort_by="combined",
            race_url="https://example.com/race",
            competition_name="42 km",
            score_threshold=680.0,
            cache_status="Cache: enabled",
            stale_cache_used=False,
            generated_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = export_report_site(snapshot=snapshot, records=records, destination=tmp)

            self.assertTrue((out_dir / REPORT_HTML_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_CSV_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_JSON_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_SNAPSHOT_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_META_FILENAME).exists())

            html = (out_dir / REPORT_HTML_FILENAME).read_text(encoding="utf-8")
            self.assertIn("Score Distribution", html)
            self.assertIn("Top Athletes", html)
            self.assertIn("No result on both UTMB and ITRA", html)
            self.assertIn("Download CSV", html)
            self.assertIn('href="report.csv"', html)
            self.assertIn("Alice Trail", html)
            self.assertIn("Bob Missing", html)
            self.assertIn("open source page", html)
            self.assertIn("https://utmb.world/runner/123.alice-trail", html)
            self.assertIn("https://itra.run/RunnerSpace/Trail.Alice/123", html)

    def test_export_report_site_can_use_snapshot_export_rows_without_records(self) -> None:
        snapshot = {
            "title": "Saved Snapshot",
            "participants_count": 2,
            "rows_evaluated": 2,
            "qualified_count": 1,
            "strategy": "participant-first",
            "same_name_mode": "highest",
            "rows": [{"Rank": 1, "Athlete": "Alice", "UTMB": "745.0", "ITRA": "730.0", "Combined": "739.0"}],
            "export_rows": [
                {"Rank": 1, "Athlete": "Alice", "UTMB": "745.0"},
                {"Rank": 2, "Athlete": "Bob", "UTMB": "-"},
            ],
            "no_result_names": ["Bob"],
            "utmb_scores": [745.0],
            "itra_scores": [730.0],
            "score_summary": {"participants": 2, "with_utmb": 1, "with_itra": 1, "with_any": 1},
        }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = export_report_site(snapshot=snapshot, records=None, destination=tmp)
            csv_text = (out_dir / REPORT_CSV_FILENAME).read_text(encoding="utf-8")
            json_text = (out_dir / REPORT_JSON_FILENAME).read_text(encoding="utf-8")

            self.assertIn("Athlete", csv_text)
            self.assertIn("Alice", csv_text)
            self.assertIn("Bob", csv_text)
            self.assertIn('"Athlete": "Alice"', json_text)


if __name__ == "__main__":
    unittest.main()
