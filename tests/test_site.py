from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from trailintel.models import AthleteRecord
from trailintel.site import (
    FORECASTS_SECTION_DIR,
    REPORT_CSV_FILENAME,
    REPORT_HTML_FILENAME,
    REPORT_JSON_FILENAME,
    REPORT_META_FILENAME,
    REPORT_SNAPSHOT_FILENAME,
    REPORTS_SECTION_DIR,
    build_report_snapshot,
    export_report_site,
    refresh_site_index,
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
                betrail_score=74.5,
                betrail_match_name="Alice Trail",
                betrail_profile_url="https://www.betrail.run/runner/alice.trail/overview",
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
            top=100,
            sort_by="combined",
            race_url="https://example.com/race",
            competition_name="42 km",
            score_threshold=680.0,
            generated_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = export_report_site(
                snapshot=snapshot, records=records, destination=tmp
            )

            self.assertTrue((out_dir / REPORT_HTML_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_CSV_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_JSON_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_SNAPSHOT_FILENAME).exists())
            self.assertTrue((out_dir / REPORT_META_FILENAME).exists())

            html = (out_dir / REPORT_HTML_FILENAME).read_text(encoding="utf-8")
            snapshot_json = (out_dir / REPORT_SNAPSHOT_FILENAME).read_text(
                encoding="utf-8"
            )
            meta_json = (out_dir / REPORT_META_FILENAME).read_text(encoding="utf-8")
            self.assertIn("Field Snapshot", html)
            self.assertIn("Leaderboard", html)
            self.assertIn("Unmatched Athletes", html)
            self.assertIn("Download CSV", html)
            self.assertIn('href="report.csv"', html)
            self.assertIn("TrailIntel Pages", html)
            self.assertIn('href="../../../index.html"', html)
            self.assertIn("Alice Trail", html)
            self.assertIn("Bob Missing", html)
            self.assertIn("open source page", html)
            self.assertIn("https://utmb.world/runner/123.alice-trail", html)
            self.assertIn("https://itra.run/RunnerSpace/Trail.Alice/123", html)
            self.assertIn("https://www.betrail.run/runner/alice.trail/overview", html)
            self.assertIn("Betrail matches", html)
            self.assertIn("Published Apr 4, 2026 at 12:00 UTC", html)
            self.assertNotIn("Same-name mode", html)
            self.assertNotIn("DuckDB cache", html)
            self.assertNotIn('"same_name_mode"', snapshot_json)
            self.assertNotIn('"same_name_mode"', meta_json)

    def test_export_report_site_can_use_snapshot_export_rows_without_records(
        self,
    ) -> None:
        snapshot = {
            "title": "Saved Snapshot",
            "participants_count": 2,
            "rows_evaluated": 2,
            "qualified_count": 1,
            "strategy": "participant-first",
            "rows": [
                {
                    "Rank": 1,
                    "Athlete": "Alice",
                    "UTMB": "745.0",
                    "ITRA": "730.0",
                    "Betrail": "74.5",
                    "Combined": "739.0",
                }
            ],
            "export_rows": [
                {"Rank": 1, "Athlete": "Alice", "UTMB": "745.0", "Betrail": "74.5"},
                {"Rank": 2, "Athlete": "Bob", "UTMB": "-"},
            ],
            "no_result_names": ["Bob"],
            "utmb_scores": [745.0],
            "itra_scores": [730.0],
            "betrail_scores": [74.5],
            "score_summary": {
                "participants": 2,
                "with_utmb": 1,
                "with_itra": 1,
                "with_betrail": 1,
                "with_any": 1,
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = export_report_site(
                snapshot=snapshot, records=None, destination=tmp
            )
            csv_text = (out_dir / REPORT_CSV_FILENAME).read_text(encoding="utf-8")
            json_text = (out_dir / REPORT_JSON_FILENAME).read_text(encoding="utf-8")

            self.assertIn("Athlete", csv_text)
            self.assertIn("Alice", csv_text)
            self.assertIn("Bob", csv_text)
            self.assertIn('"Athlete": "Alice"', json_text)

    def test_refresh_site_index_separates_races_and_forecasts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            race_dir = root / REPORTS_SECTION_DIR / "trail-du-test" / "20260404-120000"
            forecast_dir = (
                root / FORECASTS_SECTION_DIR / "dolomite-dawn" / "20260701-054500"
            )
            race_dir.mkdir(parents=True, exist_ok=True)
            forecast_dir.mkdir(parents=True, exist_ok=True)

            (race_dir / REPORT_META_FILENAME).write_text(
                json.dumps(
                    {
                        "report_kind": "race",
                        "title": "Trail du Test 2026",
                        "participants_count": 2,
                        "qualified_count": 1,
                        "strategy": "participant-first",
                        "published_at": "2026-04-04T12:00:00+00:00",
                        "report_path": "reports/trail-du-test/20260404-120000/index.html",
                        "csv_path": "reports/trail-du-test/20260404-120000/report.csv",
                        "json_path": "reports/trail-du-test/20260404-120000/report.json",
                    }
                ),
                encoding="utf-8",
            )
            (forecast_dir / REPORT_META_FILENAME).write_text(
                json.dumps(
                    {
                        "report_kind": "forecast",
                        "title": "Dolomite Dawn",
                        "route_distance_km": 18.2,
                        "start_time": "2026-07-15T06:30:00+02:00",
                        "duration": "03:30",
                        "published_at": "2026-07-01T05:45:00+00:00",
                        "report_path": "forecasts/dolomite-dawn/20260701-054500/index.html",
                        "png_path": "forecasts/dolomite-dawn/20260701-054500/forecast.png",
                        "gpx_path": "forecasts/dolomite-dawn/20260701-054500/route.gpx",
                        "json_path": "forecasts/dolomite-dawn/20260701-054500/snapshot.json",
                    }
                ),
                encoding="utf-8",
            )

            refresh_site_index(root)

            reports_index = (
                root / REPORTS_SECTION_DIR / REPORT_HTML_FILENAME
            ).read_text(encoding="utf-8")
            forecasts_index = (
                root / FORECASTS_SECTION_DIR / REPORT_HTML_FILENAME
            ).read_text(encoding="utf-8")
            landing_index = (root / REPORT_HTML_FILENAME).read_text(encoding="utf-8")

            self.assertIn("Trail du Test 2026", reports_index)
            self.assertNotIn("Dolomite Dawn", reports_index)
            self.assertIn("Dolomite Dawn", forecasts_index)
            self.assertNotIn("Trail du Test 2026", forecasts_index)
            self.assertIn("reports/index.html", landing_index)
            self.assertIn("forecasts/index.html", landing_index)
            self.assertIn("TrailIntel Pages", landing_index)
            self.assertIn("Published Reports", reports_index)
            self.assertIn("Published Forecasts", forecasts_index)
            self.assertIn("Back to home", reports_index)
            self.assertIn("Back to home", forecasts_index)
            self.assertIn(
                'href="trail-du-test/20260404-120000/index.html"', reports_index
            )
            self.assertNotIn(
                'href="reports/trail-du-test/20260404-120000/index.html"', reports_index
            )
            self.assertIn(
                'href="dolomite-dawn/20260701-054500/index.html"', forecasts_index
            )
            self.assertNotIn(
                'href="forecasts/dolomite-dawn/20260701-054500/index.html"',
                forecasts_index,
            )


if __name__ == "__main__":
    unittest.main()
