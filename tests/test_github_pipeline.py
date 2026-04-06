from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest

from trailintel.github_pipeline import (
    ReportRequest,
    build_cli_args,
    build_publish_paths,
    parse_issue_form,
    publish_report_bundle,
)
from trailintel.models import AthleteRecord
from trailintel.site import build_report_snapshot, export_report_site


class GitHubPipelineTests(unittest.TestCase):
    def test_parse_issue_form(self) -> None:
        body = """### Race Name
Trail du Sanglier 2026

### Race URL
https://in.yaka-inscription.com/trail-du-sanglier-2026?currentPage=select-competition

### Competition / Distance
Le 40 km

### Score Threshold
680

### Top
100

### Strategy
participant-first

### Notes
Run this one first.
"""
        request = parse_issue_form(body)
        self.assertEqual(request.race_name, "Trail du Sanglier 2026")
        self.assertEqual(request.competition_name, "Le 40 km")
        self.assertEqual(request.score_threshold, 680.0)
        self.assertEqual(request.top, 100)
        self.assertEqual(request.strategy, "participant-first")
        self.assertEqual(request.notes, "Run this one first.")

    def test_build_publish_paths(self) -> None:
        request = ReportRequest(
            race_name="Trail du Sanglier 2026",
            race_url="https://example.com/race",
            competition_name="Le 40 km",
        )
        report_dir, latest_dir = build_publish_paths(
            request,
            published_at=datetime(2026, 4, 4, 10, 30, 45, tzinfo=UTC),
        )
        self.assertEqual(report_dir, "reports/trail-du-sanglier-2026-le-40-km/20260404-103045")
        self.assertEqual(latest_dir, "reports/trail-du-sanglier-2026-le-40-km/latest")

    def test_build_race_slug_skips_duplicate_competition_words(self) -> None:
        request = ReportRequest(
            race_name="Trail du Sanglier 2026 - Le 40 km",
            race_url="https://example.com/race",
            competition_name="Le 40 km",
        )
        report_dir, latest_dir = build_publish_paths(
            request,
            published_at=datetime(2026, 4, 4, 10, 30, 45, tzinfo=UTC),
        )
        self.assertEqual(report_dir, "reports/trail-du-sanglier-2026-le-40-km/20260404-103045")
        self.assertEqual(latest_dir, "reports/trail-du-sanglier-2026-le-40-km/latest")

    def test_build_cli_args_includes_site_dir(self) -> None:
        request = ReportRequest(
            race_name="Trail du Sanglier 2026",
            race_url="https://example.com/race",
            competition_name="Le 40 km",
        )
        args = build_cli_args(request, site_dir="dist/site")
        self.assertIn("--site-dir", args)
        self.assertIn("dist/site", args)
        self.assertIn("--competition-name", args)
        self.assertIn("Le 40 km", args)

    def test_publish_report_bundle_updates_latest_and_index(self) -> None:
        request = ReportRequest(
            race_name="Trail du Test 2026",
            race_url="https://example.com/race",
            competition_name="42 km",
        )
        snapshot = build_report_snapshot(
            title=request.race_name,
            all_records=[
                AthleteRecord(input_name="Alice Trail", utmb_index=745.0, itra_score=730.0),
                AthleteRecord(input_name="Bob Missing"),
            ],
            qualified_records=[AthleteRecord(input_name="Alice Trail", utmb_index=745.0, itra_score=730.0)],
            participants_count=2,
            strategy=request.strategy,
            same_name_mode="highest",
            top=100,
            sort_by="combined",
            race_url=request.race_url,
            competition_name=request.competition_name,
            score_threshold=request.score_threshold,
            generated_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        )

        with tempfile.TemporaryDirectory() as tmp:
            source_dir = export_report_site(
                snapshot=snapshot,
                records=[
                    AthleteRecord(input_name="Alice Trail", utmb_index=745.0, itra_score=730.0),
                    AthleteRecord(input_name="Bob Missing"),
                ],
                destination=f"{tmp}/bundle",
            )
            result = publish_report_bundle(
                source_dir=source_dir,
                pages_root=f"{tmp}/pages",
                request=request,
                published_at=datetime(2026, 4, 4, 12, 34, 56, tzinfo=UTC),
                base_url="https://example.github.io/trailintel-reports",
            )

            self.assertEqual(
                result.report_url,
                "https://example.github.io/trailintel-reports/reports/trail-du-test-2026-42-km/20260404-123456/index.html",
            )
            self.assertEqual(
                result.latest_url,
                "https://example.github.io/trailintel-reports/reports/trail-du-test-2026-42-km/latest/index.html",
            )

            index_html = Path(f"{tmp}/pages/index.html").read_text(encoding="utf-8")
            self.assertIn("Trail du Test 2026", index_html)
            self.assertIn("reports/trail-du-test-2026-42-km/20260404-123456/index.html", index_html)


if __name__ == "__main__":
    unittest.main()
