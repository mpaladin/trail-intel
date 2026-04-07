from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from trailintel.backfill_pages import backfill_pages
from trailintel.models import AthleteRecord
from trailintel.providers.betrail import BetrailCatalogEntry
from trailintel.site import (
    REPORT_HTML_FILENAME,
    REPORT_JSON_FILENAME,
    REPORT_META_FILENAME,
    REPORT_SNAPSHOT_FILENAME,
    build_report_snapshot,
    export_report_site,
)


class BackfillPagesTests(unittest.TestCase):
    @patch("trailintel.backfill_pages.BetrailClient.fetch_catalog_above_threshold")
    def test_backfill_updates_timestamped_and_latest_bundles(self, mock_fetch_catalog) -> None:
        mock_fetch_catalog.return_value = [
            BetrailCatalogEntry(
                name="Alice Trail",
                betrail_score=74.5,
                profile_url="https://www.betrail.run/runner/alice.trail/overview",
                raw={},
            )
        ]

        records = [
            AthleteRecord(
                input_name="Alice Trail",
                utmb_index=745.0,
                itra_score=730.0,
            ),
            AthleteRecord(input_name="Bob Missing"),
        ]
        snapshot = build_report_snapshot(
            title="Trail du Test 2026",
            all_records=records,
            qualified_records=[records[0]],
            participants_count=2,
            strategy="participant-first",
            same_name_mode="highest",
            top=1,
            sort_by="combined",
            race_url="https://example.com/race",
            competition_name="42 km",
            score_threshold=680.0,
            generated_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pages"
            source = export_report_site(
                snapshot=snapshot,
                records=records,
                destination=Path(tmp) / "source",
            )

            bundle_dirs = [
                root / "reports" / "trail-du-test" / "20260404-120000",
                root / "reports" / "trail-du-test" / "latest",
            ]
            for bundle_dir in bundle_dirs:
                shutil.copytree(source, bundle_dir)
                meta_path = bundle_dir / REPORT_META_FILENAME
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                rel = bundle_dir.relative_to(root).as_posix()
                meta.update(
                    {
                        "published_at": "2026-04-04T12:34:56+00:00",
                        "report_path": f"{rel}/{REPORT_HTML_FILENAME}",
                        "json_path": f"{rel}/{REPORT_JSON_FILENAME}",
                    }
                )
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

            processed = backfill_pages(pages_root=root, timeout=5, betrail_cookie=None)

            self.assertEqual(processed, 2)
            for bundle_dir in bundle_dirs:
                report_rows = json.loads((bundle_dir / REPORT_JSON_FILENAME).read_text(encoding="utf-8"))
                self.assertEqual(report_rows[0]["betrail_score"], 74.5)
                self.assertEqual(
                    report_rows[0]["betrail_profile_url"],
                    "https://www.betrail.run/runner/alice.trail/overview",
                )

                snapshot_data = json.loads((bundle_dir / REPORT_SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
                self.assertEqual(snapshot_data["betrail_scores"], [74.5])
                self.assertEqual(snapshot_data["score_summary"]["with_betrail"], 1)

                html = (bundle_dir / REPORT_HTML_FILENAME).read_text(encoding="utf-8")
                self.assertIn("https://www.betrail.run/runner/alice.trail/overview", html)

                meta = json.loads((bundle_dir / REPORT_META_FILENAME).read_text(encoding="utf-8"))
                self.assertEqual(meta["published_at"], "2026-04-04T12:34:56+00:00")
                self.assertIn("report_path", meta)

            reports_index = (root / "reports" / REPORT_HTML_FILENAME).read_text(encoding="utf-8")
            self.assertIn("Trail du Test 2026", reports_index)
