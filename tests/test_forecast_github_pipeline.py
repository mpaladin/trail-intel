from __future__ import annotations

import builtins
import importlib
import sys
import tempfile
import unittest
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from trailintel.forecast.github_pipeline import (
    ForecastRequest,
    download_gpx_source,
    parse_issue_form,
    publish_forecast_bundle,
    resolve_gpx_source_url,
)


class ForecastGitHubPipelineTests(unittest.TestCase):
    def test_module_import_does_not_require_forecast_runtime_deps(self) -> None:
        body = """### Route Name
Dolomite Dawn

### GPX URL
https://example.com/route.gpx

### Start Date
2026-07-15

### Start Time
06:30

### Timezone
Europe/Rome

### Duration
03:30
"""
        original_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "trailintel.forecast.site":
                raise ModuleNotFoundError("simulated missing forecast runtime deps")
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=guarded_import):
            sys.modules.pop("trailintel.forecast.github_pipeline", None)
            module = importlib.import_module("trailintel.forecast.github_pipeline")
            request = module.parse_issue_form(body)

        self.assertEqual(request.route_slug, "dolomite-dawn")

    def test_parse_issue_form(self) -> None:
        body = """### Route Name
Dolomite Dawn

### GPX URL
https://example.com/route.gpx

### Start Date
2026-07-15

### Start Time
06:30

### Timezone
Europe/Rome

### Duration
03:30

### Notes
Sunrise push.
"""
        request = parse_issue_form(body)
        self.assertEqual(request.route_name, "Dolomite Dawn")
        self.assertEqual(request.gpx_url, "https://example.com/route.gpx")
        self.assertEqual(request.start_date, "2026-07-15")
        self.assertEqual(request.start_time, "06:30")
        self.assertEqual(request.timezone_name, "Europe/Rome")
        self.assertEqual(request.duration, "03:30")
        self.assertEqual(request.notes, "Sunrise push.")

    def test_resolve_gpx_source_url_prefers_explicit_url(self) -> None:
        request = ForecastRequest(
            route_name="Dolomite Dawn",
            gpx_url="https://example.com/route.gpx",
            start_date="2026-07-15",
            start_time="06:30",
            timezone_name="Europe/Rome",
            duration="03:30",
        )
        body = "Attachment https://github.com/user-attachments/files/123/route.zip"
        self.assertEqual(
            resolve_gpx_source_url(request, body), "https://example.com/route.gpx"
        )

    def test_resolve_gpx_source_url_uses_single_zip_attachment(self) -> None:
        request = ForecastRequest(
            route_name="Dolomite Dawn",
            gpx_url="",
            start_date="2026-07-15",
            start_time="06:30",
            timezone_name="Europe/Rome",
            duration="03:30",
        )
        body = "ZIP attachment https://github.com/user-attachments/files/123/route.zip"
        self.assertEqual(
            resolve_gpx_source_url(request, body),
            "https://github.com/user-attachments/files/123/route.zip",
        )

    def test_download_gpx_source_extracts_single_gpx_from_zip(self) -> None:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("route.gpx", "<gpx><trk></trk></gpx>")
        zip_bytes = buffer.getvalue()

        class FakeResponse:
            def __init__(self, content: bytes) -> None:
                self.content = content
                self.headers = {"Content-Type": "application/zip"}

            def raise_for_status(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            with patch("requests.get", return_value=FakeResponse(zip_bytes)):
                path = download_gpx_source(
                    source_url="https://example.com/route.zip",
                    output_dir=tmp,
                )
            self.assertEqual(path.name, "route.gpx")
            self.assertIn("<gpx>", path.read_text(encoding="utf-8"))

    def test_publish_forecast_bundle_updates_latest_and_indexes(self) -> None:
        request = ForecastRequest(
            route_name="Dolomite Dawn",
            gpx_url="https://example.com/route.gpx",
            start_date="2026-07-15",
            start_time="06:30",
            timezone_name="Europe/Rome",
            duration="03:30",
            notes="Sunrise push.",
        )

        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "bundle"
            source.mkdir(parents=True, exist_ok=True)
            (source / "index.html").write_text(
                "<html>forecast</html>", encoding="utf-8"
            )
            (source / "forecast.png").write_bytes(b"png")
            (source / "route.gpx").write_text("<gpx></gpx>", encoding="utf-8")
            (source / "snapshot.json").write_text(
                '{"report_kind":"forecast","title":"Dolomite Dawn"}', encoding="utf-8"
            )
            (source / "report-meta.json").write_text(
                '{"report_kind":"forecast","title":"Dolomite Dawn"}',
                encoding="utf-8",
            )

            result = publish_forecast_bundle(
                source_dir=source,
                pages_root=Path(tmp) / "pages",
                request=request,
                published_at=datetime(2026, 7, 1, 5, 45, tzinfo=UTC),
                base_url="https://example.github.io/trailintel-pages",
            )

            self.assertEqual(
                result.report_url,
                "https://example.github.io/trailintel-pages/forecasts/dolomite-dawn/20260701-054500/index.html",
            )
            self.assertEqual(
                result.latest_url,
                "https://example.github.io/trailintel-pages/forecasts/dolomite-dawn/latest/index.html",
            )

            forecast_index = (
                Path(tmp) / "pages" / "forecasts" / "index.html"
            ).read_text(encoding="utf-8")
            root_index = (Path(tmp) / "pages" / "index.html").read_text(
                encoding="utf-8"
            )
            self.assertIn("Dolomite Dawn", forecast_index)
            self.assertIn("forecasts/index.html", root_index)


if __name__ == "__main__":
    unittest.main()
