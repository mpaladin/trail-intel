from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from trailintel.forecast.bundle import ForecastBundleResult
from trailintel.forecast.cli import app
from trailintel.forecast.engine import ForecastSummary
from trailintel.forecast.errors import EpicForecastError


FIXTURE = Path(__file__).parent / "fixtures" / "sample_route.gpx"


class ForecastCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_cli_reports_validation_errors(self) -> None:
        with patch(
            "trailintel.forecast.cli.generate_forecast_assets",
            side_effect=EpicForecastError("Start time must be in the future."),
        ):
            result = self.runner.invoke(
                app,
                [
                    "forecast",
                    str(FIXTURE),
                    "--start",
                    "2026-03-20T08:00:00+00:00",
                    "--duration",
                    "02:00",
                    "--output",
                    "out.png",
                ],
            )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Start time must be in the future", result.output)

    def test_cli_smoke_reports_png_and_site_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"
            fake_summary = ForecastSummary(
                temperature_min_c=8.0,
                temperature_max_c=12.0,
                wind_max_kph=18.0,
                precipitation_total_mm=0.4,
                wettest_time=datetime(2026, 3, 28, 9, 0, tzinfo=UTC),
                wettest_probability_pct=60.0,
            )
            fake_result = ForecastBundleResult(
                report=None,  # type: ignore[arg-type]
                summary=fake_summary,
                image_path=output,
                site_dir=site_dir,
                snapshot=None,
            )

            with patch("trailintel.forecast.cli.generate_forecast_assets", return_value=fake_result):
                result = self.runner.invoke(
                    app,
                    [
                        "forecast",
                        str(FIXTURE),
                        "--start",
                        "2026-03-28T08:00:00+00:00",
                        "--duration",
                        "02:00",
                        "--output",
                        str(output),
                        "--site-dir",
                        str(site_dir),
                    ],
                )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Saved image", result.output)
        self.assertIn("Saved site bundle", result.output)


if __name__ == "__main__":
    unittest.main()
