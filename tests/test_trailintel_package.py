from __future__ import annotations

import unittest

from trailintel import __version__
from trailintel.cli import main as cli_main
from trailintel.forecast import generate_forecast_assets
from trailintel.forecast.cli import main as forecast_cli_main
from trailintel.github_pipeline import build_cli_args
from trailintel.models import AthleteRecord
from trailintel.providers.itra import ItraClient


class TrailIntelPackageTests(unittest.TestCase):
    def test_package_exports_version(self) -> None:
        self.assertEqual(__version__, "0.1.0")

    def test_cli_exports_main(self) -> None:
        self.assertTrue(callable(cli_main))

    def test_forecast_cli_exports_main(self) -> None:
        self.assertTrue(callable(forecast_cli_main))

    def test_github_pipeline_helpers_are_importable(self) -> None:
        self.assertTrue(callable(build_cli_args))

    def test_primary_model_is_importable(self) -> None:
        self.assertEqual(AthleteRecord.__name__, "AthleteRecord")

    def test_primary_provider_is_importable(self) -> None:
        self.assertEqual(ItraClient.__name__, "ItraClient")

    def test_forecast_bundle_api_is_importable(self) -> None:
        self.assertTrue(callable(generate_forecast_assets))


if __name__ == "__main__":
    unittest.main()
