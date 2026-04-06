from __future__ import annotations

import unittest

from trailintel import __version__
from trailintel.cli import main as cli_main
from trailintel.github_pipeline import build_parser as build_github_parser
from trailintel.models import AthleteRecord
from trailintel.providers.itra import ItraClient


class TrailIntelPackageTests(unittest.TestCase):
    def test_package_exports_version(self) -> None:
        self.assertEqual(__version__, "0.1.0")

    def test_cli_exports_main(self) -> None:
        self.assertTrue(callable(cli_main))

    def test_github_pipeline_builds_parser(self) -> None:
        parser = build_github_parser()
        self.assertIn("trailintel.github_pipeline", parser.prog)

    def test_primary_model_is_importable(self) -> None:
        self.assertEqual(AthleteRecord.__name__, "AthleteRecord")

    def test_primary_provider_is_importable(self) -> None:
        self.assertEqual(ItraClient.__name__, "ItraClient")


if __name__ == "__main__":
    unittest.main()
