from __future__ import annotations

import unittest

from trailintel.providers.utmb import UtmbClient


class UtmbUrlTests(unittest.TestCase):
    def test_runner_slug_uri_maps_to_runner_path(self) -> None:
        client = UtmbClient(timeout=5)
        self.assertEqual(
            client._to_profile_url("2704.kilian.jornetburgada"),
            "https://utmb.world/runner/2704.kilian.jornetburgada",
        )

    def test_existing_http_url_stays_as_is(self) -> None:
        client = UtmbClient(timeout=5)
        self.assertEqual(
            client._to_profile_url("https://utmb.world/runner/1234.name.surname"),
            "https://utmb.world/runner/1234.name.surname",
        )

    def test_runner_prefix_uri_is_normalized(self) -> None:
        client = UtmbClient(timeout=5)
        self.assertEqual(
            client._to_profile_url("/runner/1234.name.surname"),
            "https://utmb.world/runner/1234.name.surname",
        )
        self.assertEqual(
            client._to_profile_url("runner/1234.name.surname"),
            "https://utmb.world/runner/1234.name.surname",
        )


if __name__ == "__main__":
    unittest.main()
