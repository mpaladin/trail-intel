from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import unittest

from trailintel.forecast.errors import GPXParseError, InputValidationError
from trailintel.forecast.gpx_route import parse_gpx, sample_route


FIXTURE = Path(__file__).parent / "fixtures" / "sample_route.gpx"


class ForecastGpxRouteTests(unittest.TestCase):
    def test_parse_gpx_reads_track_points(self) -> None:
        route = parse_gpx(FIXTURE)
        self.assertEqual(len(route.points), 6)
        self.assertGreater(route.total_distance_m, 1000)
        self.assertGreater(route.total_ascent_m, 0)

    def test_parse_gpx_rejects_missing_file(self) -> None:
        with self.assertRaises(GPXParseError):
            parse_gpx("does-not-exist.gpx")

    def test_sample_route_clamps_to_minimum_samples(self) -> None:
        route = parse_gpx(FIXTURE)
        samples = sample_route(
            route,
            datetime(2026, 3, 28, 8, 0, tzinfo=UTC),
            timedelta(minutes=45),
            sample_minutes=10,
        )
        self.assertEqual(len(samples), 15)
        self.assertAlmostEqual(samples[-1].fraction, 1.0)

    def test_sample_route_rejects_invalid_sample_minutes(self) -> None:
        route = parse_gpx(FIXTURE)
        with self.assertRaises(InputValidationError):
            sample_route(
                route,
                datetime(2026, 3, 28, 8, 0, tzinfo=UTC),
                timedelta(hours=1),
                sample_minutes=0,
            )


if __name__ == "__main__":
    unittest.main()
