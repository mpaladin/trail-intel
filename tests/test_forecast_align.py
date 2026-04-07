from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from trailintel.forecast.align import align_forecasts
from trailintel.forecast.errors import WeatherAPIError
from trailintel.forecast.models import HourlyForecast, SamplePoint


def make_sample(index: int, minutes: int) -> SamplePoint:
    start = datetime(2026, 3, 28, 8, 0, tzinfo=UTC)
    return SamplePoint(
        index=index,
        fraction=index / 2,
        elapsed=timedelta(minutes=minutes),
        timestamp=start + timedelta(minutes=minutes),
        lat=47.0,
        lon=8.0,
        elevation_m=400.0,
        distance_m=index * 1000.0,
    )


class ForecastAlignTests(unittest.TestCase):
    def test_align_forecasts_interpolates_temperature_and_wind(self) -> None:
        forecast = HourlyForecast(
            times=[
                datetime(2026, 3, 28, 8, 0, tzinfo=UTC),
                datetime(2026, 3, 28, 9, 0, tzinfo=UTC),
            ],
            temperature_c=[10.0, 14.0],
            apparent_temperature_c=[8.0, 16.0],
            wind_kph=[20.0, 28.0],
            wind_gust_kph=[32.0, 40.0],
            wind_direction_deg=[270.0, 315.0],
            cloud_cover_pct=[30.0, 70.0],
            precipitation_mm=[1.0, 2.0],
            precipitation_probability=[40.0, 80.0],
        )

        result = align_forecasts([make_sample(0, 30)], [forecast])[0]
        self.assertAlmostEqual(result.temperature_c, 12.0)
        self.assertAlmostEqual(result.apparent_temperature_c, 12.0)
        self.assertAlmostEqual(result.wind_kph, 24.0)
        self.assertAlmostEqual(result.wind_gust_kph, 36.0)
        self.assertAlmostEqual(result.wind_direction_deg, 292.5)
        self.assertAlmostEqual(result.cloud_cover_pct, 50.0)
        self.assertAlmostEqual(result.precipitation_mm, 1.0)
        self.assertAlmostEqual(result.precipitation_probability, 40.0)

    def test_align_forecasts_wraps_wind_direction_circularly(self) -> None:
        forecast = HourlyForecast(
            times=[
                datetime(2026, 3, 28, 8, 0, tzinfo=UTC),
                datetime(2026, 3, 28, 9, 0, tzinfo=UTC),
            ],
            temperature_c=[10.0, 14.0],
            apparent_temperature_c=[8.0, 16.0],
            wind_kph=[20.0, 28.0],
            wind_gust_kph=[32.0, 40.0],
            wind_direction_deg=[350.0, 10.0],
            cloud_cover_pct=[30.0, 70.0],
            precipitation_mm=[1.0, 2.0],
            precipitation_probability=[40.0, 80.0],
        )

        result = align_forecasts([make_sample(0, 30)], [forecast])[0]
        self.assertAlmostEqual(result.wind_direction_deg, 0.0)

    def test_align_forecasts_rejects_uncovered_times(self) -> None:
        forecast = HourlyForecast(
            times=[datetime(2026, 3, 28, 9, 0, tzinfo=UTC)],
            temperature_c=[10.0],
            apparent_temperature_c=[8.0],
            wind_kph=[20.0],
            wind_gust_kph=[32.0],
            wind_direction_deg=[270.0],
            cloud_cover_pct=[30.0],
            precipitation_mm=[1.0],
            precipitation_probability=[40.0],
        )

        with self.assertRaises(WeatherAPIError):
            align_forecasts([make_sample(0, 30)], [forecast])


if __name__ == "__main__":
    unittest.main()
