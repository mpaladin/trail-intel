from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from trailintel.forecast.engine import summarize_report
from trailintel.forecast.models import (
    Bounds,
    ForecastReport,
    RouteData,
    RoutePoint,
    SampleForecast,
    SamplePoint,
)


def make_sample(
    index: int,
    *,
    minutes: int,
    temperature_c: float,
    wind_kph: float,
    precipitation_mm: float,
    precipitation_probability: float,
) -> SampleForecast:
    start = datetime(2026, 3, 28, 8, 0, tzinfo=UTC)
    timestamp = start + timedelta(minutes=minutes)
    point = SamplePoint(
        index=index,
        fraction=index / 3 if index else 0.0,
        elapsed=timedelta(minutes=minutes),
        timestamp=timestamp,
        lat=47.0 + index * 0.01,
        lon=8.0 + index * 0.01,
        elevation_m=400.0 + index * 10,
        distance_m=index * 1000.0,
    )
    return SampleForecast(
        sample=point,
        temperature_c=temperature_c,
        apparent_temperature_c=temperature_c - 1.0,
        wind_kph=wind_kph,
        wind_gust_kph=wind_kph + 5.0,
        wind_direction_deg=270.0,
        cloud_cover_pct=40.0,
        precipitation_mm=precipitation_mm,
        precipitation_probability=precipitation_probability,
    )


def build_report(samples: list[SampleForecast]) -> ForecastReport:
    start = samples[0].sample.timestamp
    end = samples[-1].sample.timestamp
    route = RouteData(
        points=[
            RoutePoint(lat=47.0, lon=8.0, elevation_m=400.0, distance_m=0.0),
            RoutePoint(lat=47.03, lon=8.03, elevation_m=430.0, distance_m=3000.0),
        ],
        total_distance_m=3000.0,
        total_ascent_m=30.0,
        bounds=Bounds(min_lat=47.0, max_lat=47.03, min_lon=8.0, max_lon=8.03),
    )
    return ForecastReport(
        route=route,
        samples=samples,
        start_time=start,
        end_time=end,
        duration=end - start,
        source_label="Open-Meteo Forecast API",
    )


class ForecastEngineTests(unittest.TestCase):
    def test_summarize_report_prefers_highest_rain_amount_over_probability(
        self,
    ) -> None:
        report = build_report(
            [
                make_sample(
                    0,
                    minutes=0,
                    temperature_c=10.0,
                    wind_kph=12.0,
                    precipitation_mm=0.3,
                    precipitation_probability=90.0,
                ),
                make_sample(
                    1,
                    minutes=30,
                    temperature_c=9.0,
                    wind_kph=15.0,
                    precipitation_mm=0.6,
                    precipitation_probability=30.0,
                ),
                make_sample(
                    2,
                    minutes=60,
                    temperature_c=8.0,
                    wind_kph=14.0,
                    precipitation_mm=0.2,
                    precipitation_probability=95.0,
                ),
            ]
        )

        summary = summarize_report(report)

        self.assertEqual(summary.wettest_time, report.samples[1].sample.timestamp)
        self.assertEqual(summary.wettest_precipitation_mm, 0.6)
        self.assertEqual(summary.wettest_probability_pct, 30.0)

    def test_summarize_report_breaks_wettest_ties_by_earliest_timestamp(self) -> None:
        report = build_report(
            [
                make_sample(
                    0,
                    minutes=0,
                    temperature_c=10.0,
                    wind_kph=12.0,
                    precipitation_mm=0.4,
                    precipitation_probability=60.0,
                ),
                make_sample(
                    1,
                    minutes=30,
                    temperature_c=9.0,
                    wind_kph=15.0,
                    precipitation_mm=0.4,
                    precipitation_probability=60.0,
                ),
                make_sample(
                    2,
                    minutes=60,
                    temperature_c=8.0,
                    wind_kph=14.0,
                    precipitation_mm=0.1,
                    precipitation_probability=95.0,
                ),
            ]
        )

        summary = summarize_report(report)

        self.assertEqual(summary.wettest_time, report.samples[0].sample.timestamp)
        self.assertEqual(summary.wettest_precipitation_mm, 0.4)
        self.assertEqual(summary.wettest_probability_pct, 60.0)


if __name__ == "__main__":
    unittest.main()
