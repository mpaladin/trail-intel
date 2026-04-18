from __future__ import annotations

import os
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import httpx

from tests.forecast_test_support import (
    FIXTURE,
    make_open_meteo_payload,
    open_meteo_batch_response,
    weatherapi_error_response,
)
from trailintel.forecast.engine import build_report as build_route_report
from trailintel.forecast.engine import build_reports_with_metadata, summarize_report
from trailintel.forecast.errors import InputValidationError
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
        provider_id="open-meteo",
        route=route,
        samples=samples,
        start_time=start,
        end_time=end,
        duration=end - start,
        source_label="Open-Meteo Forecast API",
    )


class ForecastEngineTests(unittest.TestCase):
    def build_open_meteo_client(
        self,
        *,
        payload: dict[str, object] | None = None,
        weatherapi_status_code: int | None = None,
        weatherapi_message: str = "API key is invalid.",
    ) -> httpx.Client:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                return open_meteo_batch_response(request, payload=payload)
            if request.url.host == "api.weatherapi.com" and weatherapi_status_code:
                return weatherapi_error_response(
                    status_code=weatherapi_status_code,
                    message=weatherapi_message,
                )
            raise AssertionError(f"Unexpected forecast host: {request.url.host}")

        return httpx.Client(transport=httpx.MockTransport(handler))

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

    def test_build_report_uses_provider_horizon_for_weatherapi(self) -> None:
        with self.assertRaises(InputValidationError):
            build_route_report(
                gpx_path=FIXTURE,
                start="2026-03-31T08:00:00+00:00",
                duration="02:00",
                provider="weatherapi",
                weatherapi_key="test-key",
                now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            )

    def test_build_report_requires_weatherapi_key_before_network(self) -> None:
        with patch.dict(os.environ, {"WEATHERAPI_KEY": ""}):
            with self.assertRaisesRegex(InputValidationError, "WEATHERAPI_KEY"):
                build_route_report(
                    gpx_path=FIXTURE,
                    start="2026-03-28T08:00:00+00:00",
                    duration="02:00",
                    provider="weatherapi",
                    now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
                )

    def test_build_reports_skips_comparison_provider_beyond_horizon(self) -> None:
        client = self.build_open_meteo_client(
            payload=make_open_meteo_payload(
                times=(
                    "2026-03-31T08:00",
                    "2026-03-31T09:00",
                    "2026-03-31T10:00",
                )
            )
        )

        result = build_reports_with_metadata(
            gpx_path=FIXTURE,
            start="2026-03-31T08:00:00+00:00",
            duration="02:00",
            provider="open-meteo",
            compare_providers=["weatherapi"],
            http_client=client,
            now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(len(result.reports), 1)
        self.assertEqual(result.reports[0].provider_id, "open-meteo")
        self.assertEqual(len(result.skipped_comparisons), 1)
        self.assertEqual(result.skipped_comparisons[0].provider_id, "weatherapi")
        self.assertIn("3-day forecast horizon", result.skipped_comparisons[0].reason)

    def test_build_reports_skips_comparison_provider_on_api_error(self) -> None:
        client = self.build_open_meteo_client(weatherapi_status_code=401)

        result = build_reports_with_metadata(
            gpx_path=FIXTURE,
            start="2026-03-28T08:00:00+00:00",
            duration="02:00",
            provider="open-meteo",
            compare_providers=["weatherapi"],
            weatherapi_key="test-key",
            http_client=client,
            now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(len(result.reports), 1)
        self.assertEqual(result.reports[0].provider_id, "open-meteo")
        self.assertEqual(len(result.skipped_comparisons), 1)
        self.assertEqual(result.skipped_comparisons[0].provider_id, "weatherapi")
        self.assertIn("HTTP 401", result.skipped_comparisons[0].reason)
        self.assertIn("API key is invalid", result.skipped_comparisons[0].reason)


if __name__ == "__main__":
    unittest.main()
