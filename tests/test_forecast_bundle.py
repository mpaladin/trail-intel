from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import httpx

from trailintel.forecast.bundle import generate_forecast_assets
from trailintel.forecast.render import render_report as original_render_report

FIXTURE = Path(__file__).parent / "fixtures" / "sample_route.gpx"


class ForecastBundleTests(unittest.TestCase):
    def test_generate_forecast_assets_writes_site_bundle(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = {
                "hourly": {
                    "time": [
                        "2026-03-28T08:00",
                        "2026-03-28T09:00",
                        "2026-03-28T10:00",
                    ],
                    "temperature_2m": [8, 10, 12],
                    "apparent_temperature": [7, 9, 11],
                    "wind_speed_10m": [12, 15, 18],
                    "wind_gusts_10m": [18, 22, 25],
                    "wind_direction_10m": [270, 280, 290],
                    "cloud_cover": [25, 35, 45],
                    "precipitation": [0.0, 0.2, 0.4],
                    "precipitation_probability": [10, 35, 60],
                }
            }
            latitudes = request.url.params["latitude"].split(",")
            return httpx.Response(200, json=[payload for _ in latitudes])

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"
            captured_render: dict[str, str | None] = {}

            def render_without_map(report, output_path, *, title=None):
                captured_render["title"] = title
                return original_render_report(
                    report,
                    output_path,
                    title=title,
                    use_real_map=False,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                render_without_map,
            ):
                result = generate_forecast_assets(
                    gpx_path=FIXTURE,
                    start="2026-03-28T08:00:00+00:00",
                    duration="02:00",
                    output_path=output,
                    site_dir=site_dir,
                    title="Sample Loop Forecast",
                    http_client=client,
                    now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
                    generated_at=datetime(2026, 3, 27, 12, 30, tzinfo=UTC),
                )

            self.assertEqual(result.site_dir, site_dir)
            self.assertTrue((site_dir / "index.html").exists())
            self.assertTrue((site_dir / "forecast.png").exists())
            self.assertTrue((site_dir / "route.gpx").exists())
            self.assertTrue((site_dir / "snapshot.json").exists())
            self.assertTrue((site_dir / "report-meta.json").exists())
            self.assertEqual(captured_render.get("title"), "Sample Loop Forecast")
            self.assertIsNotNone(result.snapshot)

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Sample Loop Forecast", html)
            self.assertIn("Route Forecast", html)
            self.assertIn("Forecast Overview", html)
            self.assertIn("Key Moments", html)
            self.assertIn("Route Timeline", html)
            self.assertIn('href="forecast.png"', html)
            self.assertIn('href="route.gpx"', html)
            self.assertIn('href="snapshot.json"', html)
            self.assertIn("Published Mar 27, 2026 at 12:30 UTC", html)
            self.assertNotIn("2026-03-27T12:30:00+00:00", html)
            self.assertIn("Mar 28, 08:00", html)
            self.assertNotIn("2026-03-28T08:00:00+00:00", html)

            snapshot = json.loads(
                (site_dir / "snapshot.json").read_text(encoding="utf-8")
            )
            self.assertEqual(snapshot["report_kind"], "forecast")
            self.assertEqual(snapshot["title"], "Sample Loop Forecast")
            self.assertEqual(snapshot["summary"]["wettest_precipitation_mm"], 0.4)
            self.assertEqual(
                [item["kind"] for item in snapshot["key_moments"]],
                ["start", "coldest", "windiest", "wettest", "finish"],
            )

    def test_generate_forecast_assets_writes_comparison_snapshot_and_html(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                payload = {
                    "hourly": {
                        "time": [
                            "2026-03-28T08:00",
                            "2026-03-28T09:00",
                            "2026-03-28T10:00",
                        ],
                        "temperature_2m": [8, 10, 12],
                        "apparent_temperature": [7, 9, 11],
                        "wind_speed_10m": [12, 15, 18],
                        "wind_gusts_10m": [18, 22, 25],
                        "wind_direction_10m": [270, 280, 290],
                        "cloud_cover": [25, 35, 45],
                        "precipitation": [0.0, 0.2, 0.4],
                        "precipitation_probability": [10, 35, 60],
                    }
                }
                latitudes = request.url.params["latitude"].split(",")
                return httpx.Response(200, json=[payload for _ in latitudes])

            if request.url.host == "api.met.no":
                payload = {
                    "properties": {
                        "timeseries": [
                            {
                                "time": "2026-03-28T08:00:00Z",
                                "data": {
                                    "instant": {
                                        "details": {
                                            "air_temperature": 7.0,
                                            "relative_humidity": 70.0,
                                            "wind_speed": 4.0,
                                            "wind_from_direction": 260.0,
                                            "cloud_area_fraction": 30.0,
                                        }
                                    },
                                    "next_1_hours": {
                                        "details": {
                                            "precipitation_amount": 0.1,
                                            "probability_of_precipitation": 20.0,
                                        }
                                    },
                                },
                            },
                            {
                                "time": "2026-03-28T09:00:00Z",
                                "data": {
                                    "instant": {
                                        "details": {
                                            "air_temperature": 8.5,
                                            "relative_humidity": 72.0,
                                            "wind_speed": 4.5,
                                            "wind_from_direction": 275.0,
                                            "cloud_area_fraction": 40.0,
                                        }
                                    },
                                    "next_6_hours": {
                                        "details": {
                                            "precipitation_amount": 1.2,
                                            "probability_of_precipitation": 50.0,
                                        }
                                    },
                                },
                            },
                            {
                                "time": "2026-03-28T10:00:00Z",
                                "data": {
                                    "instant": {
                                        "details": {
                                            "air_temperature": 10.0,
                                            "relative_humidity": 75.0,
                                            "wind_speed": 5.0,
                                            "wind_from_direction": 290.0,
                                            "cloud_area_fraction": 55.0,
                                        }
                                    },
                                    "next_1_hours": {
                                        "details": {
                                            "precipitation_amount": 0.3,
                                            "probability_of_precipitation": 65.0,
                                        }
                                    },
                                },
                            },
                        ]
                    }
                }
                return httpx.Response(200, json=payload)

            raise AssertionError(f"Unexpected forecast host: {request.url.host}")

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"

            def render_without_map(report, output_path, *, title=None):
                return original_render_report(
                    report,
                    output_path,
                    title=title,
                    use_real_map=False,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                render_without_map,
            ):
                result = generate_forecast_assets(
                    gpx_path=FIXTURE,
                    start="2026-03-28T08:00:00+00:00",
                    duration="02:00",
                    output_path=output,
                    site_dir=site_dir,
                    title="Sample Loop Forecast",
                    provider="open-meteo",
                    compare_providers=["met-no"],
                    http_client=client,
                    now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
                    generated_at=datetime(2026, 3, 27, 12, 30, tzinfo=UTC),
                )

            self.assertEqual(len(result.comparison_reports), 1)

            snapshot = json.loads(
                (site_dir / "snapshot.json").read_text(encoding="utf-8")
            )
            comparison = snapshot.get("comparison")
            self.assertIsInstance(comparison, dict)
            self.assertEqual(comparison["primary_provider"], "open-meteo")
            self.assertEqual(
                [item["provider_id"] for item in comparison["providers"]],
                ["open-meteo", "met-no"],
            )

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Provider Comparison", html)
            self.assertIn("Provider Key Moments", html)
            self.assertIn("MET Norway (yr.no)", html)
            self.assertIn("Open-Meteo", html)

    def test_generate_forecast_assets_skips_short_horizon_comparison_provider(
        self,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.host, "api.open-meteo.com")
            payload = {
                "hourly": {
                    "time": [
                        "2026-03-31T08:00",
                        "2026-03-31T09:00",
                        "2026-03-31T10:00",
                    ],
                    "temperature_2m": [8, 10, 12],
                    "apparent_temperature": [7, 9, 11],
                    "wind_speed_10m": [12, 15, 18],
                    "wind_gusts_10m": [18, 22, 25],
                    "wind_direction_10m": [270, 280, 290],
                    "cloud_cover": [25, 35, 45],
                    "precipitation": [0.0, 0.2, 0.4],
                    "precipitation_probability": [10, 35, 60],
                }
            }
            latitudes = request.url.params["latitude"].split(",")
            return httpx.Response(200, json=[payload for _ in latitudes])

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"

            def render_without_map(report, output_path, *, title=None):
                return original_render_report(
                    report,
                    output_path,
                    title=title,
                    use_real_map=False,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                render_without_map,
            ):
                result = generate_forecast_assets(
                    gpx_path=FIXTURE,
                    start="2026-03-31T08:00:00+00:00",
                    duration="02:00",
                    output_path=output,
                    site_dir=site_dir,
                    title="Sample Loop Forecast",
                    provider="open-meteo",
                    compare_providers=["weatherapi"],
                    http_client=client,
                    now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
                    generated_at=datetime(2026, 3, 27, 12, 30, tzinfo=UTC),
                )

            self.assertEqual(result.comparison_reports, ())
            self.assertEqual(len(result.comparison_warnings), 1)
            self.assertIn("WeatherAPI.com", result.comparison_warnings[0])
            self.assertIn("3-day forecast horizon", result.comparison_warnings[0])

            snapshot = json.loads(
                (site_dir / "snapshot.json").read_text(encoding="utf-8")
            )
            comparison = snapshot.get("comparison")
            self.assertIsInstance(comparison, dict)
            self.assertEqual(
                comparison["warnings"],
                [result.comparison_warnings[0]],
            )

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Skipped comparison sources", html)
            self.assertIn("WeatherAPI.com", html)

    def test_render_report_handles_sparse_and_dense_routes(self) -> None:
        import math
        from datetime import timedelta

        from trailintel.forecast.models import (
            Bounds,
            ForecastReport,
            RouteData,
            RoutePoint,
            SampleForecast,
            SamplePoint,
        )
        from trailintel.forecast.render import render_report

        def build_render_report(sample_count: int) -> ForecastReport:
            start = datetime(2026, 3, 28, 8, 0, tzinfo=UTC)
            duration = timedelta(hours=2, minutes=20)
            route_points = [
                RoutePoint(lat=47.37, lon=8.54, elevation_m=410.0, distance_m=0.0),
                RoutePoint(lat=47.38, lon=8.55, elevation_m=440.0, distance_m=6_000.0),
                RoutePoint(lat=47.39, lon=8.57, elevation_m=510.0, distance_m=14_000.0),
                RoutePoint(lat=47.40, lon=8.59, elevation_m=470.0, distance_m=22_000.0),
                RoutePoint(lat=47.41, lon=8.61, elevation_m=450.0, distance_m=31_000.0),
            ]
            route = RouteData(
                points=route_points,
                total_distance_m=31_000.0,
                total_ascent_m=140.0,
                bounds=Bounds(min_lat=47.37, max_lat=47.41, min_lon=8.54, max_lon=8.61),
            )
            samples: list[SampleForecast] = []
            for index in range(sample_count):
                fraction = index / (sample_count - 1)
                elapsed = duration * fraction
                sample = SamplePoint(
                    index=index,
                    fraction=fraction,
                    elapsed=elapsed,
                    timestamp=start + elapsed,
                    lat=47.37 + (0.04 * fraction),
                    lon=8.54 + (0.07 * fraction),
                    elevation_m=410 + 80 * fraction,
                    distance_m=31_000.0 * fraction,
                )
                samples.append(
                    SampleForecast(
                        sample=sample,
                        temperature_c=6.0 + 10.0 * fraction,
                        apparent_temperature_c=5.0 + 11.5 * fraction,
                        wind_kph=12.0 + 18.0 * fraction,
                        wind_gust_kph=16.0 + 23.0 * fraction,
                        wind_direction_deg=(300.0 + 90.0 * fraction) % 360,
                        cloud_cover_pct=min(
                            95.0, 25.0 + 55.0 * abs(math.sin(fraction * math.pi))
                        ),
                        precipitation_mm=0.0
                        if index < 4
                        else min(1.6, 0.2 * (index - 3)),
                        precipitation_probability=min(100.0, 8.0 + 7.0 * index),
                    )
                )
            return ForecastReport(
                provider_id="open-meteo",
                route=route,
                samples=samples,
                start_time=start,
                end_time=start + duration,
                duration=duration,
                source_label="Open-Meteo Forecast API",
            )

        with tempfile.TemporaryDirectory() as tmp:
            for sample_count in (5, 80):
                output = Path(tmp) / f"render-{sample_count}.png"
                render_report(
                    build_render_report(sample_count), output, use_real_map=False
                )
                self.assertTrue(output.exists())
                self.assertGreater(output.stat().st_size, 0)

    def test_render_report_handles_missing_optional_series(self) -> None:
        from datetime import timedelta

        from trailintel.forecast.models import (
            Bounds,
            ForecastReport,
            RouteData,
            RoutePoint,
            SampleForecast,
            SamplePoint,
        )
        from trailintel.forecast.render import render_report

        start = datetime(2026, 3, 28, 8, 0, tzinfo=UTC)
        duration = timedelta(hours=2)
        route = RouteData(
            points=[
                RoutePoint(lat=47.37, lon=8.54, elevation_m=410.0, distance_m=0.0),
                RoutePoint(lat=47.39, lon=8.57, elevation_m=510.0, distance_m=14_000.0),
            ],
            total_distance_m=14_000.0,
            total_ascent_m=100.0,
            bounds=Bounds(min_lat=47.37, max_lat=47.39, min_lon=8.54, max_lon=8.57),
        )
        samples = [
            SampleForecast(
                sample=SamplePoint(
                    index=index,
                    fraction=index / 2,
                    elapsed=timedelta(hours=index),
                    timestamp=start + timedelta(hours=index),
                    lat=47.37 + index * 0.01,
                    lon=8.54 + index * 0.01,
                    elevation_m=410.0 + index * 30,
                    distance_m=index * 7_000.0,
                ),
                temperature_c=7.0 + index,
                apparent_temperature_c=None,
                wind_kph=12.0 + index,
                wind_gust_kph=None,
                wind_direction_deg=270.0 + index * 5,
                cloud_cover_pct=35.0 + index * 10,
                precipitation_mm=0.1 * index,
                precipitation_probability=None,
            )
            for index in range(3)
        ]
        report = ForecastReport(
            provider_id="met-no",
            route=route,
            samples=samples,
            start_time=start,
            end_time=start + duration,
            duration=duration,
            source_label="MET Norway Locationforecast API (yr.no data)",
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "missing-optional.png"
            render_report(report, output, use_real_map=False)
            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
