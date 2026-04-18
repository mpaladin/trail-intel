from __future__ import annotations

import json
import math
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
from PIL import Image

from tests.forecast_test_support import (
    FIXTURE,
    make_open_meteo_payload,
    open_meteo_batch_response,
    render_without_real_map,
)
from trailintel.forecast.bundle import generate_forecast_assets
from trailintel.forecast.engine import summarize_report
from trailintel.forecast.models import (
    Bounds,
    ForecastReport,
    RouteData,
    RoutePoint,
    SampleForecast,
    SamplePoint,
)
from trailintel.forecast.site import (
    FORECAST_CHART_DATA_ID,
    UPLOT_CSS_INTEGRITY,
    UPLOT_CSS_URL,
    UPLOT_JS_INTEGRITY,
    UPLOT_JS_URL,
    build_forecast_snapshot,
    render_forecast_html,
)
from trailintel.forecast.weather import provider_definition


def build_render_fixture(
    sample_count: int,
    *,
    provider_id: str = "open-meteo",
    temperature_offset: float = 0.0,
    wind_offset: float = 0.0,
    precip_scale: float = 1.0,
    include_apparent: bool = True,
    include_gust: bool = True,
) -> ForecastReport:
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
    definition = provider_definition(provider_id)
    samples: list[SampleForecast] = []
    for index in range(sample_count):
        fraction = 0.0 if sample_count <= 1 else index / (sample_count - 1)
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
        precipitation = (
            0.0 if index < 4 else min(1.6, 0.2 * (index - 3))
        ) * precip_scale
        samples.append(
            SampleForecast(
                sample=sample,
                temperature_c=6.0 + 10.0 * fraction + temperature_offset,
                apparent_temperature_c=(
                    5.0 + 11.5 * fraction + temperature_offset
                    if include_apparent
                    else None
                ),
                wind_kph=12.0 + 18.0 * fraction + wind_offset,
                wind_gust_kph=(
                    16.0 + 23.0 * fraction + wind_offset
                    if include_gust
                    else None
                ),
                wind_direction_deg=(300.0 + 90.0 * fraction) % 360,
                cloud_cover_pct=min(
                    95.0, 25.0 + 55.0 * abs(math.sin(fraction * math.pi))
                ),
                precipitation_mm=precipitation,
                precipitation_probability=min(100.0, 8.0 + 7.0 * index),
            )
        )
    return ForecastReport(
        provider_id=definition.provider_id,
        route=route,
        samples=samples,
        start_time=start,
        end_time=start + duration,
        duration=duration,
        source_label=definition.source_label,
    )


class ForecastBundleTests(unittest.TestCase):
    def test_generate_forecast_assets_writes_site_bundle(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return open_meteo_batch_response(request)

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"
            captured_render: dict[str, object] = {}

            def capture_render(
                report,
                output_path,
                *,
                title=None,
                comparison_reports=(),
                comparison_warnings=(),
            ):
                captured_render["title"] = title
                captured_render["comparison_report_ids"] = [
                    item.provider_id for item in comparison_reports
                ]
                captured_render["comparison_warnings"] = list(comparison_warnings)
                return render_without_real_map(
                    report,
                    output_path,
                    title=title,
                    comparison_reports=comparison_reports,
                    comparison_warnings=comparison_warnings,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                capture_render,
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
            self.assertEqual(captured_render.get("comparison_report_ids"), [])
            self.assertEqual(captured_render.get("comparison_warnings"), [])
            self.assertIsNotNone(result.snapshot)

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Sample Loop Forecast", html)
            self.assertIn("Route Forecast", html)
            self.assertIn("Forecast Charts", html)
            self.assertIn("Forecast Overview", html)
            self.assertNotIn("<h2>Key Moments</h2>", html)
            self.assertNotIn("<h2>Route Timeline</h2>", html)
            self.assertNotIn("route timeline below", html)
            self.assertIn("Temperature", html)
            self.assertIn("Feels Like", html)
            self.assertIn("Precipitation", html)
            self.assertIn("Cloud Cover", html)
            self.assertIn("Wind", html)
            self.assertIn("Elevation", html)
            self.assertIn('href="forecast.png"', html)
            self.assertIn('href="route.gpx"', html)
            self.assertIn('href="snapshot.json"', html)
            self.assertIn(UPLOT_CSS_URL, html)
            self.assertIn(UPLOT_JS_URL, html)
            self.assertIn(UPLOT_CSS_INTEGRITY, html)
            self.assertIn(UPLOT_JS_INTEGRITY, html)
            self.assertIn(f'id="{FORECAST_CHART_DATA_ID}"', html)
            self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr));", html)
            self.assertIn("space: 120", html)
            self.assertIn("size: 78", html)
            self.assertIn("Wind (km/h)", html)
            self.assertIn("function axisValueLabel(metricId, value)", html)
            self.assertIn("Interactive charts need JavaScript enabled.", html)
            self.assertIn("Interactive charts could not load.", html)
            self.assertIn("Published Mar 27, 2026 at 12:30 UTC", html)
            self.assertNotIn("2026-03-27T12:30:00+00:00", html)
            self.assertIn("Mar 28, 2026 at 08:00 UTC", html)

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
            chart_data = snapshot.get("chart_data")
            self.assertIsInstance(chart_data, dict)
            self.assertEqual(chart_data["x_axis"], "time")
            self.assertEqual(len(chart_data["providers"]), 1)
            self.assertEqual(chart_data["providers"][0]["provider_id"], "open-meteo")
            self.assertTrue(chart_data["providers"][0]["is_primary"])
            self.assertEqual(
                len(chart_data["providers"][0]["samples"]),
                snapshot["sample_count"],
            )
            self.assertEqual(
                len(chart_data["route_profile"]),
                snapshot["sample_count"],
            )

    def test_generate_forecast_assets_writes_comparison_snapshot_and_html(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                return open_meteo_batch_response(request)

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
            captured_render: dict[str, object] = {}

            def capture_render(
                report,
                output_path,
                *,
                title=None,
                comparison_reports=(),
                comparison_warnings=(),
            ):
                captured_render["title"] = title
                captured_render["comparison_report_ids"] = [
                    item.provider_id for item in comparison_reports
                ]
                captured_render["comparison_warnings"] = list(comparison_warnings)
                return render_without_real_map(
                    report,
                    output_path,
                    title=title,
                    comparison_reports=comparison_reports,
                    comparison_warnings=comparison_warnings,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                capture_render,
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
            self.assertEqual(captured_render.get("title"), "Sample Loop Forecast")
            self.assertEqual(captured_render.get("comparison_report_ids"), ["met-no"])
            self.assertEqual(captured_render.get("comparison_warnings"), [])

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
            chart_data = snapshot.get("chart_data")
            self.assertIsInstance(chart_data, dict)
            self.assertEqual(
                [item["provider_id"] for item in chart_data["providers"]],
                ["open-meteo", "met-no"],
            )
            self.assertTrue(chart_data["providers"][0]["is_primary"])
            self.assertIn(
                "has_apparent_temperature",
                chart_data["providers"][1]["coverage"],
            )
            self.assertEqual(
                len(chart_data["providers"][0]["samples"]),
                snapshot["sample_count"],
            )
            self.assertEqual(
                len(chart_data["providers"][1]["samples"]),
                snapshot["sample_count"],
            )

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Provider Comparison", html)
            self.assertNotIn("Provider Key Moments", html)
            self.assertNotIn("route timeline below", html)
            self.assertIn("MET Norway (yr.no)", html)
            self.assertIn("Open-Meteo", html)
            self.assertIn("Forecast Charts", html)
            self.assertIn("hour12: false", html)
            self.assertIn("size: 64", html)
            self.assertIn("Values shown in km/h.", html)

    def test_generate_forecast_assets_skips_short_horizon_comparison_provider(
        self,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return open_meteo_batch_response(
                request,
                payload=make_open_meteo_payload(
                    times=(
                        "2026-03-31T08:00",
                        "2026-03-31T09:00",
                        "2026-03-31T10:00",
                    )
                ),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "forecast.png"
            site_dir = Path(tmp) / "site"
            captured_render: dict[str, object] = {}

            def capture_render(
                report,
                output_path,
                *,
                title=None,
                comparison_reports=(),
                comparison_warnings=(),
            ):
                captured_render["comparison_report_ids"] = [
                    item.provider_id for item in comparison_reports
                ]
                captured_render["comparison_warnings"] = list(comparison_warnings)
                return render_without_real_map(
                    report,
                    output_path,
                    title=title,
                    comparison_reports=comparison_reports,
                    comparison_warnings=comparison_warnings,
                )

            with patch(
                "trailintel.forecast.bundle.render_report",
                capture_render,
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
            self.assertEqual(captured_render.get("comparison_report_ids"), [])
            self.assertEqual(
                captured_render.get("comparison_warnings"),
                [result.comparison_warnings[0]],
            )
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
            chart_data = snapshot.get("chart_data")
            self.assertIsInstance(chart_data, dict)
            self.assertEqual(len(chart_data["providers"]), 1)
            self.assertEqual(chart_data["providers"][0]["provider_id"], "open-meteo")

            html = (site_dir / "index.html").read_text(encoding="utf-8")
            self.assertIn("Forecast Charts", html)
            self.assertIn("Skipped comparison sources", html)
            self.assertIn("WeatherAPI.com", html)

    def test_render_forecast_html_handles_missing_optional_chart_series(self) -> None:
        from datetime import timedelta

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
        snapshot = build_forecast_snapshot(
            title="Missing Optional Series",
            report=report,
            summary=summarize_report(report),
            generated_at=datetime(2026, 3, 27, 12, 30, tzinfo=UTC),
        )

        chart_data = snapshot.get("chart_data")
        self.assertIsInstance(chart_data, dict)
        self.assertFalse(
            chart_data["providers"][0]["coverage"]["has_apparent_temperature"]
        )
        self.assertFalse(chart_data["providers"][0]["coverage"]["has_wind_gust"])

        html = render_forecast_html(snapshot)
        self.assertIn("Forecast Charts", html)
        self.assertIn('"has_apparent_temperature": false', html)
        self.assertIn('"has_wind_gust": false', html)
        self.assertIn(f'id="{FORECAST_CHART_DATA_ID}"', html)

    def test_render_report_handles_sparse_and_dense_routes(self) -> None:
        from trailintel.forecast.render import render_report

        with tempfile.TemporaryDirectory() as tmp:
            for sample_count in (5, 80):
                output = Path(tmp) / f"render-{sample_count}.png"
                render_report(
                    build_render_fixture(sample_count), output, use_real_map=False
                )
                self.assertTrue(output.exists())
                self.assertGreater(output.stat().st_size, 0)
                with Image.open(output) as image:
                    self.assertEqual(image.size, (1800, 2400))

    def test_render_report_handles_multi_provider_comparison(self) -> None:
        from trailintel.forecast.render import render_report

        primary = build_render_fixture(24, provider_id="open-meteo")
        comparison_reports = (
            build_render_fixture(
                24,
                provider_id="met-no",
                temperature_offset=-1.0,
                wind_offset=2.0,
                precip_scale=1.25,
                include_apparent=False,
                include_gust=False,
            ),
            build_render_fixture(
                24,
                provider_id="weatherapi",
                temperature_offset=0.8,
                wind_offset=4.0,
                precip_scale=0.7,
            ),
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "comparison.png"
            render_report(
                primary,
                output,
                comparison_reports=comparison_reports,
                comparison_warnings=(
                    "Skipped comparison provider Example: unavailable for this run.",
                ),
                use_real_map=False,
            )
            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)
            with Image.open(output) as image:
                self.assertEqual(image.size, (1800, 2400))

    def test_render_report_handles_missing_optional_series(self) -> None:
        from trailintel.forecast.render import render_report

        report = build_render_fixture(
            12,
            provider_id="met-no",
            include_apparent=False,
            include_gust=False,
        )
        comparison = build_render_fixture(
            12,
            provider_id="open-meteo",
            temperature_offset=1.2,
            wind_offset=3.0,
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "missing-optional.png"
            render_report(
                report,
                output,
                comparison_reports=(comparison,),
                use_real_map=False,
            )
            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)
            with Image.open(output) as image:
                self.assertEqual(image.size, (1800, 2400))


if __name__ == "__main__":
    unittest.main()
