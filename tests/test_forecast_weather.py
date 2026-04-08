from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx

from trailintel.forecast.models import SamplePoint
from trailintel.forecast.weather import MetNoClient, OpenMeteoClient, WeatherAPIClient


def make_sample(index: int) -> SamplePoint:
    start = datetime(2026, 3, 28, 8, 0, tzinfo=UTC)
    return SamplePoint(
        index=index,
        fraction=index / 60 if index else 0.0,
        elapsed=timedelta(minutes=index * 10),
        timestamp=start + timedelta(minutes=index * 10),
        lat=47.37 + index * 0.001,
        lon=8.54 + index * 0.001,
        elevation_m=400.0 + index,
        distance_m=index * 1000.0,
    )


def payload_for_count(count: int) -> list[dict]:
    base = {
        "hourly": {
            "time": ["2026-03-28T08:00", "2026-03-28T09:00", "2026-03-28T10:00"],
            "temperature_2m": [10, 11, 12],
            "apparent_temperature": [8, 10, 11],
            "wind_speed_10m": [20, 21, 22],
            "wind_gusts_10m": [30, 31, 32],
            "wind_direction_10m": [270, 280, 290],
            "cloud_cover": [35, 45, 55],
            "precipitation": [0.1, 0.2, 0.3],
            "precipitation_probability": [30, 40, 50],
        }
    }
    return [base for _ in range(count)]


class ForecastWeatherTests(unittest.TestCase):
    def test_fetch_hourly_batches_requests(self) -> None:
        requests_seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests_seen.append(request)
            params = parse_qs(urlparse(str(request.url)).query)
            count = len(params["latitude"][0].split(","))
            return httpx.Response(200, json=payload_for_count(count))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        service = OpenMeteoClient(http_client=client, chunk_size=50)
        samples = [make_sample(index) for index in range(55)]

        forecasts = service.fetch_hourly(samples)

        self.assertEqual(len(requests_seen), 2)
        self.assertEqual(len(forecasts), 55)
        self.assertEqual(requests_seen[0].url.params["timezone"], "GMT")

    def test_fetch_hourly_parses_single_payload(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload_for_count(1)[0])

        client = httpx.Client(transport=httpx.MockTransport(handler))
        service = OpenMeteoClient(http_client=client, chunk_size=50)

        forecasts = service.fetch_hourly([make_sample(0)])

        self.assertEqual(len(forecasts), 1)
        self.assertEqual(forecasts[0].temperature_c[0], 10.0)
        self.assertEqual(forecasts[0].apparent_temperature_c[1], 10.0)
        self.assertEqual(forecasts[0].wind_gust_kph[2], 32.0)
        self.assertEqual(forecasts[0].wind_direction_deg[1], 280.0)
        self.assertEqual(forecasts[0].cloud_cover_pct[0], 35.0)

    def test_met_no_client_parses_hourly_and_period_derived_values(self) -> None:
        payload = {
            "properties": {
                "timeseries": [
                    {
                        "time": "2026-03-28T08:00:00Z",
                        "data": {
                            "instant": {
                                "details": {
                                    "air_temperature": 8.0,
                                    "relative_humidity": 70.0,
                                    "wind_speed": 4.0,
                                    "wind_from_direction": 270.0,
                                    "cloud_area_fraction": 35.0,
                                }
                            },
                            "next_1_hours": {
                                "details": {
                                    "precipitation_amount": 0.2,
                                    "probability_of_precipitation": 30.0,
                                }
                            },
                        },
                    },
                    {
                        "time": "2026-03-28T09:00:00Z",
                        "data": {
                            "instant": {
                                "details": {
                                    "air_temperature": 9.0,
                                    "relative_humidity": 72.0,
                                    "wind_speed": 5.0,
                                    "wind_from_direction": 280.0,
                                    "cloud_area_fraction": 45.0,
                                }
                            },
                            "next_6_hours": {
                                "details": {
                                    "precipitation_amount": 1.8,
                                    "probability_of_precipitation": 60.0,
                                }
                            },
                        },
                    },
                ]
            }
        }

        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        service = MetNoClient(http_client=client)

        forecasts = service.fetch_hourly([make_sample(0)])

        self.assertEqual(len(forecasts), 1)
        self.assertAlmostEqual(forecasts[0].wind_kph[0], 14.4)
        self.assertIsNone(forecasts[0].wind_gust_kph[0])
        self.assertAlmostEqual(forecasts[0].precipitation_mm[1], 0.3)
        self.assertEqual(forecasts[0].precipitation_probability[1], 60.0)
        self.assertIn("next_6_hours", " ".join(forecasts[0].notes))

    def test_weatherapi_client_parses_hourly_payload(self) -> None:
        payload = {
            "forecast": {
                "forecastday": [
                    {
                        "hour": [
                            {
                                "time_epoch": 1774684800,
                                "temp_c": 8.0,
                                "feelslike_c": 6.0,
                                "wind_kph": 12.0,
                                "gust_kph": 18.0,
                                "wind_degree": 260.0,
                                "cloud": 40.0,
                                "precip_mm": 0.0,
                                "chance_of_rain": 10.0,
                            },
                            {
                                "time_epoch": 1774688400,
                                "temp_c": 9.0,
                                "feelslike_c": 7.0,
                                "wind_kph": 13.0,
                                "gust_kph": 19.0,
                                "wind_degree": 270.0,
                                "cloud": 45.0,
                                "precip_mm": 0.2,
                                "chance_of_rain": 20.0,
                            },
                        ]
                    }
                ]
            }
        }

        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        service = WeatherAPIClient(
            http_client=client,
            api_key="test-key",
            request_interval_seconds=0.0,
        )

        forecasts = service.fetch_hourly([make_sample(0)])

        self.assertEqual(len(forecasts), 1)
        self.assertEqual(forecasts[0].temperature_c[1], 9.0)
        self.assertEqual(forecasts[0].apparent_temperature_c[0], 6.0)
        self.assertEqual(forecasts[0].wind_gust_kph[1], 19.0)
        self.assertEqual(forecasts[0].precipitation_probability[1], 20.0)


if __name__ == "__main__":
    unittest.main()
