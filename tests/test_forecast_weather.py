from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx

from trailintel.forecast.models import SamplePoint
from trailintel.forecast.weather import OpenMeteoClient


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


if __name__ == "__main__":
    unittest.main()
