from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Iterable, Iterator

import httpx

from trailintel.forecast.errors import WeatherAPIError
from trailintel.forecast.models import HourlyForecast, SamplePoint

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_FIELDS = (
    "temperature_2m",
    "apparent_temperature",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "cloud_cover",
    "precipitation",
    "precipitation_probability",
)


class OpenMeteoClient:
    def __init__(
        self,
        http_client: httpx.Client | None = None,
        *,
        base_url: str = FORECAST_URL,
        chunk_size: int = 50,
    ) -> None:
        self.base_url = base_url
        self.chunk_size = chunk_size
        self._owns_client = http_client is None
        self.http_client = http_client or httpx.Client(timeout=30.0)

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()

    def fetch_hourly(self, samples: list[SamplePoint]) -> list[HourlyForecast]:
        if not samples:
            return []

        start_utc = min(sample.timestamp.astimezone(UTC) for sample in samples)
        end_utc = max(sample.timestamp.astimezone(UTC) for sample in samples)
        end_date = (end_utc + timedelta(hours=1)).date().isoformat()
        start_date = start_utc.date().isoformat()

        all_forecasts: list[HourlyForecast] = []
        for batch in chunked(samples, self.chunk_size):
            params = {
                "latitude": ",".join(f"{sample.lat:.6f}" for sample in batch),
                "longitude": ",".join(f"{sample.lon:.6f}" for sample in batch),
                "elevation": ",".join(
                    "nan"
                    if sample.elevation_m is None
                    else f"{sample.elevation_m:.1f}"
                    for sample in batch
                ),
                "hourly": ",".join(HOURLY_FIELDS),
                "timezone": "GMT",
                "wind_speed_unit": "kmh",
                "start_date": start_date,
                "end_date": end_date,
            }
            response = self._request(params)
            payloads = response if isinstance(response, list) else [response]
            if len(payloads) != len(batch):
                raise WeatherAPIError(
                    "Open-Meteo returned an unexpected number of forecast payloads."
                )
            all_forecasts.extend(self._parse_payload(payload) for payload in payloads)

        return all_forecasts

    def _request(self, params: dict[str, str]) -> dict | list[dict]:
        try:
            response = self.http_client.get(self.base_url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise WeatherAPIError(
                f"Weather API request failed with HTTP {exc.response.status_code}."
            ) from exc
        except httpx.HTTPError as exc:
            raise WeatherAPIError(f"Weather API request failed: {exc}") from exc

        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            reason = payload.get("reason", "unknown error")
            raise WeatherAPIError(f"Weather API error: {reason}")
        return payload

    def _parse_payload(self, payload: dict) -> HourlyForecast:
        hourly = payload.get("hourly")
        if not isinstance(hourly, dict):
            raise WeatherAPIError("Weather API response is missing hourly data.")

        try:
            raw_times = hourly["time"]
            temperature = hourly["temperature_2m"]
            apparent_temperature = hourly["apparent_temperature"]
            wind = hourly["wind_speed_10m"]
            wind_gust = hourly["wind_gusts_10m"]
            wind_direction = hourly["wind_direction_10m"]
            cloud_cover = hourly["cloud_cover"]
            precipitation = hourly["precipitation"]
            precipitation_probability = hourly["precipitation_probability"]
        except KeyError as exc:
            raise WeatherAPIError(
                f"Weather API response is missing field {exc.args[0]}."
            ) from exc

        times = [
            datetime.fromisoformat(value).replace(tzinfo=UTC) for value in raw_times
        ]
        lengths = {
            len(times),
            len(temperature),
            len(apparent_temperature),
            len(wind),
            len(wind_gust),
            len(wind_direction),
            len(cloud_cover),
            len(precipitation),
            len(precipitation_probability),
        }
        if len(lengths) != 1:
            raise WeatherAPIError("Weather API hourly arrays are different lengths.")

        return HourlyForecast(
            times=times,
            temperature_c=[float(value) for value in temperature],
            apparent_temperature_c=[float(value) for value in apparent_temperature],
            wind_kph=[float(value) for value in wind],
            wind_gust_kph=[float(value) for value in wind_gust],
            wind_direction_deg=[float(value) % 360 for value in wind_direction],
            cloud_cover_pct=[float(value) for value in cloud_cover],
            precipitation_mm=[float(value) for value in precipitation],
            precipitation_probability=[float(value) for value in precipitation_probability],
        )


def chunked(values: Iterable[SamplePoint], chunk_size: int) -> Iterator[list[SamplePoint]]:
    iterator = iter(values)
    while chunk := list(islice(iterator, chunk_size)):
        yield chunk
