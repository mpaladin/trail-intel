from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Callable, Iterable, Iterator

import httpx

from trailintel.forecast import __version__
from trailintel.forecast.errors import InputValidationError, WeatherAPIError
from trailintel.forecast.models import HourlyForecast, SamplePoint

OPEN_METEO_PROVIDER = "open-meteo"
MET_NO_PROVIDER = "met-no"
WEATHERAPI_PROVIDER = "weatherapi"

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
MET_NO_FORECAST_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
WEATHERAPI_FORECAST_URL = "https://api.weatherapi.com/v1/forecast.json"

MET_NO_USER_AGENT = (
    f"trailintel-forecast/{__version__} (+https://github.com/mpaladin/trail-intel)"
)

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


@dataclass(frozen=True)
class ForecastProviderDefinition:
    provider_id: str
    label: str
    source_label: str
    horizon_days: int


PROVIDER_DEFINITIONS: dict[str, ForecastProviderDefinition] = {
    OPEN_METEO_PROVIDER: ForecastProviderDefinition(
        provider_id=OPEN_METEO_PROVIDER,
        label="Open-Meteo",
        source_label="Open-Meteo Forecast API",
        horizon_days=16,
    ),
    MET_NO_PROVIDER: ForecastProviderDefinition(
        provider_id=MET_NO_PROVIDER,
        label="MET Norway (yr.no)",
        source_label="MET Norway Locationforecast API (yr.no data)",
        horizon_days=9,
    ),
    WEATHERAPI_PROVIDER: ForecastProviderDefinition(
        provider_id=WEATHERAPI_PROVIDER,
        label="WeatherAPI.com",
        source_label="WeatherAPI.com Forecast API",
        horizon_days=3,
    ),
}


class BaseForecastClient:
    provider_id = ""
    label = ""
    source_label = ""
    horizon_days = 0

    def __init__(self, http_client: httpx.Client | None = None) -> None:
        self._owns_client = http_client is None
        self.http_client = http_client or httpx.Client(timeout=30.0)

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()


class OpenMeteoClient(BaseForecastClient):
    provider_id = OPEN_METEO_PROVIDER
    label = PROVIDER_DEFINITIONS[OPEN_METEO_PROVIDER].label
    source_label = PROVIDER_DEFINITIONS[OPEN_METEO_PROVIDER].source_label
    horizon_days = PROVIDER_DEFINITIONS[OPEN_METEO_PROVIDER].horizon_days

    def __init__(
        self,
        http_client: httpx.Client | None = None,
        *,
        base_url: str = OPEN_METEO_FORECAST_URL,
        chunk_size: int = 50,
    ) -> None:
        super().__init__(http_client=http_client)
        self.base_url = base_url
        self.chunk_size = chunk_size

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
                    "nan" if sample.elevation_m is None else f"{sample.elevation_m:.1f}"
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
            message = response_error_message(exc.response)
            if message:
                raise WeatherAPIError(
                    f"Weather API request failed with HTTP {exc.response.status_code}: {message}"
                ) from exc
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

        times = [parse_datetime(value) for value in raw_times]
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
            precipitation_probability=[
                float(value) for value in precipitation_probability
            ],
        )


class MetNoClient(BaseForecastClient):
    provider_id = MET_NO_PROVIDER
    label = PROVIDER_DEFINITIONS[MET_NO_PROVIDER].label
    source_label = PROVIDER_DEFINITIONS[MET_NO_PROVIDER].source_label
    horizon_days = PROVIDER_DEFINITIONS[MET_NO_PROVIDER].horizon_days

    def __init__(
        self,
        http_client: httpx.Client | None = None,
        *,
        base_url: str = MET_NO_FORECAST_URL,
    ) -> None:
        super().__init__(http_client=http_client)
        self.base_url = base_url
        self.http_client.headers.setdefault("User-Agent", MET_NO_USER_AGENT)

    def fetch_hourly(self, samples: list[SamplePoint]) -> list[HourlyForecast]:
        return [self._parse_payload(self._request(sample)) for sample in samples]

    def _request(self, sample: SamplePoint) -> dict:
        params = {
            "lat": f"{sample.lat:.6f}",
            "lon": f"{sample.lon:.6f}",
        }
        if sample.elevation_m is not None and math.isfinite(sample.elevation_m):
            params["altitude"] = f"{sample.elevation_m:.0f}"

        try:
            response = self.http_client.get(self.base_url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            message = response_error_message(exc.response)
            if message:
                raise WeatherAPIError(
                    f"Weather API request failed with HTTP {exc.response.status_code}: {message}"
                ) from exc
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
        properties = payload.get("properties")
        if not isinstance(properties, dict):
            raise WeatherAPIError("Weather API response is missing properties data.")

        timeseries = properties.get("timeseries")
        if not isinstance(timeseries, list) or not timeseries:
            raise WeatherAPIError("Weather API response is missing time series data.")

        times: list[datetime] = []
        temperature_c: list[float] = []
        apparent_temperature_c: list[float | None] = []
        wind_kph: list[float] = []
        wind_gust_kph: list[float | None] = []
        wind_direction_deg: list[float] = []
        cloud_cover_pct: list[float] = []
        precipitation_mm: list[float] = []
        precipitation_probability: list[float | None] = []
        notes: list[str] = []

        for entry in timeseries:
            if not isinstance(entry, dict):
                continue
            timestamp = entry.get("time")
            data = entry.get("data")
            if not isinstance(timestamp, str) or not isinstance(data, dict):
                continue

            instant = data.get("instant")
            instant_details = (
                instant.get("details")
                if isinstance(instant, dict)
                and isinstance(instant.get("details"), dict)
                else None
            )
            if instant_details is None:
                raise WeatherAPIError(
                    "Weather API response is missing instant details."
                )

            try:
                air_temperature = float(instant_details["air_temperature"])
                relative_humidity = float(instant_details["relative_humidity"])
                wind_speed_mps = float(instant_details["wind_speed"])
                wind_from_direction = float(instant_details["wind_from_direction"])
                cloud_area_fraction = float(instant_details["cloud_area_fraction"])
            except KeyError as exc:
                raise WeatherAPIError(
                    f"Weather API response is missing field {exc.args[0]}."
                ) from exc

            gust_value = instant_details.get("wind_speed_of_gust")
            gust_kph = float(gust_value) * 3.6 if gust_value is not None else None
            if gust_kph is None:
                notes.append("Wind gusts unavailable for some MET Norway timestamps.")

            precipitation_value, probability_value, note = met_no_precipitation(data)
            if note:
                notes.append(note)
            if probability_value is None:
                notes.append(
                    "Precipitation probability unavailable for some MET Norway timestamps."
                )

            times.append(parse_datetime(timestamp))
            temperature_c.append(air_temperature)
            apparent_temperature_c.append(
                apparent_temperature(
                    air_temperature,
                    relative_humidity_pct=relative_humidity,
                    wind_speed_mps=wind_speed_mps,
                )
            )
            wind_kph.append(wind_speed_mps * 3.6)
            wind_gust_kph.append(gust_kph)
            wind_direction_deg.append(wind_from_direction % 360)
            cloud_cover_pct.append(cloud_area_fraction)
            precipitation_mm.append(precipitation_value)
            precipitation_probability.append(probability_value)

        lengths = {
            len(times),
            len(temperature_c),
            len(apparent_temperature_c),
            len(wind_kph),
            len(wind_gust_kph),
            len(wind_direction_deg),
            len(cloud_cover_pct),
            len(precipitation_mm),
            len(precipitation_probability),
        }
        if len(lengths) != 1 or not times:
            raise WeatherAPIError("Weather API hourly arrays are different lengths.")

        return HourlyForecast(
            times=times,
            temperature_c=temperature_c,
            apparent_temperature_c=apparent_temperature_c,
            wind_kph=wind_kph,
            wind_gust_kph=wind_gust_kph,
            wind_direction_deg=wind_direction_deg,
            cloud_cover_pct=cloud_cover_pct,
            precipitation_mm=precipitation_mm,
            precipitation_probability=precipitation_probability,
            notes=tuple(dict.fromkeys(notes)),
        )


class WeatherAPIClient(BaseForecastClient):
    provider_id = WEATHERAPI_PROVIDER
    label = PROVIDER_DEFINITIONS[WEATHERAPI_PROVIDER].label
    source_label = PROVIDER_DEFINITIONS[WEATHERAPI_PROVIDER].source_label
    horizon_days = PROVIDER_DEFINITIONS[WEATHERAPI_PROVIDER].horizon_days

    def __init__(
        self,
        http_client: httpx.Client | None = None,
        *,
        api_key: str,
        base_url: str = WEATHERAPI_FORECAST_URL,
        request_interval_seconds: float = 1.05,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        super().__init__(http_client=http_client)
        self.base_url = base_url
        self.api_key = api_key.strip()
        if not self.api_key:
            raise InputValidationError(
                "WEATHERAPI_KEY is required for provider weatherapi."
            )
        self.request_interval_seconds = request_interval_seconds
        self.sleep = sleep
        self._last_request_at: float | None = None

    def fetch_hourly(self, samples: list[SamplePoint]) -> list[HourlyForecast]:
        forecasts: list[HourlyForecast] = []
        for sample in samples:
            self._throttle()
            forecasts.append(self._parse_payload(self._request(sample)))
        return forecasts

    def _throttle(self) -> None:
        if self.request_interval_seconds <= 0:
            return
        if self._last_request_at is None:
            self._last_request_at = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.request_interval_seconds - elapsed
        if remaining > 0:
            self.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _request(self, sample: SamplePoint) -> dict:
        params = {
            "key": self.api_key,
            "q": f"{sample.lat:.6f},{sample.lon:.6f}",
            "days": str(self.horizon_days),
            "aqi": "no",
            "alerts": "no",
        }

        try:
            response = self.http_client.get(self.base_url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            message = response_error_message(exc.response)
            if message:
                raise WeatherAPIError(
                    f"Weather API request failed with HTTP {exc.response.status_code}: {message}"
                ) from exc
            raise WeatherAPIError(
                f"Weather API request failed with HTTP {exc.response.status_code}."
            ) from exc
        except httpx.HTTPError as exc:
            raise WeatherAPIError(f"Weather API request failed: {exc}") from exc

        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
            error = payload["error"]
            message = error.get("message", "unknown error")
            raise WeatherAPIError(f"Weather API error: {message}")
        return payload

    def _parse_payload(self, payload: dict) -> HourlyForecast:
        forecast = payload.get("forecast")
        if not isinstance(forecast, dict):
            raise WeatherAPIError("Weather API response is missing forecast data.")

        forecast_days = forecast.get("forecastday")
        if not isinstance(forecast_days, list) or not forecast_days:
            raise WeatherAPIError(
                "Weather API response is missing hourly forecast data."
            )

        times: list[datetime] = []
        temperature_c: list[float] = []
        apparent_temperature_c: list[float | None] = []
        wind_kph: list[float] = []
        wind_gust_kph: list[float | None] = []
        wind_direction_deg: list[float] = []
        cloud_cover_pct: list[float] = []
        precipitation_mm: list[float] = []
        precipitation_probability: list[float | None] = []

        for day in forecast_days:
            if not isinstance(day, dict):
                continue
            hours = day.get("hour")
            if not isinstance(hours, list):
                continue
            for hour in hours:
                if not isinstance(hour, dict):
                    continue
                try:
                    times.append(
                        datetime.fromtimestamp(
                            float(hour["time_epoch"]),
                            tz=UTC,
                        )
                    )
                    temperature_c.append(float(hour["temp_c"]))
                    apparent_temperature_c.append(float(hour["feelslike_c"]))
                    wind_kph.append(float(hour["wind_kph"]))
                    gust_value = hour.get("gust_kph")
                    wind_gust_kph.append(
                        float(gust_value) if gust_value is not None else None
                    )
                    wind_direction_deg.append(float(hour["wind_degree"]) % 360)
                    cloud_cover_pct.append(float(hour["cloud"]))
                    precipitation_mm.append(float(hour["precip_mm"]))
                    chance_of_rain = hour.get("chance_of_rain")
                    precipitation_probability.append(
                        float(chance_of_rain) if chance_of_rain is not None else None
                    )
                except KeyError as exc:
                    raise WeatherAPIError(
                        f"Weather API response is missing field {exc.args[0]}."
                    ) from exc

        lengths = {
            len(times),
            len(temperature_c),
            len(apparent_temperature_c),
            len(wind_kph),
            len(wind_gust_kph),
            len(wind_direction_deg),
            len(cloud_cover_pct),
            len(precipitation_mm),
            len(precipitation_probability),
        }
        if len(lengths) != 1 or not times:
            raise WeatherAPIError("Weather API hourly arrays are different lengths.")

        return HourlyForecast(
            times=times,
            temperature_c=temperature_c,
            apparent_temperature_c=apparent_temperature_c,
            wind_kph=wind_kph,
            wind_gust_kph=wind_gust_kph,
            wind_direction_deg=wind_direction_deg,
            cloud_cover_pct=cloud_cover_pct,
            precipitation_mm=precipitation_mm,
            precipitation_probability=precipitation_probability,
        )


def available_provider_ids() -> tuple[str, ...]:
    return tuple(PROVIDER_DEFINITIONS)


def provider_definition(provider_id: str) -> ForecastProviderDefinition:
    normalized = provider_id.strip().lower()
    definition = PROVIDER_DEFINITIONS.get(normalized)
    if definition is None:
        choices = ", ".join(sorted(PROVIDER_DEFINITIONS))
        raise InputValidationError(
            f"Unknown forecast provider '{provider_id}'. Choose from: {choices}."
        )
    return definition


def create_forecast_client(
    provider_id: str,
    *,
    http_client: httpx.Client | None = None,
    weatherapi_key: str | None = None,
) -> BaseForecastClient:
    normalized = provider_definition(provider_id).provider_id
    if normalized == OPEN_METEO_PROVIDER:
        return OpenMeteoClient(http_client=http_client)
    if normalized == MET_NO_PROVIDER:
        return MetNoClient(http_client=http_client)
    if normalized == WEATHERAPI_PROVIDER:
        api_key = (weatherapi_key or os.getenv("WEATHERAPI_KEY", "")).strip()
        if not api_key:
            raise InputValidationError(
                "WEATHERAPI_KEY is required for provider weatherapi."
            )
        return WeatherAPIClient(http_client=http_client, api_key=api_key)
    raise InputValidationError(f"Unsupported forecast provider '{provider_id}'.")


def met_no_precipitation(
    data: dict,
) -> tuple[float, float | None, str | None]:
    periods = (
        ("next_1_hours", 1, None),
        (
            "next_6_hours",
            6,
            "Precipitation values are hourly averages derived from MET Norway next_6_hours periods.",
        ),
        (
            "next_12_hours",
            12,
            "Precipitation values are hourly averages derived from MET Norway next_12_hours periods.",
        ),
    )

    for key, divisor, note in periods:
        period = data.get(key)
        if not isinstance(period, dict):
            continue
        details = period.get("details")
        if not isinstance(details, dict):
            continue
        precipitation_amount = details.get("precipitation_amount")
        if precipitation_amount is None:
            continue
        probability = details.get("probability_of_precipitation")
        return (
            float(precipitation_amount) / divisor,
            float(probability) if probability is not None else None,
            note,
        )

    return 0.0, None, None


def apparent_temperature(
    air_temperature_c: float,
    *,
    relative_humidity_pct: float,
    wind_speed_mps: float,
) -> float:
    vapor_pressure = (
        relative_humidity_pct
        / 100.0
        * 6.105
        * math.exp((17.27 * air_temperature_c) / (237.7 + air_temperature_c))
    )
    return air_temperature_c + 0.33 * vapor_pressure - 0.70 * wind_speed_mps - 4.0


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def response_error_message(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        return None

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()
        reason = payload.get("reason")
        if isinstance(reason, str) and reason.strip():
            return reason.strip()
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()

    return None


def chunked(
    values: Iterable[SamplePoint], chunk_size: int
) -> Iterator[list[SamplePoint]]:
    iterator = iter(values)
    while chunk := list(islice(iterator, chunk_size)):
        yield chunk
