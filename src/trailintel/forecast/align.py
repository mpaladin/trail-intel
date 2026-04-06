from __future__ import annotations

from bisect import bisect_right
from datetime import UTC

from trailintel.forecast.errors import WeatherAPIError
from trailintel.forecast.models import HourlyForecast, SampleForecast, SamplePoint


def align_forecasts(
    samples: list[SamplePoint],
    forecasts: list[HourlyForecast],
) -> list[SampleForecast]:
    if len(samples) != len(forecasts):
        raise WeatherAPIError("Sample count does not match forecast count.")

    aligned: list[SampleForecast] = []
    for sample, forecast in zip(samples, forecasts, strict=True):
        timestamp = sample.timestamp.astimezone(UTC)
        hour_index = containing_hour_index(forecast.times, timestamp)
        next_index = min(hour_index + 1, len(forecast.times) - 1)

        lower_time = forecast.times[hour_index]
        upper_time = forecast.times[next_index]
        if upper_time == lower_time:
            ratio = 0.0
        else:
            ratio = (timestamp - lower_time) / (upper_time - lower_time)

        aligned.append(
            SampleForecast(
                sample=sample,
                temperature_c=lerp(
                    forecast.temperature_c[hour_index],
                    forecast.temperature_c[next_index],
                    ratio,
                ),
                apparent_temperature_c=lerp(
                    forecast.apparent_temperature_c[hour_index],
                    forecast.apparent_temperature_c[next_index],
                    ratio,
                ),
                wind_kph=lerp(
                    forecast.wind_kph[hour_index],
                    forecast.wind_kph[next_index],
                    ratio,
                ),
                wind_gust_kph=lerp(
                    forecast.wind_gust_kph[hour_index],
                    forecast.wind_gust_kph[next_index],
                    ratio,
                ),
                wind_direction_deg=circular_lerp(
                    forecast.wind_direction_deg[hour_index],
                    forecast.wind_direction_deg[next_index],
                    ratio,
                ),
                cloud_cover_pct=lerp(
                    forecast.cloud_cover_pct[hour_index],
                    forecast.cloud_cover_pct[next_index],
                    ratio,
                ),
                precipitation_mm=forecast.precipitation_mm[hour_index],
                precipitation_probability=forecast.precipitation_probability[hour_index],
            )
        )

    return aligned


def containing_hour_index(times: list, timestamp) -> int:
    if not times:
        raise WeatherAPIError("Forecast has no time samples.")
    if timestamp < times[0]:
        raise WeatherAPIError("Forecast data does not cover the ride start.")

    index = bisect_right(times, timestamp) - 1
    if index < 0:
        raise WeatherAPIError("Forecast data does not cover the ride start.")
    if index >= len(times):
        raise WeatherAPIError("Forecast data does not cover the ride end.")
    return index


def lerp(start: float, end: float, ratio: float) -> float:
    return start + (end - start) * float(ratio)


def circular_lerp(start: float, end: float, ratio: float) -> float:
    delta = ((end - start + 540.0) % 360.0) - 180.0
    return (start + delta * float(ratio)) % 360.0
