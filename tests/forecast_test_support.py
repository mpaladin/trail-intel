from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from trailintel.forecast.render import render_report as original_render_report

FIXTURE = Path(__file__).parent / "fixtures" / "sample_route.gpx"


def make_open_meteo_payload(
    *,
    times: tuple[str, ...] = (
        "2026-03-28T08:00",
        "2026-03-28T09:00",
        "2026-03-28T10:00",
    ),
    temperature_2m: tuple[float, ...] = (8, 10, 12),
    apparent_temperature: tuple[float, ...] = (7, 9, 11),
    wind_speed_10m: tuple[float, ...] = (12, 15, 18),
    wind_gusts_10m: tuple[float, ...] = (18, 22, 25),
    wind_direction_10m: tuple[float, ...] = (270, 280, 290),
    cloud_cover: tuple[float, ...] = (25, 35, 45),
    precipitation: tuple[float, ...] = (0.0, 0.2, 0.4),
    precipitation_probability: tuple[float, ...] = (10, 35, 60),
) -> dict[str, dict[str, list[Any]]]:
    return {
        "hourly": {
            "time": list(times),
            "temperature_2m": list(temperature_2m),
            "apparent_temperature": list(apparent_temperature),
            "wind_speed_10m": list(wind_speed_10m),
            "wind_gusts_10m": list(wind_gusts_10m),
            "wind_direction_10m": list(wind_direction_10m),
            "cloud_cover": list(cloud_cover),
            "precipitation": list(precipitation),
            "precipitation_probability": list(precipitation_probability),
        }
    }


def open_meteo_batch_response(
    request: httpx.Request,
    *,
    payload: dict[str, object] | None = None,
) -> httpx.Response:
    if request.url.host != "api.open-meteo.com":
        raise AssertionError(f"Unexpected forecast host: {request.url.host}")
    latitudes = request.url.params["latitude"].split(",")
    return httpx.Response(
        200, json=[payload or make_open_meteo_payload()] * len(latitudes)
    )


def weatherapi_error_response(
    *,
    status_code: int = 401,
    message: str = "API key is invalid.",
    code: int = 2006,
) -> httpx.Response:
    return httpx.Response(
        status_code,
        json={"error": {"code": code, "message": message}},
    )


def render_without_real_map(report, output_path, *, title=None):
    return original_render_report(
        report,
        output_path,
        title=title,
        use_real_map=False,
    )
