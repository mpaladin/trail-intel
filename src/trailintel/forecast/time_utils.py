from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from trailintel.forecast.errors import InputValidationError

FORECAST_HORIZON_DAYS = 16


def parse_duration(value: str) -> timedelta:
    parts = value.split(":")
    if len(parts) not in {2, 3}:
        raise InputValidationError("Duration must be in HH:MM or HH:MM:SS format.")

    try:
        numbers = [int(part) for part in parts]
    except ValueError as exc:
        raise InputValidationError("Duration contains non-numeric parts.") from exc

    hours, minutes = numbers[0], numbers[1]
    seconds = numbers[2] if len(numbers) == 3 else 0

    if hours < 0 or minutes < 0 or seconds < 0:
        raise InputValidationError("Duration cannot be negative.")
    if minutes >= 60 or seconds >= 60:
        raise InputValidationError("Duration minutes and seconds must be below 60.")

    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    if duration <= timedelta(0):
        raise InputValidationError("Duration must be greater than zero.")
    return duration


def resolve_timezone_name(timezone_name: str | None) -> str:
    if timezone_name:
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise InputValidationError(
                f"Unknown timezone '{timezone_name}'. Use an IANA timezone name."
            ) from exc
        return timezone_name

    local_tz = datetime.now().astimezone().tzinfo
    if local_tz is None or getattr(local_tz, "key", None) is None:
        raise InputValidationError(
            "Could not determine the local timezone. Pass --timezone explicitly."
        )
    return local_tz.key


def parse_start_time(value: str, timezone_name: str | None = None) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise InputValidationError(
            "Start time must be a valid ISO8601 datetime."
        ) from exc

    if parsed.tzinfo is not None:
        return parsed

    tz_name = resolve_timezone_name(timezone_name)
    return parsed.replace(tzinfo=ZoneInfo(tz_name))


def validate_forecast_window(
    start_time: datetime,
    duration: timedelta,
    *,
    now: datetime | None = None,
    horizon_days: int = FORECAST_HORIZON_DAYS,
) -> None:
    if start_time.tzinfo is None:
        raise InputValidationError("Start time must be timezone-aware.")

    now_utc = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    start_utc = start_time.astimezone(UTC)
    end_utc = (start_time + duration).astimezone(UTC)
    horizon_utc = now_utc + timedelta(days=horizon_days)

    if start_utc < now_utc:
        raise InputValidationError("Start time must be in the future.")
    if end_utc > horizon_utc:
        raise InputValidationError(
            f"Ride end exceeds the forecast horizon of {horizon_days} days."
        )
