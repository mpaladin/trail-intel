from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from trailintel.forecast.errors import InputValidationError
from trailintel.forecast.time_utils import (
    parse_duration,
    parse_start_time,
    validate_forecast_window,
)


class ForecastTimeUtilsTests(unittest.TestCase):
    def test_parse_duration_accepts_hours_and_minutes(self) -> None:
        self.assertEqual(parse_duration("03:45"), timedelta(hours=3, minutes=45))

    def test_parse_duration_rejects_invalid_minutes(self) -> None:
        with self.assertRaises(InputValidationError):
            parse_duration("01:75")

    def test_parse_start_time_applies_timezone_to_naive_values(self) -> None:
        value = parse_start_time("2026-03-28T08:00:00", "Europe/Zurich")
        self.assertIsNotNone(value.tzinfo)
        self.assertEqual(value.utcoffset(), timedelta(hours=1))

    def test_validate_forecast_window_rejects_past_times(self) -> None:
        now = datetime(2026, 3, 27, 12, 0, tzinfo=UTC)
        with self.assertRaises(InputValidationError):
            validate_forecast_window(
                datetime(2026, 3, 27, 11, 0, tzinfo=UTC),
                timedelta(hours=2),
                now=now,
            )

    def test_validate_forecast_window_rejects_out_of_range_ride_end(self) -> None:
        now = datetime(2026, 3, 27, 12, 0, tzinfo=UTC)
        with self.assertRaises(InputValidationError):
            validate_forecast_window(
                datetime(2026, 4, 12, 12, 0, tzinfo=UTC),
                timedelta(hours=13),
                now=now,
            )


if __name__ == "__main__":
    unittest.main()
