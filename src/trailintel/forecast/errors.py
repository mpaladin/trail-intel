class EpicForecastError(Exception):
    """Base error for user-facing failures."""


class InputValidationError(EpicForecastError):
    """Raised when user input cannot be used safely."""


class GPXParseError(EpicForecastError):
    """Raised when a GPX file is missing or invalid."""


class WeatherAPIError(EpicForecastError):
    """Raised when the weather API cannot satisfy a request."""
