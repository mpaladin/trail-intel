"""TrailIntel forecast generation package."""

from trailintel.forecast.bundle import ForecastBundleResult, generate_forecast_assets
from trailintel.forecast.engine import ForecastSummary, build_report, summarize_report

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "ForecastBundleResult",
    "ForecastSummary",
    "build_report",
    "generate_forecast_assets",
    "summarize_report",
]
