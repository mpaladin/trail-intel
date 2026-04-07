"""TrailIntel forecast generation package."""

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from trailintel.forecast.bundle import ForecastBundleResult
    from trailintel.forecast.engine import ForecastSummary

__all__ = [
    "__version__",
    "ForecastBundleResult",
    "ForecastSummary",
    "build_report",
    "generate_forecast_assets",
    "summarize_report",
]


def __getattr__(name: str) -> Any:
    if name in {"ForecastBundleResult", "generate_forecast_assets"}:
        from trailintel.forecast.bundle import (
            ForecastBundleResult,
            generate_forecast_assets,
        )

        exports = {
            "ForecastBundleResult": ForecastBundleResult,
            "generate_forecast_assets": generate_forecast_assets,
        }
        return exports[name]
    if name in {"ForecastSummary", "build_report", "summarize_report"}:
        from trailintel.forecast.engine import (
            ForecastSummary,
            build_report,
            summarize_report,
        )

        exports = {
            "ForecastSummary": ForecastSummary,
            "build_report": build_report,
            "summarize_report": summarize_report,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
