from __future__ import annotations

from pathlib import Path

import typer

from trailintel.forecast.bundle import generate_forecast_assets
from trailintel.forecast.errors import EpicForecastError
from trailintel.forecast.weather import available_provider_ids

app = typer.Typer(
    help="Generate route weather forecast charts from GPX rides.",
    no_args_is_help=True,
)


@app.callback()
def cli() -> None:
    """TrailIntel forecast CLI."""


@app.command("forecast")
def forecast(
    gpx_path: Path = typer.Argument(
        ..., exists=True, readable=True, help="Input GPX file."
    ),
    start: str = typer.Option(..., "--start", help="Ride start as ISO8601."),
    duration: str = typer.Option(..., "--duration", help="Ride duration as HH:MM."),
    output: Path = typer.Option(..., "--output", help="PNG output path."),
    timezone: str | None = typer.Option(
        None,
        "--timezone",
        help="IANA timezone for naive start timestamps.",
    ),
    sample_minutes: int = typer.Option(
        10,
        "--sample-minutes",
        min=1,
        help="Target minutes between route samples before clamping to 15-120 points.",
    ),
    site_dir: Path | None = typer.Option(
        None,
        "--site-dir",
        help="Optional static site bundle output directory.",
    ),
    provider: str = typer.Option(
        "open-meteo",
        "--provider",
        help=(
            "Primary forecast provider. Choices: "
            + ", ".join(available_provider_ids())
            + "."
        ),
    ),
    compare_provider: list[str] = typer.Option(
        [],
        "--compare-provider",
        help="Additional provider(s) to compare in the HTML bundle.",
    ),
) -> None:
    try:
        result = generate_forecast_assets(
            gpx_path=gpx_path,
            start=start,
            duration=duration,
            timezone_name=timezone,
            sample_minutes=sample_minutes,
            output_path=output,
            site_dir=site_dir,
            provider=provider,
            compare_providers=compare_provider,
        )
        summary = result.summary
    except EpicForecastError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    chance_text = (
        f"{summary.wettest_probability_pct:.0f}% chance"
        if summary.wettest_probability_pct is not None
        else "chance unavailable"
    )

    typer.echo(
        "\n".join(
            line
            for line in [
                f"Temperature: {summary.temperature_min_c:.1f}C to {summary.temperature_max_c:.1f}C",
                f"Max wind: {summary.wind_max_kph:.1f} km/h",
                f"Estimated precipitation: {summary.precipitation_total_mm:.1f} mm",
                (
                    "Wettest segment: "
                    f"{summary.wettest_time.isoformat()} "
                    f"({summary.wettest_precipitation_mm:.1f} mm, {chance_text})"
                ),
                f"Saved image: {result.image_path}",
                (
                    f"Saved site bundle: {result.site_dir}"
                    if result.site_dir is not None
                    else ""
                ),
                *result.comparison_warnings,
            ]
            if line
        )
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
