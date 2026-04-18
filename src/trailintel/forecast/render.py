from __future__ import annotations

import math
import textwrap
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Sequence

import matplotlib

matplotlib.use("Agg")

import numpy as np
from matplotlib import dates as mdates
from matplotlib import patheffects
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator

from trailintel.forecast.engine import ForecastSummary, summarize_report
from trailintel.forecast.map_tiles import fetch_basemap, lonlat_series_to_web_mercator
from trailintel.forecast.models import ForecastReport, SampleForecast
from trailintel.forecast.weather import provider_definition

BACKGROUND = "#000000"
PANEL = "#050505"
PANEL_EDGE = "#333333"
GRID = "#626262"
TEXT = "#f4f1ea"
MUTED = "#d0ccc4"
HEADER = "#ff5a24"
ROUTE = "#ff6124"
START = "#6fe04d"
FINISH = "#ff3c2f"
ELEVATION = "#d1912b"
TERRAIN_CMAP = LinearSegmentedColormap.from_list(
    "epic_dark_terrain",
    ["#10202b", "#193544", "#21414b", "#28564f", "#1e3f45"],
)
PROVIDER_COLORS = {
    "open-meteo": "#0e5b85",
    "met-no": "#1c7c63",
    "weatherapi": "#c45d1c",
}
FALLBACK_PROVIDER_COLORS = (
    "#7d4ec6",
    "#ac3b61",
    "#1f7a8c",
    "#5f6c2f",
)
ROUTE_PAD = 0.08
MAP_ARROW_LENGTH = 0.045


@dataclass(frozen=True)
class RenderedProvider:
    report: ForecastReport
    label: str
    source_label: str
    color: str
    summary: ForecastSummary
    is_primary: bool


def render_report(
    report: ForecastReport,
    output_path: str | Path,
    *,
    title: str | None = None,
    comparison_reports: Sequence[ForecastReport] = (),
    comparison_warnings: Sequence[str] = (),
    use_real_map: bool = True,
) -> Path:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "figure.facecolor": BACKGROUND,
            "savefig.facecolor": BACKGROUND,
            "axes.facecolor": PANEL,
            "axes.edgecolor": PANEL_EDGE,
            "axes.labelcolor": MUTED,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": TEXT,
        }
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    providers = build_rendered_providers(report, comparison_reports)
    primary = providers[0]

    fig = plt.figure(figsize=(12, 18), dpi=150, facecolor=BACKGROUND)
    grid = fig.add_gridspec(
        6,
        2,
        height_ratios=[0.14, 0.15, 0.15, 0.15, 0.205, 0.205],
        width_ratios=[1.04, 1.0],
        hspace=0.10,
        wspace=0.08,
        left=0.03,
        right=0.97,
        top=0.978,
        bottom=0.03,
    )

    header_ax = fig.add_subplot(grid[0, :])
    temperature_panel = fig.add_subplot(grid[1, 0])
    feels_like_panel = fig.add_subplot(grid[1, 1])
    precipitation_panel = fig.add_subplot(grid[2, 0])
    cloud_cover_panel = fig.add_subplot(grid[2, 1])
    wind_panel = fig.add_subplot(grid[3, 0])
    elevation_panel = fig.add_subplot(grid[3, 1])
    map_panel = fig.add_subplot(grid[4:, 0])
    summary_panel = fig.add_subplot(grid[4:, 1])

    render_header(header_ax, primary.report, providers, title=title)
    render_temperature_panel(temperature_panel, providers)
    render_feels_like_panel(feels_like_panel, providers)
    render_precipitation_panel(precipitation_panel, providers)
    render_cloud_cover_panel(cloud_cover_panel, providers)
    render_wind_panel(wind_panel, providers)
    render_elevation_panel(elevation_panel, primary.report)
    render_wind_direction_panel(
        map_panel,
        primary.report,
        provider_label=primary.label,
        use_real_map=use_real_map,
    )
    render_provider_summary_panel(summary_panel, providers, comparison_warnings)

    fig.text(
        0.5,
        0.012,
        build_footer_text(providers),
        ha="center",
        va="bottom",
        fontsize=9,
        color=MUTED,
    )

    fig.savefig(output)
    plt.close(fig)
    return output


def build_rendered_providers(
    report: ForecastReport,
    comparison_reports: Sequence[ForecastReport],
) -> list[RenderedProvider]:
    rendered: list[RenderedProvider] = []
    for index, current in enumerate([report, *comparison_reports]):
        definition = provider_definition(current.provider_id)
        rendered.append(
            RenderedProvider(
                report=current,
                label=definition.label,
                source_label=current.source_label,
                color=provider_color(current.provider_id),
                summary=summarize_report(current),
                is_primary=index == 0,
            )
        )
    return rendered


def provider_color(provider_id: str) -> str:
    if provider_id in PROVIDER_COLORS:
        return PROVIDER_COLORS[provider_id]
    fallback_index = sum(ord(char) for char in provider_id) % len(
        FALLBACK_PROVIDER_COLORS
    )
    return FALLBACK_PROVIDER_COLORS[fallback_index]


def render_header(
    axis,
    report: ForecastReport,
    providers: Sequence[RenderedProvider],
    *,
    title: str | None = None,
) -> None:
    axis.set_facecolor(HEADER)
    axis.set_xticks([])
    axis.set_yticks([])
    for spine in axis.spines.values():
        spine.set_visible(False)

    title_lines = wrap_header_title(title)
    axis.text(
        0.5,
        0.72,
        "\n".join(title_lines),
        transform=axis.transAxes,
        ha="center",
        va="center",
        fontsize=23 if len(title_lines) == 1 else 20,
        fontweight="bold",
        color="white",
        linespacing=0.95,
    )
    axis.text(
        0.5,
        0.42,
        format_header_datetime(report.start_time),
        transform=axis.transAxes,
        ha="center",
        va="center",
        fontsize=12,
        color="white",
    )
    descriptor = (
        "Primary provider highlighted across multi-source comparison charts"
        if len(providers) > 1
        else "Single-provider forecast dashboard"
    )
    axis.text(
        0.5,
        0.24,
        descriptor,
        transform=axis.transAxes,
        ha="center",
        va="center",
        fontsize=10,
        color="white",
        alpha=0.92,
    )
    add_global_provider_legend(axis, providers)


def add_global_provider_legend(axis, providers: Sequence[RenderedProvider]) -> None:
    handles = [
        Line2D(
            [0],
            [0],
            color=item.color,
            linewidth=3.0 if item.is_primary else 2.0,
            label=(f"{item.label} (primary)" if item.is_primary else item.label),
        )
        for item in providers
    ]
    legend = axis.legend(
        handles=handles,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=min(3, len(handles)),
        frameon=False,
        fontsize=10,
        handlelength=2.2,
        handletextpad=0.6,
        columnspacing=1.4,
    )
    for text in legend.get_texts():
        text.set_color("white")


def render_temperature_panel(axis, providers: Sequence[RenderedProvider]) -> None:
    render_provider_overlay_panel(
        axis,
        providers,
        title="Temperature (°C)",
        extractor=lambda sample: sample.temperature_c,
        limits_fn=lambda series: padded_limits(flatten_series(series), pad=1.5),
        primary_fill=True,
    )


def render_feels_like_panel(axis, providers: Sequence[RenderedProvider]) -> None:
    render_provider_overlay_panel(
        axis,
        providers,
        title="Feels Like (°C)",
        extractor=lambda sample: sample.apparent_temperature_c,
        limits_fn=lambda series: padded_limits(flatten_series(series), pad=1.5),
        empty_message="No apparent-temperature comparison data is available.",
    )


def render_precipitation_panel(axis, providers: Sequence[RenderedProvider]) -> None:
    render_provider_overlay_panel(
        axis,
        providers,
        title="Precipitation (mm)",
        extractor=lambda sample: sample.precipitation_mm,
        limits_fn=lambda series: (0.0, precipitation_axis_ceiling(flatten_series(series))),
        primary_fill=True,
    )


def render_cloud_cover_panel(axis, providers: Sequence[RenderedProvider]) -> None:
    render_provider_overlay_panel(
        axis,
        providers,
        title="Cloud Cover (%)",
        extractor=lambda sample: sample.cloud_cover_pct,
        limits_fn=lambda _series: (0.0, 100.0),
        fixed_yticks=[0, 25, 50, 75, 100],
    )


def render_wind_panel(axis, providers: Sequence[RenderedProvider]) -> None:
    render_provider_overlay_panel(
        axis,
        providers,
        title="Wind (km/h)",
        extractor=lambda sample: sample.wind_kph,
        limits_fn=lambda series: wind_limits(flatten_series(series)),
        note_text="Sustained wind only; gusts appear in the provider summary.",
        primary_fill=True,
    )


def render_provider_overlay_panel(
    axis,
    providers: Sequence[RenderedProvider],
    *,
    title: str,
    extractor: Callable[[SampleForecast], float | None],
    limits_fn: Callable[[list[np.ndarray]], tuple[float, float]],
    note_text: str | None = None,
    primary_fill: bool = False,
    fixed_yticks: Sequence[float] | None = None,
    empty_message: str = "No comparison data is available for this metric.",
) -> None:
    prepare_panel(axis, title)
    plot_ax = axis.inset_axes([0.08, 0.14, 0.86, 0.52])

    primary_timestamps = display_timestamps(providers[0].report)
    series_entries: list[tuple[RenderedProvider, list[datetime], np.ndarray]] = []
    unavailable_labels: list[str] = []
    for provider in providers:
        values = optional_series([extractor(sample) for sample in provider.report.samples])
        if not series_has_values(values):
            unavailable_labels.append(provider.label)
            continue
        series_entries.append((provider, display_timestamps(provider.report), values))

    if not series_entries:
        render_empty_panel_message(axis, empty_message)
        return

    baseline, ceiling = limits_fn([values for _, _, values in series_entries])
    if primary_fill:
        primary_series = next(
            values
            for provider, _, values in series_entries
            if provider.is_primary
        )
        plot_ax.fill_between(
            primary_timestamps,
            baseline,
            primary_series,
            color=providers[0].color,
            alpha=0.14,
            zorder=1,
        )

    for provider, timestamps, values in sorted(
        series_entries,
        key=lambda item: item[0].is_primary,
    ):
        plot_ax.plot(
            timestamps,
            values,
            color=provider.color,
            linewidth=2.5 if provider.is_primary else 1.45,
            alpha=1.0 if provider.is_primary else 0.92,
            zorder=4 if provider.is_primary else 3,
        )

    style_chart_axis(plot_ax, primary_timestamps)
    plot_ax.set_ylim(baseline, ceiling)
    if fixed_yticks is not None:
        plot_ax.set_yticks(list(fixed_yticks))

    notes: list[str] = []
    if note_text:
        notes.append(note_text)
    if unavailable_labels:
        notes.append(f"Unavailable from: {', '.join(unavailable_labels)}")
    if notes:
        add_panel_note(axis, "  ".join(notes))


def render_elevation_panel(axis, report: ForecastReport) -> None:
    prepare_panel(axis, "Elevation (m)", subtitle="Shared route profile across providers")
    plot_ax = axis.inset_axes([0.08, 0.28, 0.84, 0.40])

    timestamps = display_timestamps(report)
    elevation = np.array(
        [
            sample.sample.elevation_m
            if sample.sample.elevation_m is not None
            else np.nan
            for sample in report.samples
        ],
        dtype=float,
    )
    if np.all(np.isnan(elevation)):
        elevation = np.zeros(len(report.samples), dtype=float)

    baseline = math.floor(float(np.nanmin(elevation)) / 100) * 100
    ceiling = math.ceil(float(np.nanmax(elevation)) / 100) * 100
    if ceiling <= baseline:
        ceiling = baseline + 100

    plot_ax.fill_between(
        timestamps, baseline, elevation, color="#7e551a", alpha=0.48, zorder=1
    )
    plot_ax.plot(timestamps, elevation, color=ELEVATION, linewidth=1.25, zorder=3)

    style_chart_axis(plot_ax, timestamps)
    plot_ax.set_ylim(baseline, ceiling)

    avg_speed = average_speed_kph(report)
    axis.text(
        0.05,
        0.22,
        f"Elevation gain: {report.route.total_ascent_m:,.0f} m",
        ha="left",
        va="center",
        fontsize=10,
        color=MUTED,
    )
    axis.text(
        0.05,
        0.10,
        (
            f"Distance: {report.route.total_distance_m / 1000:.1f} km"
            f"   Average speed: {avg_speed:.1f} km/h"
        ),
        ha="left",
        va="center",
        fontsize=10,
        color=MUTED,
    )


def render_wind_direction_panel(
    axis,
    report: ForecastReport,
    *,
    provider_label: str,
    use_real_map: bool,
) -> None:
    prepare_panel(axis, "Wind Direction", subtitle=f"{provider_label} only")
    map_ax = axis.inset_axes([0.04, 0.08, 0.92, 0.80])
    map_ax.set_facecolor("#0b1318")
    map_ax.set_xticks([])
    map_ax.set_yticks([])
    for spine in map_ax.spines.values():
        spine.set_visible(False)

    route_lons = [point.lon for point in report.route.points]
    route_lats = [point.lat for point in report.route.points]
    basemap = fetch_basemap(route_lons, route_lats) if use_real_map else None

    if basemap is not None:
        map_ax.imshow(
            np.asarray(basemap.image),
            extent=basemap.extent,
            origin="upper",
            zorder=0,
        )
        x_route, y_route = lonlat_series_to_web_mercator(route_lons, route_lats)
        sample_lons = [sample.sample.lon for sample in report.samples]
        sample_lats = [sample.sample.lat for sample in report.samples]
        x_samples, y_samples = lonlat_series_to_web_mercator(sample_lons, sample_lats)
        route_extent = basemap.extent
        arrow_length = 0.035 * max(
            route_extent[1] - route_extent[0],
            route_extent[3] - route_extent[2],
        )
    else:
        route_lons_array = np.array(route_lons, dtype=float)
        route_lats_array = np.array(route_lats, dtype=float)
        x_route, y_route = project_coordinates(route_lons_array, route_lats_array)
        draw_terrain_background(map_ax, report)
        sample_lons = np.array(
            [sample.sample.lon for sample in report.samples], dtype=float
        )
        sample_lats = np.array(
            [sample.sample.lat for sample in report.samples], dtype=float
        )
        x_samples, y_samples = project_coordinates(
            sample_lons,
            sample_lats,
            reference_lons=route_lons_array,
            reference_lats=route_lats_array,
        )
        route_extent = (0.0, 1.0, 0.0, 1.0)
        arrow_length = MAP_ARROW_LENGTH

    route_shadow = patheffects.withStroke(linewidth=4.8, foreground="white", alpha=0.18)
    route_line = map_ax.plot(
        x_route,
        y_route,
        color=ROUTE,
        linewidth=2.2,
        solid_capstyle="round",
        solid_joinstyle="round",
        zorder=4,
    )[0]
    route_line.set_path_effects([route_shadow])
    add_route_wind_arrows(
        map_ax, x_samples, y_samples, report, arrow_length=arrow_length
    )

    map_ax.scatter(
        x_route[0],
        y_route[0],
        s=72,
        color=START,
        edgecolors=BACKGROUND,
        linewidths=1.0,
        zorder=6,
    )
    map_ax.scatter(
        x_route[-1],
        y_route[-1],
        s=72,
        color=FINISH,
        edgecolors=BACKGROUND,
        linewidths=1.0,
        zorder=6,
    )
    map_ax.set_xlim(route_extent[0], route_extent[1])
    map_ax.set_ylim(route_extent[2], route_extent[3])
    map_ax.set_aspect("equal", adjustable="box")
    if basemap is not None:
        map_ax.text(
            0.99,
            0.01,
            basemap.attribution,
            transform=map_ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=6,
            color="#d7d7d7",
            bbox={
                "facecolor": (0, 0, 0, 0.45),
                "edgecolor": "none",
                "pad": 1.2,
            },
            zorder=10,
        )
    add_panel_note(
        axis,
        "Route and wind-direction arrows use the primary provider only.",
        y=0.03,
    )


def render_provider_summary_panel(
    axis,
    providers: Sequence[RenderedProvider],
    comparison_warnings: Sequence[str],
) -> None:
    prepare_panel(
        axis,
        "Provider Summary",
        subtitle="Primary provider emphasized in charts; gust maxima appear here",
    )

    row_top = 0.82
    row_height = 0.20 if len(providers) <= 2 else 0.17
    divider_x = (0.05, 0.95)

    for index, provider in enumerate(providers):
        y = row_top - index * row_height
        axis.plot(
            [0.05, 0.12],
            [y, y],
            transform=axis.transAxes,
            color=provider.color,
            linewidth=4.2 if provider.is_primary else 3.0,
            solid_capstyle="round",
        )
        axis.text(
            0.15,
            y + 0.028,
            f"{provider.label}{' (primary)' if provider.is_primary else ''}",
            transform=axis.transAxes,
            ha="left",
            va="center",
            fontsize=11,
            fontweight="bold",
            color=TEXT,
        )
        axis.text(
            0.15,
            y - 0.035,
            textwrap.fill(build_provider_summary_line(provider), width=44),
            transform=axis.transAxes,
            ha="left",
            va="center",
            fontsize=9.2,
            color=MUTED,
            linespacing=1.35,
        )
        if index < len(providers) - 1:
            axis.plot(
                list(divider_x),
                [y - 0.09, y - 0.09],
                transform=axis.transAxes,
                color=PANEL_EDGE,
                linewidth=0.8,
                alpha=0.8,
            )

    if comparison_warnings:
        warning_y = row_top - len(providers) * row_height - 0.02
        axis.text(
            0.05,
            warning_y,
            "Skipped comparison sources",
            transform=axis.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            fontweight="bold",
            color="#ffba70",
        )
        warning_lines = []
        for warning in comparison_warnings:
            compact = warning.replace("Skipped comparison provider ", "")
            warning_lines.append(textwrap.fill(compact, width=46))
        axis.text(
            0.05,
            warning_y - 0.06,
            "\n\n".join(warning_lines),
            transform=axis.transAxes,
            ha="left",
            va="top",
            fontsize=8.8,
            color="#ffcf9a",
            linespacing=1.35,
        )


def build_provider_summary_line(provider: RenderedProvider) -> str:
    summary = provider.summary
    gust_max = max_optional_gust(provider.report)
    bits = [
        (
            f"Temp {summary.temperature_min_c:.1f}-{summary.temperature_max_c:.1f} C"
        ),
        f"Rain {summary.precipitation_total_mm:.1f} mm",
        f"Wind {summary.wind_max_kph:.0f} km/h",
    ]
    if gust_max is not None:
        bits.append(f"Gust {gust_max:.0f} km/h")
    return "   •   ".join(bits)


def max_optional_gust(report: ForecastReport) -> float | None:
    gusts = [sample.wind_gust_kph for sample in report.samples if sample.wind_gust_kph is not None]
    if not gusts:
        return None
    return max(gusts)


def render_empty_panel_message(axis, text: str) -> None:
    axis.text(
        0.5,
        0.50,
        textwrap.fill(text, width=30),
        transform=axis.transAxes,
        ha="center",
        va="center",
        fontsize=10,
        color=MUTED,
        linespacing=1.35,
    )


def prepare_panel(axis, title: str, *, subtitle: str | None = None) -> None:
    axis.set_facecolor(PANEL)
    axis.set_xticks([])
    axis.set_yticks([])
    for spine in axis.spines.values():
        spine.set_color(PANEL_EDGE)
        spine.set_linewidth(1.0)
    axis.text(
        0.03,
        0.94,
        title,
        ha="left",
        va="top",
        fontsize=16,
        color=TEXT,
    )
    if subtitle:
        axis.text(
            0.03,
            0.86,
            subtitle,
            ha="left",
            va="top",
            fontsize=8.8,
            color=MUTED,
        )


def add_panel_note(axis, text: str, *, y: float = 0.07) -> None:
    axis.text(
        0.03,
        y,
        textwrap.fill(text, width=56),
        transform=axis.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.6,
        color=MUTED,
        linespacing=1.3,
    )


def style_chart_axis(axis, timestamps: list[datetime]) -> None:
    axis.set_facecolor(PANEL)
    axis.spines["top"].set_color(PANEL_EDGE)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_color(PANEL_EDGE)
    axis.spines["bottom"].set_color(PANEL_EDGE)
    axis.grid(True, which="major", color=GRID, alpha=0.52, linewidth=0.6)
    axis.tick_params(
        axis="x",
        top=True,
        labeltop=True,
        bottom=False,
        labelbottom=False,
        colors=MUTED,
        labelsize=8,
        length=0,
        pad=4,
    )
    axis.tick_params(
        axis="y",
        left=True,
        labelleft=True,
        right=False,
        labelright=False,
        colors=MUTED,
        labelsize=8,
        length=0,
        pad=4,
    )
    axis.yaxis.set_major_locator(MaxNLocator(nbins=4))
    ticks = choose_time_ticks(timestamps, max_ticks=5)
    axis.set_xticks(ticks)
    axis.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=timestamps[0].tzinfo)
    )
    axis.set_xlim(timestamps[0], timestamps[-1])


def display_timestamps(report: ForecastReport) -> list[datetime]:
    timezone = report.start_time.tzinfo
    return [sample.sample.timestamp.astimezone(timezone) for sample in report.samples]


def optional_series(values: list[float | None]) -> np.ndarray:
    return np.array(
        [np.nan if value is None else float(value) for value in values],
        dtype=float,
    )


def series_has_values(values: np.ndarray) -> bool:
    return not np.all(np.isnan(values))


def flatten_series(series_list: Sequence[np.ndarray]) -> np.ndarray:
    arrays = [series[~np.isnan(series)] for series in series_list if series.size]
    if not arrays:
        return np.array([0.0], dtype=float)
    return np.concatenate(arrays)


def choose_time_ticks(timestamps: list[datetime], max_ticks: int) -> list[datetime]:
    if len(timestamps) <= max_ticks:
        return timestamps
    raw_indices = np.linspace(0, len(timestamps) - 1, num=max_ticks)
    indices = sorted({int(round(value)) for value in raw_indices})
    return [timestamps[index] for index in indices]


def padded_limits(values: np.ndarray, *, pad: float) -> tuple[float, float]:
    lower = math.floor(float(np.nanmin(values) - pad))
    upper = math.ceil(float(np.nanmax(values) + pad))
    if upper <= lower:
        upper = lower + 2
    return lower, upper


def precipitation_axis_ceiling(precipitation_mm: np.ndarray) -> float:
    if precipitation_mm.size == 0:
        return 1.0
    peak = float(np.nanmax(precipitation_mm))
    if peak <= 0:
        return 1.0
    if peak <= 5:
        return max(1.0, math.ceil(peak * 2) / 2)
    return math.ceil(peak)


def wind_limits(winds: np.ndarray) -> tuple[float, float]:
    baseline = max(0.0, math.floor(float(np.nanmin(winds)) - 1))
    ceiling = math.ceil(float(np.nanmax(winds)) + 1)
    if ceiling <= baseline:
        ceiling = baseline + 2
    return baseline, ceiling


def average_speed_kph(report: ForecastReport) -> float:
    hours = report.duration.total_seconds() / 3600
    if hours <= 0:
        return 0.0
    return (report.route.total_distance_m / 1000) / hours


def build_footer_text(providers: Sequence[RenderedProvider]) -> str:
    labels = "  •  ".join(item.label for item in providers)
    return f"Created with TrailIntel Forecast  •  Providers: {labels}"


def format_header_datetime(value: datetime) -> str:
    zone = value.tzname() or "UTC"
    return f"{value:%b %d, %Y at %H:%M} {zone}"


def wrap_header_title(title: str | None) -> list[str]:
    normalized = " ".join((title or "Route Forecast").split())
    lines = textwrap.wrap(
        normalized,
        width=26,
        break_long_words=False,
        break_on_hyphens=False,
    )
    if not lines:
        return ["Route Forecast"]
    if len(lines) <= 2:
        return lines
    return [
        lines[0],
        textwrap.shorten(" ".join(lines[1:]), width=26, placeholder="..."),
    ]


def draw_terrain_background(axis, report: ForecastReport) -> None:
    seed = int(
        abs(report.route.bounds.min_lat * 1000)
        + abs(report.route.bounds.max_lat * 1000)
        + abs(report.route.bounds.min_lon * 1000)
        + abs(report.route.bounds.max_lon * 1000)
    )
    xs = np.linspace(0, 1, 420)
    ys = np.linspace(0, 1, 420)
    xx, yy = np.meshgrid(xs, ys)
    phase_a = (seed % 97) / 97
    phase_b = (seed % 43) / 43
    phase_c = (seed % 29) / 29
    field = (
        0.44 * np.sin((xx * 3.5 + phase_a) * 2 * np.pi)
        + 0.31 * np.cos((yy * 4.4 + phase_b) * 2 * np.pi)
        + 0.19 * np.sin(((xx + yy) * 5.6 + phase_c) * 2 * np.pi)
        + 0.12 * np.cos(((xx - yy) * 7.2 + 0.17) * 2 * np.pi)
    )
    field = (field - field.min()) / (field.max() - field.min())
    axis.imshow(
        field,
        extent=(0, 1, 0, 1),
        origin="lower",
        cmap=TERRAIN_CMAP,
        alpha=0.95,
        zorder=0,
    )
    axis.contour(
        xx,
        yy,
        field,
        levels=np.linspace(0.18, 0.92, 12),
        colors="#96a7b3",
        linewidths=0.45,
        alpha=0.13,
        zorder=1,
    )


def project_coordinates(
    lons: np.ndarray,
    lats: np.ndarray,
    *,
    reference_lons: np.ndarray | None = None,
    reference_lats: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    ref_lons = reference_lons if reference_lons is not None else lons
    ref_lats = reference_lats if reference_lats is not None else lats

    mean_lat = math.radians(float(np.mean(ref_lats)))
    x = (lons - np.mean(ref_lons)) * math.cos(mean_lat)
    y = lats - np.mean(ref_lats)
    ref_x = (ref_lons - np.mean(ref_lons)) * math.cos(mean_lat)
    ref_y = ref_lats - np.mean(ref_lats)

    ref_x_min, ref_x_max = float(ref_x.min()), float(ref_x.max())
    ref_y_min, ref_y_max = float(ref_y.min()), float(ref_y.max())
    x_span = max(ref_x_max - ref_x_min, 1e-12)
    y_span = max(ref_y_max - ref_y_min, 1e-12)

    usable = 1 - 2 * ROUTE_PAD
    if x_span >= y_span:
        x_scale = usable
        y_scale = usable * (y_span / x_span)
        x_offset = ROUTE_PAD
        y_offset = (1 - y_scale) / 2
    else:
        y_scale = usable
        x_scale = usable * (x_span / y_span)
        x_offset = (1 - x_scale) / 2
        y_offset = ROUTE_PAD

    x_norm = x_offset + ((x - ref_x_min) / x_span) * x_scale
    y_norm = y_offset + ((y - ref_y_min) / y_span) * y_scale
    return x_norm, y_norm


def add_route_wind_arrows(
    axis,
    x_values,
    y_values,
    report: ForecastReport,
    *,
    arrow_length: float,
) -> None:
    for index in route_arrow_indices(len(report.samples)):
        sample = report.samples[index]
        dx, dy = direction_to_route_vector(sample.wind_direction_deg)
        x = float(x_values[index])
        y = float(y_values[index])
        half = arrow_length / 2
        start = (x - dx * half, y - dy * half)
        end = (x + dx * half, y + dy * half)
        axis.annotate(
            "",
            xy=end,
            xytext=start,
            arrowprops={
                "arrowstyle": "-|>",
                "color": "#12171d",
                "linewidth": 1.9,
                "mutation_scale": 11,
                "shrinkA": 0,
                "shrinkB": 0,
                "alpha": 0.9,
            },
            zorder=6,
        )


def route_arrow_indices(sample_count: int) -> list[int]:
    target = max(6, min(22, round(sample_count * 0.45)))
    raw = np.linspace(0, sample_count - 1, num=target)
    return sorted({int(round(value)) for value in raw})


def direction_to_route_vector(direction_from_deg: float) -> tuple[float, float]:
    direction_to_deg = (direction_from_deg + 180.0) % 360.0
    radians = math.radians(direction_to_deg)
    return math.sin(radians), math.cos(radians)
