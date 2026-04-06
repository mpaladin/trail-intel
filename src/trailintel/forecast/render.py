from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from matplotlib import dates as mdates
from matplotlib import patheffects
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
import numpy as np

from trailintel.forecast.map_tiles import fetch_basemap, lonlat_series_to_web_mercator
from trailintel.forecast.models import ForecastReport

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
TEMPERATURE = "#66b9ff"
FEELS_LIKE = "#ffb03a"
PROBABILITY = "#73bdf5"
CLOUD = "#a7abb3"
INTENSITY = "#ffae38"
WIND = "#8fc2df"
GUST = "#ffad2f"
ELEVATION = "#d1912b"
TERRAIN_CMAP = LinearSegmentedColormap.from_list(
    "epic_dark_terrain",
    ["#10202b", "#193544", "#21414b", "#28564f", "#1e3f45"],
)
ROUTE_PAD = 0.08
MAP_ARROW_LENGTH = 0.045


def render_report(
    report: ForecastReport,
    output_path: str | Path,
    *,
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

    fig = plt.figure(figsize=(12, 12), dpi=150, facecolor=BACKGROUND)
    grid = fig.add_gridspec(
        4,
        2,
        height_ratios=[0.14, 0.26, 0.30, 0.24],
        width_ratios=[1.08, 1.0],
        hspace=0.12,
        wspace=0.08,
        left=0.025,
        right=0.975,
        top=0.975,
        bottom=0.035,
    )

    header_ax = fig.add_subplot(grid[0, :])
    temperature_panel = fig.add_subplot(grid[1, 0])
    precipitation_panel = fig.add_subplot(grid[1, 1])
    map_panel = fig.add_subplot(grid[2:, 0])
    wind_panel = fig.add_subplot(grid[2, 1])
    elevation_panel = fig.add_subplot(grid[3, 1])

    render_header(header_ax, report)
    render_temperature_panel(temperature_panel, report)
    render_precipitation_panel(precipitation_panel, report)
    render_wind_direction_panel(map_panel, report, use_real_map=use_real_map)
    render_wind_panel(wind_panel, report)
    render_elevation_panel(elevation_panel, report)

    fig.text(
        0.5,
        0.012,
        (
            "Created with TrailIntel Forecast"
            f"  •  Source: {report.source_label}"
        ),
        ha="center",
        va="bottom",
        fontsize=9,
        color=MUTED,
    )

    fig.savefig(output)
    plt.close(fig)
    return output


def render_header(axis, report: ForecastReport) -> None:
    axis.set_facecolor(HEADER)
    axis.set_xticks([])
    axis.set_yticks([])
    for spine in axis.spines.values():
        spine.set_visible(False)
    axis.text(
        0.5,
        0.5,
        format_header_datetime(report.start_time),
        transform=axis.transAxes,
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold",
        color="white",
    )


def render_temperature_panel(axis, report: ForecastReport) -> None:
    prepare_panel(axis, "Temperature")
    plot_ax = axis.inset_axes([0.085, 0.23, 0.86, 0.58])

    timestamps = display_timestamps(report)
    temperature = np.array([sample.temperature_c for sample in report.samples], dtype=float)
    apparent = np.array(
        [sample.apparent_temperature_c for sample in report.samples],
        dtype=float,
    )

    baseline, ceiling = padded_limits(np.concatenate([temperature, apparent]), pad=1.5)
    fill_top = np.maximum(temperature, apparent)
    plot_ax.fill_between(
        timestamps,
        baseline,
        fill_top,
        color="#8a6a2f",
        alpha=0.42,
        zorder=1,
    )
    plot_ax.plot(timestamps, temperature, color=TEMPERATURE, linewidth=1.4, zorder=3)
    plot_ax.plot(timestamps, apparent, color=FEELS_LIKE, linewidth=1.1, zorder=4)

    style_chart_axis(plot_ax, timestamps)
    plot_ax.set_ylim(baseline, ceiling)
    add_line_legend(
        axis,
        [
            ("Temperature (°C)", TEMPERATURE),
            ("Feels Like (°C)", FEELS_LIKE),
        ],
    )


def render_precipitation_panel(axis, report: ForecastReport) -> None:
    prepare_panel(axis, "Precipitation and Cloud Cover")
    plot_ax = axis.inset_axes([0.08, 0.19, 0.68, 0.60])

    timestamps = display_timestamps(report)
    probability = np.array(
        [sample.precipitation_probability for sample in report.samples],
        dtype=float,
    )
    cloud_cover = np.array(
        [sample.cloud_cover_pct for sample in report.samples],
        dtype=float,
    )
    intensity = precipitation_intensity_scale(
        np.array([sample.precipitation_mm for sample in report.samples], dtype=float)
    )

    plot_ax.fill_between(timestamps, 0, cloud_cover, color=CLOUD, alpha=0.22, zorder=1)
    plot_ax.plot(timestamps, cloud_cover, color=CLOUD, linewidth=1.0, zorder=2)
    plot_ax.plot(timestamps, probability, color=PROBABILITY, linewidth=1.1, zorder=3)
    plot_ax.fill_between(timestamps, 0, probability, color=PROBABILITY, alpha=0.08, zorder=2)

    intensity_ax = plot_ax.twinx()
    intensity_ax.fill_between(
        timestamps,
        0,
        intensity,
        color=INTENSITY,
        alpha=0.24,
        zorder=1,
    )
    intensity_ax.plot(timestamps, intensity, color=INTENSITY, linewidth=1.15, zorder=4)

    style_chart_axis(plot_ax, timestamps)
    plot_ax.set_ylim(0, 100)
    plot_ax.set_yticks([0, 20, 40, 60, 80])

    intensity_ax.set_ylim(0, 100)
    intensity_ax.set_yticks([10, 30, 50, 70, 90])
    intensity_ax.set_yticklabels(
        ["Very Light", "Light", "Moderate", "Heavy", "Very Heavy"],
        color=MUTED,
        fontsize=8,
    )
    intensity_ax.tick_params(axis="y", length=0, colors=MUTED, pad=6)
    intensity_ax.spines["right"].set_color(PANEL_EDGE)
    intensity_ax.spines["top"].set_visible(False)
    intensity_ax.spines["left"].set_visible(False)
    intensity_ax.grid(False)

    add_line_legend(
        axis,
        [
            ("Probability (%)", PROBABILITY),
            ("Intensity", INTENSITY),
            ("Cloud Cover (%)", CLOUD),
        ],
    )


def render_wind_direction_panel(
    axis,
    report: ForecastReport,
    *,
    use_real_map: bool,
) -> None:
    prepare_panel(axis, "Wind Direction")
    map_ax = axis.inset_axes([0.04, 0.06, 0.92, 0.86])
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
        sample_lons = np.array([sample.sample.lon for sample in report.samples], dtype=float)
        sample_lats = np.array([sample.sample.lat for sample in report.samples], dtype=float)
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
    add_route_wind_arrows(map_ax, x_samples, y_samples, report, arrow_length=arrow_length)

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


def render_wind_panel(axis, report: ForecastReport) -> None:
    prepare_panel(axis, "Wind")
    plot_ax = axis.inset_axes([0.08, 0.23, 0.84, 0.58])

    timestamps = display_timestamps(report)
    wind = np.array([sample.wind_kph for sample in report.samples], dtype=float)
    gust = np.array([sample.wind_gust_kph for sample in report.samples], dtype=float)

    baseline = max(0.0, math.floor(min(wind.min(), gust.min()) - 1))
    ceiling = math.ceil(max(wind.max(), gust.max()) + 1)
    plot_ax.fill_between(timestamps, baseline, gust, color="#8c5d17", alpha=0.35, zorder=1)
    plot_ax.fill_between(timestamps, baseline, wind, color="#72808a", alpha=0.22, zorder=2)
    plot_ax.plot(timestamps, wind, color=WIND, linewidth=1.1, zorder=3)
    plot_ax.plot(timestamps, gust, color=GUST, linewidth=1.0, zorder=4)

    style_chart_axis(plot_ax, timestamps)
    plot_ax.set_ylim(baseline, ceiling)
    add_line_legend(
        axis,
        [
            ("Wind (km/h)", WIND),
            ("Wind Gust (km/h)", GUST),
        ],
    )


def render_elevation_panel(axis, report: ForecastReport) -> None:
    prepare_panel(axis, "Elevation")
    plot_ax = axis.inset_axes([0.08, 0.47, 0.84, 0.35])

    timestamps = display_timestamps(report)
    elevation = np.array(
        [sample.sample.elevation_m if sample.sample.elevation_m is not None else np.nan for sample in report.samples],
        dtype=float,
    )
    if np.all(np.isnan(elevation)):
        elevation = np.zeros(len(report.samples), dtype=float)

    baseline = math.floor(np.nanmin(elevation) / 100) * 100
    ceiling = math.ceil(np.nanmax(elevation) / 100) * 100
    if ceiling <= baseline:
        ceiling = baseline + 100

    plot_ax.fill_between(timestamps, baseline, elevation, color="#7e551a", alpha=0.48, zorder=1)
    plot_ax.plot(timestamps, elevation, color=ELEVATION, linewidth=1.05, zorder=2)

    style_chart_axis(plot_ax, timestamps)
    plot_ax.set_ylim(baseline, ceiling)

    avg_speed = average_speed_kph(report)
    axis.text(
        0.05,
        0.23,
        f"Elevation Gain: {report.route.total_ascent_m:,.0f} m↑",
        ha="left",
        va="center",
        fontsize=11,
        color=MUTED,
    )
    axis.text(
        0.05,
        0.10,
        (
            f"Distance: {report.route.total_distance_m / 1000:.1f} km"
            f"   Speed: {avg_speed:.1f} km/h"
        ),
        ha="left",
        va="center",
        fontsize=11,
        color=MUTED,
    )


def prepare_panel(axis, title: str) -> None:
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
        fontsize=17,
        color=TEXT,
    )


def style_chart_axis(axis, timestamps: list[datetime]) -> None:
    axis.set_facecolor(PANEL)
    axis.spines["top"].set_color(PANEL_EDGE)
    axis.spines["right"].set_color(PANEL_EDGE)
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
        labelsize=9,
        length=0,
        pad=4,
    )
    axis.tick_params(
        axis="y",
        left=True,
        labelleft=True,
        right=True,
        labelright=True,
        colors=MUTED,
        labelsize=9,
        length=0,
        pad=4,
    )
    axis.yaxis.set_major_locator(MaxNLocator(nbins=4))
    ticks = choose_time_ticks(timestamps, max_ticks=6)
    axis.set_xticks(ticks)
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timestamps[0].tzinfo))
    axis.set_xlim(timestamps[0], timestamps[-1])


def add_line_legend(axis, items: list[tuple[str, str]]) -> None:
    handles = [
        Line2D(
            [0],
            [0],
            marker="s",
            linestyle="None",
            markerfacecolor=color,
            markeredgecolor=color,
            markersize=7,
            label=label,
        )
        for label, color in items
    ]
    legend = axis.legend(
        handles=handles,
        loc="lower left",
        bbox_to_anchor=(0.04, 0.03),
        ncol=min(3, len(handles)),
        frameon=False,
        fontsize=9,
        handlelength=0.8,
        handletextpad=0.45,
        columnspacing=1.1,
    )
    for text in legend.get_texts():
        text.set_color(MUTED)


def display_timestamps(report: ForecastReport) -> list[datetime]:
    timezone = report.start_time.tzinfo
    return [sample.sample.timestamp.astimezone(timezone) for sample in report.samples]


def choose_time_ticks(timestamps: list[datetime], max_ticks: int) -> list[datetime]:
    if len(timestamps) <= max_ticks:
        return timestamps
    raw_indices = np.linspace(0, len(timestamps) - 1, num=max_ticks)
    indices = sorted({int(round(value)) for value in raw_indices})
    return [timestamps[index] for index in indices]


def precipitation_intensity_scale(precipitation_mm: np.ndarray) -> np.ndarray:
    thresholds = np.array([0.0, 0.1, 0.5, 1.5, 4.0, 10.0], dtype=float)
    severity = np.array([0.0, 18.0, 35.0, 55.0, 78.0, 100.0], dtype=float)
    clamped = np.clip(precipitation_mm, thresholds[0], thresholds[-1])
    return np.interp(clamped, thresholds, severity)


def padded_limits(values: np.ndarray, *, pad: float) -> tuple[float, float]:
    lower = math.floor(float(np.nanmin(values) - pad))
    upper = math.ceil(float(np.nanmax(values) + pad))
    if upper <= lower:
        upper = lower + 2
    return lower, upper


def average_speed_kph(report: ForecastReport) -> float:
    hours = report.duration.total_seconds() / 3600
    if hours <= 0:
        return 0.0
    return (report.route.total_distance_m / 1000) / hours


def format_header_datetime(value: datetime) -> str:
    return value.strftime("%b %d, %Y at %H:%M")


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
