import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import { formatNumber } from "../lib/forecast/format";
import type { ForecastReport } from "../lib/forecast/types";

const CHART_HEIGHT = 300;
const CHART_MIN_WIDTH = 280;
const CHART_RESIZE_PADDING = 24;
const CHART_SYNC_KEY = "trailintel-pwa-forecast-sync";
const AXIS_LABEL_SPACE = 120;

interface ChartSpec {
  id: "temperature" | "feels-like" | "precipitation" | "cloud-cover" | "wind" | "elevation";
  title: string;
  caption: string;
}

interface LineSeriesEntry {
  label: string;
  color: string;
  values: Array<number | null>;
}

const CHART_SPECS: ChartSpec[] = [
  {
    id: "temperature",
    title: "Temperature",
    caption: "Ambient temperature across the sampled route.",
  },
  {
    id: "feels-like",
    title: "Feels Like",
    caption: "Apparent temperature along the route when the metric is available.",
  },
  {
    id: "precipitation",
    title: "Precipitation",
    caption: "Estimated precipitation intensity along the sampled route.",
  },
  {
    id: "cloud-cover",
    title: "Cloud Cover",
    caption: "Cloud cover percentage at each sampled route point.",
  },
  {
    id: "wind",
    title: "Wind (km/h)",
    caption: "Sustained wind speed aligned to the route timeline.",
  },
  {
    id: "elevation",
    title: "Elevation",
    caption: "Route profile aligned to the same forecast timeline.",
  },
];

let activeCharts: Array<{ chart: uPlot; container: HTMLDivElement }> = [];
let resizeObserver: ResizeObserver | null = null;
let activeHoverKey = "";

export function renderRouteOverviewFallback(report: ForecastReport): string {
  const width = 620;
  const height = 320;
  const pad = 28;
  const { points, bounds } = report.route;
  const lonSpan = Math.max(bounds.maxLon - bounds.minLon, 0.0001);
  const latSpan = Math.max(bounds.maxLat - bounds.minLat, 0.0001);
  const scale = Math.min((width - pad * 2) / lonSpan, (height - pad * 2) / latSpan);
  const drawWidth = lonSpan * scale;
  const drawHeight = latSpan * scale;
  const originX = (width - drawWidth) / 2;
  const originY = (height - drawHeight) / 2;

  const routePolyline = points
    .map((point) => {
      const x = originX + (point.lon - bounds.minLon) * scale;
      const y = originY + (bounds.maxLat - point.lat) * scale;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  const firstPoint = points[0];
  const lastPoint = points.at(-1) ?? points[0];
  const startX = originX + (firstPoint.lon - bounds.minLon) * scale;
  const startY = originY + (bounds.maxLat - firstPoint.lat) * scale;
  const finishX = originX + (lastPoint.lon - bounds.minLon) * scale;
  const finishY = originY + (bounds.maxLat - lastPoint.lat) * scale;

  return `
    <svg class="route-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Route overview">
      <defs>
        <linearGradient id="route-bg" x1="0%" x2="100%" y1="0%" y2="100%">
          <stop offset="0%" stop-color="#dff0f8" />
          <stop offset="100%" stop-color="#f7ead7" />
        </linearGradient>
        <linearGradient id="route-line" x1="0%" x2="100%" y1="0%" y2="0%">
          <stop offset="0%" stop-color="#0e5b85" />
          <stop offset="100%" stop-color="#f2682a" />
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="${width}" height="${height}" rx="28" fill="url(#route-bg)" />
      <g opacity="0.5">
        <circle cx="88" cy="76" r="54" fill="#ffffff" />
        <circle cx="508" cy="84" r="42" fill="#ffffff" />
        <path d="M0 284 C102 224 168 228 264 258 C362 288 452 218 620 242 L620 320 L0 320 Z" fill="#d4e6db" />
      </g>
      <polyline
        points="${routePolyline}"
        fill="none"
        stroke="url(#route-line)"
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="12"
      />
      <circle cx="${startX.toFixed(1)}" cy="${startY.toFixed(1)}" r="10" fill="#30a46c" stroke="#ffffff" stroke-width="4" />
      <circle cx="${finishX.toFixed(1)}" cy="${finishY.toFixed(1)}" r="10" fill="#cf4b2b" stroke="#ffffff" stroke-width="4" />
    </svg>
  `;
}

export function renderForecastChartsSection(report: ForecastReport): string {
  return `
    <section class="panel chart-panel">
      <div class="panel-head">
        <div>
          <p class="section-tag">Charts</p>
          <h2>uPlot Forecast Charts</h2>
        </div>
        <span class="pill">${report.sourceLabel}</span>
      </div>
      <p class="section-copy">Interactive forecast panels tied to the sampled route timeline.</p>
      <div id="forecast-chart-hover" class="forecast-chart-hover" aria-live="polite">
        Hover a chart to inspect that route moment in detail.
      </div>
      <div class="forecast-chart-grid">
        ${CHART_SPECS.map((spec) => renderChartCard(spec)).join("")}
      </div>
    </section>
  `;
}

export function mountForecastCharts(report: ForecastReport): void {
  teardownForecastCharts();

  const hover = document.querySelector<HTMLDivElement>("#forecast-chart-hover");
  const dateFormatter = buildDateFormatter(report.timezoneName);
  const axisDateFormatter = buildAxisDateFormatter(report.timezoneName);
  const xValues = report.samples.map((sample) => sample.sample.timestampMs);

  for (const spec of CHART_SPECS) {
    const container = document.querySelector<HTMLDivElement>(`#chart-${spec.id}`);
    const note = document.querySelector<HTMLParagraphElement>(`#chart-note-${spec.id}`);
    if (!container) {
      continue;
    }

    if (spec.id === "elevation") {
      const values = report.samples.map((sample) => sample.sample.elevationM);
      if (!values.some((value) => value !== null)) {
        container.innerHTML = '<div class="empty-state">Route elevation is unavailable for this forecast.</div>';
        if (note) {
          note.textContent = "Elevation comes from the GPX route profile instead of the forecast provider.";
        }
        continue;
      }

      const seriesEntries: LineSeriesEntry[] = [
        {
          label: "Elevation",
          color: "#8a6a2f",
          values,
        },
      ];
      const chart = new uPlot(
        buildOptions(spec, axisDateFormatter, seriesEntries, computeChartWidth(container)),
        [xValues, values] as uPlot.AlignedData,
        container,
      );
      bindHover(chart, spec, report, hover, seriesEntries, dateFormatter);
      activeCharts.push({ chart, container });
      if (note) {
        note.textContent = "Elevation comes from the GPX route profile and stays aligned with the forecast time axis.";
      }
      continue;
    }

    const seriesEntries = buildMetricSeries(report, spec.id);
    if (!seriesEntries.length) {
      container.innerHTML = `<div class="empty-state">No ${spec.title.toLowerCase()} data is available for this forecast.</div>`;
      if (note) {
        note.textContent = spec.id === "feels-like"
          ? "The active forecast source did not provide apparent temperature for this route."
          : "This forecast did not include enough data to draw the chart.";
      }
      continue;
    }

    const chart = new uPlot(
      buildOptions(spec, axisDateFormatter, seriesEntries, computeChartWidth(container)),
      [xValues, ...seriesEntries.map((entry) => entry.values)] as uPlot.AlignedData,
      container,
    );
    bindHover(chart, spec, report, hover, seriesEntries, dateFormatter);
    activeCharts.push({ chart, container });
    if (note) {
      note.textContent = spec.id === "wind"
        ? "Values shown in km/h. Gusts remain available in the sample-by-sample detail below."
        : spec.caption;
    }
  }

  if (typeof ResizeObserver === "function") {
    resizeObserver = new ResizeObserver(() => {
      for (const entry of activeCharts) {
        entry.chart.setSize({
          width: computeChartWidth(entry.container),
          height: CHART_HEIGHT,
        });
      }
    });
    for (const entry of activeCharts) {
      resizeObserver.observe(entry.container);
    }
  }
}

export function teardownForecastCharts(): void {
  resizeObserver?.disconnect();
  resizeObserver = null;
  for (const entry of activeCharts) {
    entry.chart.destroy();
  }
  activeCharts = [];
  activeHoverKey = "";
}

function renderChartCard(spec: ChartSpec): string {
  return `
    <article class="forecast-chart-card">
      <div class="panel-head">
        <h3>${spec.title}</h3>
      </div>
      <p class="section-copy">${spec.caption}</p>
      <p class="chart-note" id="chart-note-${spec.id}">Preparing interactive chart…</p>
      <div class="forecast-chart-canvas" id="chart-${spec.id}" data-chart-id="${spec.id}"></div>
    </article>
  `;
}

function buildMetricSeries(
  report: ForecastReport,
  metricId: ChartSpec["id"],
): LineSeriesEntry[] {
  switch (metricId) {
    case "temperature":
      return [
        {
          label: "Temperature",
          color: "#0e5b85",
          values: report.samples.map((sample) => sample.temperatureC),
        },
      ];
    case "feels-like": {
      const values = report.samples.map((sample) => sample.apparentTemperatureC);
      return values.some((value) => value !== null)
        ? [
            {
              label: "Feels like",
              color: "#f2682a",
              values,
            },
          ]
        : [];
    }
    case "precipitation":
      return [
        {
          label: "Precipitation",
          color: "#c45d1c",
          values: report.samples.map((sample) => sample.precipitationMm),
        },
      ];
    case "cloud-cover":
      return [
        {
          label: "Cloud cover",
          color: "#6c7d8b",
          values: report.samples.map((sample) => sample.cloudCoverPct),
        },
      ];
    case "wind":
      return [
        {
          label: "Wind",
          color: "#1c7c63",
          values: report.samples.map((sample) => sample.windKph),
        },
      ];
    default:
      return [];
  }
}

function buildOptions(
  spec: ChartSpec,
  axisDateFormatter: Intl.DateTimeFormat,
  seriesEntries: LineSeriesEntry[],
  width: number,
): uPlot.Options {
  return {
    width,
    height: CHART_HEIGHT,
    legend: { show: false },
    cursor: {
      sync: { key: CHART_SYNC_KEY },
      drag: { x: false, y: false },
    },
    scales: {
      x: { time: true },
    },
    axes: [
      {
        size: 76,
        space: AXIS_LABEL_SPACE,
        values: (_u, values) => values.map((value) => axisDateFormatter.format(new Date(Number(value)))),
      },
      {
        size: spec.id === "elevation" ? 78 : 64,
        values: (_u, values) => values.map((value) => axisValueLabel(spec.id, Number(value))),
      },
    ],
    series: [
      {},
      ...seriesEntries.map((entry) => ({
        label: entry.label,
        stroke: entry.color,
        width: 2,
        spanGaps: true,
        points: { show: false },
      })),
    ],
  };
}

function bindHover(
  chart: uPlot,
  spec: ChartSpec,
  report: ForecastReport,
  hover: HTMLDivElement | null,
  seriesEntries: LineSeriesEntry[],
  dateFormatter: Intl.DateTimeFormat,
): void {
  const originalSetCursor = chart.setCursor.bind(chart);
  chart.setCursor = (cursor, fireHook) => {
    originalSetCursor(cursor, fireHook);
    if (!hover) {
      return;
    }
    const idx = chart.cursor.idx;
    if (idx === null || idx === undefined || idx < 0) {
      return;
    }
    const hoverKey = `${spec.id}:${idx}`;
    if (hoverKey === activeHoverKey) {
      return;
    }
    activeHoverKey = hoverKey;
    renderHoverContent(spec, report, hover, idx, seriesEntries, dateFormatter);
  };
}

function renderHoverContent(
  spec: ChartSpec,
  report: ForecastReport,
  hover: HTMLDivElement,
  sampleIndex: number,
  seriesEntries: LineSeriesEntry[],
  dateFormatter: Intl.DateTimeFormat,
): void {
  const sample = report.samples[sampleIndex];
  if (!sample) {
    hover.textContent = "Hover a chart to inspect that route moment in detail.";
    return;
  }

  const rows = seriesEntries
    .map((entry) => {
      const value = entry.values[sampleIndex];
      return `
        <div class="forecast-chart-hover-row">
          <strong style="color:${entry.color}">${entry.label}</strong>
          <span>${formatMetricValue(spec.id, value)}</span>
        </div>
      `;
    })
    .join("");

  hover.innerHTML = `
    <strong>${spec.title}</strong><br>
    ${dateFormatter.format(new Date(sample.sample.timestampMs))} •
    ${formatNumber(sample.sample.distanceM / 1000, 2)} km •
    ${sample.sample.elevationM === null ? "n/a" : `${formatNumber(sample.sample.elevationM, 0)} m`}
    <div class="forecast-chart-hover-grid">${rows}</div>
  `;
}

function formatMetricValue(metricId: ChartSpec["id"], value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "n/a";
  }
  const digits = metricDigits(metricId);
  const unit = metricUnit(metricId);
  if (metricId === "wind") {
    return `${Number(value).toFixed(digits)}`;
  }
  return `${Number(value).toFixed(digits)} ${unit}`;
}

function axisValueLabel(metricId: ChartSpec["id"], value: number): string {
  const digits = metricDigits(metricId);
  if (metricId === "wind") {
    return Number(value).toFixed(digits);
  }
  return `${Number(value).toFixed(digits)} ${metricUnit(metricId)}`;
}

function metricUnit(metricId: ChartSpec["id"]): string {
  switch (metricId) {
    case "temperature":
    case "feels-like":
      return "C";
    case "precipitation":
      return "mm";
    case "cloud-cover":
      return "%";
    case "wind":
      return "km/h";
    case "elevation":
      return "m";
  }
}

function metricDigits(metricId: ChartSpec["id"]): number {
  switch (metricId) {
    case "precipitation":
      return 2;
    case "wind":
    case "cloud-cover":
    case "elevation":
      return 0;
    default:
      return 1;
  }
}

function buildDateFormatter(timezoneName: string): Intl.DateTimeFormat {
  const options: Intl.DateTimeFormatOptions = {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  };
  try {
    return new Intl.DateTimeFormat("en-US", { ...options, timeZone: timezoneName });
  } catch {
    return new Intl.DateTimeFormat("en-US", options);
  }
}

function buildAxisDateFormatter(timezoneName: string): Intl.DateTimeFormat {
  const options: Intl.DateTimeFormatOptions = {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  };
  try {
    return new Intl.DateTimeFormat("en-US", { ...options, timeZone: timezoneName });
  } catch {
    return new Intl.DateTimeFormat("en-US", options);
  }
}

function computeChartWidth(container: HTMLDivElement): number {
  return Math.max(container.clientWidth - CHART_RESIZE_PADDING, CHART_MIN_WIDTH);
}
