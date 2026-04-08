import { formatDateTime, formatNullableNumber, formatNumber } from "../lib/forecast/format";
import type { ForecastReport, SampleForecast } from "../lib/forecast/types";

const CHART_WIDTH = 720;
const CHART_HEIGHT = 260;
const PADDING = { top: 18, right: 18, bottom: 36, left: 46 };

interface LineSeries {
  label: string;
  color: string;
  values: Array<number | null>;
  dashed?: boolean;
}

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

  const markers = markerSamples(report.samples).map((sample) => {
    const x = originX + (sample.sample.lon - bounds.minLon) * scale;
    const y = originY + (bounds.maxLat - sample.sample.lat) * scale;
    return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="5.5" fill="#f7efe2" stroke="#0e5b85" stroke-width="2" />`;
  });

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
      ${markers.join("")}
      <circle cx="${startX.toFixed(1)}" cy="${startY.toFixed(1)}" r="10" fill="#30a46c" stroke="#ffffff" stroke-width="4" />
      <circle cx="${finishX.toFixed(1)}" cy="${finishY.toFixed(1)}" r="10" fill="#cf4b2b" stroke="#ffffff" stroke-width="4" />
    </svg>
  `;
}

export function renderRouteOverview(report: ForecastReport): string {
  return renderRouteOverviewFallback(report);
}

export function renderTemperatureChart(report: ForecastReport): string {
  return renderLineChart({
    title: "Temperature",
    caption: "Ambient and feels-like temperature across the route.",
    timezoneName: report.timezoneName,
    yUnit: "C",
    series: [
      {
        label: "Temperature",
        color: "#0e5b85",
        values: report.samples.map((sample) => sample.temperatureC),
      },
      {
        label: "Feels like",
        color: "#f2682a",
        values: report.samples.map((sample) => sample.apparentTemperatureC),
        dashed: true,
      },
    ],
    samples: report.samples,
  });
}

export function renderWindChart(report: ForecastReport): string {
  return renderLineChart({
    title: "Wind",
    caption: "Sustained wind and gusts, in km/h.",
    timezoneName: report.timezoneName,
    yUnit: "km/h",
    series: [
      {
        label: "Wind",
        color: "#246a73",
        values: report.samples.map((sample) => sample.windKph),
      },
      {
        label: "Gusts",
        color: "#8a4b00",
        values: report.samples.map((sample) => sample.windGustKph),
        dashed: true,
      },
    ],
    samples: report.samples,
  });
}

export function renderPrecipitationChart(report: ForecastReport): string {
  const width = CHART_WIDTH;
  const height = CHART_HEIGHT;
  const plotWidth = width - PADDING.left - PADDING.right;
  const plotHeight = height - PADDING.top - PADDING.bottom;
  const precipitationValues = report.samples.map((sample) => sample.precipitationMm);
  const cloudValues = report.samples.map((sample) => sample.cloudCoverPct);
  const chanceValues = report.samples.map((sample) => sample.precipitationProbability ?? 0);
  const precipitationMax = Math.max(0.4, ...precipitationValues);
  const labels = buildTimeLabels(report.samples, report.timezoneName);

  const cloudPath = buildPath(cloudValues, plotWidth, plotHeight, 0, 100);
  const chancePath = buildPath(chanceValues, plotWidth, plotHeight, 0, 100);
  const bars = precipitationValues
    .map((value, index) => {
      const x = PADDING.left + (plotWidth * index) / Math.max(1, precipitationValues.length - 1);
      const normalized = value / precipitationMax;
      const barHeight = normalized * (plotHeight * 0.55);
      const y = PADDING.top + plotHeight - barHeight;
      return `<rect x="${(x - 8).toFixed(1)}" y="${y.toFixed(1)}" width="16" height="${barHeight.toFixed(1)}" rx="8" fill="#f0b34a" opacity="0.8" />`;
    })
    .join("");

  return `
    <figure class="chart-frame">
      <figcaption class="chart-head">
        <div>
          <h3>Rain and Clouds</h3>
          <p>Rain amount, rain chance, and cloud cover across the forecast window.</p>
        </div>
        <div class="chart-legend">
          <span><i style="background:#f0b34a"></i>Rain (mm)</span>
          <span><i style="background:#0e5b85"></i>Rain chance</span>
          <span><i style="background:#93a7b4"></i>Cloud cover</span>
        </div>
      </figcaption>
      <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Rain and cloud chart">
        ${gridLines(width, height)}
        ${axisLabels(labels, precipitationMax, "mm")}
        ${bars}
        <path d="${cloudPath}" fill="none" stroke="#93a7b4" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
        <path d="${chancePath}" fill="none" stroke="#0e5b85" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <div class="chart-summary">
        <span>Max rain ${formatNumber(precipitationMax, 1)} mm</span>
        <span>Peak cloud ${formatNumber(Math.max(...cloudValues), 0)}%</span>
        <span>Peak rain chance ${formatNumber(Math.max(...chanceValues), 0)}%</span>
      </div>
    </figure>
  `;
}

function renderLineChart(options: {
  title: string;
  caption: string;
  timezoneName: string;
  yUnit: string;
  series: LineSeries[];
  samples: SampleForecast[];
}): string {
  const width = CHART_WIDTH;
  const height = CHART_HEIGHT;
  const plotWidth = width - PADDING.left - PADDING.right;
  const plotHeight = height - PADDING.top - PADDING.bottom;
  const allValues = options.series
    .flatMap((series) => series.values)
    .filter((value): value is number => value !== null);
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const span = Math.max(maxValue - minValue, 1);
  const paddedMin = minValue - span * 0.12;
  const paddedMax = maxValue + span * 0.12;
  const labels = buildTimeLabels(options.samples, options.timezoneName);

  const paths = options.series
    .map((series) => {
      const path = buildPath(series.values, plotWidth, plotHeight, paddedMin, paddedMax);
      const dashArray = series.dashed ? ' stroke-dasharray="10 8"' : "";
      return `<path d="${path}" fill="none" stroke="${series.color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"${dashArray} />`;
    })
    .join("");

  return `
    <figure class="chart-frame">
      <figcaption class="chart-head">
        <div>
          <h3>${options.title}</h3>
          <p>${options.caption}</p>
        </div>
        <div class="chart-legend">
          ${options.series
            .map(
              (series) =>
                `<span><i style="background:${series.color}"></i>${series.label}</span>`,
            )
            .join("")}
        </div>
      </figcaption>
      <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${options.title} chart">
        ${gridLines(width, height)}
        ${axisLabels(labels, paddedMax, options.yUnit)}
        ${paths}
      </svg>
      <div class="chart-summary">
        ${options.series
          .map((series) => {
            const tail = series.values.at(-1);
            return `<span>${series.label}: ${formatNullableNumber(tail ?? null, 1)} ${options.yUnit}</span>`;
          })
          .join("")}
      </div>
    </figure>
  `;
}

function buildPath(
  values: Array<number | null>,
  plotWidth: number,
  plotHeight: number,
  minValue: number,
  maxValue: number,
): string {
  const span = Math.max(maxValue - minValue, 1);
  const commands: string[] = [];
  values.forEach((value, index) => {
    if (value === null) {
      return;
    }
    const x = PADDING.left + (plotWidth * index) / Math.max(1, values.length - 1);
    const normalized = (value - minValue) / span;
    const y = PADDING.top + plotHeight - normalized * plotHeight;
    commands.push(`${commands.length ? "L" : "M"} ${x.toFixed(1)} ${y.toFixed(1)}`);
  });
  return commands.join(" ");
}

function gridLines(width: number, height: number): string {
  const plotWidth = width - PADDING.left - PADDING.right;
  const plotHeight = height - PADDING.top - PADDING.bottom;
  return Array.from({ length: 4 }, (_, index) => {
    const y = PADDING.top + (plotHeight * index) / 3;
    return `<line x1="${PADDING.left}" y1="${y.toFixed(1)}" x2="${PADDING.left + plotWidth}" y2="${y.toFixed(1)}" stroke="rgba(58,73,88,0.15)" stroke-width="2" />`;
  }).join("");
}

function axisLabels(labels: string[], maxValue: number, unit: string): string {
  const plotWidth = CHART_WIDTH - PADDING.left - PADDING.right;
  const leftLabels = [
    { y: PADDING.top + 8, text: `${formatNumber(maxValue, 0)} ${unit}` },
    { y: CHART_HEIGHT - PADDING.bottom, text: `0 ${unit}` },
  ];
  const bottomLabels = labels.map((label, index) => {
    const x = PADDING.left + (plotWidth * index) / Math.max(1, labels.length - 1);
    return `<text x="${x.toFixed(1)}" y="${CHART_HEIGHT - 10}" text-anchor="middle">${label}</text>`;
  });

  return `
    <g class="chart-axis">
      ${leftLabels.map((entry) => `<text x="8" y="${entry.y.toFixed(1)}">${entry.text}</text>`).join("")}
      ${bottomLabels.join("")}
    </g>
  `;
}

function buildTimeLabels(samples: SampleForecast[], timezoneName: string): string[] {
  const first = samples[0];
  const middle = samples[Math.floor(samples.length / 2)];
  const last = samples.at(-1) ?? middle;
  return [
    formatDateTime(first.sample.timestampMs, timezoneName),
    formatDateTime(middle.sample.timestampMs, timezoneName),
    formatDateTime(last.sample.timestampMs, timezoneName),
  ];
}

function markerSamples(samples: SampleForecast[]): SampleForecast[] {
  if (samples.length <= 6) {
    return samples;
  }

  const step = Math.max(1, Math.floor(samples.length / 5));
  const markers = new Set<number>([0, samples.length - 1]);
  for (let index = step; index < samples.length - 1; index += step) {
    markers.add(index);
  }
  return [...markers].sort((left, right) => left - right).map((index) => samples[index]);
}
