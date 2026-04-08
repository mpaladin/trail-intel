import {
  formatDistanceKm,
  formatDuration,
  formatFullDateTime,
  formatNumber,
} from "../forecast/format";
import type { BuildForecastResult, ForecastReport, SampleForecast } from "../forecast/types";
import { buildRouteArrows } from "../map/overlay";
import { buildBasemapRaster, MAP_ATTRIBUTION, rasterPixelForLonLat } from "../map/tiles";

export interface ReportImageRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface ReportImageLayout {
  size: number;
  header: ReportImageRect;
  temperature: ReportImageRect;
  precipitation: ReportImageRect;
  map: ReportImageRect;
  wind: ReportImageRect;
  elevation: ReportImageRect;
}

interface AxisScale {
  min: number;
  max: number;
  ticks: number[];
}

interface LegendItem {
  label: string;
  color: string;
}

const IMAGE_SIZE = 1600;
const OUTER = 44;
const GAP = 24;
const COLUMN_GAP = 28;
const HEADER_HEIGHT = 230;
const TOP_PANEL_HEIGHT = 300;
const RIGHT_PANEL_HEIGHT = 390;

export function buildReportImageLayout(size = IMAGE_SIZE): ReportImageLayout {
  const innerWidth = size - OUTER * 2;
  const columnWidth = (innerWidth - COLUMN_GAP) / 2;
  const topY = OUTER + HEADER_HEIGHT + GAP;
  const lowerY = topY + TOP_PANEL_HEIGHT + GAP;

  return {
    size,
    header: { x: OUTER, y: OUTER, width: innerWidth, height: HEADER_HEIGHT },
    temperature: { x: OUTER, y: topY, width: columnWidth, height: TOP_PANEL_HEIGHT },
    precipitation: {
      x: OUTER + columnWidth + COLUMN_GAP,
      y: topY,
      width: columnWidth,
      height: TOP_PANEL_HEIGHT,
    },
    map: {
      x: OUTER,
      y: lowerY,
      width: columnWidth,
      height: size - lowerY - OUTER,
    },
    wind: {
      x: OUTER + columnWidth + COLUMN_GAP,
      y: lowerY,
      width: columnWidth,
      height: RIGHT_PANEL_HEIGHT,
    },
    elevation: {
      x: OUTER + columnWidth + COLUMN_GAP,
      y: lowerY + RIGHT_PANEL_HEIGHT + GAP,
      width: columnWidth,
      height: size - (lowerY + RIGHT_PANEL_HEIGHT + GAP) - OUTER,
    },
  };
}

export async function renderReportImageBlob(
  result: BuildForecastResult,
  fetchImpl: typeof fetch = fetch,
): Promise<Blob> {
  const canvas = document.createElement("canvas");
  canvas.width = IMAGE_SIZE;
  canvas.height = IMAGE_SIZE;
  const context = canvas.getContext("2d");
  if (!context) {
    throw new Error("Could not create canvas context for report export.");
  }

  const layout = buildReportImageLayout();
  const basemap = await buildBasemapRaster(result.report.route, fetchImpl);

  drawBackground(context, layout.size);
  drawHeader(context, layout.header, result);
  drawTemperaturePanel(context, layout.temperature, result.report);
  drawPrecipitationPanel(context, layout.precipitation, result.report);
  drawMapPanel(context, layout.map, result.report, basemap);
  drawWindPanel(context, layout.wind, result.report);
  drawElevationPanel(context, layout.elevation, result.report);

  const blob = await canvasToBlob(canvas);
  if (!blob) {
    throw new Error("Could not encode report image.");
  }
  return blob;
}

function drawBackground(context: CanvasRenderingContext2D, size: number): void {
  const gradient = context.createLinearGradient(0, 0, size, size);
  gradient.addColorStop(0, "#04070c");
  gradient.addColorStop(0.45, "#0b1117");
  gradient.addColorStop(1, "#111a22");
  context.fillStyle = gradient;
  context.fillRect(0, 0, size, size);

  const halo = context.createRadialGradient(size * 0.18, size * 0.14, 10, size * 0.18, size * 0.14, size * 0.5);
  halo.addColorStop(0, "rgba(255, 90, 36, 0.24)");
  halo.addColorStop(1, "rgba(255, 90, 36, 0)");
  context.fillStyle = halo;
  context.fillRect(0, 0, size, size);
}

function drawHeader(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  result: BuildForecastResult,
): void {
  drawPanel(context, rect, "#ff5a24", 28);
  const { report, summary } = result;
  const dateLabel = formatFullDateTime(report.startTimeMs, report.timezoneName);
  const statPills = [
    formatDistanceKm(report.route.totalDistanceM),
    `${formatNumber(report.route.totalAscentM, 0)} m gain`,
    `${formatNumber(summary.windMaxKph)} km/h max wind`,
    `${formatNumber(summary.precipitationTotalMm, 1)} mm rain`,
  ];
  const titleStyles = [
    { font: "700 58px Iowan Old Style, Georgia, serif", lineHeight: 62 },
    { font: "700 52px Iowan Old Style, Georgia, serif", lineHeight: 58 },
    { font: "700 48px Iowan Old Style, Georgia, serif", lineHeight: 54 },
  ];
  const titleX = rect.x + 36;
  const titleY = rect.y + 28;
  const titleWidth = rect.width - 72;

  let titleLines = [report.title];
  let titleLineHeight = 58;
  for (const style of titleStyles) {
    context.font = style.font;
    const candidate = wrapTextLines(context, report.title, titleWidth);
    titleLines = candidate;
    titleLineHeight = style.lineHeight;
    if (candidate.length <= 2) {
      break;
    }
  }

  context.fillStyle = "#ffffff";
  context.textBaseline = "top";
  context.font = titleStyles.find((style) => style.lineHeight === titleLineHeight)?.font ?? titleStyles[1].font;
  drawWrappedText(context, report.title, titleX, titleY, titleWidth, titleLineHeight, 2);

  const dateY = titleY + Math.min(titleLines.length, 2) * titleLineHeight + 8;
  context.font = "500 22px Avenir Next, Segoe UI, sans-serif";
  context.fillText(dateLabel, rect.x + 38, dateY);

  drawPillRows(context, statPills, rect.x + 36, dateY + 42, rect.width - 72, 12, 10);
}

function drawTemperaturePanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
): void {
  drawChartPanel(
    context,
    rect,
    "Temperature",
    "Ambient and feels-like temperature across the route.",
  );
  drawChartLegend(context, rect, [
    { label: "Temperature", color: "#66b9ff" },
    { label: "Feels like", color: "#ffb03a" },
  ]);
  drawLineChart(
    context,
    insetRect(rect, 26, 134, 22, 34),
    report.samples,
    report.timezoneName,
    [
      { color: "#66b9ff", values: report.samples.map((sample) => sample.temperatureC) },
      {
        color: "#ffb03a",
        values: report.samples.map((sample) => sample.apparentTemperatureC ?? sample.temperatureC),
      },
    ],
    {
      unit: "°C",
      pad: 1.5,
      minimumStep: 1,
    },
  );
}

function drawPrecipitationPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
): void {
  drawChartPanel(
    context,
    rect,
    "Precipitation and Cloud Cover",
    "Rain amount, chance, and cloud cover through the forecast window.",
  );
  drawChartLegend(context, rect, [
    { label: "Rain chance", color: "#73bdf5" },
    { label: "Cloud cover", color: "#a7abb3" },
    { label: "Rain mm", color: "#ffae38" },
  ]);

  const frame = insetRect(rect, 26, 134, 22, 34);
  const plot = insetRect(frame, 58, 10, 52, 38);
  const precipitation = report.samples.map((sample) => sample.precipitationMm);
  const chance = report.samples.map((sample) => sample.precipitationProbability ?? 0);
  const cloud = report.samples.map((sample) => sample.cloudCoverPct);
  const probabilityScale = buildNiceScale(0, 100, 5, { fixedMin: 0, fixedMax: 100, minimumStep: 20 });
  const precipMax = Math.max(0.4, ...precipitation);
  const precipitationScale = buildNiceScale(0, precipMax, 4, { fixedMin: 0, minimumStep: 0.5 });
  drawChartGrid(context, plot, probabilityScale);

  precipitation.forEach((value, index) => {
    const x = pointX(plot, precipitation.length, index);
    const barWidth = Math.max(10, plot.width / Math.max(16, precipitation.length * 1.8));
    const y = pointY(plot, value, precipitationScale.min, precipitationScale.max);
    const barHeight = plot.y + plot.height - y;
    context.fillStyle = "rgba(255, 174, 56, 0.8)";
    roundRectPath(context, x - barWidth / 2, y, barWidth, barHeight, 10);
    context.fill();
  });

  drawSeries(context, plot, chance, probabilityScale.min, probabilityScale.max, "#73bdf5", 3.5);
  drawSeries(context, plot, cloud, probabilityScale.min, probabilityScale.max, "#a7abb3", 2.8);
  drawYAxisLabels(context, plot, probabilityScale, "%", "left");
  drawYAxisLabels(context, plot, precipitationScale, "mm", "right");
  drawTimeLabels(context, plot, report.samples, report.timezoneName);
}

function drawMapPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
  basemap: Awaited<ReturnType<typeof buildBasemapRaster>>,
): void {
  drawChartPanel(context, rect, "Wind Direction", "Route context, tile basemap, and sampled wind arrows.");
  const mapRect = insetRect(rect, 26, 86, 26, 42);
  clipRoundedRect(context, mapRect, 24);
  if (basemap) {
    context.drawImage(basemap.canvas, mapRect.x, mapRect.y, mapRect.width, mapRect.height);
  } else {
    const mapGradient = context.createLinearGradient(mapRect.x, mapRect.y, mapRect.x, mapRect.y + mapRect.height);
    mapGradient.addColorStop(0, "#183544");
    mapGradient.addColorStop(1, "#0d1820");
    context.fillStyle = mapGradient;
    context.fillRect(mapRect.x, mapRect.y, mapRect.width, mapRect.height);
  }
  context.restore();

  const routePixels = report.route.points.map((point) =>
    projectRoutePoint(basemap, point.lon, point.lat, mapRect, report),
  );
  const start = routePixels[0];
  if (!start) {
    return;
  }
  context.save();
  context.beginPath();
  roundRectPath(context, mapRect.x, mapRect.y, mapRect.width, mapRect.height, 24);
  context.clip();
  context.lineJoin = "round";
  context.lineCap = "round";
  context.strokeStyle = "#ff6124";
  context.lineWidth = 8;
  context.beginPath();
  routePixels.forEach((point, index) => {
    if (index === 0) {
      context.moveTo(point.x, point.y);
    } else {
      context.lineTo(point.x, point.y);
    }
  });
  context.stroke();

  for (const arrow of buildRouteArrows(report.samples)) {
    const center = projectRoutePoint(
      basemap,
      arrow.sample.sample.lon,
      arrow.sample.sample.lat,
      mapRect,
      report,
    );
    drawArrow(context, center.x, center.y, arrow.dx, arrow.dy, 20, "#12171d");
  }

  drawMarker(context, start.x, start.y, "#6fe04d");
  const finish = routePixels.at(-1) ?? start;
  drawMarker(context, finish.x, finish.y, "#ff3c2f");
  context.restore();

  context.fillStyle = "rgba(8, 10, 12, 0.66)";
  context.font = "500 12px Avenir Next, Segoe UI, sans-serif";
  context.textAlign = "right";
  context.fillText(MAP_ATTRIBUTION, mapRect.x + mapRect.width - 12, mapRect.y + mapRect.height - 12);
  context.textAlign = "left";
}

function drawWindPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
): void {
  drawChartPanel(context, rect, "Wind", "Sustained wind and gusts along the forecast route.");
  drawChartLegend(context, rect, [
    { label: "Wind", color: "#8fc2df" },
    { label: "Gusts", color: "#ffad2f" },
  ]);
  drawLineChart(
    context,
    insetRect(rect, 26, 134, 22, 38),
    report.samples,
    report.timezoneName,
    [
      { color: "#8fc2df", values: report.samples.map((sample) => sample.windKph) },
      { color: "#ffad2f", values: report.samples.map((sample) => sample.windGustKph ?? sample.windKph) },
    ],
    {
      unit: "km/h",
      fixedMin: 0,
      pad: 1,
      minimumStep: 2,
    },
  );
}

function drawElevationPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
): void {
  drawChartPanel(context, rect, "Elevation", "Route profile plus quick ride summary.");
  const plot = insetRect(rect, 26, 108, 22, 92);
  const values = report.samples.map((sample) => sample.sample.elevationM ?? 0);
  drawAreaChart(context, plot, values, report.samples, report.timezoneName, "#d1912b", "#7e551a");

  context.fillStyle = "#d0ccc4";
  context.font = "600 22px Avenir Next, Segoe UI, sans-serif";
  context.fillText(`Elevation Gain: ${formatNumber(report.route.totalAscentM, 0)} m↑`, rect.x + 32, rect.y + rect.height - 54);
  context.fillText(
    `Distance: ${formatDistanceKm(report.route.totalDistanceM)}   Duration: ${formatDuration(report.durationMs)}`,
    rect.x + 32,
    rect.y + rect.height - 24,
  );
}

function drawChartPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  title: string,
  subtitle: string,
): void {
  drawPanel(context, rect, "#050505", 28);
  context.fillStyle = "#f4f1ea";
  context.textBaseline = "top";
  context.font = "700 34px Iowan Old Style, Georgia, serif";
  context.fillText(title, rect.x + 26, rect.y + 22);
  context.fillStyle = "#d0ccc4";
  context.font = "500 17px Avenir Next, Segoe UI, sans-serif";
  drawWrappedText(context, subtitle, rect.x + 26, rect.y + 66, rect.width - 52, 21, 2);
}

function drawChartLegend(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  items: LegendItem[],
): void {
  let x = rect.x + 26;
  const y = rect.y + 106;
  context.save();
  context.font = "600 16px Avenir Next, Segoe UI, sans-serif";
  context.textBaseline = "middle";
  for (const item of items) {
    context.strokeStyle = item.color;
    context.fillStyle = item.color;
    context.lineWidth = 4;
    context.beginPath();
    context.moveTo(x, y);
    context.lineTo(x + 18, y);
    context.stroke();

    context.fillStyle = "#d0ccc4";
    context.fillText(item.label, x + 28, y);
    x += 28 + context.measureText(item.label).width + 24;
  }
  context.restore();
}

function drawLineChart(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  samples: SampleForecast[],
  timezoneName: string,
  series: Array<{ color: string; values: number[] }>,
  options: {
    unit: string;
    fixedMin?: number;
    fixedMax?: number;
    pad?: number;
    minimumStep?: number;
  },
): void {
  const plot = insetRect(rect, 58, 10, 16, 38);
  const allValues = series.flatMap((entry) => entry.values);
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const padding = options.pad ?? 0;
  const scale = buildNiceScale(minValue - padding, maxValue + padding, 4, {
    fixedMin: options.fixedMin,
    fixedMax: options.fixedMax,
    minimumStep: options.minimumStep,
  });
  drawChartGrid(context, plot, scale);
  for (const entry of series) {
    drawSeries(context, plot, entry.values, scale.min, scale.max, entry.color, 4);
  }
  drawYAxisLabels(context, plot, scale, options.unit, "left");
  drawTimeLabels(context, plot, samples, timezoneName);
}

function drawAreaChart(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  values: number[],
  samples: SampleForecast[],
  timezoneName: string,
  lineColor: string,
  fillColor: string,
): void {
  const plot = insetRect(rect, 58, 10, 16, 38);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const baseline = Math.floor(minValue / 100) * 100;
  const ceiling = Math.max(baseline + 100, Math.ceil(maxValue / 100) * 100);
  const scale = buildNiceScale(baseline, ceiling, 4, {
    fixedMin: baseline,
    fixedMax: ceiling,
    minimumStep: 50,
  });
  drawChartGrid(context, plot, scale);
  context.beginPath();
  values.forEach((value, index) => {
    const x = pointX(plot, values.length, index);
    const y = pointY(plot, value, scale.min, scale.max);
    if (index === 0) {
      context.moveTo(x, plot.y + plot.height);
      context.lineTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.lineTo(plot.x + plot.width, plot.y + plot.height);
  context.closePath();
  context.fillStyle = `${fillColor}aa`;
  context.fill();
  drawSeries(context, plot, values, scale.min, scale.max, lineColor, 3.5);
  drawYAxisLabels(context, plot, scale, "m", "left");
  drawTimeLabels(context, plot, samples, timezoneName);
}

function drawSeries(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  values: number[],
  minValue: number,
  maxValue: number,
  color: string,
  lineWidth: number,
): void {
  context.strokeStyle = color;
  context.lineWidth = lineWidth;
  context.beginPath();
  values.forEach((value, index) => {
    const x = pointX(rect, values.length, index);
    const y = pointY(rect, value, minValue, maxValue);
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();
}

function drawChartGrid(context: CanvasRenderingContext2D, rect: ReportImageRect, scale: AxisScale): void {
  context.strokeStyle = "rgba(98, 98, 98, 0.45)";
  context.lineWidth = 1;
  for (const tick of scale.ticks) {
    const y = pointY(rect, tick, scale.min, scale.max);
    context.beginPath();
    context.moveTo(rect.x, y);
    context.lineTo(rect.x + rect.width, y);
    context.stroke();
  }
}

function drawTimeLabels(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  samples: SampleForecast[],
  timezoneName: string,
): void {
  if (!samples.length) {
    return;
  }
  const chosen = [samples[0], samples[Math.floor(samples.length / 2)], samples.at(-1)].filter(
    (sample): sample is SampleForecast => Boolean(sample),
  );
  context.fillStyle = "#d0ccc4";
  context.font = "500 14px Avenir Next, Segoe UI, sans-serif";
  chosen.forEach((sample, index) => {
    const x = rect.x + (rect.width * index) / Math.max(1, chosen.length - 1);
    context.textAlign = index === 0 ? "left" : index === chosen.length - 1 ? "right" : "center";
    context.fillText(formatChartTimeLabel(sample.sample.timestampMs, timezoneName), x, rect.y + rect.height + 26);
  });
  context.textAlign = "left";
}

function drawYAxisLabels(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  scale: AxisScale,
  unit: string,
  side: "left" | "right",
): void {
  const digits = axisDigits(scale);
  context.fillStyle = "#d0ccc4";
  context.font = "500 14px Avenir Next, Segoe UI, sans-serif";
  context.textBaseline = "middle";
  context.textAlign = side === "left" ? "right" : "left";
  for (const tick of scale.ticks) {
    const y = pointY(rect, tick, scale.min, scale.max);
    const x = side === "left" ? rect.x - 12 : rect.x + rect.width + 12;
    context.fillText(formatAxisLabel(tick, unit, digits), x, y);
  }
  context.textBaseline = "top";
  context.textAlign = "left";
}

function buildNiceScale(
  minValue: number,
  maxValue: number,
  tickCount: number,
  options: {
    fixedMin?: number;
    fixedMax?: number;
    minimumStep?: number;
  } = {},
): AxisScale {
  const baseMin = options.fixedMin ?? minValue;
  let baseMax = options.fixedMax ?? maxValue;
  if (!Number.isFinite(baseMin) || !Number.isFinite(baseMax)) {
    return { min: 0, max: 1, ticks: [0, 0.5, 1] };
  }
  if (baseMax <= baseMin) {
    baseMax = baseMin + 1;
  }

  const rawRange = baseMax - baseMin;
  let step = niceNumber(rawRange / Math.max(1, tickCount - 1), true);
  if (options.minimumStep) {
    step = Math.max(step, options.minimumStep);
  }

  const scaleMin = options.fixedMin ?? Math.floor(baseMin / step) * step;
  let scaleMax = options.fixedMax ?? Math.ceil(baseMax / step) * step;
  if (scaleMax <= scaleMin) {
    scaleMax = scaleMin + step;
  }

  const ticks: number[] = [];
  for (let value = scaleMin; value <= scaleMax + step * 0.25; value += step) {
    ticks.push(Number(value.toFixed(4)));
  }
  return {
    min: scaleMin,
    max: scaleMax,
    ticks,
  };
}

function niceNumber(value: number, round: boolean): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 1;
  }
  const exponent = Math.floor(Math.log10(value));
  const fraction = value / 10 ** exponent;
  let niceFraction: number;
  if (round) {
    if (fraction < 1.5) {
      niceFraction = 1;
    } else if (fraction < 3) {
      niceFraction = 2;
    } else if (fraction < 7) {
      niceFraction = 5;
    } else {
      niceFraction = 10;
    }
  } else if (fraction <= 1) {
    niceFraction = 1;
  } else if (fraction <= 2) {
    niceFraction = 2;
  } else if (fraction <= 5) {
    niceFraction = 5;
  } else {
    niceFraction = 10;
  }
  return niceFraction * 10 ** exponent;
}

function axisDigits(scale: AxisScale): number {
  const step = scale.ticks.length > 1 ? Math.abs(scale.ticks[1] - scale.ticks[0]) : Math.abs(scale.max - scale.min);
  if (step >= 1) {
    return 0;
  }
  if (step >= 0.1) {
    return 1;
  }
  return 2;
}

function formatAxisLabel(value: number, unit: string, digits: number): string {
  const spacer = unit === "%" || unit.startsWith("°") ? "" : " ";
  return `${value.toFixed(digits)}${spacer}${unit}`;
}

function formatChartTimeLabel(timestampMs: number, timezoneName: string): string {
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: timezoneName,
  }).format(new Date(timestampMs));
}

function projectRoutePoint(
  basemap: Awaited<ReturnType<typeof buildBasemapRaster>>,
  lon: number,
  lat: number,
  rect: ReportImageRect,
  report: ForecastReport,
): { x: number; y: number } {
  if (basemap) {
    const pixel = rasterPixelForLonLat(basemap, lon, lat);
    return {
      x: rect.x + (pixel.x / basemap.canvas.width) * rect.width,
      y: rect.y + (pixel.y / basemap.canvas.height) * rect.height,
    };
  }

  const bounds = report.route.bounds;
  const lonSpan = Math.max(bounds.maxLon - bounds.minLon, 0.0001);
  const latSpan = Math.max(bounds.maxLat - bounds.minLat, 0.0001);
  const usable = 0.84;
  const x = 0.08 + ((lon - bounds.minLon) / lonSpan) * usable;
  const y = 0.08 + ((bounds.maxLat - lat) / latSpan) * usable;
  return {
    x: rect.x + x * rect.width,
    y: rect.y + y * rect.height,
  };
}

function pointX(rect: ReportImageRect, count: number, index: number): number {
  return rect.x + (rect.width * index) / Math.max(1, count - 1);
}

function pointY(rect: ReportImageRect, value: number, minValue: number, maxValue: number): number {
  const span = Math.max(maxValue - minValue, 1);
  const normalized = (value - minValue) / span;
  return rect.y + rect.height - normalized * rect.height;
}

function drawPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  fill: string,
  radius: number,
): void {
  context.save();
  roundRectPath(context, rect.x, rect.y, rect.width, rect.height, radius);
  context.fillStyle = fill;
  context.shadowColor = "rgba(0, 0, 0, 0.36)";
  context.shadowBlur = 24;
  context.shadowOffsetY = 10;
  context.fill();
  context.restore();

  context.save();
  roundRectPath(context, rect.x, rect.y, rect.width, rect.height, radius);
  context.strokeStyle = "rgba(255, 255, 255, 0.08)";
  context.lineWidth = 2;
  context.stroke();
  context.restore();
}

function drawPill(context: CanvasRenderingContext2D, text: string, x: number, y: number): number {
  context.save();
  context.font = "700 18px Avenir Next, Segoe UI, sans-serif";
  const textWidth = context.measureText(text).width;
  const width = textWidth + 24;
  roundRectPath(context, x, y, width, 32, 16);
  context.fillStyle = "rgba(255, 255, 255, 0.18)";
  context.fill();
  context.fillStyle = "#ffffff";
  context.textBaseline = "middle";
  context.fillText(text, x + 12, y + 16);
  context.restore();
  return width;
}

function drawPillRows(
  context: CanvasRenderingContext2D,
  pills: string[],
  x: number,
  y: number,
  maxWidth: number,
  columnGap: number,
  rowGap: number,
): void {
  let cursorX = x;
  let cursorY = y;
  for (const pill of pills) {
    const pillWidth = measurePillWidth(context, pill);
    if (cursorX > x && cursorX + pillWidth > x + maxWidth) {
      cursorX = x;
      cursorY += 32 + rowGap;
    }
    cursorX += drawPill(context, pill, cursorX, cursorY) + columnGap;
  }
}

function measurePillWidth(context: CanvasRenderingContext2D, text: string): number {
  context.save();
  context.font = "700 18px Avenir Next, Segoe UI, sans-serif";
  const width = context.measureText(text).width + 24;
  context.restore();
  return width;
}

function drawWrappedText(
  context: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  maxWidth: number,
  lineHeight: number,
  maxLines: number,
): void {
  wrapTextLines(context, text, maxWidth)
    .slice(0, maxLines)
    .forEach((line, index) => {
    context.fillText(line, x, y + index * lineHeight);
  });
}

function wrapTextLines(
  context: CanvasRenderingContext2D,
  text: string,
  maxWidth: number,
): string[] {
  const words = text.split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  let current = "";
  for (const word of words) {
    const next = current ? `${current} ${word}` : word;
    if (!current || context.measureText(next).width <= maxWidth) {
      current = next;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) {
    lines.push(current);
  }
  return lines.length ? lines : [text];
}

function roundRectPath(
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number,
): void {
  context.beginPath();
  context.moveTo(x + radius, y);
  context.arcTo(x + width, y, x + width, y + height, radius);
  context.arcTo(x + width, y + height, x, y + height, radius);
  context.arcTo(x, y + height, x, y, radius);
  context.arcTo(x, y, x + width, y, radius);
  context.closePath();
}

function clipRoundedRect(context: CanvasRenderingContext2D, rect: ReportImageRect, radius: number): void {
  context.save();
  roundRectPath(context, rect.x, rect.y, rect.width, rect.height, radius);
  context.clip();
}

function insetRect(
  rect: ReportImageRect,
  left: number,
  top: number,
  right: number,
  bottom: number,
): ReportImageRect {
  return {
    x: rect.x + left,
    y: rect.y + top,
    width: rect.width - left - right,
    height: rect.height - top - bottom,
  };
}

function drawMarker(context: CanvasRenderingContext2D, x: number, y: number, fill: string): void {
  context.fillStyle = fill;
  context.strokeStyle = "#000000";
  context.lineWidth = 3;
  context.beginPath();
  context.arc(x, y, 11, 0, Math.PI * 2);
  context.fill();
  context.stroke();
}

function drawArrow(
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  dx: number,
  dy: number,
  length: number,
  color: string,
): void {
  const startX = x - dx * length * 0.5;
  const startY = y - dy * length * 0.5;
  const endX = x + dx * length * 0.5;
  const endY = y + dy * length * 0.5;
  context.strokeStyle = color;
  context.fillStyle = color;
  context.lineWidth = 3;
  context.beginPath();
  context.moveTo(startX, startY);
  context.lineTo(endX, endY);
  context.stroke();

  const angle = Math.atan2(dy, dx);
  const head = 8;
  context.beginPath();
  context.moveTo(endX, endY);
  context.lineTo(endX - head * Math.cos(angle - Math.PI / 6), endY - head * Math.sin(angle - Math.PI / 6));
  context.lineTo(endX - head * Math.cos(angle + Math.PI / 6), endY - head * Math.sin(angle + Math.PI / 6));
  context.closePath();
  context.fill();
}

function canvasToBlob(canvas: HTMLCanvasElement): Promise<Blob | null> {
  return new Promise((resolve) => canvas.toBlob((blob) => resolve(blob), "image/png"));
}
