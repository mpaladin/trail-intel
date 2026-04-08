import {
  formatDateTime,
  formatDistanceKm,
  formatDuration,
  formatFullDateTime,
  formatNumber,
  formatPercent,
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

const IMAGE_SIZE = 1600;
const OUTER = 44;
const GAP = 24;
const COLUMN_GAP = 28;
const HEADER_HEIGHT = 190;
const TOP_PANEL_HEIGHT = 320;
const RIGHT_PANEL_HEIGHT = 410;

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

  context.fillStyle = "#ffffff";
  context.textBaseline = "top";
  context.font = "700 58px Iowan Old Style, Georgia, serif";
  drawWrappedText(context, report.title, rect.x + 36, rect.y + 26, rect.width - 72, 64, 2);

  context.font = "500 24px Avenir Next, Segoe UI, sans-serif";
  context.fillText(dateLabel, rect.x + 38, rect.y + 128);

  let pillX = rect.x + 36;
  const pillY = rect.y + rect.height - 56;
  for (const pill of statPills) {
    pillX += drawPill(context, pill, pillX, pillY) + 12;
  }
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
  drawLineChart(
    context,
    insetRect(rect, 30, 112, 30, 40),
    report.samples,
    report.timezoneName,
    [
      { color: "#66b9ff", values: report.samples.map((sample) => sample.temperatureC) },
      {
        color: "#ffb03a",
        values: report.samples.map((sample) => sample.apparentTemperatureC ?? sample.temperatureC),
      },
    ],
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

  const plot = insetRect(rect, 30, 112, 30, 40);
  const precipitation = report.samples.map((sample) => sample.precipitationMm);
  const chance = report.samples.map((sample) => sample.precipitationProbability ?? 0);
  const cloud = report.samples.map((sample) => sample.cloudCoverPct);
  const precipMax = Math.max(0.4, ...precipitation);
  drawChartGrid(context, plot, 4);

  precipitation.forEach((value, index) => {
    const x = pointX(plot, precipitation.length, index);
    const barWidth = Math.max(10, plot.width / Math.max(16, precipitation.length * 1.8));
    const barHeight = (value / precipMax) * plot.height * 0.58;
    context.fillStyle = "rgba(255, 174, 56, 0.75)";
    roundRectPath(context, x - barWidth / 2, plot.y + plot.height - barHeight, barWidth, barHeight, 10);
    context.fill();
  });

  drawSeries(context, plot, chance, 0, 100, "#73bdf5", 3.5);
  drawSeries(context, plot, cloud, 0, 100, "#a7abb3", 2.8);
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
  drawLineChart(
    context,
    insetRect(rect, 30, 112, 30, 44),
    report.samples,
    report.timezoneName,
    [
      { color: "#8fc2df", values: report.samples.map((sample) => sample.windKph) },
      { color: "#ffad2f", values: report.samples.map((sample) => sample.windGustKph ?? sample.windKph) },
    ],
  );
}

function drawElevationPanel(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  report: ForecastReport,
): void {
  drawChartPanel(context, rect, "Elevation", "Route profile plus quick ride summary.");
  const plot = insetRect(rect, 30, 112, 30, 90);
  const values = report.samples.map((sample) => sample.sample.elevationM ?? 0);
  drawAreaChart(context, plot, values, "#d1912b", "#7e551a");
  drawTimeLabels(context, plot, report.samples, report.timezoneName);

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
  context.font = "500 18px Avenir Next, Segoe UI, sans-serif";
  context.fillText(subtitle, rect.x + 26, rect.y + 68, rect.width - 52);
}

function drawLineChart(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  samples: SampleForecast[],
  timezoneName: string,
  series: Array<{ color: string; values: number[] }>,
): void {
  drawChartGrid(context, rect, 4);
  const allValues = series.flatMap((entry) => entry.values);
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const span = Math.max(maxValue - minValue, 1);
  const paddedMin = minValue - span * 0.12;
  const paddedMax = maxValue + span * 0.12;
  for (const entry of series) {
    drawSeries(context, rect, entry.values, paddedMin, paddedMax, entry.color, 4);
  }
  drawTimeLabels(context, rect, samples, timezoneName);
}

function drawAreaChart(
  context: CanvasRenderingContext2D,
  rect: ReportImageRect,
  values: number[],
  lineColor: string,
  fillColor: string,
): void {
  drawChartGrid(context, rect, 4);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = Math.max(maxValue - minValue, 100);
  const paddedMin = minValue - span * 0.12;
  const paddedMax = maxValue + span * 0.12;
  context.beginPath();
  values.forEach((value, index) => {
    const x = pointX(rect, values.length, index);
    const y = pointY(rect, value, paddedMin, paddedMax);
    if (index === 0) {
      context.moveTo(x, rect.y + rect.height);
      context.lineTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.lineTo(rect.x + rect.width, rect.y + rect.height);
  context.closePath();
  context.fillStyle = `${fillColor}aa`;
  context.fill();
  drawSeries(context, rect, values, paddedMin, paddedMax, lineColor, 3.5);
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

function drawChartGrid(context: CanvasRenderingContext2D, rect: ReportImageRect, rows: number): void {
  context.strokeStyle = "rgba(98, 98, 98, 0.45)";
  context.lineWidth = 1;
  for (let index = 0; index < rows; index += 1) {
    const y = rect.y + (rect.height * index) / Math.max(1, rows - 1);
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
  context.font = "500 16px Avenir Next, Segoe UI, sans-serif";
  context.textAlign = "center";
  chosen.forEach((sample, index) => {
    const x = rect.x + (rect.width * index) / Math.max(1, chosen.length - 1);
    context.fillText(formatDateTime(sample.sample.timestampMs, timezoneName), x, rect.y + rect.height + 28);
  });
  context.textAlign = "left";
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
  context.font = "700 20px Avenir Next, Segoe UI, sans-serif";
  const textWidth = context.measureText(text).width;
  const width = textWidth + 28;
  roundRectPath(context, x, y, width, 34, 17);
  context.fillStyle = "rgba(255, 255, 255, 0.18)";
  context.fill();
  context.fillStyle = "#ffffff";
  context.textBaseline = "middle";
  context.fillText(text, x + 14, y + 17);
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
  const words = text.split(/\s+/);
  const lines: string[] = [];
  let current = "";
  for (const word of words) {
    const next = current ? `${current} ${word}` : word;
    if (context.measureText(next).width <= maxWidth || !current) {
      current = next;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) {
    lines.push(current);
  }

  lines.slice(0, maxLines).forEach((line, index) => {
    context.fillText(line, x, y + index * lineHeight);
  });
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
