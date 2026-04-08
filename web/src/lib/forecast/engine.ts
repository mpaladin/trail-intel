import { alignForecasts } from "./align";
import { parseGpx, sampleRoute } from "./gpx";
import { summarizeReport, buildKeyMoments } from "./summary";
import { parseStartTime, parseDuration, validateForecastWindow } from "./time";
import {
  OPEN_METEO_PROVIDER_ID,
  OPEN_METEO_SOURCE_LABEL,
  type BuildForecastInput,
  type BuildForecastResult,
} from "./types";
import { fetchOpenMeteoHourly } from "./weather";

export async function buildForecastReport(
  input: BuildForecastInput,
): Promise<BuildForecastResult> {
  const start = parseStartTime(input.start, input.timezoneName);
  const durationMs = parseDuration(input.duration);
  validateForecastWindow(start.timestampMs, durationMs, input.nowMs);

  const route = parseGpx(input.gpxText);
  const samples = sampleRoute(route, start.timestampMs, durationMs, input.sampleMinutes ?? 10);
  const forecasts = await fetchOpenMeteoHourly(samples, input.fetchImpl);
  const alignedSamples = alignForecasts(samples, forecasts);
  const title = resolveForecastTitle(input.title, route.name);

  const report = {
    title,
    providerId: OPEN_METEO_PROVIDER_ID,
    route,
    samples: alignedSamples,
    startTimeMs: start.timestampMs,
    endTimeMs: start.timestampMs + durationMs,
    durationMs,
    timezoneName: start.timezoneName,
    sourceLabel: OPEN_METEO_SOURCE_LABEL,
  };

  return {
    report,
    summary: summarizeReport(report),
    keyMoments: buildKeyMoments(report),
  };
}

function resolveForecastTitle(title?: string, routeName?: string | null): string {
  const candidates = [title, routeName, "Route Forecast"];
  for (const candidate of candidates) {
    const cleaned = candidate?.trim();
    if (cleaned) {
      return cleaned;
    }
  }
  return "Route Forecast";
}
