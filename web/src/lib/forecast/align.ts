import { WeatherApiError } from "./errors";
import type { HourlyForecast, SampleForecast, SamplePoint } from "./types";
import { lerp } from "./gpx";

export function alignForecasts(
  samples: SamplePoint[],
  forecasts: HourlyForecast[],
): SampleForecast[] {
  if (samples.length !== forecasts.length) {
    throw new WeatherApiError("Sample count does not match forecast count.");
  }

  return samples.map((sample, index) => {
    const forecast = forecasts[index];
    const hourIndex = containingHourIndex(forecast.timesMs, sample.timestampMs);
    const nextIndex = Math.min(hourIndex + 1, forecast.timesMs.length - 1);

    const lowerTimeMs = forecast.timesMs[hourIndex];
    const upperTimeMs = forecast.timesMs[nextIndex];
    const ratio =
      upperTimeMs === lowerTimeMs
        ? 0
        : (sample.timestampMs - lowerTimeMs) / (upperTimeMs - lowerTimeMs);

    return {
      sample,
      temperatureC: lerp(forecast.temperatureC[hourIndex], forecast.temperatureC[nextIndex], ratio),
      apparentTemperatureC: lerpNullable(
        forecast.apparentTemperatureC[hourIndex],
        forecast.apparentTemperatureC[nextIndex],
        ratio,
      ),
      windKph: lerp(forecast.windKph[hourIndex], forecast.windKph[nextIndex], ratio),
      windGustKph: lerpNullable(
        forecast.windGustKph[hourIndex],
        forecast.windGustKph[nextIndex],
        ratio,
      ),
      windDirectionDeg: circularLerp(
        forecast.windDirectionDeg[hourIndex],
        forecast.windDirectionDeg[nextIndex],
        ratio,
      ),
      cloudCoverPct: lerp(
        forecast.cloudCoverPct[hourIndex],
        forecast.cloudCoverPct[nextIndex],
        ratio,
      ),
      precipitationMm: forecast.precipitationMm[hourIndex],
      precipitationProbability: forecast.precipitationProbability[hourIndex],
    };
  });
}

export function containingHourIndex(timesMs: number[], timestampMs: number): number {
  if (!timesMs.length) {
    throw new WeatherApiError("Forecast has no time samples.");
  }
  if (timestampMs < timesMs[0]) {
    throw new WeatherApiError("Forecast data does not cover the ride start.");
  }

  let low = 0;
  let high = timesMs.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (timestampMs < timesMs[mid]) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }

  const index = low - 1;
  if (index < 0) {
    throw new WeatherApiError("Forecast data does not cover the ride start.");
  }
  if (index >= timesMs.length) {
    throw new WeatherApiError("Forecast data does not cover the ride end.");
  }
  return index;
}

export function circularLerp(start: number, end: number, ratio: number): number {
  const delta = ((end - start + 540) % 360) - 180;
  return (start + delta * ratio + 360) % 360;
}

function lerpNullable(start: number | null, end: number | null, ratio: number): number | null {
  if (start === null || end === null) {
    return ratio >= 0.5 ? end : start;
  }
  return lerp(start, end, ratio);
}
