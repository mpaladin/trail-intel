import { Temporal } from "@js-temporal/polyfill";

import { WeatherApiError } from "./errors";
import {
  OPEN_METEO_BATCH_SIZE,
  OPEN_METEO_HOURLY_FIELDS,
  OPEN_METEO_URL,
  type HourlyForecast,
  type SamplePoint,
} from "./types";

type JsonValue = null | boolean | number | string | JsonObject | JsonValue[];
interface JsonObject {
  [key: string]: JsonValue;
}

export async function fetchOpenMeteoHourly(
  samples: SamplePoint[],
  fetchImpl: typeof fetch = fetch,
  baseUrl = OPEN_METEO_URL,
  batchSize = OPEN_METEO_BATCH_SIZE,
): Promise<HourlyForecast[]> {
  if (!samples.length) {
    return [];
  }

  const startUtcMs = Math.min(...samples.map((sample) => sample.timestampMs));
  const endUtcMs = Math.max(...samples.map((sample) => sample.timestampMs));
  const startDate = utcDateString(startUtcMs);
  const endDate = utcDateString(endUtcMs + 60 * 60 * 1000);

  const allForecasts: HourlyForecast[] = [];
  for (const batch of chunked(samples, batchSize)) {
    const params = new URLSearchParams({
      latitude: batch.map((sample) => sample.lat.toFixed(6)).join(","),
      longitude: batch.map((sample) => sample.lon.toFixed(6)).join(","),
      elevation: batch
        .map((sample) =>
          sample.elevationM === null ? "nan" : sample.elevationM.toFixed(1),
        )
        .join(","),
      hourly: OPEN_METEO_HOURLY_FIELDS.join(","),
      timezone: "GMT",
      wind_speed_unit: "kmh",
      start_date: startDate,
      end_date: endDate,
    });

    const response = await fetchImpl(`${baseUrl}?${params.toString()}`);
    if (!response.ok) {
      throw new WeatherApiError(`Weather API request failed with HTTP ${response.status}.`);
    }

    const payload = (await response.json()) as JsonValue;
    if (isJsonObject(payload) && payload.error) {
      const reason = typeof payload.reason === "string" ? payload.reason : "unknown error";
      throw new WeatherApiError(`Weather API error: ${reason}`);
    }

    const payloads = Array.isArray(payload) ? payload : [payload];
    if (payloads.length !== batch.length) {
      throw new WeatherApiError(
        "Open-Meteo returned an unexpected number of forecast payloads.",
      );
    }

    for (const entry of payloads) {
      allForecasts.push(parseOpenMeteoPayload(entry));
    }
  }

  return allForecasts;
}

export function parseOpenMeteoPayload(payload: JsonValue): HourlyForecast {
  if (!isJsonObject(payload)) {
    throw new WeatherApiError("Weather API response is missing hourly data.");
  }

  const hourly = payload.hourly;
  if (!isJsonObject(hourly)) {
    throw new WeatherApiError("Weather API response is missing hourly data.");
  }

  const rawTimes = readNumberlessStringArray(hourly.time, "time");
  const temperature = readNumberArray(hourly.temperature_2m, "temperature_2m");
  const apparentTemperature = readNullableNumberArray(
    hourly.apparent_temperature,
    "apparent_temperature",
  );
  const wind = readNumberArray(hourly.wind_speed_10m, "wind_speed_10m");
  const windGust = readNullableNumberArray(hourly.wind_gusts_10m, "wind_gusts_10m");
  const windDirection = readNumberArray(hourly.wind_direction_10m, "wind_direction_10m");
  const cloudCover = readNumberArray(hourly.cloud_cover, "cloud_cover");
  const precipitation = readNumberArray(hourly.precipitation, "precipitation");
  const precipitationProbability = readNullableNumberArray(
    hourly.precipitation_probability,
    "precipitation_probability",
  );

  const timesMs = rawTimes.map(parseUtcDateTime);
  const lengths = new Set([
    timesMs.length,
    temperature.length,
    apparentTemperature.length,
    wind.length,
    windGust.length,
    windDirection.length,
    cloudCover.length,
    precipitation.length,
    precipitationProbability.length,
  ]);
  if (lengths.size !== 1) {
    throw new WeatherApiError("Weather API hourly arrays are different lengths.");
  }

  return {
    timesMs,
    temperatureC: temperature,
    apparentTemperatureC: apparentTemperature,
    windKph: wind,
    windGustKph: windGust,
    windDirectionDeg: windDirection.map((value) => ((value % 360) + 360) % 360),
    cloudCoverPct: cloudCover,
    precipitationMm: precipitation,
    precipitationProbability,
  };
}

function parseUtcDateTime(value: string): number {
  return Number(
    Temporal.PlainDateTime.from(value).toZonedDateTime("UTC").epochMilliseconds,
  );
}

function utcDateString(timestampMs: number): string {
  return new Date(timestampMs).toISOString().slice(0, 10);
}

function chunked<T>(values: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function isJsonObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readNumberlessStringArray(value: JsonValue, fieldName: string): string[] {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
    throw new WeatherApiError(`Weather API response is missing field ${fieldName}.`);
  }
  return value as string[];
}

function readNumberArray(value: JsonValue, fieldName: string): number[] {
  if (!Array.isArray(value)) {
    throw new WeatherApiError(`Weather API response is missing field ${fieldName}.`);
  }

  return value.map((entry) => {
    const number = typeof entry === "number" ? entry : Number(entry);
    if (!Number.isFinite(number)) {
      throw new WeatherApiError(`Weather API response is missing field ${fieldName}.`);
    }
    return number;
  });
}

function readNullableNumberArray(value: JsonValue, fieldName: string): Array<number | null> {
  if (!Array.isArray(value)) {
    throw new WeatherApiError(`Weather API response is missing field ${fieldName}.`);
  }

  return value.map((entry) => {
    if (entry === null) {
      return null;
    }
    const number = typeof entry === "number" ? entry : Number(entry);
    if (!Number.isFinite(number)) {
      throw new WeatherApiError(`Weather API response is missing field ${fieldName}.`);
    }
    return number;
  });
}
