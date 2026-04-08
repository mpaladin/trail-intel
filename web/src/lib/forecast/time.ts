import { Temporal } from "@js-temporal/polyfill";

import { ForecastInputError } from "./errors";
import { FORECAST_HORIZON_DAYS } from "./types";

const OFFSET_RE = /(?:[zZ]|[+\-]\d{2}:\d{2})$/;

export interface ParsedStartTime {
  timestampMs: number;
  timezoneName: string;
}

export function parseDuration(value: string): number {
  const parts = value.split(":");
  if (parts.length !== 2 && parts.length !== 3) {
    throw new ForecastInputError("Duration must be in HH:MM or HH:MM:SS format.");
  }

  const numbers = parts.map((part) => {
    const number = Number.parseInt(part, 10);
    if (Number.isNaN(number)) {
      throw new ForecastInputError("Duration contains non-numeric parts.");
    }
    return number;
  });

  const [hours, minutes, seconds = 0] = numbers;
  if (hours < 0 || minutes < 0 || seconds < 0) {
    throw new ForecastInputError("Duration cannot be negative.");
  }
  if (minutes >= 60 || seconds >= 60) {
    throw new ForecastInputError("Duration minutes and seconds must be below 60.");
  }

  const durationMs = ((hours * 60 + minutes) * 60 + seconds) * 1000;
  if (durationMs <= 0) {
    throw new ForecastInputError("Duration must be greater than zero.");
  }
  return durationMs;
}

export function resolveTimezoneName(timezoneName?: string): string {
  const candidate = timezoneName?.trim() || Intl.DateTimeFormat().resolvedOptions().timeZone;
  if (!candidate) {
    throw new ForecastInputError(
      "Could not determine the local timezone. Enter an IANA timezone name.",
    );
  }

  try {
    Temporal.Now.instant().toZonedDateTimeISO(candidate);
  } catch {
    throw new ForecastInputError(`Unknown timezone '${candidate}'. Use an IANA timezone name.`);
  }

  return candidate;
}

export function parseStartTime(value: string, timezoneName?: string): ParsedStartTime {
  const normalized = value.trim();
  if (!normalized) {
    throw new ForecastInputError("Start time must be a valid ISO8601 datetime.");
  }

  if (OFFSET_RE.test(normalized)) {
    try {
      const instant = Temporal.Instant.from(normalized.replace("Z", "z"));
      return {
        timestampMs: Number(instant.epochMilliseconds),
        timezoneName: resolveTimezoneName(timezoneName),
      };
    } catch {
      throw new ForecastInputError("Start time must be a valid ISO8601 datetime.");
    }
  }

  const resolvedTimezoneName = resolveTimezoneName(timezoneName);
  try {
    const plainDateTime = Temporal.PlainDateTime.from(normalized);
    const zonedDateTime = plainDateTime.toZonedDateTime(resolvedTimezoneName);
    return {
      timestampMs: Number(zonedDateTime.epochMilliseconds),
      timezoneName: resolvedTimezoneName,
    };
  } catch {
    throw new ForecastInputError("Start time must be a valid ISO8601 datetime.");
  }
}

export function validateForecastWindow(
  startTimeMs: number,
  durationMs: number,
  nowMs = Date.now(),
  horizonDays = FORECAST_HORIZON_DAYS,
): void {
  if (startTimeMs < nowMs) {
    throw new ForecastInputError("Start time must be in the future.");
  }

  const horizonMs = horizonDays * 24 * 60 * 60 * 1000;
  if (startTimeMs + durationMs > nowMs + horizonMs) {
    throw new ForecastInputError(
      `Ride end exceeds the forecast horizon of ${horizonDays} days.`,
    );
  }
}
