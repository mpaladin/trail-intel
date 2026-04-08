import type {
  ForecastKeyMoment,
  ForecastReport,
  ForecastSummary,
  SampleForecast,
} from "./types";

export function summarizeReport(report: ForecastReport): ForecastSummary {
  if (!report.samples.length) {
    throw new Error("Forecast report has no samples to summarize.");
  }

  const temperatures = report.samples.map((sample) => sample.temperatureC);
  const winds = report.samples.map((sample) => sample.windKph);
  const wettestSample = selectWettestSample(report.samples);

  return {
    temperatureMinC: Math.min(...temperatures),
    temperatureMaxC: Math.max(...temperatures),
    windMaxKph: Math.max(...winds),
    precipitationTotalMm: integratePrecipitation(report.samples),
    wettestTimeMs: wettestSample.sample.timestampMs,
    wettestPrecipitationMm: wettestSample.precipitationMm,
    wettestProbabilityPct: wettestSample.precipitationProbability,
  };
}

export function buildKeyMoments(report: ForecastReport): ForecastKeyMoment[] {
  if (!report.samples.length) {
    return [];
  }

  const coldest = minBy(report.samples, (sample) => [sample.temperatureC, sample.sample.timestampMs]);
  const windiest = minBy(report.samples, (sample) => [-sample.windKph, sample.sample.timestampMs]);
  const wettest = selectWettestSample(report.samples);
  const keyMoments: ForecastKeyMoment[] = [
    { kind: "start", label: "Start", sample: report.samples[0] },
    { kind: "coldest", label: "Coldest", sample: coldest },
    { kind: "windiest", label: "Windiest", sample: windiest },
    { kind: "wettest", label: "Wettest", sample: wettest },
    { kind: "finish", label: "Finish", sample: report.samples.at(-1) as SampleForecast },
  ];

  return keyMoments;
}

export function selectWettestSample(samples: SampleForecast[]): SampleForecast {
  if (!samples.length) {
    throw new Error("Forecast sample list is empty.");
  }

  return minBy(samples, (sample) => [
    -sample.precipitationMm,
    -(sample.precipitationProbability ?? -1),
    sample.sample.timestampMs,
  ]);
}

export function integratePrecipitation(samples: SampleForecast[]): number {
  let totalMm = 0;
  for (let index = 0; index < samples.length - 1; index += 1) {
    const current = samples[index];
    const following = samples[index + 1];
    const hours = (following.sample.timestampMs - current.sample.timestampMs) / 3_600_000;
    totalMm += current.precipitationMm * hours;
  }
  return totalMm;
}

function minBy<T>(values: T[], makeKey: (value: T) => number[]): T {
  let winner = values[0];
  let winnerKey = makeKey(winner);
  for (const value of values.slice(1)) {
    const key = makeKey(value);
    if (compareKey(key, winnerKey) < 0) {
      winner = value;
      winnerKey = key;
    }
  }
  return winner;
}

function compareKey(left: number[], right: number[]): number {
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const leftValue = left[index] ?? 0;
    const rightValue = right[index] ?? 0;
    if (leftValue < rightValue) {
      return -1;
    }
    if (leftValue > rightValue) {
      return 1;
    }
  }
  return 0;
}
