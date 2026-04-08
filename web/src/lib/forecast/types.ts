export const FORECAST_HORIZON_DAYS = 16;
export const OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast";
export const OPEN_METEO_PROVIDER_ID = "open-meteo";
export const OPEN_METEO_SOURCE_LABEL = "Open-Meteo Forecast API";
export const OPEN_METEO_BATCH_SIZE = 50;
export const OPEN_METEO_HOURLY_FIELDS = [
  "temperature_2m",
  "apparent_temperature",
  "wind_speed_10m",
  "wind_gusts_10m",
  "wind_direction_10m",
  "cloud_cover",
  "precipitation",
  "precipitation_probability",
] as const;

export interface Bounds {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
}

export interface RoutePoint {
  lat: number;
  lon: number;
  elevationM: number | null;
  distanceM: number;
}

export interface RouteData {
  name: string | null;
  points: RoutePoint[];
  totalDistanceM: number;
  totalAscentM: number;
  bounds: Bounds;
}

export interface SamplePoint {
  index: number;
  fraction: number;
  elapsedMs: number;
  timestampMs: number;
  lat: number;
  lon: number;
  elevationM: number | null;
  distanceM: number;
}

export interface HourlyForecast {
  timesMs: number[];
  temperatureC: number[];
  apparentTemperatureC: Array<number | null>;
  windKph: number[];
  windGustKph: Array<number | null>;
  windDirectionDeg: number[];
  cloudCoverPct: number[];
  precipitationMm: number[];
  precipitationProbability: Array<number | null>;
}

export interface SampleForecast {
  sample: SamplePoint;
  temperatureC: number;
  apparentTemperatureC: number | null;
  windKph: number;
  windGustKph: number | null;
  windDirectionDeg: number;
  cloudCoverPct: number;
  precipitationMm: number;
  precipitationProbability: number | null;
}

export interface ForecastReport {
  title: string;
  providerId: string;
  route: RouteData;
  samples: SampleForecast[];
  startTimeMs: number;
  endTimeMs: number;
  durationMs: number;
  timezoneName: string;
  sourceLabel: string;
}

export interface ForecastSummary {
  temperatureMinC: number;
  temperatureMaxC: number;
  windMaxKph: number;
  precipitationTotalMm: number;
  wettestTimeMs: number;
  wettestPrecipitationMm: number;
  wettestProbabilityPct: number | null;
}

export interface ForecastKeyMoment {
  kind: "start" | "coldest" | "windiest" | "wettest" | "finish";
  label: string;
  sample: SampleForecast;
}

export interface BuildForecastInput {
  gpxText: string;
  title?: string;
  start: string;
  duration: string;
  timezoneName?: string;
  sampleMinutes?: number;
  nowMs?: number;
  fetchImpl?: typeof fetch;
}

export interface BuildForecastResult {
  report: ForecastReport;
  summary: ForecastSummary;
  keyMoments: ForecastKeyMoment[];
}
