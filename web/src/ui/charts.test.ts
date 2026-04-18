import type { ForecastReport } from "../lib/forecast/types";

let renderForecastChartsSection: typeof import("./charts").renderForecastChartsSection;
let renderRouteOverviewFallback: typeof import("./charts").renderRouteOverviewFallback;

function buildReport(): ForecastReport {
  return {
    title: "Sample Route",
    providerId: "open-meteo",
    startTimeMs: Date.parse("2026-03-28T08:00:00Z"),
    endTimeMs: Date.parse("2026-03-28T10:00:00Z"),
    durationMs: 2 * 60 * 60 * 1000,
    timezoneName: "UTC",
    sourceLabel: "Open-Meteo Forecast API",
    route: {
      name: "Sample Route",
      totalDistanceM: 12000,
      totalAscentM: 480,
      bounds: {
        minLat: 47.37,
        maxLat: 47.4,
        minLon: 8.54,
        maxLon: 8.6,
      },
      points: [
        { lat: 47.37, lon: 8.54, elevationM: 410, distanceM: 0 },
        { lat: 47.385, lon: 8.57, elevationM: 520, distanceM: 6000 },
        { lat: 47.4, lon: 8.6, elevationM: 460, distanceM: 12000 },
      ],
    },
    samples: [
      {
        sample: {
          index: 0,
          fraction: 0,
          elapsedMs: 0,
          timestampMs: Date.parse("2026-03-28T08:00:00Z"),
          lat: 47.37,
          lon: 8.54,
          elevationM: 410,
          distanceM: 0,
        },
        temperatureC: 7,
        apparentTemperatureC: 6,
        windKph: 12,
        windGustKph: 18,
        windDirectionDeg: 280,
        cloudCoverPct: 20,
        precipitationMm: 0,
        precipitationProbability: 15,
      },
      {
        sample: {
          index: 1,
          fraction: 0.5,
          elapsedMs: 60 * 60 * 1000,
          timestampMs: Date.parse("2026-03-28T09:00:00Z"),
          lat: 47.385,
          lon: 8.57,
          elevationM: 520,
          distanceM: 6000,
        },
        temperatureC: 10,
        apparentTemperatureC: 9,
        windKph: 18,
        windGustKph: 24,
        windDirectionDeg: 300,
        cloudCoverPct: 45,
        precipitationMm: 0.6,
        precipitationProbability: 40,
      },
      {
        sample: {
          index: 2,
          fraction: 1,
          elapsedMs: 2 * 60 * 60 * 1000,
          timestampMs: Date.parse("2026-03-28T10:00:00Z"),
          lat: 47.4,
          lon: 8.6,
          elevationM: 460,
          distanceM: 12000,
        },
        temperatureC: 12,
        apparentTemperatureC: 11,
        windKph: 20,
        windGustKph: 27,
        windDirectionDeg: 320,
        cloudCoverPct: 55,
        precipitationMm: 0.2,
        precipitationProbability: 30,
      },
    ],
  };
}

beforeAll(async () => {
  vi.stubGlobal(
    "matchMedia",
    vi.fn().mockImplementation(() => ({
      matches: false,
      media: "",
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  );

  ({ renderForecastChartsSection, renderRouteOverviewFallback } = await import("./charts"));
});

afterAll(() => {
  vi.unstubAllGlobals();
});

describe("forecast charts", () => {
  it("renders six uPlot chart cards", () => {
    const html = renderForecastChartsSection(buildReport());

    expect(html).toContain("uPlot Forecast Charts");
    expect(html).toContain("forecast-chart-hover");
    expect(html).toContain("chart-temperature");
    expect(html).toContain("chart-feels-like");
    expect(html).toContain("chart-precipitation");
    expect(html).toContain("chart-cloud-cover");
    expect(html).toContain("chart-wind");
    expect(html).toContain("chart-elevation");
  });

  it("renders the static route overview fallback svg", () => {
    const html = renderRouteOverviewFallback(buildReport());

    expect(html).toContain("<svg");
    expect(html).toContain("Route overview");
  });
});
