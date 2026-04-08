import sampleRouteGpx from "../../fixtures/sample_route.gpx?raw";
import openMeteoHourly from "../../fixtures/openmeteo-hourly.json";

import { buildForecastReport } from "./engine";

describe("forecast engine", () => {
  it("matches the Python route and summary reference values", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const count = new URL(String(input)).searchParams.get("latitude")?.split(",").length ?? 0;
      const payload = count === 1 ? openMeteoHourly : Array.from({ length: count }, () => openMeteoHourly);
      return {
        ok: true,
        status: 200,
        json: async () => payload,
      } as Response;
    });

    const result = await buildForecastReport({
      gpxText: sampleRouteGpx,
      start: "2026-03-28T08:00",
      timezoneName: "UTC",
      duration: "02:00",
      sampleMinutes: 10,
      nowMs: Date.parse("2026-03-27T00:00:00Z"),
      fetchImpl: fetchImpl as typeof fetch,
    });

    expect(result.report.title).toBe("Sample Loop");
    expect(result.report.route.totalDistanceM).toBeCloseTo(2264.2998095459207, 6);
    expect(result.report.route.totalAscentM).toBe(62);
    expect(result.report.samples).toHaveLength(15);

    expect(result.report.samples[4].sample.timestampMs).toBe(
      Date.parse("2026-03-28T08:34:17.143Z"),
    );
    expect(result.report.samples[4].temperatureC).toBeCloseTo(10.57142857138889, 6);
    expect(result.report.samples[4].apparentTemperatureC).toBeCloseTo(9.142857142777778, 6);
    expect(result.report.samples[4].windDirectionDeg).toBeCloseTo(275.7142857138889, 6);

    expect(result.summary.temperatureMinC).toBe(10);
    expect(result.summary.temperatureMaxC).toBe(12);
    expect(result.summary.windMaxKph).toBe(22);
    expect(result.summary.precipitationTotalMm).toBeCloseTo(0.3, 6);
    expect(result.summary.wettestTimeMs).toBe(Date.parse("2026-03-28T10:00:00Z"));
    expect(result.summary.wettestPrecipitationMm).toBe(0.3);
    expect(result.summary.wettestProbabilityPct).toBe(50);
  });
});
