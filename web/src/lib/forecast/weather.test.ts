import openMeteoHourly from "../../fixtures/openmeteo-hourly.json";

import { fetchOpenMeteoHourly, parseOpenMeteoPayload } from "./weather";
import type { SamplePoint } from "./types";

function makeSample(index: number): SamplePoint {
  const start = Date.parse("2026-03-28T08:00:00Z");
  return {
    index,
    fraction: index / 60,
    elapsedMs: index * 10 * 60 * 1000,
    timestampMs: start + index * 10 * 60 * 1000,
    lat: 47.37 + index * 0.001,
    lon: 8.54 + index * 0.001,
    elevationM: 400 + index,
    distanceM: index * 1000,
  };
}

describe("weather client", () => {
  it("parses a single Open-Meteo payload", () => {
    const forecast = parseOpenMeteoPayload(openMeteoHourly);

    expect(forecast.temperatureC[0]).toBe(10);
    expect(forecast.apparentTemperatureC[1]).toBe(10);
    expect(forecast.windGustKph[2]).toBe(32);
    expect(forecast.windDirectionDeg[1]).toBe(280);
    expect(forecast.cloudCoverPct[0]).toBe(35);
  });

  it("batches requests like the Python client", async () => {
    const requestsSeen: string[] = [];
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requestsSeen.push(url);
      const count = new URL(url).searchParams.get("latitude")?.split(",").length ?? 0;
      const payload = count === 1 ? openMeteoHourly : Array.from({ length: count }, () => openMeteoHourly);
      return {
        ok: true,
        status: 200,
        json: async () => payload,
      } as Response;
    });

    const forecasts = await fetchOpenMeteoHourly(
      Array.from({ length: 55 }, (_, index) => makeSample(index)),
      fetchImpl as typeof fetch,
      "https://example.com/forecast",
      50,
    );

    expect(requestsSeen).toHaveLength(2);
    expect(forecasts).toHaveLength(55);
    expect(new URL(requestsSeen[0]).searchParams.get("timezone")).toBe("GMT");
  });
});
