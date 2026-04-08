import { buildRouteArrows, directionToArrowRotation, directionToRouteVector, routeArrowIndices } from "./overlay";
import type { SampleForecast } from "../forecast/types";

function makeSample(index: number): SampleForecast {
  return {
    sample: {
      index,
      fraction: index / 14,
      elapsedMs: index * 600_000,
      timestampMs: Date.parse("2026-03-28T08:00:00Z") + index * 600_000,
      lat: 47.37 + index * 0.001,
      lon: 8.54 + index * 0.001,
      elevationM: 400 + index,
      distanceM: index * 1000,
    },
    temperatureC: 10,
    apparentTemperatureC: 9,
    windKph: 20,
    windGustKph: 25,
    windDirectionDeg: 270,
    cloudCoverPct: 30,
    precipitationMm: 0.1,
    precipitationProbability: 40,
  };
}

describe("map overlay helpers", () => {
  it("returns no arrows for an empty route", () => {
    expect(routeArrowIndices(0)).toEqual([]);
    expect(buildRouteArrows([])).toEqual([]);
  });

  it("matches the Python route arrow sampling pattern", () => {
    expect(routeArrowIndices(15)).toEqual([0, 2, 5, 7, 9, 12, 14]);
  });

  it("converts wind-from degrees into route vectors and rotations", () => {
    const vector = directionToRouteVector(0);
    expect(vector.dx).toBeCloseTo(0, 6);
    expect(vector.dy).toBeCloseTo(-1, 6);
    expect(directionToArrowRotation(270)).toBe(90);
  });

  it("builds sparse route arrows from samples", () => {
    const arrows = buildRouteArrows(Array.from({ length: 15 }, (_, index) => makeSample(index)));
    expect(arrows).toHaveLength(7);
    expect(arrows[0].sample.sample.index).toBe(0);
    expect(arrows.at(-1)?.sample.sample.index).toBe(14);
  });
});
