import sampleRouteGpx from "../../fixtures/sample_route.gpx?raw";

import { parseGpx, sampleRoute } from "./gpx";

describe("gpx", () => {
  it("parses track points and route metrics", () => {
    const route = parseGpx(sampleRouteGpx);

    expect(route.name).toBe("Sample Loop");
    expect(route.points).toHaveLength(6);
    expect(route.totalDistanceM).toBeCloseTo(2264.2998095459207, 6);
    expect(route.totalAscentM).toBe(62);
  });

  it("samples the route with the Python-compatible clamp behavior", () => {
    const route = parseGpx(sampleRouteGpx);

    const samples = sampleRoute(route, Date.parse("2026-03-28T08:00:00Z"), 45 * 60 * 1000, 10);

    expect(samples).toHaveLength(15);
    expect(samples[0].fraction).toBe(0);
    expect(samples.at(-1)?.fraction).toBe(1);
  });
});
