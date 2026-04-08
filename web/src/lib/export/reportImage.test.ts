import { buildReportImageLayout } from "./reportImage";

describe("report image layout", () => {
  it("creates a square layout with the expected sections", () => {
    const layout = buildReportImageLayout(1600);

    expect(layout.size).toBe(1600);
    expect(layout.header.width).toBeGreaterThan(layout.temperature.width);
    expect(layout.temperature.y).toBe(layout.precipitation.y);
    expect(layout.map.y).toBeGreaterThan(layout.temperature.y);
    expect(layout.wind.y).toBe(layout.map.y);
    expect(layout.elevation.y).toBeGreaterThan(layout.wind.y);
  });
});
