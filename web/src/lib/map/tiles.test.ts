import { chooseZoom, lonLatToTile, padLonLatBounds, worldPixelFromLonLat } from "./tiles";

describe("map tiles", () => {
  it("matches the Python padded bounds and zoom for the sample route", () => {
    const padded = padLonLatBounds({
      minLon: 8.5417,
      maxLon: 8.5614,
      minLat: 47.3769,
      maxLat: 47.3921,
    });

    expect(padded.minLon).toBeCloseTo(8.5217, 4);
    expect(padded.maxLon).toBeCloseTo(8.5814, 4);
    expect(padded.minLat).toBeCloseTo(47.3569, 4);
    expect(padded.maxLat).toBeCloseTo(47.4121, 4);
    expect(chooseZoom(padded, 20)).toBe(14);

    expect(lonLatToTile(padded.minLon, padded.maxLat, 14)).toEqual({ x: 8579, y: 5735 });
    expect(lonLatToTile(padded.maxLon, padded.minLat, 14)).toEqual({ x: 8582, y: 5738 });
  });

  it("projects world pixels monotonically", () => {
    const a = worldPixelFromLonLat(8.5417, 47.3769, 14);
    const b = worldPixelFromLonLat(8.5614, 47.3921, 14);

    expect(b.x).toBeGreaterThan(a.x);
    expect(b.y).toBeLessThan(a.y);
  });
});
