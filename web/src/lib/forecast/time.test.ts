import { ForecastInputError } from "./errors";
import { parseDuration, parseStartTime, validateForecastWindow } from "./time";

describe("time helpers", () => {
  it("parses ISO local times with an explicit timezone", () => {
    const parsed = parseStartTime("2026-03-28T08:00", "UTC");

    expect(parsed.timezoneName).toBe("UTC");
    expect(parsed.timestampMs).toBe(Date.parse("2026-03-28T08:00:00Z"));
  });

  it("rejects malformed durations", () => {
    expect(() => parseDuration("1h30")).toThrow(ForecastInputError);
    expect(() => parseDuration("00:99")).toThrow(ForecastInputError);
  });

  it("rejects forecast windows outside the provider horizon", () => {
    expect(() =>
      validateForecastWindow(
        Date.parse("2026-03-10T08:00:00Z"),
        60 * 60 * 1000,
        Date.parse("2026-03-11T08:00:00Z"),
      ),
    ).toThrow("Start time must be in the future.");

    expect(() =>
      validateForecastWindow(
        Date.parse("2026-03-28T08:00:00Z"),
        4 * 24 * 60 * 60 * 1000,
        Date.parse("2026-03-14T08:00:00Z"),
      ),
    ).toThrow("Ride end exceeds the forecast horizon of 16 days.");
  });
});
