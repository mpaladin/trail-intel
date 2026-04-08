import type { SampleForecast } from "../forecast/types";

export interface RouteArrow {
  sample: SampleForecast;
  directionToDeg: number;
  dx: number;
  dy: number;
}

export function routeArrowIndices(sampleCount: number): number[] {
  if (sampleCount <= 0) {
    return [];
  }
  const target = Math.max(6, Math.min(22, Math.round(sampleCount * 0.45)));
  const raw = Array.from({ length: target }, (_, index) =>
    (index * (sampleCount - 1)) / Math.max(1, target - 1),
  );
  return [...new Set(raw.map((value) => Math.round(value)))].sort((left, right) => left - right);
}

export function directionToRouteVector(directionFromDeg: number): { dx: number; dy: number } {
  const directionToDeg = (directionFromDeg + 180) % 360;
  const radians = (directionToDeg * Math.PI) / 180;
  return {
    dx: Math.sin(radians),
    dy: Math.cos(radians),
  };
}

export function directionToArrowRotation(directionFromDeg: number): number {
  return (directionFromDeg + 180) % 360;
}

export function buildRouteArrows(samples: SampleForecast[]): RouteArrow[] {
  if (!samples.length) {
    return [];
  }
  return routeArrowIndices(samples.length).map((index) => {
    const sample = samples[index];
    const directionToDeg = directionToArrowRotation(sample.windDirectionDeg);
    const { dx, dy } = directionToRouteVector(sample.windDirectionDeg);
    return {
      sample,
      directionToDeg,
      dx,
      dy,
    };
  });
}
