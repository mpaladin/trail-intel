import { ForecastInputError } from "./errors";
import type { Bounds, RouteData, RoutePoint, SamplePoint } from "./types";

const EARTH_RADIUS_M = 6_371_000;

export function parseGpx(gpxText: string): RouteData {
  const parser = new DOMParser();
  const xml = parser.parseFromString(gpxText, "application/xml");
  const parserError = xml.querySelector("parsererror");
  if (parserError) {
    throw new ForecastInputError("Could not parse GPX file.");
  }

  const track = xml.getElementsByTagNameNS("*", "trk")[0];
  const route = xml.getElementsByTagNameNS("*", "rte")[0];
  const routeName = readRouteName(track, route);

  const rawPoints = track ? readTrackPoints(track) : readRoutePoints(route);
  if (!rawPoints.length) {
    throw new ForecastInputError("GPX file has no tracks or routes.");
  }
  if (rawPoints.length < 2) {
    throw new ForecastInputError("GPX file must contain at least two points.");
  }

  let totalDistance = 0;
  let totalAscent = 0;
  let [prevLat, prevLon, prevEle] = rawPoints[0];
  let minLat = prevLat;
  let maxLat = prevLat;
  let minLon = prevLon;
  let maxLon = prevLon;

  const routePoints: RoutePoint[] = [
    {
      lat: prevLat,
      lon: prevLon,
      elevationM: prevEle,
      distanceM: 0,
    },
  ];

  for (const [lat, lon, elevationM] of rawPoints.slice(1)) {
    const segmentDistance = haversineM(prevLat, prevLon, lat, lon);
    totalDistance += segmentDistance;
    if (prevEle !== null && elevationM !== null && elevationM > prevEle) {
      totalAscent += elevationM - prevEle;
    }

    routePoints.push({
      lat,
      lon,
      elevationM,
      distanceM: totalDistance,
    });

    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
    minLon = Math.min(minLon, lon);
    maxLon = Math.max(maxLon, lon);
    prevLat = lat;
    prevLon = lon;
    prevEle = elevationM;
  }

  if (totalDistance <= 0) {
    throw new ForecastInputError("GPX route distance is zero.");
  }

  const bounds: Bounds = { minLat, maxLat, minLon, maxLon };
  return {
    name: routeName,
    points: routePoints,
    totalDistanceM: totalDistance,
    totalAscentM: totalAscent,
    bounds,
  };
}

export function sampleRoute(
  route: RouteData,
  startTimeMs: number,
  durationMs: number,
  sampleMinutes = 10,
): SamplePoint[] {
  if (sampleMinutes <= 0) {
    throw new ForecastInputError("Sample minutes must be greater than zero.");
  }

  const durationMinutes = durationMs / 60_000;
  const requestedSteps = Math.ceil(durationMinutes / sampleMinutes);
  const sampleCount = Math.max(15, Math.min(120, requestedSteps + 1));

  return Array.from({ length: sampleCount }, (_, index) =>
    interpolateSample(route, index / (sampleCount - 1), index, startTimeMs, durationMs),
  );
}

export function haversineM(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const lat1Rad = (lat1 * Math.PI) / 180;
  const lat2Rad = (lat2 * Math.PI) / 180;
  const deltaLat = ((lat2 - lat1) * Math.PI) / 180;
  const deltaLon = ((lon2 - lon1) * Math.PI) / 180;

  const a =
    Math.sin(deltaLat / 2) ** 2 +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(deltaLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return EARTH_RADIUS_M * c;
}

export function lerp(start: number, end: number, ratio: number): number {
  return start + (end - start) * ratio;
}

function readRouteName(track?: Element, route?: Element): string | null {
  const source = track ?? route;
  if (!source) {
    return null;
  }
  const nameElement = source.getElementsByTagNameNS("*", "name")[0];
  const text = nameElement?.textContent?.trim() ?? "";
  return text || null;
}

function readTrackPoints(track: Element): Array<[number, number, number | null]> {
  const segments = Array.from(track.getElementsByTagNameNS("*", "trkseg"));
  const rawPoints: Array<[number, number, number | null]> = [];
  for (const segment of segments) {
    for (const point of Array.from(segment.getElementsByTagNameNS("*", "trkpt"))) {
      rawPoints.push(readPoint(point));
    }
  }
  return rawPoints;
}

function readRoutePoints(route?: Element): Array<[number, number, number | null]> {
  if (!route) {
    return [];
  }
  return Array.from(route.getElementsByTagNameNS("*", "rtept")).map(readPoint);
}

function readPoint(point: Element): [number, number, number | null] {
  const lat = Number.parseFloat(point.getAttribute("lat") ?? "");
  const lon = Number.parseFloat(point.getAttribute("lon") ?? "");
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new ForecastInputError("GPX point is missing valid latitude/longitude.");
  }

  const elevationText = point.getElementsByTagNameNS("*", "ele")[0]?.textContent?.trim() ?? "";
  const elevation = elevationText ? Number.parseFloat(elevationText) : Number.NaN;
  return [lat, lon, Number.isFinite(elevation) ? elevation : null];
}

function interpolateSample(
  route: RouteData,
  fraction: number,
  index: number,
  startTimeMs: number,
  durationMs: number,
): SamplePoint {
  const targetDistance = route.totalDistanceM * fraction;
  const routePoint = interpolateRoutePoint(route, targetDistance);
  const elapsedMs = Math.round(durationMs * fraction);
  return {
    index,
    fraction,
    elapsedMs,
    timestampMs: startTimeMs + elapsedMs,
    lat: routePoint.lat,
    lon: routePoint.lon,
    elevationM: routePoint.elevationM,
    distanceM: targetDistance,
  };
}

function interpolateRoutePoint(route: RouteData, targetDistanceM: number): RoutePoint {
  const distances = route.points.map((point) => point.distanceM);
  const index = bisectLeft(distances, targetDistanceM);
  if (index <= 0) {
    return route.points[0];
  }
  if (index >= route.points.length) {
    return route.points.at(-1) as RoutePoint;
  }

  const left = route.points[index - 1];
  const right = route.points[index];
  const span = right.distanceM - left.distanceM;
  if (span <= 0) {
    return right;
  }

  const ratio = (targetDistanceM - left.distanceM) / span;
  return {
    lat: lerp(left.lat, right.lat, ratio),
    lon: lerp(left.lon, right.lon, ratio),
    elevationM: interpolateOptional(left.elevationM, right.elevationM, ratio),
    distanceM: targetDistanceM,
  };
}

function interpolateOptional(
  start: number | null,
  end: number | null,
  ratio: number,
): number | null {
  if (start === null || end === null) {
    return ratio >= 0.5 ? end : start;
  }
  return lerp(start, end, ratio);
}

function bisectLeft(values: number[], target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid] < target) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return low;
}
