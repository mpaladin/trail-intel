import L from "leaflet";

import type { ForecastReport } from "../lib/forecast/types";
import { buildRouteArrows } from "../lib/map/overlay";
import { MAP_ATTRIBUTION, MAP_TILE_URL, padLonLatBounds } from "../lib/map/tiles";
import { renderRouteOverviewFallback } from "./charts";

let activeMap: L.Map | null = null;
let activeTileLayer: L.TileLayer | null = null;

export function renderForecastMapCard(report: ForecastReport): string {
  return `
    <section class="panel">
      <div class="panel-head">
        <div>
          <p class="section-tag">Route</p>
          <h2>OpenStreetMap Overview</h2>
        </div>
        <p class="section-copy">Pan, zoom, and inspect the route over an OpenStreetMap-style basemap.</p>
      </div>
      <div class="map-stage">
        <div id="forecast-map" class="interactive-map" role="img" aria-label="Interactive route map"></div>
        <div id="forecast-map-fallback" class="map-fallback" hidden>
          ${renderRouteOverviewFallback(report)}
        </div>
      </div>
      <p id="forecast-map-note" class="map-note">Loading OpenStreetMap-style basemap tiles and route overlays…</p>
    </section>
  `;
}

export async function mountForecastMap(report: ForecastReport): Promise<void> {
  teardownForecastMap();

  const container = document.querySelector<HTMLDivElement>("#forecast-map");
  const fallback = document.querySelector<HTMLDivElement>("#forecast-map-fallback");
  const note = document.querySelector<HTMLParagraphElement>("#forecast-map-note");
  if (!container || !fallback || !note) {
    return;
  }

  note.textContent = "Loading OpenStreetMap-style basemap tiles and route overlays…";
  try {
    activeMap = L.map(container, {
      zoomControl: true,
      attributionControl: false,
    });
  } catch {
    showFallback(fallback, container, note, "Interactive map unavailable; showing static route overview.");
    return;
  }

  activeMap.createPane("windArrows");
  const arrowsPane = activeMap.getPane("windArrows");
  if (arrowsPane) {
    arrowsPane.style.zIndex = "450";
    arrowsPane.style.pointerEvents = "none";
  }

  L.control
    .attribution({
      position: "bottomright",
      prefix: false,
    })
    .addTo(activeMap)
    .addAttribution(MAP_ATTRIBUTION);

  const padded = padLonLatBounds(report.route.bounds);
  activeMap.fitBounds(
    [
      [padded.minLat, padded.minLon],
      [padded.maxLat, padded.maxLon],
    ],
    { padding: [20, 20] },
  );

  activeTileLayer = L.tileLayer(MAP_TILE_URL, {
    maxZoom: 18,
    attribution: MAP_ATTRIBUTION,
    crossOrigin: true,
  }).addTo(activeMap);

  let loadedTileCount = 0;
  let failedTileCount = 0;
  let resolved = false;
  const maybeFallback = (message: string): void => {
    if (resolved || loadedTileCount > 0) {
      return;
    }
    resolved = true;
    showFallback(fallback, container, note, message);
  };

  activeTileLayer.on("tileload", () => {
    loadedTileCount += 1;
    if (!resolved) {
      resolved = true;
      fallback.hidden = true;
      container.hidden = false;
      note.textContent = "CARTO Voyager tiles with route overlays and wind-direction arrows.";
      window.setTimeout(() => activeMap?.invalidateSize(), 0);
    }
  });
  activeTileLayer.on("tileerror", () => {
    failedTileCount += 1;
    if (failedTileCount >= 4) {
      maybeFallback("Map tiles were unavailable; showing a static route overview instead.");
    }
  });
  window.setTimeout(() => {
    maybeFallback("Map tiles did not load in time; showing a static route overview instead.");
  }, 3500);

  L.polyline(
    report.route.points.map((point) => [point.lat, point.lon] as L.LatLngTuple),
    {
      color: "#f2682a",
      weight: 5,
      opacity: 0.95,
    },
  ).addTo(activeMap);

  const firstPoint = report.route.points[0];
  const lastPoint = report.route.points.at(-1) ?? firstPoint;
  L.circleMarker([firstPoint.lat, firstPoint.lon], {
    radius: 8,
    color: "#ffffff",
    weight: 3,
    fillColor: "#30a46c",
    fillOpacity: 1,
  }).addTo(activeMap);
  L.circleMarker([lastPoint.lat, lastPoint.lon], {
    radius: 8,
    color: "#ffffff",
    weight: 3,
    fillColor: "#cf4b2b",
    fillOpacity: 1,
  }).addTo(activeMap);

  for (const arrow of buildRouteArrows(report.samples)) {
    const marker = L.marker([arrow.sample.sample.lat, arrow.sample.sample.lon], {
      pane: "windArrows",
      interactive: false,
      icon: L.divIcon({
        className: "wind-arrow-marker",
        html: `<span style="transform: rotate(${arrow.directionToDeg}deg)">↑</span>`,
        iconSize: [24, 24],
        iconAnchor: [12, 12],
      }),
    });
    marker.addTo(activeMap);
  }
}

export function teardownForecastMap(): void {
  if (activeTileLayer) {
    activeTileLayer.off();
    activeTileLayer = null;
  }
  if (activeMap) {
    activeMap.remove();
    activeMap = null;
  }
}

function showFallback(
  fallback: HTMLDivElement,
  container: HTMLDivElement,
  note: HTMLParagraphElement,
  message: string,
): void {
  container.hidden = true;
  fallback.hidden = false;
  note.textContent = message;
}
