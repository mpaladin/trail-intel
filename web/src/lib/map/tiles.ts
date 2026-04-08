import type { Bounds, RouteData } from "../forecast/types";

export const TILE_SIZE = 256;
export const MIN_ZOOM = 3;
export const MAX_ZOOM = 15;
export const MAP_TILE_URL = "https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png";
export const MAP_ATTRIBUTION = "Map © OpenStreetMap contributors © CARTO";

export interface PaddedBounds {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}

export interface BasemapRaster {
  canvas: HTMLCanvasElement;
  zoom: number;
  minTileX: number;
  minTileY: number;
  maxTileX: number;
  maxTileY: number;
  paddedBounds: PaddedBounds;
}

export function boundsFromRoute(route: RouteData): PaddedBounds {
  return {
    minLon: route.bounds.minLon,
    maxLon: route.bounds.maxLon,
    minLat: route.bounds.minLat,
    maxLat: route.bounds.maxLat,
  };
}

export function padLonLatBounds(bounds: PaddedBounds | Bounds): PaddedBounds {
  const lonSpan = Math.max(bounds.maxLon - bounds.minLon, 0.02);
  const latSpan = Math.max(bounds.maxLat - bounds.minLat, 0.02);
  const lonPad = Math.max(lonSpan * 0.18, 0.02);
  const latPad = Math.max(latSpan * 0.18, 0.02);

  return {
    minLon: Math.max(-180, bounds.minLon - lonPad),
    maxLon: Math.min(180, bounds.maxLon + lonPad),
    minLat: Math.max(-85, bounds.minLat - latPad),
    maxLat: Math.min(85, bounds.maxLat + latPad),
  };
}

export function lonLatToTile(lon: number, lat: number, zoom: number): { x: number; y: number } {
  const safeLat = clampLat(lat);
  const latRad = (safeLat * Math.PI) / 180;
  const tileScale = 2 ** zoom;
  const x = ((lon + 180) / 360) * tileScale;
  const y =
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * tileScale;
  const maxIndex = tileScale - 1;
  return {
    x: Math.min(maxIndex, Math.max(0, Math.floor(x))),
    y: Math.min(maxIndex, Math.max(0, Math.floor(y))),
  };
}

export function chooseZoom(bounds: PaddedBounds, maxTiles = 20): number {
  for (let zoom = MAX_ZOOM; zoom >= MIN_ZOOM; zoom -= 1) {
    const minTile = lonLatToTile(bounds.minLon, bounds.maxLat, zoom);
    const maxTile = lonLatToTile(bounds.maxLon, bounds.minLat, zoom);
    const tileCount = (maxTile.x - minTile.x + 1) * (maxTile.y - minTile.y + 1);
    if (tileCount <= maxTiles) {
      return zoom;
    }
  }
  return MIN_ZOOM;
}

export function tileUrl(zoom: number, x: number, y: number): string {
  return MAP_TILE_URL.replace("{z}", String(zoom)).replace("{x}", String(x)).replace("{y}", String(y));
}

export function worldPixelFromLonLat(lon: number, lat: number, zoom: number): { x: number; y: number } {
  const safeLat = clampLat(lat);
  const sinLat = Math.sin((safeLat * Math.PI) / 180);
  const scale = TILE_SIZE * 2 ** zoom;
  return {
    x: ((lon + 180) / 360) * scale,
    y:
      (0.5 - Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI)) *
      scale,
  };
}

export function rasterPixelForLonLat(
  raster: BasemapRaster,
  lon: number,
  lat: number,
): { x: number; y: number } {
  const world = worldPixelFromLonLat(lon, lat, raster.zoom);
  return {
    x: world.x - raster.minTileX * TILE_SIZE,
    y: world.y - raster.minTileY * TILE_SIZE,
  };
}

export async function buildBasemapRaster(
  route: RouteData,
  fetchImpl: typeof fetch = fetch,
  maxTiles = 20,
): Promise<BasemapRaster | null> {
  const paddedBounds = padLonLatBounds(boundsFromRoute(route));
  const zoom = chooseZoom(paddedBounds, maxTiles);
  const minTile = lonLatToTile(paddedBounds.minLon, paddedBounds.maxLat, zoom);
  const maxTile = lonLatToTile(paddedBounds.maxLon, paddedBounds.minLat, zoom);

  const tileCountX = maxTile.x - minTile.x + 1;
  const tileCountY = maxTile.y - minTile.y + 1;
  if (tileCountX <= 0 || tileCountY <= 0) {
    return null;
  }

  const canvas = document.createElement("canvas");
  canvas.width = tileCountX * TILE_SIZE;
  canvas.height = tileCountY * TILE_SIZE;
  const context = canvas.getContext("2d");
  if (!context) {
    return null;
  }

  const tilePromises: Array<Promise<{ image: CanvasImageSource; tileX: number; tileY: number }>> = [];
  for (let tileX = minTile.x; tileX <= maxTile.x; tileX += 1) {
    for (let tileY = minTile.y; tileY <= maxTile.y; tileY += 1) {
      tilePromises.push(
        loadTile(fetchImpl, zoom, tileX, tileY).then((image) => ({
          image,
          tileX,
          tileY,
        })),
      );
    }
  }

  try {
    const images = await Promise.all(tilePromises);
    for (const { image, tileX, tileY } of images) {
      context.drawImage(
        image,
        (tileX - minTile.x) * TILE_SIZE,
        (tileY - minTile.y) * TILE_SIZE,
        TILE_SIZE,
        TILE_SIZE,
      );
    }
  } catch {
    return null;
  }

  return {
    canvas,
    zoom,
    minTileX: minTile.x,
    minTileY: minTile.y,
    maxTileX: maxTile.x,
    maxTileY: maxTile.y,
    paddedBounds,
  };
}

async function loadTile(
  fetchImpl: typeof fetch,
  zoom: number,
  tileX: number,
  tileY: number,
): Promise<CanvasImageSource> {
  const response = await fetchImpl(tileUrl(zoom, tileX, tileY), { mode: "cors" });
  if (!response.ok) {
    throw new Error(`Tile request failed with HTTP ${response.status}.`);
  }
  const blob = await response.blob();
  return blobToImage(blob);
}

function blobToImage(blob: Blob): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(blob);
    const image = new Image();
    image.decoding = "async";
    image.onload = () => {
      URL.revokeObjectURL(url);
      resolve(image);
    };
    image.onerror = () => {
      URL.revokeObjectURL(url);
      reject(new Error("Could not decode tile image."));
    };
    image.src = url;
  });
}

function clampLat(lat: number): number {
  return Math.max(Math.min(lat, 85.05112878), -85.05112878);
}
