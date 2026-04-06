from __future__ import annotations

import math
from dataclasses import dataclass
from io import BytesIO

import httpx
from PIL import Image

TILE_SIZE = 256
MIN_ZOOM = 3
MAX_ZOOM = 15
WEB_MERCATOR_LIMIT = 20_037_508.342789244
TILE_URL = "https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png"
ATTRIBUTION = "Map © OpenStreetMap contributors © CARTO"
USER_AGENT = "trailintel-forecast/0.1"


@dataclass(frozen=True)
class BasemapImage:
    image: Image.Image
    extent: tuple[float, float, float, float]
    attribution: str


def fetch_basemap(
    lons: list[float],
    lats: list[float],
    *,
    http_client: httpx.Client | None = None,
    max_tiles: int = 20,
) -> BasemapImage | None:
    if not lons or not lats:
        return None

    padded_bounds = pad_lonlat_bounds(
        min(lons),
        max(lons),
        min(lats),
        max(lats),
    )
    zoom = choose_zoom(*padded_bounds, max_tiles=max_tiles)

    min_tile_x, min_tile_y = lonlat_to_tile(padded_bounds[0], padded_bounds[3], zoom)
    max_tile_x, max_tile_y = lonlat_to_tile(padded_bounds[1], padded_bounds[2], zoom)

    tile_count_x = max_tile_x - min_tile_x + 1
    tile_count_y = max_tile_y - min_tile_y + 1
    if tile_count_x <= 0 or tile_count_y <= 0:
        return None

    owns_client = http_client is None
    client = http_client or httpx.Client(
        timeout=12.0,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        stitched = Image.new("RGB", (tile_count_x * TILE_SIZE, tile_count_y * TILE_SIZE))
        for tile_x in range(min_tile_x, max_tile_x + 1):
            for tile_y in range(min_tile_y, max_tile_y + 1):
                image = fetch_tile(client, zoom, tile_x, tile_y)
                if image is None:
                    return None
                stitched.paste(
                    image,
                    (
                        (tile_x - min_tile_x) * TILE_SIZE,
                        (tile_y - min_tile_y) * TILE_SIZE,
                    ),
                )
    finally:
        if owns_client:
            client.close()

    west, north = tile_corner_lonlat(min_tile_x, min_tile_y, zoom)
    east, south = tile_corner_lonlat(max_tile_x + 1, max_tile_y + 1, zoom)
    min_x, min_y = lonlat_to_web_mercator(west, south)
    max_x, max_y = lonlat_to_web_mercator(east, north)

    return BasemapImage(
        image=stitched,
        extent=(min_x, max_x, min_y, max_y),
        attribution=ATTRIBUTION,
    )


def fetch_tile(client: httpx.Client, zoom: int, tile_x: int, tile_y: int) -> Image.Image | None:
    try:
        response = client.get(TILE_URL.format(z=zoom, x=tile_x, y=tile_y))
        response.raise_for_status()
    except httpx.HTTPError:
        return None

    try:
        return Image.open(BytesIO(response.content)).convert("RGB")
    except OSError:
        return None


def choose_zoom(
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
    *,
    max_tiles: int,
) -> int:
    for zoom in range(MAX_ZOOM, MIN_ZOOM - 1, -1):
        min_tile_x, min_tile_y = lonlat_to_tile(min_lon, max_lat, zoom)
        max_tile_x, max_tile_y = lonlat_to_tile(max_lon, min_lat, zoom)
        tile_count = (max_tile_x - min_tile_x + 1) * (max_tile_y - min_tile_y + 1)
        if tile_count <= max_tiles:
            return zoom
    return MIN_ZOOM


def pad_lonlat_bounds(
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
) -> tuple[float, float, float, float]:
    lon_span = max(max_lon - min_lon, 0.02)
    lat_span = max(max_lat - min_lat, 0.02)
    lon_pad = max(lon_span * 0.18, 0.02)
    lat_pad = max(lat_span * 0.18, 0.02)

    padded_min_lon = max(-180.0, min_lon - lon_pad)
    padded_max_lon = min(180.0, max_lon + lon_pad)
    padded_min_lat = max(-85.0, min_lat - lat_pad)
    padded_max_lat = min(85.0, max_lat + lat_pad)
    return padded_min_lon, padded_max_lon, padded_min_lat, padded_max_lat


def lonlat_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    x = lon * WEB_MERCATOR_LIMIT / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * WEB_MERCATOR_LIMIT / 180.0
    return x, y


def lonlat_series_to_web_mercator(
    lons: list[float],
    lats: list[float],
) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    for lon, lat in zip(lons, lats, strict=True):
        x, y = lonlat_to_web_mercator(lon, lat)
        xs.append(x)
        ys.append(y)
    return xs, ys


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    lat_rad = math.radians(lat)
    tile_scale = 2**zoom
    x = (lon + 180.0) / 360.0 * tile_scale
    y = (
        (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi)
        / 2.0
        * tile_scale
    )
    max_index = tile_scale - 1
    return int(min(max_index, max(0, math.floor(x)))), int(
        min(max_index, max(0, math.floor(y)))
    )


def tile_corner_lonlat(tile_x: int, tile_y: int, zoom: int) -> tuple[float, float]:
    tile_scale = 2**zoom
    lon = tile_x / tile_scale * 360.0 - 180.0
    n = math.pi - (2.0 * math.pi * tile_y) / tile_scale
    lat = math.degrees(math.atan(math.sinh(n)))
    return lon, lat
