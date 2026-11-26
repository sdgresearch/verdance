"""Ground elevation (DTM) under buildings and trees, for Visibility's raster engine.

Visibility needs bare-earth ground: building and tree heights are measured
from it, and the ground itself (hills, valleys) blocks or opens views.
FABDEM — Copernicus GLO-30 with buildings and forests removed — is the
default; Copernicus GLO-30 and NASADEM are surface models (in cities they
include rooftops and canopy, so building heights would be counted twice).
GEE sources are downloaded at terrain.resolution metres onto tiles aligned to
a fixed grid in the study CRS (cached under the CHM cache dir) and resampled
bilinearly; a local DTM raster file or directory can be given instead.
"""

from pathlib import Path

from .config.schema import VerdanceConfig

# name -> (GEE asset, band, is ImageCollection, licence)
DEM_SOURCES = {
    "fabdem": ("projects/sat-io/open-datasets/FABDEM", "b1", True, "CC BY-NC-SA 4.0 (University of Bristol)"),
    "copernicus": ("COPERNICUS/DEM/GLO30", "DEM", True, "Copernicus DEM licence (ESA)"),
    "nasadem": ("NASA/NASADEM_HGT/001", "elevation", False, "public domain (NASA)"),
}


def uses_terrain(cfg: VerdanceConfig) -> bool:
    """True when a terrain source is configured, so Visibility rays start from ground level."""
    return cfg.terrain.source is not None


_DEM_SUFFIXES = (".tif", ".tiff", ".vrt", ".img")


def dem_layers(cfg: VerdanceConfig, bounds: tuple) -> list[Path]:
    """DEM rasters covering bounds (cfg.crs): downloaded GEE tiles or the local DTM files."""
    source = cfg.terrain.source
    if source is None:
        return []
    key = source.strip().lower()
    if key in DEM_SOURCES:
        return _gee_dem(cfg, key, bounds)
    path = Path(source)
    if path.is_dir():
        all_paths = sorted(p for p in path.rglob("*") if p.is_file() and p.suffix.lower() in _DEM_SUFFIXES)
        if not all_paths:
            raise FileNotFoundError(f"No DEM rasters ({_DEM_SUFFIXES}) under {path}")
        import rasterio
        from rasterio.warp import transform_bounds

        minx, miny, maxx, maxy = bounds
        paths = []
        for p in all_paths:
            try:
                with rasterio.open(p) as src:
                    b = transform_bounds(src.crs, cfg.crs, *src.bounds) if src.crs else tuple(src.bounds)
                if b[2] >= minx and b[0] <= maxx and b[3] >= miny and b[1] <= maxy:
                    paths.append(p)
            except Exception:
                continue
        return paths
    if path.exists():
        return [path]
    raise FileNotFoundError(
        f"terrain.source {source!r} is neither one of {sorted(DEM_SOURCES)} nor an existing raster path"
    )


def _gee_dem(cfg: VerdanceConfig, source: str, bounds: tuple) -> list[Path]:
    from pyproj import Transformer

    from .heights.gee import TILE_PX, download_image_tiles, tag, tiles_for_extent
    from .utils.cache import cache_dir_for

    asset, band, is_collection, _ = DEM_SOURCES[source]
    res = float(cfg.terrain.resolution)
    out_dir = cache_dir_for(cfg) / "terrain" / source / tag({"asset": asset, "crs": cfg.crs, "res": res})
    tiles = tiles_for_extent(bounds, res)
    if not tiles:
        return []

    side = res * TILE_PX
    minx_p = min(c for _, c in tiles) * side
    maxx_p = (max(c for _, c in tiles) + 1) * side
    miny_p = min(r for r, _ in tiles) * side
    maxy_p = (max(r for r, _ in tiles) + 1) * side
    b4326 = Transformer.from_crs(cfg.crs, "EPSG:4326", always_xy=True).transform_bounds(
        minx_p, miny_p, maxx_p, maxy_p
    )

    def image():
        import ee

        if is_collection:
            region = ee.Geometry.Rectangle(list(b4326), proj="EPSG:4326", geodesic=False)
            col = ee.ImageCollection(asset).filterBounds(region).select(band)
            img = col.mosaic().setDefaultProjection(col.first().projection())
        else:
            img = ee.Image(asset).select(band)
        return img.resample("bilinear").rename(band)

    return download_image_tiles(image, band, cfg.crs, res, tiles, out_dir, cfg.gee_project)
