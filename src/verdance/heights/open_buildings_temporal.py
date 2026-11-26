"""Google Open Buildings 2.5D Temporal: building height rasters from Sentinel-2 (2016-2023).

Annual 0.5 m rasters with an effective resolution of ~4 m, covering Africa,
South and Southeast Asia, and Latin America & the Caribbean only. Pixels
whose building_presence is below options.presence_threshold (default 0.5)
are masked; the year's mosaic is fetched at options.resolution metres
(default 2) and summarised by the median over each footprint.
"""

from pathlib import Path

import geopandas as gpd
from loguru import logger

from .base import HeightContext, RasterHeightSource
from .gee import TILE_PX, _with_retry, download_image_tiles, tag, tiles_with_footprints

COLLECTION = "GOOGLE/Research/open-buildings-temporal/v1"
BAND = "building_height"
YEARS = range(2016, 2024)


class OpenBuildingsTemporalHeights(RasterHeightSource):
    """Google Open Buildings 2.5D Temporal annual height rasters fetched from GEE."""

    license = "CC BY 4.0 (Google)"
    stat = "median"

    def rasters(self, buildings: gpd.GeoDataFrame, ctx: HeightContext) -> list[Path]:
        """Download and cache Open Buildings 2.5D Temporal tiles covering `buildings`."""
        year = int(self.options.get("year", max(YEARS)))
        if year not in YEARS:
            raise ValueError(f"open_buildings_temporal.year must be in {YEARS.start}-{YEARS.stop - 1}, got {year}")
        threshold = float(self.options.get("presence_threshold", 0.5))
        if not (0.0 <= threshold <= 1.0):
            raise ValueError(f"open_buildings_temporal.presence_threshold must be in [0, 1], got {threshold!r}")
        res = float(self.options.get("resolution", 2.0))
        if res <= 0:
            raise ValueError(f"open_buildings_temporal.resolution must be positive, got {res!r}")
        out_dir = ctx.cache_dir / tag({
            "collection": COLLECTION, "year": year, "presence": threshold, "crs": self.cfg.crs, "res": res,
        })
        tiles = tiles_with_footprints(buildings, res)
        if not tiles:
            return []

        # Filter the GEE collection by the union extent of the cached tiles (not just
        # `buildings`) so a partial study area never caches a half-empty tile into the
        # shared cache directory.
        from pyproj import Transformer

        side = res * TILE_PX
        minx_p = min(c for _, c in tiles) * side
        maxx_p = (max(c for _, c in tiles) + 1) * side
        miny_p = min(r for r, _ in tiles) * side
        maxy_p = (max(r for r, _ in tiles) + 1) * side
        minx, miny, maxx, maxy = Transformer.from_crs(self.cfg.crs, "EPSG:4326", always_xy=True).transform_bounds(
            minx_p, miny_p, maxx_p, maxy_p
        )

        def image():
            import ee

            region = ee.Geometry.Rectangle([minx, miny, maxx, maxy], proj="EPSG:4326", geodesic=False)
            col = ee.ImageCollection(COLLECTION).filterBounds(region).filterDate(f"{year}-01-01", f"{year + 1}-01-01")
            if _with_retry(lambda: col.size().getInfo(), what="open_buildings_temporal coverage check") == 0:
                logger.warning(
                    "Open Buildings 2.5D Temporal has no images over the study area — it covers Africa, "
                    "South/Southeast Asia and Latin America & the Caribbean only"
                )
                return ee.Image.constant(0).toFloat().updateMask(0).rename(BAND)
            # mosaic() drops the source projection; pin the native 0.5 m grid back
            img = col.mosaic().setDefaultProjection(col.first().select(BAND).projection())
            return img.select(BAND).updateMask(img.select("building_presence").gte(threshold))

        return download_image_tiles(image, BAND, self.cfg.crs, res, tiles, out_dir, self.cfg.gee_project)
