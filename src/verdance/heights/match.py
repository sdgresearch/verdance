"""Attach source heights to footprints: vector overlap matching and raster zonal statistics."""

import hashlib
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.errors
import rasterio.features
import rasterio.windows
import shapely
from loguru import logger

from .base import empty_result

# Footprints are matched / summarised in chunks of this many to bound memory
_CHUNK = 200_000


def match_by_overlap(
    footprints: gpd.GeoDataFrame, source: gpd.GeoDataFrame, min_overlap: float
) -> pd.DataFrame:
    """Area-weighted height of the source polygons covering each footprint.

    Every source polygon intersecting a footprint contributes its height,
    weighted by the intersection area, so a footprint the source splits into
    several buildings (or merges into a larger one) is still matched. quality
    is the covered share of the footprint (capped at 1); footprints covered
    less than min_overlap get NaN. Duplicate source polygons (e.g. fetched by
    two overlapping chunks) and polygons with a non-positive height are
    dropped first.
    """
    out = empty_result(footprints)
    if source.empty or footprints.empty:
        return out
    if source.crs is not None and footprints.crs is not None and source.crs != footprints.crs:
        source = source.to_crs(footprints.crs)

    src = source[["height", "geometry"]].copy()
    src["height"] = pd.to_numeric(src["height"], errors="coerce")
    # non-positive heights mean "unknown" in several datasets (e.g. UT-GLOBUS 0 m)
    src = src[(src["height"] > 0) & src.geometry.notna() & ~src.geometry.is_empty]
    src = src.loc[~src.geometry.to_wkb().duplicated()]
    src_geoms = shapely.make_valid(src.geometry.values)
    src_h = src["height"].to_numpy(dtype=float)
    tree = shapely.STRtree(src_geoms)

    fp_geoms = shapely.make_valid(footprints.geometry.values)
    fp_area = shapely.area(fp_geoms)
    height = np.full(len(footprints), np.nan)
    cover = np.zeros(len(footprints))
    for start in range(0, len(footprints), _CHUNK):
        chunk = fp_geoms[start:start + _CHUNK]
        fi, si = tree.query(chunk, predicate="intersects")
        if fi.size == 0:
            continue
        inter = shapely.area(shapely.intersection(chunk[fi], src_geoms[si]))
        keep = inter > 0
        fi, si, inter = fi[keep] + start, si[keep], inter[keep]
        area_sum = np.bincount(fi, weights=inter, minlength=len(footprints))
        h_sum = np.bincount(fi, weights=inter * src_h[si], minlength=len(footprints))
        hit = area_sum > 0  # only this chunk's footprints have entries
        height[hit] = h_sum[hit] / area_sum[hit]
        with np.errstate(divide="ignore", invalid="ignore"):
            cover[hit] = np.minimum(area_sum[hit] / fp_area[hit], 1.0)

    ok = cover >= min_overlap
    out["height"] = np.where(ok, height, np.nan)
    out["quality"] = cover
    out["res_m"] = 0.0
    return out


def zonal_heights(
    footprints: gpd.GeoDataFrame, raster_paths: list, stat: str = "median",
    vrt_dir: Path | None = None, block: int = 2048,
) -> pd.DataFrame:
    """Per-footprint statistic (median, mean or max) of the raster pixels inside it.

    Pixels count when their centre lies inside the footprint; footprints that
    contain no pixel centre (smaller than a pixel) fall back to every pixel
    they touch. quality is the share of the footprint's pixels with data.
    Rasters must share CRS and pixel size; footprints are reprojected to them.
    Footprints are grouped by their position on a `block`-pixel grid and each
    group reads one window spanning its footprints, so no footprint is split.
    """
    from ..optional.chm_sources import build_vrt

    out = empty_result(footprints)
    if not raster_paths or footprints.empty:
        return out
    if stat not in ("median", "mean", "max"):
        raise ValueError(f"stat must be 'median', 'mean' or 'max', got {stat!r}")

    paths = [str(p) for p in raster_paths]
    if len(paths) == 1:
        src_path = paths[0]
    else:
        vrt_dir = Path(vrt_dir) if vrt_dir else Path(paths[0]).parent
        key = hashlib.sha1("|".join(sorted(paths)).encode()).hexdigest()[:12]
        src_path = str(build_vrt(paths, vrt_dir / f"heights_{key}.vrt"))

    with rasterio.open(src_path) as src:
        fps = footprints.to_crs(src.crs) if footprints.crs is not None and src.crs is not None else footprints
        transform, nodata = src.transform, src.nodata
        if transform.b != 0 or transform.d != 0 or transform.a <= 0 or transform.e >= 0:
            raise ValueError(
                f"Height raster {src_path} must be north-up and unrotated (got transform {transform})"
            )
        res_x, res_y = float(transform.a), float(-transform.e)
        mid_lat = (src.bounds.bottom + src.bounds.top) / 2  # ignored for projected CRSs
        res_m = math.sqrt(res_x * res_y) * _metres_per_unit(src.crs, latitude=mid_lat)
        geoms = fps.geometry.values
        valid_geom = ~shapely.is_empty(geoms) & pd.notna(geoms)

        heights = np.full(len(fps), np.nan)
        quality = np.zeros(len(fps))
        if valid_geom.any():
            valid_idx = np.flatnonzero(valid_geom)
            b = shapely.bounds(geoms[valid_idx])
            # group footprints by the block their bounds' centre falls in
            cx = (b[:, 0] + b[:, 2]) / 2
            cy = (b[:, 1] + b[:, 3]) / 2
            col = np.floor((cx - transform.c) / res_x / block).astype(np.int64)
            row = np.floor((transform.f - cy) / res_y / block).astype(np.int64)
            groups = pd.Series(np.arange(len(valid_idx))).groupby([row, col]).indices

            full = rasterio.windows.Window(0, 0, src.width, src.height)
            for local_idx in groups.values():
                idx = valid_idx[local_idx]
                gb = b[local_idx]
                c0 = math.floor((gb[:, 0].min() - transform.c) / res_x) - 1
                c1 = math.ceil((gb[:, 2].max() - transform.c) / res_x) + 1
                r0 = math.floor((transform.f - gb[:, 3].max()) / res_y) - 1
                r1 = math.ceil((transform.f - gb[:, 1].min()) / res_y) + 1
                win = rasterio.windows.Window(c0, r0, c1 - c0, r1 - r0)
                try:
                    win = win.intersection(full)
                except rasterio.errors.WindowError:
                    continue  # footprints entirely outside the raster
                values = src.read(1, window=win, masked=False).astype(np.float64)
                if nodata is not None and not np.isnan(nodata):
                    values[values == nodata] = np.nan
                wt = rasterio.windows.transform(win, transform)
                shape = values.shape
                h, q = _zonal_block(geoms[idx], values, wt, shape, stat, all_touched=False)
                small = np.isnan(q)  # no pixel centre inside
                if small.any():
                    # Evaluate sub-pixel footprints individually so adjacent houses sharing
                    # a coarse pixel (e.g. 10 m GHS-BUILT-H) don't overwrite each other's label
                    h2, q2 = _zonal_small(geoms[idx][small], values, wt, shape, stat)
                    h[small], q[small] = h2, q2
                heights[idx] = h
                quality[idx] = np.nan_to_num(q)

    out["height"] = heights
    out["quality"] = quality
    out["res_m"] = res_m
    logger.debug(f"Zonal {stat} over {Path(src_path).name}: {np.isfinite(heights).sum()}/{len(heights)} footprints")
    return out


def _zonal_small(geoms, values, transform, shape, stat) -> tuple[np.ndarray, np.ndarray]:
    """Per-footprint all_touched statistic for sub-pixel footprints without label collisions."""
    n = len(geoms)
    h = np.full(n, np.nan)
    q = np.full(n, np.nan)
    nrows, ncols = shape
    res_x, res_y = float(transform.a), float(-transform.e)
    agg_fn = {"median": np.median, "mean": np.mean, "max": np.max}[stat]
    bounds = shapely.bounds(geoms)
    for k, (geom, (minx, miny, maxx, maxy)) in enumerate(zip(geoms, bounds)):
        c0 = max(0, math.floor((minx - transform.c) / res_x))
        c1 = min(ncols, math.ceil((maxx - transform.c) / res_x))
        r0 = max(0, math.floor((transform.f - maxy) / res_y))
        r1 = min(nrows, math.ceil((transform.f - miny) / res_y))
        if c1 <= c0 or r1 <= r0:
            continue
        sub_vals = values[r0:r1, c0:c1]
        sub_t = rasterio.windows.transform(rasterio.windows.Window(c0, r0, c1 - c0, r1 - r0), transform)
        mask = rasterio.features.rasterize(
            [(geom, 1)], out_shape=sub_vals.shape, transform=sub_t, fill=0, dtype="uint8", all_touched=True
        ).astype(bool)
        if not mask.any():
            continue
        picked = sub_vals[mask]
        finite = picked[np.isfinite(picked)]
        q[k] = len(finite) / len(picked)
        if finite.size:
            h[k] = float(agg_fn(finite))
    return h, q


def _zonal_block(geoms, values, transform, shape, stat, all_touched) -> tuple[np.ndarray, np.ndarray]:
    """(stat, valid share) per geometry over one window; valid share NaN when it covers no pixel.

    Overlapping footprints keep the pixels of the one drawn last (rasterised
    labels), a negligible bias for building footprints.
    """
    n = len(geoms)
    labels = rasterio.features.rasterize(
        zip(geoms, range(1, n + 1)), out_shape=shape, transform=transform,
        fill=0, dtype="int32", all_touched=all_touched,
    ).ravel()
    vals = values.ravel()
    inside = labels > 0
    lab, v = labels[inside] - 1, vals[inside]
    total = np.bincount(lab, minlength=n).astype(float)
    finite = np.isfinite(v)
    valid = np.bincount(lab[finite], minlength=n).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        share = np.where(total > 0, valid / total, np.nan)
    h = np.full(n, np.nan)
    if finite.any():
        agg = pd.Series(v[finite]).groupby(lab[finite]).agg(stat)
        h[agg.index.to_numpy()] = agg.to_numpy()
    return h, share


def _metres_per_unit(crs, latitude: float | None = None) -> float:
    """Ground metres per CRS unit (1 for metric CRSs; ~111 km per degree for geographic).

    For a geographic CRS the east-west metres per degree shrink with latitude
    (~half at 60°), so when a latitude is given it scales the equatorial value by
    cos(latitude); without one it falls back to the equatorial figure.
    """
    if crs is None:
        return 1.0
    from pyproj import CRS
    c = CRS.from_user_input(crs)
    if c.is_geographic:
        if latitude is None:
            return 111_320.0
        import math
        return 111_320.0 * max(math.cos(math.radians(latitude)), 1e-6)
    unit = c.axis_info[0].unit_conversion_factor if c.axis_info else 1.0
    return float(unit) if unit else 1.0


def footprint_bounds_4326(buildings: gpd.GeoDataFrame) -> tuple[float, float, float, float]:
    """Footprint extent in EPSG:4326 (minx, miny, maxx, maxy)."""
    b = buildings.to_crs(4326).total_bounds
    return tuple(float(x) for x in b)


def grid_chunks(bounds: tuple, chunk_deg: float) -> list[tuple]:
    """Split a (minx, miny, maxx, maxy) box into a grid of chunk_deg squares."""
    minx, miny, maxx, maxy = bounds
    nx = max(1, math.ceil((maxx - minx) / chunk_deg))
    ny = max(1, math.ceil((maxy - miny) / chunk_deg))
    return [
        (minx + i * chunk_deg, miny + j * chunk_deg,
         min(maxx, minx + (i + 1) * chunk_deg), min(maxy, miny + (j + 1) * chunk_deg))
        for i in range(nx) for j in range(ny)
    ]
