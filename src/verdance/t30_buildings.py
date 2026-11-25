"""Canopy cover within a buffer of each building.

The per-building counterpart of `verdance.t30`: the same canopy sources,
reported for the neighbourhood of every footprint rather than per census unit.
"""

from pathlib import Path

import time
import pandas as pd
import geopandas as gpd
import rioxarray as rxr
from loguru import logger
from pyproj import CRS
from pyspark.sql.session import SparkSession

from .config.schema import VerdanceConfig
from .utils.data_processing import (
    get_geometries,
    filter_buffer_geometries,
    load_trees_gdf,
    find_overlapping_rasters,
    view_suffix,
    drop_geo_views,
    write_csv_atomic,
)

RESULT_COLUMNS = ["building_id", "tree_pixels", "total_pixels", "canopy_cover"]


def get_canopy_cover_buildings_raster(
    sedona: SparkSession,
    chm_paths: list,
    geo_code: str,
    epsg: int,
    low_threshold: float = 3,
    high_threshold: float = 60,
    already_binary: bool = False,
) -> pd.DataFrame:
    """Per-building canopy cover (%) via distributed Sedona RS_ZonalStats.

    Requires the `buildings_buffers_<geo_code>` temp view to be registered
    (done by filter_buffer_geometries()). All tiles are binarised and loaded
    into one Sedona DataFrame so Spark distributes the (building x tile) join
    in a single job. SUM across tiles assumes non-overlapping tiles (callers
    composite overlapping ones with chm_sources.resolve_overlap): buffers
    spanning a tile boundary aggregate correctly.

    With already_binary=True the tiles are assumed to hold a 1/0/nodata canopy
    mask already (e.g. the cached GEE download) and the height-threshold
    binarisation is skipped.

    Returns a DataFrame with building_id, tree_pixels, total_pixels and
    canopy_cover (0-100, 3 dp); buildings falling entirely in nodata are dropped.
    """
    sfx = view_suffix(geo_code)

    # Pre-filter: skip tiles that can't be decoded (corrupt compression or
    # invalid format), sampling rows at 25/50/75% of tile height to catch
    # mid-file corruption cheaply, and fail loudly on CRS mismatches — a tile
    # in another CRS would silently miss every building in RS_Intersects.
    expected_crs = CRS.from_epsg(epsg)
    valid_paths = []
    for path in chm_paths:
        try:
            with rxr.open_rasterio(path) as r:
                tile_crs = CRS.from_user_input(r.rio.crs) if r.rio.crs is not None else None
                if tile_crs != expected_crs:
                    raise ValueError(
                        f"Tile {path} is in {tile_crs} but the config CRS is "
                        f"EPSG:{epsg} — reproject the tiles or fix cfg.crs"
                    )
                n_rows = r.shape[1]
                for frac in [0.25, 0.50, 0.75]:
                    _ = r.isel(y=slice(int(n_rows * frac), int(n_rows * frac) + 1)).values
            valid_paths.append(str(path))
        except ValueError:
            raise
        except Exception as e:
            logger.warning(f"Skipping unreadable tile {Path(path).name}: {e}")

    if not valid_paths:
        raise FileNotFoundError(f"No valid CHM tiles found for {geo_code}")

    logger.debug(f"Loading {len(valid_paths)}/{len(chm_paths)} valid CHM tiles into Sedona")

    raster_sdf = (
        sedona.read.format("binaryFile")
        .load(valid_paths)
        .selectExpr("RS_FromGeoTiff(content) AS raster")
    )
    raster_sdf.createOrReplaceTempView(f"raw_tiles_{sfx}")

    # Tile-explode large rasters into 512×512 chunks, then binarise in-SQL:
    # pixels with CHM height in [low_threshold, high_threshold] → 1.0 (tree), else → 0.0.
    # RS_MapAlgebra's output carries no nodata, so CHM nodata pixels become 0.0
    # there; the raw tile is kept alongside to count only valid pixels.
    tile_expr = (
        "tile"
        if already_binary
        else (
            f"RS_MapAlgebra(tile, 'D', "
            f"'out = (rast[0] >= {low_threshold} && rast[0] <= {high_threshold}) ? 1.0 : 0.0;')"
        )
    )
    binary_sdf = sedona.sql(f"""
        SELECT {tile_expr} AS tile, tile AS raw_tile
        FROM (
            SELECT RS_TileExplode(raster, 512, 512) AS (x_idx, y_idx, tile)
            FROM raw_tiles_{sfx}
        )
    """)
    binary_sdf.createOrReplaceTempView(f"binary_tiles_{sfx}")

    buildings_sdf = sedona.sql(
        f"SELECT building_id, ST_SetSRID(geometry, {epsg}) AS geometry FROM buildings_buffers_{sfx}"
    )
    buildings_sdf.createOrReplaceTempView(f"buildings_partitioned_{sfx}")

    # 'sum' on the binary raster = tree pixels (canopy fraction for coarsened GEE
    # masks); 'count' on the raw raster = valid (non-nodata) pixels. allTouched
    # is true: every pixel touching the buffer counts, unlike T30's pixel-centre rule.
    result_sdf = sedona.sql(f"""
        SELECT
            b.building_id,
            SUM(RS_ZonalStats(bt.tile,     b.geometry, 1, 'sum',   true)) AS tree_pixels,
            SUM(RS_ZonalStats(bt.raw_tile, b.geometry, 1, 'count', true)) AS total_pixels
        FROM buildings_partitioned_{sfx} b, binary_tiles_{sfx} bt
        WHERE RS_Intersects(bt.tile, b.geometry)
        GROUP BY b.building_id
    """)

    result_df = result_sdf.toPandas()
    logger.debug(f"RS_ZonalStats returned {len(result_df)} buildings before filtering")

    if result_df.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    # Drop buildings whose geometry fell entirely in nodata regions of the raster
    result_df = result_df[result_df["total_pixels"] > 0].copy()
    result_df["tree_pixels"] = result_df["tree_pixels"].fillna(0)
    result_df["canopy_cover"] = (100.0 * result_df["tree_pixels"] / result_df["total_pixels"]).round(3)
    return result_df


def get_canopy_cover_buildings_vector(
    sedona: SparkSession,
    geo_code: str,
    trees_gdf: gpd.GeoDataFrame,
    low_threshold: float | None = None,
    high_threshold: float | None = None,
) -> pd.DataFrame:
    """Per-building canopy cover (%) from tree vectors via a Sedona spatial join.

    For polygon trees the area of the union of crowns clipped to the buffer is
    used, so overlapping crowns are not double-counted (mirroring
    t30.get_canopy_cover_vector); for points with a stored `tree_area` the
    whole tree area counts, so overlapping point canopies can exceed 100%.
    `total_pixels` holds the buffer area in m² (1 m² ≈ one pixel) for
    consistency with the raster path. Buildings with no trees get 0.

    The spatial join is an inner join, which Sedona plans as a partitioned
    spatial join; a LEFT JOIN on ST_Intersects falls back to a nested loop over
    every building x tree pair (hours for a city). Buildings without trees are
    added back with 0 afterwards.
    """
    sfx = view_suffix(geo_code)

    result_df = sedona.sql(f"""
        SELECT building_id, ST_Area(geometry) AS _area
        FROM buildings_buffers_{sfx}
    """).toPandas()
    if result_df.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    trees_clean = trees_gdf[trees_gdf.geometry.notna() & ~trees_gdf.geometry.is_empty]
    if "tree_height" in trees_clean.columns:
        if low_threshold is not None:
            trees_clean = trees_clean[trees_clean["tree_height"] >= low_threshold]
        if high_threshold is not None:
            trees_clean = trees_clean[trees_clean["tree_height"] <= high_threshold]

    if trees_clean.empty:
        result_df["tree_pixels"] = 0.0
        result_df["total_pixels"] = result_df["_area"].round()
        result_df["canopy_cover"] = 0.0
        return result_df[RESULT_COLUMNS]

    polygonal = set(trees_clean.geometry.geom_type.unique()) <= {"Polygon", "MultiPolygon"}
    cols = ["geometry"] + (["tree_area"] if "tree_area" in trees_clean.columns else [])
    if not polygonal and "tree_area" not in cols:
        raise ValueError(
            "Non-polygon trees need a tree_area column (columns.tree_area_col) "
            "to compute canopy cover per building"
        )
    sedona.createDataFrame(trees_clean[cols]).createOrReplaceTempView(f"t30b_trees_{sfx}")

    canopy_expr = (
        "ST_Area(ST_Intersection(FIRST(b.geometry), ST_Union_Aggr(t.geometry)))"
        if polygonal else "SUM(t.tree_area)"
    )
    canopy_df = sedona.sql(f"""
        SELECT b.building_id, {canopy_expr} AS _tree_area
        FROM buildings_buffers_{sfx} b
        JOIN t30b_trees_{sfx} t ON ST_Intersects(b.geometry, t.geometry)
        GROUP BY b.building_id
    """).toPandas()
    result_df = result_df.merge(canopy_df, on="building_id", how="left")
    result_df["_tree_area"] = result_df["_tree_area"].fillna(0.0)
    result_df = result_df[result_df["_area"] > 0].copy()
    result_df["tree_pixels"] = result_df["_tree_area"].round(3)
    result_df["total_pixels"] = result_df["_area"].round()
    result_df["canopy_cover"] = (100.0 * result_df["_tree_area"] / result_df["_area"]).round(3)
    return result_df[RESULT_COLUMNS]


def process_geo_code(
    sedona: SparkSession,
    geo_level: str,
    geo_code: str,
    cfg: VerdanceConfig,
    output_dir: Path,
    buffer: int = 100,
    low_threshold: int = 3,
    high_threshold: int = 60,
    gee_scale: float = 1.0,
    overwrite: bool = True,
) -> pd.DataFrame | None:
    """Compute canopy cover (%) within `buffer` metres of each building in one geo_code.

    Canopy source, in T30's priority order: local CHM raster tiles
    (cfg.data.chm_tiles_dir) > the Meta/WRI CHM from AWS (cfg.data.meta_chm)
    > a GEE canopy-height asset binarised server-side
    (cfg.data.canopy_height_ee_path) > tree polygon areas (cfg.data.trees_dir).
    Writes `T30_buildings_<geo_code>_<buffer>m.csv` with columns building_id,
    tree_pixels, total_pixels, canopy_cover. Returns the DataFrame, the cached
    CSV when overwrite is False, or None on error.
    """
    start_time = time.time()
    logger.info(f"T30_buildings: processing {geo_code} with buffer {buffer}m")

    out_path = output_dir / f"T30_buildings_{geo_code}_{buffer}m.csv"

    if out_path.exists() and not overwrite:
        return pd.read_csv(out_path)

    try:
        epsg = CRS.from_user_input(cfg.crs).to_epsg()
        if epsg is None:
            raise ValueError(
                f"cfg.crs '{cfg.crs}' has no EPSG code — T30_buildings needs one for ST_SetSRID"
            )

        get_geometries(sedona, geo_level, geo_code, dissolve=True)
        filter_buffer_geometries(sedona, geo_level, geo_code, "buildings", buffer, id_col="building_id")
        sfx = view_suffix(geo_code)

        # Buffered buildings extend up to `buffer` metres past the geo boundary,
        # so canopy tiles/trees must be selected against the widened boundary too
        boundary_gdf = gpd.GeoDataFrame(
            sedona.table(f"geo_boundary_{sfx}").toPandas(), geometry="geometry", crs=cfg.crs
        )
        search_gdf = boundary_gdf.copy()
        search_gdf["geometry"] = search_gdf.buffer(buffer or 0)

        if cfg.data.chm_tiles_dir:
            chm_paths = find_overlapping_rasters(search_gdf, Path(cfg.data.chm_tiles_dir), pattern=cfg.data.chm_pattern)
            if not chm_paths:
                raise FileNotFoundError(
                    f"No CHM tiles in {cfg.data.chm_tiles_dir} (pattern {cfg.data.chm_pattern!r}) overlap {geo_code}"
                )
            # Validate tile CRS before compositing so mixed-CRS tiles fail loudly
            expected_crs = CRS.from_epsg(epsg)
            for p in chm_paths:
                with rxr.open_rasterio(p) as r:
                    tile_crs = CRS.from_user_input(r.rio.crs) if r.rio.crs is not None else None
                    if tile_crs != expected_crs:
                        raise ValueError(
                            f"Tile {p} is in {tile_crs} but the config CRS is "
                            f"EPSG:{epsg} — reproject the tiles or fix cfg.crs"
                        )
            # pixel counts are summed per tile, so overlapping tiles (e.g. survey
            # years) are first composited into non-overlapping chunks
            from .optional.chm_sources import resolve_overlap
            from .utils.cache import cache_dir_for
            chm_paths = resolve_overlap(
                chm_paths, cfg.data.chm_overlap,
                cache_dir_for(cfg) / "composite" / f"T30_buildings_{geo_code}_b{buffer}m",
                bounds=tuple(search_gdf.total_bounds),
            )
            result_df = get_canopy_cover_buildings_raster(
                sedona, chm_paths, geo_code, epsg, low_threshold, high_threshold
            )

        elif cfg.data.meta_chm:
            from .optional.canopy_meta import meta_binary_canopy, meta_cache_path
            # buffer-suffixed cache: T30's raster only covers the unbuffered boundary bounds
            cache = meta_cache_path(cfg, f"{geo_code}_b{buffer}m", low_threshold, high_threshold)
            meta_binary_canopy(search_gdf, cfg, low_threshold, high_threshold, cache_path=cache, overwrite=overwrite)
            result_df = get_canopy_cover_buildings_raster(
                sedona, [cache], geo_code, epsg, already_binary=True
            )

        elif cfg.data.canopy_height_ee_path:
            from .optional.canopy_gee import download_binary_canopy, gee_cache_path  # lazy: keeps ee/xee optional
            # buffer-suffixed cache: T30's raster only covers the unbuffered boundary bounds
            cache = gee_cache_path(cfg, f"{geo_code}_b{buffer}m", low_threshold, high_threshold, gee_scale)
            download_binary_canopy(
                search_gdf, cfg, low_threshold, high_threshold,
                scale=gee_scale, cache_path=cache, overwrite=overwrite,
            )
            result_df = get_canopy_cover_buildings_raster(
                sedona, [cache], geo_code, epsg, already_binary=True
            )

        elif cfg.data.trees_dir:
            trees_gdf = load_trees_gdf(Path(cfg.data.trees_dir), search_gdf, cfg)
            result_df = get_canopy_cover_buildings_vector(
                sedona, geo_code, trees_gdf, low_threshold=low_threshold, high_threshold=high_threshold
            )

        else:
            raise ValueError(
                "No canopy source configured — set one of chm_tiles_dir, meta_chm, "
                "canopy_height_ee_path, or trees_dir to compute T30_buildings"
            )

        result_df = result_df[RESULT_COLUMNS]
        write_csv_atomic(result_df, out_path)

        end_time = time.time()
        logger.info(f"T30_buildings: {geo_code} — {len(result_df)} records in {end_time - start_time:.2f}s")
        return result_df

    except Exception:
        logger.exception(f"T30_buildings: error processing {geo_code}")
        return None
    finally:
        drop_geo_views(sedona, geo_code)
