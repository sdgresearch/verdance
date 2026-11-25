"""Shared helpers for moving data between Spark, GeoPandas and disk.

Includes the SQL literal quoting every module uses to build Sedona queries
safely, geometry filtering helpers, and the single-part file writers.
"""

import hashlib
import os
import re
import tempfile
import threading
import pandas as pd
from loguru import logger
import geopandas as gpd
from pathlib import Path
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def view_suffix(geo_code: str) -> str:
    """Sanitise a geo code into a valid, collision-free ASCII Spark temp-view suffix.

    Per-geo views are suffixed with this so parallel workers processing
    different geo codes never overwrite each other's views. Non-ASCII or
    punctuation-containing codes append a short hash so distinct codes like
    `"A-1"` and `"A 1"` or accented names like `"Bogotá"` stay valid and unique.
    """
    raw = str(geo_code)
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    if cleaned != raw:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
        return f"{cleaned}_{digest}"
    return cleaned


def write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV via a unique sibling temp file and atomic rename."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.part")
    try:
        df.to_csv(tmp, index=False)
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def sql_str(value) -> str:
    """Quote a value as a Spark SQL string literal, safe against quotes in the value.

    Census codes can contain apostrophes (e.g. `L'Hospitalet`, `King's Cross`);
    interpolating them raw into `WHERE col = '...'` would break or distort the
    query. Backslash and single quote are both escaped because Spark SQL treats
    backslash as an escape character in string literals by default.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def drop_geo_views(sedona: SparkSession, geo_code: str) -> None:
    """Drop all per-geo temp views created while processing geo_code."""
    sfx = view_suffix(geo_code)
    for name in (
        f"geo_boundary_{sfx}",
        f"geo_sub_boundaries_{sfx}",
        f"geo_buildings_{sfx}",
        f"buildings_buffers_{sfx}",
        f"geo_trees_{sfx}",
        f"raw_tiles_{sfx}",
        f"binary_tiles_{sfx}",
        f"buildings_partitioned_{sfx}",
        f"t30b_trees_{sfx}",
        f"vis_rays_{sfx}",
        f"vis_obstacles_{sfx}",
        f"vis_crowns_{sfx}",
    ):
        sedona.catalog.dropTempView(name)


def rename_tree_columns(gdf: gpd.GeoDataFrame, cfg) -> gpd.GeoDataFrame:
    """Rename user-configured tree columns to canonical tree_height/tree_area/tree_id."""
    col = cfg.columns
    mapping = {
        col.tree_height_col: "tree_height",
        col.tree_area_col: "tree_area",
        col.tree_id_col: "tree_id",
    }
    return gdf.rename(columns={k: v for k, v in mapping.items() if k in gdf.columns})


def load_trees_gdf(
    trees_dir: Path,
    geo_boundary_gdf: gpd.GeoDataFrame,
    cfg,
) -> gpd.GeoDataFrame:
    """Read tree vector files for a boundary into one GeoDataFrame in cfg.crs.

    Reads either a single tree file or the .gpkg/.parquet files in trees_dir
    (e.g. written by the Trees process) whose extent overlaps the boundary.
    Callers that look beyond the boundary (e.g. building buffers) must pass
    the boundary buffered accordingly, or trees in neighbouring tiles are
    missed. Columns are renamed to the canonical tree_height/tree_area/tree_id
    names; original geometries are kept.
    """
    if trees_dir.is_file():
        suffix = trees_dir.suffix.lower()
        trees_gdf = gpd.read_parquet(trees_dir) if suffix in (".parquet", ".geoparquet") else gpd.read_file(trees_dir)
    elif not trees_dir.exists():
        raise FileNotFoundError(f"Tree path does not exist: {trees_dir}")
    else:
        paths = find_overlapping_files(geo_boundary_gdf, trees_dir, pattern="*.gpkg")
        paths += find_overlapping_files(geo_boundary_gdf, trees_dir, pattern="*.parquet")
        paths += find_overlapping_files(geo_boundary_gdf, trees_dir, pattern="*.geoparquet")
        logger.debug(f"Found {len(paths)} tree vector files")
        if not paths:
            raise FileNotFoundError(f"No tree .gpkg/.parquet files in {trees_dir} overlap the boundary")
        parts = [
            gpd.read_parquet(p) if p.suffix.lower() in (".parquet", ".geoparquet") else gpd.read_file(p)
            for p in paths
        ]
        norm_parts = []
        for g in parts:
            g = g.to_crs(cfg.crs) if g.crs is not None else g.set_crs(cfg.crs)
            if g.geometry.name != "geometry":
                g = g.rename_geometry("geometry")
            norm_parts.append(g)
        trees_gdf = gpd.GeoDataFrame(pd.concat(norm_parts, ignore_index=True), geometry="geometry", crs=cfg.crs)

    if trees_gdf.geometry.name != "geometry":
        trees_gdf = trees_gdf.rename_geometry("geometry")
    trees_gdf = rename_tree_columns(trees_gdf, cfg)
    return trees_gdf.to_crs(cfg.crs) if trees_gdf.crs is not None else trees_gdf.set_crs(cfg.crs)


def _vector_bounds_and_crs(path: Path) -> tuple[tuple[float, float, float, float], object | None]:
    """File 2D extent and CRS without reading features: GeoParquet metadata or pyogrio.read_info."""
    if path.suffix.lower() in (".parquet", ".geoparquet"):
        import json
        import pyarrow.parquet as pq

        meta = pq.read_schema(path).metadata or {}
        if b"geo" in meta:
            geo = json.loads(meta[b"geo"])
            col_meta = geo["columns"][geo["primary_column"]]
            crs = col_meta.get("crs") or "EPSG:4326"
            bbox = col_meta.get("bbox")
            if bbox:
                if len(bbox) == 6:
                    return (float(bbox[0]), float(bbox[1]), float(bbox[3]), float(bbox[4])), crs
                return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])), crs
        gdf = gpd.read_parquet(path)
        return tuple(gdf.total_bounds), gdf.crs

    import pyogrio

    info = pyogrio.read_info(path, force_total_bounds=True)
    bounds = info.get("total_bounds")
    if bounds is not None and len(bounds) >= 4:
        return (float(bounds[0]), float(bounds[1]), float(bounds[2]), float(bounds[3])), info.get("crs")
    gdf = gpd.read_file(path)
    return tuple(gdf.total_bounds), gdf.crs


def _vector_bounds(path: Path) -> tuple:
    """File extent without reading features: GeoParquet bbox metadata, or the OGR layer extent."""
    return _vector_bounds_and_crs(path)[0]


def find_overlapping_files(boundary_gdf: gpd.GeoDataFrame, files_dir: Path, pattern: str = "*.gpkg") -> list[Path]:
    """
    Returns paths from files_dir whose bounding box intersects boundary_gdf.
    Uses file extent metadata and transforms bounds to boundary_gdf.crs when
    a file is stored in another CRS — no tile naming convention required.
    """
    if boundary_gdf.empty:
        return []
    files = sorted(files_dir.glob(pattern))
    if not files:
        return []

    import math
    from pyproj import CRS, Transformer
    from shapely.geometry import box

    target_crs = CRS.from_user_input(boundary_gdf.crs) if boundary_gdf.crs is not None else None
    dissolved = boundary_gdf.geometry.union_all()
    if dissolved is None or dissolved.is_empty:
        return []

    hits: list[Path] = []
    for p in files:
        try:
            bbox, file_crs = _vector_bounds_and_crs(p)
            if any(math.isnan(v) for v in bbox):
                continue
            minx, miny, maxx, maxy = bbox
            if target_crs is not None and file_crs is not None:
                src_crs = CRS.from_user_input(file_crs)
                if src_crs != target_crs:
                    minx, miny, maxx, maxy = Transformer.from_crs(
                        src_crs, target_crs, always_xy=True
                    ).transform_bounds(minx, miny, maxx, maxy)
            if box(minx, miny, maxx, maxy).intersects(dissolved):
                hits.append(p)
        except Exception as e:
            logger.warning(f"Could not read extent of {p}: {e}")

    return hits


# Rasters that sit beside CHM tiles but are not heights: Defra VOM ships a
# uint8 hillshade (VOM_HS_*) next to every height tile, and "*.tif" picks both up.
NON_CHM_PREFIXES = ("VOM_HS_",)


def is_chm_tile(path) -> bool:
    """False for known non-height rasters stored alongside CHM tiles (e.g. Defra VOM hillshades)."""
    return not Path(path).name.startswith(NON_CHM_PREFIXES)


def find_overlapping_rasters(boundary_gdf: gpd.GeoDataFrame, files_dir: Path, pattern: str = "*.tif") -> list[Path]:
    """Returns raster paths under files_dir (searched recursively) whose bounding box intersects boundary_gdf.

    Tile bounds are transformed to the boundary CRS before the test, so tiles
    stored in another CRS are still selected correctly. Known non-height
    rasters (see is_chm_tile) are skipped.
    """
    if boundary_gdf.empty:
        return []
    import rasterio
    from rasterio.warp import transform_bounds
    from shapely.geometry import box

    files = sorted(p for p in files_dir.rglob(pattern) if is_chm_tile(p))
    if not files:
        return []

    dissolved = boundary_gdf.geometry.union_all()
    if dissolved is None or dissolved.is_empty:
        return []

    result = []
    for p in files:
        try:
            with rasterio.open(p) as src:
                if src.crs is not None and boundary_gdf.crs is not None and src.crs != boundary_gdf.crs:
                    bounds = transform_bounds(src.crs, boundary_gdf.crs, *src.bounds)
                else:
                    bounds = tuple(src.bounds)
            if box(*bounds).intersects(dissolved):
                result.append(p)
        except Exception as e:
            logger.warning(f"Could not read bounds of {p}: {e}")
    return result


def filter_buffer_geometries(
    sedona: SparkSession,
    geo_level: str,
    geo_code: str,
    table_name: str,
    buffer: int | None = None,
    id_col: str = "building_id",
) -> DataFrame:
    """Filter table_name features belonging to the geo_code, optionally buffered.

    Buildings are selected through the `boundaries_buildings_overlay` lookup
    (representative point in the unit), so a building straddling two geo codes
    is processed — and output — by exactly one of them, the same unit Merge
    aggregates it into. Other tables are selected by intersection with the
    `geo_boundary_<geo_code>` view created by get_geometries() or
    get_sub_geo_boundaries(). Registers `geo_<table_name>_<geo_code>` and, when a
    buffer is given, `<table_name>_buffers_<geo_code>` (geometry buffered by
    `buffer` metres plus id_col).
    """
    sfx = view_suffix(geo_code)
    if table_name == "buildings":
        query = f"""
        SELECT b.* FROM buildings b
        JOIN boundaries_buildings_overlay o ON b.building_id = o.building_id
        WHERE o.{geo_level} = {sql_str(geo_code)}
        """
    else:
        query = f"""
        SELECT b.* FROM {table_name} b, geo_boundary_{sfx} g
        WHERE ST_Intersects(b.geometry, g.geometry)
        """
    geo_sdf = sedona.sql(query)
    geo_sdf.createOrReplaceTempView(f"geo_{table_name}_{sfx}")

    if buffer is not None:
        geom_expr = f"ST_Buffer(b.geometry, {buffer})" if buffer else "b.geometry"
        geo_buffer_sdf = sedona.sql(
            f"""
            SELECT {geom_expr} AS geometry, b.{id_col}
            FROM geo_{table_name}_{sfx} b
            """
        )
        geo_buffer_sdf.createOrReplaceTempView(f"{table_name}_buffers_{sfx}")
        return geo_buffer_sdf

    return geo_sdf


def get_geometries(sedona: SparkSession, geo_level: str, geo_code: str, dissolve: bool = True) -> DataFrame:
    """Select boundary rows where geo_level = geo_code, optionally dissolved to one geometry.

    Registers the result as the `geo_boundary_<geo_code>` temp view used by
    filter_buffer_geometries().
    """
    query = "ST_Union_Aggr(geometry) AS geometry" if dissolve else "*"
    geo_boundary_sdf = sedona.sql(
        f"""
        SELECT {query}
        FROM boundaries
        WHERE {geo_level} = {sql_str(geo_code)}
        """
    )
    geo_boundary_sdf.createOrReplaceTempView(f"geo_boundary_{view_suffix(geo_code)}")
    return geo_boundary_sdf


def get_sub_geo_boundaries(
    sedona: SparkSession, geo_level: str, geo_code: str, sub_geo_level: str
) -> DataFrame:
    """Return one dissolved geometry per sub_geo_level unit within geo_level = geo_code.

    Also refreshes the `geo_boundary_<geo_code>` temp view (whole-region
    dissolve) so that filter_buffer_geometries() continues to work correctly.
    """
    sfx = view_suffix(geo_code)
    sdf = sedona.sql(
        f"""
        SELECT {sub_geo_level}, ST_Union_Aggr(geometry) AS geometry
        FROM boundaries
        WHERE {geo_level} = {sql_str(geo_code)}
        GROUP BY {sub_geo_level}
        """
    )
    sdf.createOrReplaceTempView(f"geo_sub_boundaries_{sfx}")

    sedona.sql(
        f"""
        SELECT ST_Union_Aggr(geometry) AS geometry
        FROM boundaries
        WHERE {geo_level} = {sql_str(geo_code)}
        """
    ).createOrReplaceTempView(f"geo_boundary_{sfx}")

    return sdf


def save_csv_as_parquet(in_directory: Path, path_pattern: str, out_path: Path) -> pd.DataFrame:
    """Concatenate all CSVs matching path_pattern into a single parquet file."""
    csv_files = list(in_directory.glob(path_pattern))
    dataframes_lst = [pd.read_csv(file) for file in csv_files]
    concatenated_df = pd.concat(dataframes_lst, ignore_index=True)
    concatenated_df.to_parquet(out_path, index=False)
    return concatenated_df


def save_temp_file(
    spark_df: DataFrame,
    output_path: Path,
    coalesce: int = 1,
    file_format: str = "csv",
    return_df: bool = True,
) -> pd.DataFrame | None:
    """
    Saves a Spark DataFrame to a single named file (atomically) and optionally returns a Pandas DataFrame.
    Workaround for Spark writing to a directory instead of a single file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_path.parent) as temp_dir:
        temp_path = Path(temp_dir)
        spark_df.coalesce(coalesce) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .format(file_format) \
            .save(str(temp_path))

        part_files = list(temp_path.glob(f"part-*.{file_format}*"))
        if not part_files:
            raise FileNotFoundError(f"No part file found with format '{file_format}' in {temp_dir}")
        if len(part_files) > 1:
            # moving one part would silently drop the rest; the single-file contract needs coalesce=1
            raise ValueError(
                f"save_temp_file expected one part file but Spark wrote {len(part_files)} "
                f"(coalesce={coalesce}); pass coalesce=1 so no rows are dropped"
            )
        part_files[0].replace(output_path)

    if not return_df:
        return None
    if file_format == "parquet":
        return pd.read_parquet(output_path)
    elif file_format == "csv":
        return pd.read_csv(output_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
