"""Regression tests covering bugs and edge cases flagged in the codebase review."""

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
import shapely
from rasterio.transform import from_origin
from shapely.geometry import MultiPolygon, Polygon, box

from verdance.config.loader import load_config
from verdance.config.schema import HeightSourceSpec
from verdance.dggs.a5 import A5System
from verdance.dggs.rhealpix import RHEALPixSystem
from verdance.dggs.s2 import S2System
from verdance.heights.base import HeightContext
from verdance.heights.enrich import heights_cache_path
from verdance.heights.file import FileHeights
from verdance.heights.gee import _is_too_large, _is_transient
from verdance.heights.match import zonal_heights
from verdance.merge import evaluate_rule
from verdance.optional.tree_eval import box_iou
from verdance.optional.tree_segmentation import SegmentationParams, detect_treetops
from verdance.optional.visibility.dsm import _burn_max
from verdance.optional.visibility.los import _ray_zreq
from verdance.optional.visibility.observers import facade_points
from verdance.osm import parse_osm_height
from verdance.utils.data_processing import (
    _vector_bounds_and_crs,
    find_overlapping_files,
    view_suffix,
)
from verdance.utils.manifest import read_run_manifest, write_run_manifest
from verdance.viz.tiles import tile_bounds


# --- view_suffix & vector bounds ---


def test_view_suffix_unicode_and_punctuation_collision():
    s_accent = view_suffix("Bogotá")
    assert s_accent.isascii()
    assert s_accent.replace("_", "").isalnum()
    assert view_suffix("A-1") != view_suffix("A 1")
    assert view_suffix("E09000001") == "E09000001"


def test_find_overlapping_files_gpkg_and_cross_crs(tmp_path):
    # Write a GeoPackage in EPSG:4326 and query with a boundary in EPSG:32630
    poly_4326 = gpd.GeoDataFrame(
        {"treeID": [1]},
        geometry=[box(-0.1, 51.5, 0.0, 51.6)],
        crs="EPSG:4326",
    )
    gpkg = tmp_path / "trees.gpkg"
    poly_4326.to_file(gpkg, driver="GPKG")

    bounds, crs = _vector_bounds_and_crs(gpkg)
    assert bounds is not None
    assert crs is not None

    boundary_utm = poly_4326.to_crs("EPSG:32630")
    matched = find_overlapping_files(boundary_utm, tmp_path, pattern="*.gpkg")
    assert matched == [gpkg]

    far_boundary = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[box(500_000, 100_000, 500_100, 100_100)], crs="EPSG:32630"
    )
    assert find_overlapping_files(far_boundary, tmp_path, pattern="*.gpkg") == []


def test_parquet_6_element_bbox(tmp_path):
    # 3D GeoParquet has a 6-element bbox [minx, miny, minz, maxx, maxy, maxz]
    poly_3d = Polygon([(0, 0, 1), (10, 0, 1), (10, 10, 1), (0, 10, 1)])
    pq = tmp_path / "trees3d.parquet"
    gpd.GeoDataFrame({"treeID": [1]}, geometry=[poly_3d], crs="EPSG:27700").to_parquet(pq)
    bounds, crs = _vector_bounds_and_crs(pq)
    assert bounds == (0.0, 0.0, 10.0, 10.0)
    assert crs is not None


# --- config validation ---


def _base_yaml(tmp_path):
    return {
        "study_area_name": "TestCity",
        "crs": "EPSG:32630",
        "data": {
            "buildings": "osm",
            "parks_sites": "osm",
            "parks_access": "osm",
            "roads": "osm",
            "census_boundaries": str(tmp_path / "bounds.gpkg"),
        },
        "columns": {"geo_levels": ["l1", "l2"]},
        "output": {"base_dir": str(tmp_path / "out")},
    }


def _write_yaml(tmp_path, data):
    import yaml

    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


@pytest.mark.parametrize(
    "mutator,match",
    [
        (lambda d: d.update({"dggs": "h3", "dggs_resolution": True}), "dggs_resolution"),
        (lambda d: d.update({"visibility": {"end_skip": True}}), "end_skip"),
        (lambda d: d.update({"osm": {"fetch_buffer": True}}), "fetch_buffer"),
        (lambda d: d.update({"data": None}), "data"),
        (lambda d: d["columns"].update({"geo_levels": "l1"}), "geo_levels"),
        (lambda d: d["columns"].update({"geo_levels": ["l1", "l1"]}), "geo_levels"),
        (lambda d: d.update({"crs": "EPSG:2263"}), "metre"),
        (lambda d: d.update({"terrain": {"source": ""}}), "terrain.source"),
        (lambda d: d.update({"heights": {"sources": [{"source": "native", "name": "../evil"}]}}), "name"),
    ],
)
def test_config_loader_rejects_invalid_values(tmp_path, mutator, match):
    raw = _base_yaml(tmp_path)
    mutator(raw)
    with pytest.raises(ValueError, match=match):
        load_config(_write_yaml(tmp_path, raw))


# --- manifest atomic & merge ---


def test_manifest_merge_existing_preserves_prior_entries(tmp_path):
    out_dir = tmp_path / "T3"
    write_run_manifest(
        out_dir, "T3", ["a", "b"], ["a"], ["b"], extra={"geo_level": "L1", "failed": ["injected"]}
    )
    # Core key `failed` cannot be overwritten by `extra`, and retry of `b` merges into existing
    m1 = read_run_manifest(out_dir)
    assert m1["failed"] == ["b"]
    assert m1["succeeded"] == ["a"]

    write_run_manifest(out_dir, "T3", ["b"], ["b"], [], merge_existing=True)
    m2 = read_run_manifest(out_dir)
    assert sorted(m2["succeeded"]) == ["a", "b"]
    assert m2["failed"] == []


def test_manifest_corrupt_returns_none(tmp_path):
    out_dir = tmp_path / "T30"
    out_dir.mkdir()
    (out_dir / "_manifest.json").write_text("{not valid json")
    assert read_run_manifest(out_dir) is None


# --- heights ---


def test_heights_gee_error_classification():
    assert _is_too_large(RuntimeError("Collection query aborted after accumulating over 5000 elements."))
    assert not _is_too_large(RuntimeError("Quota exceeded for quota metric"))
    assert _is_transient(RuntimeError("HTTP 503 Service Unavailable"))
    assert not _is_transient(RuntimeError("User memory limit exceeded on 5000 elements"))


def test_file_heights_cache_key_and_column_collision(tmp_path):
    # Vector file already having a `height` column while options.column requests another column
    gpkg = tmp_path / "h.gpkg"
    gdf = gpd.GeoDataFrame(
        {"height": [99.0], "custom_h": [12.0]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:32630",
    )
    gdf.to_file(gpkg, driver="GPKG")

    cfg = load_config(_write_yaml(tmp_path, {
        **_base_yaml(tmp_path),
        "heights": {"sources": [{"source": "file", "path": str(gpkg), "column": "custom_h"}]},
    }))
    db_dir = tmp_path / "out" / "database"
    db_dir.mkdir(parents=True)
    buildings = gpd.GeoDataFrame({"building_id": ["b1"]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:32630")
    buildings.to_parquet(db_dir / "buildings.parquet")

    key1 = heights_cache_path(cfg)

    src = FileHeights(HeightSourceSpec("file", {"path": str(gpkg), "column": "custom_h"}), cfg)
    out = src.heights(buildings, HeightContext(cfg=cfg, boundaries=buildings, cache_dir=tmp_path))
    assert out["height"].iloc[0] == pytest.approx(12.0)

    # Modifying the file updates heights_cache_path
    gdf["custom_h"] = [15.0]
    gdf.to_file(gpkg, driver="GPKG")
    key2 = heights_cache_path(cfg)
    assert key1 != key2


def test_zonal_heights_adjacent_subpixel_footprints(tmp_path):
    # Two adjacent 2x2m footprints inside a single 10x10m pixel should both get its value (not overwrite each other)
    tif = tmp_path / "coarse.tif"
    arr = np.full((2, 2), 14.0, dtype=np.float32)
    with rasterio.open(
        tif, "w", driver="GTiff", height=2, width=2, count=1,
        dtype="float32", crs="EPSG:32630", transform=from_origin(0, 20, 10, 10),
    ) as dst:
        dst.write(arr, 1)

    buildings = gpd.GeoDataFrame(
        {"building_id": ["b1", "b2"]},
        geometry=[box(1, 11, 3, 13), box(4, 11, 6, 13)],
        crs="EPSG:32630",
    )
    out = zonal_heights(buildings, [tif], stat="median", vrt_dir=tmp_path)
    assert list(out["height"]) == [pytest.approx(14.0), pytest.approx(14.0)]


# --- OSM height parsing ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("250 cm", 2.5),
        ("1200 mm", 1.2),
        ("12,5 m", 12.5),
        ("12'6\"", pytest.approx(12 * 0.3048 + 6 * 0.0254)),
        ("12 m (40 ft)", 12.0),
        ("30 ft", pytest.approx(30 * 0.3048)),
    ],
)
def test_parse_osm_height_units(raw, expected):
    assert parse_osm_height(pd.Series([raw])).iloc[0] == expected


# --- Visibility edge cases ---


def test_visibility_end_skip_zero_and_facade_offset_zero():
    # end_skip=0.0 must not raise ZeroDivisionError
    dsm = np.zeros((10, 10), dtype=np.float32)
    bldg = np.zeros((10, 10), dtype=np.float32)
    crown_id = np.zeros((1, 10, 10), dtype=np.int32)
    z = _ray_zreq(dsm, bldg, crown_id, 0.0, 10.0, 1.0, 0.5, 0.5, 5.5, 5.5, 10.0, 0, 0.0, 0.0, np.inf, True)
    assert z == -np.inf or np.isfinite(z)

    # facade_offset=0.0 should still keep observers for a standalone building
    geoms = np.array([box(10, 10, 20, 20)], dtype=object)
    obs = facade_points(geoms, spacing=5.0, offset=0.0, blockers=shapely.STRtree(geoms))
    assert len(obs) > 0

    # _burn_max with all heights <= 0 must not crash
    out = _burn_max([box(0, 0, 5, 5)], np.array([0.0]), (10, 10), from_origin(0, 10, 1, 1))
    assert out.shape == (10, 10)
    assert float(out.max()) == 0.0


# --- Tree segmentation & eval ---


def test_detect_treetops_anisotropic_pixels_and_box_iou_zero_area():
    chm = np.zeros((20, 20), dtype=np.float32)
    chm[10, 10] = 15.0
    p = SegmentationParams(hmin=3.0, ws_min=1.0, ws_max=2.0)
    # Anisotropic pixel spacing previously could index out of bounds in 3x3 refinement
    rows, cols = detect_treetops(chm, gx=0.5, gy=2.0, p=p, cap=2.0)
    assert len(rows) == 1 and rows[0] == 10 and cols[0] == 10

    z = np.array([[0.0, 0.0, 0.0, 0.0]])
    iou = box_iou(z, z)
    assert iou.shape == (1, 1)
    assert iou[0, 0] == 0.0


# --- Merge null_distance_fails ---


def test_evaluate_rule_null_distance_fails():
    df = pd.DataFrame({
        "tree_count_50m": [5, 5],
        "canopy_cover": [40.0, 40.0],
        "park_distance_euclidean": [100.0, np.nan],
    })
    r_unknown = evaluate_rule(
        df, "tree_count_50m", "canopy_cover", "park_distance_euclidean", null_distance_fails=False
    )
    assert pd.isna(r_unknown["meets_300"].iloc[1])

    r_fail = evaluate_rule(
        df, "tree_count_50m", "canopy_cover", "park_distance_euclidean", null_distance_fails=True
    )
    assert bool(r_fail["meets_300"].iloc[1]) is False
    assert bool(r_fail["meets_3_30_300"].iloc[1]) is False


# --- DGGS ---


def test_dggs_s2_lpad_and_a5_multipolygon_and_rhealpix_polar():
    s2 = S2System()
    assert "LPAD" in s2.cell_id_str_sql("cell_id")

    # A5: MultiPolygon with a tiny island that has no interior cell centroid must still get a cell
    a5 = A5System()
    mp = MultiPolygon([
        box(0.0, 51.5, 0.05, 51.55),
        box(0.5, 51.5, 0.50001, 51.50001),
    ])
    cells = list(a5.cells_covering(mp, 10))
    assert len(cells) >= 2

    # rHEALPix in polar cap (lat > 41.81 deg) must cover all corners of a box
    from rhealpixdggs.dggs import WGS84_003

    rh = RHEALPixSystem()
    polar_box = box(-2.0, 51.0, 0.0, 52.5)
    rh_cells = list(rh.cells_covering(polar_box, 5))
    rh_ids = {cid for cid, _ in rh_cells}
    for pt in [(-2.0, 51.0), (0.0, 51.0), (-2.0, 52.5), (0.0, 52.5), (-1.0, 51.75)]:
        assert str(WGS84_003.cell_from_point(5, pt, plane=False)) in rh_ids
    union = gpd.GeoSeries([geom for _, geom in rh_cells]).union_all()
    assert polar_box.difference(union).area < 1e-6


# --- Viz tile bounds ---


def test_viz_tile_bounds_validation():
    assert len(tile_bounds(0, 0, 0)) == 4
    with pytest.raises(ValueError):
        tile_bounds(99999999, 0, 0)
    with pytest.raises(ValueError):
        tile_bounds(2, 4, 0)
    with pytest.raises(ValueError):
        tile_bounds(-1, 0, 0)
