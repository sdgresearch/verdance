"""Mapbox Vector Tiles straight from viz.duckdb.

Each tile carries only the feature id and the metric being mapped; the full
attribute set of one feature is fetched on click. Tile bboxes are passed as
constants so DuckDB's zone maps skip row groups of the Hilbert-sorted tables.
"""

import contextlib
import gzip
import threading
from collections import OrderedDict

from .store import UNIT_SIMPLIFY, connect

EXTENT = 4096
BUFFER = 64
HALF_WORLD = 20037508.342789244
MIN_ZOOM = {"buildings": 14, "trees": 14, "parks": 0}
MAX_ZOOM = 24
_INTERNAL_COLS = {"geom", "minx", "miny", "maxx", "maxy", *(c for c, _, _ in UNIT_SIMPLIFY)}


def tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Web Mercator bounds of an XYZ tile."""
    if not (0 <= z <= MAX_ZOOM):
        raise ValueError(f"Tile zoom {z} out of range [0, {MAX_ZOOM}]")
    n = 1 << z
    if not (0 <= x < n and 0 <= y < n):
        raise ValueError(f"Tile ({z}/{x}/{y}) out of range [0, {n - 1}]")
    size = 2 * HALF_WORLD / n
    minx = -HALF_WORLD + x * size
    maxy = HALF_WORLD - y * size
    return minx, maxy - size, minx + size, maxy


def _quote(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


class TileStore:
    """Thread-safe tile and feature lookups over a read-only viz.duckdb."""

    def __init__(self, path, meta: dict, cache_size: int = 4096):
        self._con = connect(path, read_only=True)
        self.meta = meta
        self._cache: OrderedDict = OrderedDict()
        self._cache_size = cache_size
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def _cursor(self):
        with self._lock:
            cur = self._con.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _metrics(self, layer: str) -> set[str]:
        return {m["name"] for m in self.meta["layers"][layer]["metrics"]}

    def has_layer(self, layer: str) -> bool:
        """True if `layer` is a configured building/unit layer or an available overlay (trees/parks)."""
        return layer in self.meta["layers"] or (layer in ("trees", "parks") and self.meta.get(layer) is not None)

    def tile(self, layer: str, metric: str | None, z: int, x: int, y: int) -> bytes | None:
        """Gzipped MVT bytes, or None for an empty tile. Raises KeyError for unknown layers/metrics."""
        # Validate XYZ coordinates before caching or querying DuckDB
        tile_bounds(z, x, y)
        key = (layer, metric, z, x, y)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        data = self._render(layer, metric, z, x, y)
        with self._lock:
            self._cache[key] = data
            if len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)
        return data

    def _render(self, layer: str, metric: str | None, z: int, x: int, y: int) -> bytes | None:
        if layer in ("trees", "parks"):
            if self.meta.get(layer) is None:
                raise KeyError(layer)
            table, geom = layer, "geom"
            props = ["radius_m", "height"] if layer == "trees" else ["id", "name", "area_ha", "used"]
        else:
            info = self.meta["layers"][layer]
            table = info["table"]
            props = ["id"]
            if metric:
                if metric not in self._metrics(layer):
                    raise KeyError(metric)
                props.append(metric)
            geom = "geom"
            if layer != "buildings":
                geom = next((col for col, max_z, _ in UNIT_SIMPLIFY if z <= max_z), "geom")
        if z < MIN_ZOOM.get(layer, 0):
            return None

        minx, miny, maxx, maxy = tile_bounds(z, x, y)
        pad = (maxx - minx) * BUFFER / EXTENT
        sel = ", ".join(_quote(p) for p in props)
        with self._cursor() as cur:
            row = cur.execute(f"""
                SELECT count(*), ST_AsMVT(t, 'features', {EXTENT}, 'geom') FROM (
                    SELECT {sel}, ST_AsMVTGeom({geom}, ST_Extent(ST_TileEnvelope({z}, {x}, {y})), {EXTENT}, {BUFFER}, true) AS geom
                    FROM {_quote(table)}
                    WHERE maxx >= {minx - pad} AND minx <= {maxx + pad} AND maxy >= {miny - pad} AND miny <= {maxy + pad}
                ) t WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
            """).fetchone()
        if not row[0]:
            return None
        return gzip.compress(row[1], compresslevel=5)

    def feature(self, layer: str, fid: str) -> dict | None:
        """Every attribute of one building or unit (geometry and bbox columns excluded)."""
        table = self.meta["layers"][layer]["table"]
        with self._cursor() as cur:
            internal = [_quote(c) for c in cur.table(table).columns if c in _INTERNAL_COLS]
            exclude = f" EXCLUDE ({', '.join(internal)})" if internal else ""
            cur.execute(f"SELECT *{exclude} FROM {_quote(table)} WHERE id = ?", [fid])
            row = cur.fetchone()
            if row is None:
                return None
            return dict(zip([d[0] for d in cur.description], row))

    def stats(self, layer: str, metric: str) -> dict | None:
        """Precomputed JSON legend statistics for one `(layer, metric)` pair, or None if missing."""
        with self._cursor() as cur:
            row = cur.execute(
                "SELECT stats FROM metric_stats WHERE layer = ? AND metric = ?", [layer, metric]
            ).fetchone()
        return None if row is None else row[0]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        with self._lock:
            self._con.close()
