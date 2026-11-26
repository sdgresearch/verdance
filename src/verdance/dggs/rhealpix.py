"""rHEALPix equal-area grid, generated driver-side with the rhealpixdggs library.

Uses the predefined WGS84 ellipsoid instance with N_side=3. Cell ids are the
standard string form (e.g. "N24785"). cells_from_region covers the rectangle
between the *projected* corners, which can miss cells at the lon/lat corners,
so the bounding box is first padded by one cell's angular extent; the shared
finalizer trims cells not touching the unit.
"""

from typing import Iterable

from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry

from .base import PythonDGGS


class RHEALPixSystem(PythonDGGS):
    name = "rhealpix"
    min_resolution = 0
    max_resolution = 15

    def cells_covering(
        self, geom_wgs84: BaseGeometry, resolution: int
    ) -> Iterable[tuple[str, Polygon]]:
        from rhealpixdggs.dggs import WGS84_003

        if geom_wgs84 is None or geom_wgs84.is_empty:
            return

        # Project all densified boundary vertices (plus bbox corners and centre) onto the
        # rHEALPix plane and cover the resulting planar bounding box padded by one cell width.
        # This avoids missing NE/SW corners on rotated polar-cap squares (|lat| > 41.81°).
        minx, miny, maxx, maxy = geom_wgs84.bounds
        cell_w = WGS84_003.cell_width(resolution)
        centre = geom_wgs84.representative_point()
        sample_lonlat: list[tuple[float, float]] = [
            (centre.x, centre.y),
            (minx, miny), (minx, maxy), (maxx, miny), (maxx, maxy),
        ]

        def _collect_rings(g):
            if g.is_empty:
                return
            if g.geom_type == "Polygon":
                yield g.exterior
                yield from g.interiors
            elif hasattr(g, "geoms"):
                for sub in g.geoms:
                    yield from _collect_rings(sub)

        # Estimate a sensible angular spacing so long edges along seams are sampled
        seed = WGS84_003.cell_from_point(resolution, (centre.x, centre.y), plane=False)
        sminx, sminy, smaxx, smaxy = Polygon(seed.vertices(plane=False, trim_dart=True)).bounds
        step_deg = max(min(smaxx - sminx, smaxy - sminy) / 2.0, 1e-4)
        for ring in _collect_rings(geom_wgs84):
            length = ring.length
            n_seg = min(max(int(length / step_deg) + 1, 1), 2000)
            for k in range(n_seg):
                pt = ring.interpolate(k / n_seg, normalized=True)
                sample_lonlat.append((pt.x, pt.y))
            for x, y, *_ in ring.coords:
                sample_lonlat.append((x, y))

        pxs, pys = [], []
        for lon, lat in sample_lonlat:
            px, py = WGS84_003.rhealpix(lon, lat, inverse=False)
            pxs.append(px)
            pys.append(py)

        ul = (min(pxs) - cell_w, max(pys) + cell_w)
        dr = (max(pxs) + cell_w, min(pys) - cell_w)
        cell_rows = WGS84_003.cells_from_region(resolution, ul=ul, dr=dr, plane=True)
        seen: set[str] = set()
        for row in cell_rows:
            for cell in row:
                cid = str(cell)
                if cid not in seen:
                    seen.add(cid)
                    yield cid, Polygon(cell.vertices(plane=False, trim_dart=True))
