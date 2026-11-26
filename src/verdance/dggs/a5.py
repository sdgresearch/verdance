"""A5 pentagonal equal-area grid, generated driver-side with the pya5 library.

Cell ids are stored as the 16-char hex of the uint64 id (a5.u64_to_hex).
a5.polygon_to_cells does the polyfill but is not a full cover (cells only
partially overlapping the polygon can be missed), so the result is expanded by
one neighbour ring with a5.grid_disk (matching H3's full-cover semantics) and
the shared finalizer trims the excess. Both return compacted sets (coarser
parents where every child is included), so each is uncompacted back to the
requested resolution.
"""

from typing import Iterable

from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry

from .base import PythonDGGS


class A5System(PythonDGGS):
    name = "a5"
    min_resolution = 0
    max_resolution = 30

    def cells_covering(
        self, geom_wgs84: BaseGeometry, resolution: int
    ) -> Iterable[tuple[str, Polygon]]:
        import a5

        if geom_wgs84 is None or geom_wgs84.is_empty:
            return

        def _polygons(geom):
            if geom.is_empty:
                return
            if geom.geom_type == "Polygon":
                yield geom
            elif geom.geom_type in ("MultiPolygon", "GeometryCollection"):
                for sub in geom.geoms:
                    yield from _polygons(sub)

        base = set()
        for part in _polygons(geom_wgs84):
            rings = (
                [[(x, y) for x, y, *_ in part.exterior.coords]]
                + [[(x, y) for x, y, *_ in r.coords] for r in part.interiors]
            )
            part_cells = set(a5.uncompact(a5.polygon_to_cells(rings, resolution)))
            if not part_cells:
                # part smaller than one cell — seed from its representative point
                point = part.representative_point()
                part_cells.add(a5.lonlat_to_cell((point.x, point.y), resolution))
            base.update(part_cells)
        if not base:
            point = geom_wgs84.representative_point()
            base.add(a5.lonlat_to_cell((point.x, point.y), resolution))
        cells = set()
        for cell in base:
            cells.update(a5.uncompact(a5.grid_disk(cell, 1)))
        for cell in cells:
            yield a5.u64_to_hex(cell), Polygon(a5.cell_to_boundary(cell))
