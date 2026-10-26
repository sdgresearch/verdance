"""Coverage checks for the driver-side cell generators (a5, rhealpix)."""

from shapely.geometry import Polygon
from shapely.ops import unary_union

from verdance.dggs import get_system

# ~600 m square near Cambridge, UK
POLY = Polygon([(0.10, 52.20), (0.109, 52.20), (0.109, 52.2054), (0.10, 52.2054)])


def _covers(system_name, resolution, tolerance=0.0):
    cells = list(get_system(system_name).cells_covering(POLY, resolution))
    assert cells
    ids = [c for c, _ in cells]
    assert len(ids) == len(set(ids))
    union = unary_union([p for _, p in cells])
    uncovered = POLY.difference(union)
    assert uncovered.area <= tolerance * POLY.area
    return cells


def test_a5_cells_cover_polygon():
    import a5

    # small tolerance: pentagon edges are great-circle arcs drawn as chords
    cells = _covers("a5", 15, tolerance=1e-3)
    # polygon_to_cells compacts its result; every cell must be at the requested resolution
    assert {a5.get_resolution(a5.hex_to_u64(c)) for c, _ in cells} == {15}


def test_rhealpix_cells_cover_polygon():
    _covers("rhealpix", 8, tolerance=1e-6)
