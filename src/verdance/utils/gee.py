"""Google Earth Engine session shared by every GEE-backed source."""

import threading

from loguru import logger

_GEE_READY = False
_LOCK = threading.Lock()

_HIGH_VOLUME_URL = "https://earthengine-highvolume.googleapis.com"


def ensure_gee(project: str | None = None) -> None:
    """Initialise Google Earth Engine once per process, with a clear message when auth is missing.

    Does not call ``ee.Authenticate()`` (which blocks on an interactive flow in a
    headless run); credentials must already be stored via ``earthengine
    authenticate``. The project falls back to GEE_PROJECT_NAME from the env/.env.
    """
    global _GEE_READY
    with _LOCK:
        if _GEE_READY:
            return
        import ee

        from .constants import get_gee_project_name

        project = project or get_gee_project_name() or None
        logger.debug(f"Initialising Google Earth Engine for project: {project}")
        try:
            ee.Initialize(project=project, opt_url=_HIGH_VOLUME_URL)
        except Exception as e:
            raise RuntimeError(
                "Could not initialise Google Earth Engine. Run `earthengine authenticate` once, and set "
                "gee_project in the config (or GEE_PROJECT_NAME) to your Earth Engine Cloud project. "
                f"Original error: {e}"
            ) from e
        _GEE_READY = True


def write_raster(da, path, **kwargs) -> None:
    """Write an xee-downloaded DataArray (CRS and nodata already set) to a GeoTIFF, atomically.

    xee records the download scale in encoding["scale_factor"]; rioxarray's
    to_raster would divide the stored values by it and tag the band with that
    scale, which readers such as Sedona's RS_FromGeoTiff ignore — so the
    encoding is dropped and values are written as they are.
    """
    import os
    from pathlib import Path

    # Shallow copy shares the underlying array without duplicating ~1.5 GB in RAM
    da = da.copy(deep=False)
    da.encoding = {}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.part")
    # compressed and tiled by default: city-wide 1 m canopy masks are ~1.5 GB
    # uncompressed, and Spark's binaryFile reader (T30_buildings) caps files at 2 GB
    options = {"compress": "deflate", "tiled": True, "blockxsize": 512, "blockysize": 512,
               "bigtiff": "IF_SAFER", **kwargs}
    try:
        da.rio.to_raster(tmp, driver="GTiff", **options)
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
