"""Where verdance keeps downloaded data shared between runs."""

import os
from pathlib import Path


def cache_dir_for(cfg) -> Path:
    """Download cache: data.chm_cache_dir, else $VERDANCE_CACHE_DIR, else <output.base_dir>/database/chm_cache.

    Holds CHM tiles, Earth Engine downloads (heights, terrain), composites and
    OpenStreetMap responses, so reruns and neighbouring study areas reuse them.
    """
    d = cfg.data.chm_cache_dir or os.environ.get("VERDANCE_CACHE_DIR")
    return Path(d) if d else Path(cfg.output.base_dir) / "database" / "chm_cache"
