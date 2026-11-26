"""Per-module run manifests: which geo codes a process attempted, completed, and failed.

A module writes `_manifest.json` beside its CSV outputs after a run. Merge reads
these back and warns loudly when a module left regions unfinished, so a partial
run (e.g. 30 of 200 geo codes failed) is visible instead of silently
consolidating into an under-reporting merged table. The manifest also records
the config hash and timings, giving `--no-overwrite` resumes an auditable record
of what actually completed.
"""

import json
import os
import threading
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path

from loguru import logger


def config_hash(cfg) -> str:
    """Short stable hash of the resolved config, tying a manifest to the run that produced it."""
    try:
        payload = asdict(cfg) if is_dataclass(cfg) else cfg
        blob = json.dumps(payload, sort_keys=True, default=str)
    except TypeError:
        blob = repr(cfg)
    return sha1(blob.encode()).hexdigest()[:10]


def write_run_manifest(
    output_dir: Path,
    process: str,
    attempted,
    succeeded,
    failed,
    *,
    cfg=None,
    duration_s: float | None = None,
    extra: dict | None = None,
    merge_existing: bool = False,
) -> Path:
    """Write `<output_dir>/_manifest.json` atomically and log an error if any region failed.

    When `merge_existing=True` (e.g. a single-`--geo_code` retry), results for
    the attempted codes are merged into the existing manifest rather than
    erasing previously recorded failures for other regions.

    Returns the manifest path. Codes are stringified so numeric census codes
    round-trip through JSON unchanged.
    """
    attempted = [str(c) for c in attempted]
    succeeded = [str(c) for c in succeeded]
    failed = [str(c) for c in failed]

    output_dir = Path(output_dir)
    if merge_existing:
        prev = read_run_manifest(output_dir)
        if isinstance(prev, dict) and prev.get("process") == process:
            new_ok = set(succeeded)
            new_fail = set(failed)
            prev_attempted = [str(c) for c in prev.get("attempted", [])]
            prev_ok = [str(c) for c in prev.get("succeeded", []) if str(c) not in new_fail]
            prev_fail = [str(c) for c in prev.get("failed", []) if str(c) not in new_ok]
            attempted = list(dict.fromkeys(prev_attempted + attempted))
            succeeded = list(dict.fromkeys(prev_ok + succeeded))
            failed = list(dict.fromkeys(prev_fail + failed))

    manifest = {
        "process": process,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "config_hash": config_hash(cfg) if cfg is not None else None,
        "n_attempted": len(attempted),
        "n_succeeded": len(succeeded),
        "n_failed": len(failed),
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
    }
    if duration_s is not None:
        manifest["duration_s"] = round(duration_s, 2)
    if extra:
        # Never let extra metadata clobber core manifest fields
        for k, v in extra.items():
            if k not in manifest:
                manifest[k] = v

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "_manifest.json"
    tmp = output_dir / f"._manifest.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        tmp.write_text(json.dumps(manifest, indent=2))
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)

    if failed:
        logger.error(f"{process}: {len(failed)}/{len(attempted)} regions FAILED: {failed} (see {path})")
    else:
        logger.info(f"{process}: {len(succeeded)}/{len(attempted)} regions succeeded (manifest: {path.name})")
    return path


def read_run_manifest(output_dir: Path) -> dict | None:
    """The `_manifest.json` in output_dir, or None when it is absent or unreadable."""
    path = Path(output_dir) / "_manifest.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            logger.warning(f"Manifest {path} is not a JSON object — ignoring")
            return None
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read manifest {path}: {e}")
        return None


def warn_on_incomplete_runs(base_dir: Path, module_dirs: dict[str, Path]) -> list[str]:
    """Warn for each module whose manifest records failed regions. Returns modules with failures.

    `module_dirs` maps a display name to the directory holding that module's
    outputs (and manifest). Modules with no manifest (run before manifests
    existed, or never run) are skipped silently — the merge already reports
    missing outputs separately.
    """
    incomplete = []
    for name, directory in module_dirs.items():
        manifest = read_run_manifest(directory)
        if manifest and manifest.get("n_failed"):
            incomplete.append(name)
            logger.warning(
                f"{name}: {manifest['n_failed']} of {manifest['n_attempted']} regions failed in the last run "
                f"({manifest.get('failed')}) — the merged table will under-report them. "
                "Re-run that module before trusting the output."
            )
    return incomplete
