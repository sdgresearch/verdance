# Status

## Current state
- Branch: `codebase-audit-fixes`, pushed to `origin`, three commits ahead of `upstream/master` and fast-forwardable onto it. A PR into `sdgresearch/verdance` is open (or waiting to be opened from the compare link); `gh` is not installed on this HPC, so PRs have to be opened in the browser.
- `3ebadbd` — the whole codebase & documentation audit, 84 files, one regression test per fix.
- `b145575` — `AGENTS.md` memory references fixed to the real uppercase filenames; `STATUS.md` rewritten.
- Settled the 0.4.0 release mechanics: `CITATION.cff` `date-released` is now `2026-11-26`, and `v*` tags are documented as going to `upstream` (see DECISIONS.md), with a `CITATION.cff` check added to the release workflow.
- `examples/booth_london.yaml` is deliberately left uncommitted (local Booth study edits), as are the untracked local study files listed below.
- Verification: `pytest` 396 passed, `ruff check .` clean, `mkdocs build --strict` clean, `uv build --sdist` clean.

## What's broken / watch out for
- `/home` is a shared 212 G volume that other users keep filling (it hit 0 bytes free on 2026-11-25). Always `source /scratch/acz25/greenpy_env.sh`, and set `VERDANCE_IVY_DIR=/scratch/acz25/ivy` and `RUFF_CACHE_DIR=/scratch/acz25/tmp/ruff-cache`. Without the Ivy dir, `tests/test_t3_join.py` fails with `Cannot resolve function ST_Buffer` — that is the disk, not the code.
- `tests/conftest.py` swallows the Sedona session failure (`except Exception: return None`) and falls back to a plain Spark session, which is what turns the above into a confusing unresolved-function error. Should log instead.
- `viz/store.py`'s `unit_table()` sanitises layer names with `[^a-z0-9_]` but without the collision hash that `view_suffix()` got, so distinct names can still collide.
- `merge_t3_and_t300` drops the inner `JOIN buildings` when there are no extra columns, which also drops that join's row-filtering side effect.
- `t30.py` now filters vector trees by `low_threshold`/`high_threshold` — results-changing, and not recorded in DECISIONS.md.
- `binarise_tiles` closes the source arrays in `finally` before `binary` is computed, relying on `merge_arrays` having materialised the data. No test covers it.
- `pyproject.toml` still pins `gdal==3.10.0` in `[dependency-groups] dev`; removing it needs a `uv lock` refresh in its own commit.
- Untracked local study files, keep out of commits: `chm_processing.R`, `requests_wms.py`, `tree-data-2024-11.csv`, `examples/{bogota,cambridge_local,cambridge_local_residential,cambridge_osm,cambridge_osm_residential,kolkata}.yaml`, `scripts/prepare_{bogota_boundaries,booth_trees,kolkata_wards}.py`.

## Next steps
1. Merge the PR into `sdgresearch/verdance`.
2. Release 0.4.0 from `sdgresearch/verdance`: move the `[Unreleased]` CHANGELOG entries under `## [0.4.0]`, then tag the merge commit and `git push upstream v0.4.0`. Still needs doing on the org repo first: enable Pages (source `gh-pages`), grant Actions write access, upload the social preview, and configure the PyPI Trusted Publisher (project `verdance`, owner `sdgresearch`, workflow `release.yml`, environment `pypi`).
3. Follow-up commits for the small items above: the `conftest.py` fallback, `unit_table()` collisions, the `t30.py` threshold decision entry, and the `gdal` dev pin with a refreshed `uv.lock`.
