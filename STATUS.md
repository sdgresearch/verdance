# Status

## Current state
- Branch: `docs-site-0.4.0` (uncommitted review fixes; `examples/booth_london.yaml` left untouched with prior local edits).
- All review fixes from the full codebase & documentation audit are implemented across packaging/CI/Docker, core pipeline/config/CLI/API, DGGS, analysis/merge, heights/data sources, optional modules/visibility, viz, docs, and examples.
- Verification passing: `pytest -q` (396 passed, 0 failed, including new `tests/test_review_regressions.py` and expanded `tests/test_docs_consistency.py`), `uvx ruff check .` (0 errors), `mkdocs build --strict` (clean), and `uv build --sdist` (clean, no `netcheck-venv/`).

## Open / known items
- `AGENTS.md` references `docs/project.md`, `docs/status.md`, `docs/decisions.md`, and `docs/changelog.md`, while the actual project memory files live at the repo root (`PROJECT.md`, `STATUS.md`, `DECISIONS.md`, `CHANGELOG.md`).
- `pyproject.toml` still pins `gdal==3.10.0` in `[dependency-groups] dev` (kept untouched to avoid desyncing `uv.lock`).
- Repo contains untracked local study configs/scripts/data (`examples/bogota.yaml`, `examples/cambridge_*.yaml`, `examples/kolkata.yaml`, `chm_processing.R`, `requests_wms.py`, `tree-data-2024-11.csv`, `scripts/prepare_*.py`) — keep out of commits.

## Next steps
1. Review the diff on `docs-site-0.4.0` and commit the review fixes (excluding `examples/booth_london.yaml` and untracked local study files).
2. Decide whether to align `AGENTS.md` paths with the root `PROJECT.md`/`STATUS.md`/`DECISIONS.md`/`CHANGELOG.md` (or move the memory files into `docs/`).
3. In a follow-up commit, drop the unused `gdal==3.10.0` dev pin and refresh `uv.lock`.
