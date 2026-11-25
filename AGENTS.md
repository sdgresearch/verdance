## Project Overview

verdance (formerly greenpy; PyPI name `verdance`, import `verdance`, command `verdance`) calculates the 3-30-300 rule for any city that has the necessary data.

## Commands

```bash
source /maps/acz25/envs/greenpy/bin/activate
```

## Environment

- **Python 3.12**, managed with **uv**
- Optional `.env` in the working directory: `JDK_HOME` (Java 17 at `/usr/lib/jvm/java-17-openjdk-amd64`), `GEE_PROJECT_NAME`
- Caches and temp files go under /scratch/acz25 (never /home or the repo): `source /scratch/acz25/greenpy_env.sh` sets `TMPDIR`, `VERDANCE_CACHE_DIR`, `VERDANCE_DUCKDB_EXTENSIONS`, `UV_CACHE_DIR`
- Installing a dependency with uv needs the --active flag as the environment folder is located in a different path

## Git workflow

The canonical repo is `sdgresearch/verdance` (renamed from greenpy); `ancazugo/verdance` is a fork of it.

- Remotes: `origin` = the fork (`git@github.com:ancazugo/verdance.git`), `upstream` = the org repo (`git@github.com:sdgresearch/verdance.git`). Local `master` tracks `upstream/master`.
- Push branches to `origin` only, never to `upstream`; changes reach `sdgresearch/verdance` through pull requests from the fork.
- Start work on a new branch off an up-to-date `master`:

```bash
git fetch upstream && git switch master && git merge --ff-only upstream/master   # sync master with the org
git switch -c my-feature master                                                 # branch off it
git push -u origin my-feature                                                   # then open a PR into sdgresearch/verdance
```

## Project memory

Read at the start of every session: PROJECT.md, STATUS.md.
Read when relevant: DECISIONS.md; only the latest entries of CHANGELOG.md.

Update rules (each fact lives in one place):
- CHANGELOG.md — append one dated line per meaningful change (what changed, which files).
- DECISIONS.md — append when choosing between alternatives (method, parameter,
  dataset, model, framing). Format: date · decision · alternatives · reason.
- STATUS.md — rewrite (don't append) at the end of each session: current state,
  what's broken, next 1–3 steps. Keep under ~40 lines; history belongs in the changelog.
- PROJECT.md — only change if the goal, scope or methods change, and log
  that in DECISIONS.md too.

