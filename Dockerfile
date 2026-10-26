# syntax=docker/dockerfile:1
# verdance — self-contained image with OpenJDK 17, Python 3.12 and the Sedona
# jars baked in, so the 3-30-300 pipeline runs without installing Java or Spark.
#
#   docker build -t verdance .
#   docker run --rm -v /path/to/project:/work verdance run -c /work/config.yaml -p T3 --buffer 50
#
FROM python:3.12-slim-bookworm

# --- System deps: OpenJDK 17 (Sedona 1.9 needs Java 11+, Spark 3.5 supports up to 17)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    # Resolve the arch-specific JDK dir and expose it at a stable path.
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-17

ENV JAVA_HOME=/opt/java-17
ENV PATH="/app/.venv/bin:${JAVA_HOME}/bin:${PATH}"

# --- uv for dependency install
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_PYTHON_PREFERENCE=only-system \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy

# --- Fixed, world-readable locations for the Sedona jars (Spark's Ivy cache) and
# DuckDB's spatial extension, so the container also runs as any --user.
ENV VERDANCE_IVY_DIR=/opt/ivy \
    VERDANCE_DUCKDB_EXTENSIONS=/opt/duckdb_extensions \
    HOME=/home/verdance

# /etc/passwd is writable so docker/entrypoint.sh can name an arbitrary --user (Spark needs one);
# HOME is writable for libraries that keep settings there (matplotlib, Earth Engine)
RUN useradd --create-home --home-dir /home/verdance --shell /bin/bash verdance \
    && mkdir -p /work /opt/ivy /opt/duckdb_extensions \
    && chown verdance:verdance /work /opt/ivy /opt/duckdb_extensions \
    && chmod a+w /etc/passwd && chmod a+rwx /home/verdance

WORKDIR /app

# Install dependencies first (better layer caching), then the project itself.
# `--no-cache` keeps uv's download cache out of the image layers (pyspark ships
# sdist-only and unpacks ~300 MB of Spark jars). Plain RUN (no BuildKit `--mount`)
# so this builds on both the legacy and BuildKit builders.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache

COPY src/ ./src/
COPY docker/ ./docker/
COPY examples/generic.yaml ./examples/generic.yaml
RUN uv sync --frozen --no-dev --no-cache

# --- Pre-download the Sedona/GeoTools/PostGIS jars and DuckDB's spatial extension,
# so the container starts fast and needs no network for them at runtime.
USER verdance
RUN SPARK_DRIVER_MEMORY=2g python /app/docker/warmup.py \
    && chmod -R a+rwX /opt/ivy && chmod -R a+rX /opt/duckdb_extensions

# --- Runtime defaults (laptop-friendly; override with -e on bigger hosts)
ENV SPARK_DRIVER_MEMORY=4g

# Outputs go where the config's output.base_dir points; mount your project at /work.
WORKDIR /work
EXPOSE 8765

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["--help"]
