"""Build-time Sedona/Spark JAR warm-up.

Starting a Sedona session forces Ivy to resolve and download every jar listed
in `spark.jars.packages` (Sedona, GeoTools, PostGIS, PostgreSQL) into the Ivy
cache. Running this during `docker build` bakes those jars into the image so
the container starts fast and works with no network at runtime.

Uses the real `get_spark()` so the cached jars exactly match what runtime asks
for. Keep the driver/executor memory small at build time via the
SPARK_DRIVER_MEMORY / SPARK_EXECUTOR_MEMORY env vars (set in the Dockerfile).
Also installs DuckDB's spatial extension for `verdance viz`.
"""

from verdance.utils.sedona_config import get_spark


def main() -> None:
    sedona = get_spark()
    print(f"Sedona/Spark {sedona.sparkContext.version} session started for warm-up; jars cached.")
    sedona.stop()
    print("JAR warm-up complete.")

    # `verdance viz` needs DuckDB's spatial extension, which is otherwise downloaded on first use;
    # connect() installs it into $VERDANCE_DUCKDB_EXTENSIONS
    from verdance.viz.store import connect

    connect(":memory:").close()
    print("DuckDB spatial extension installed.")


if __name__ == "__main__":
    main()
