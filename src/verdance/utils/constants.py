import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# .env in the working directory or a parent of it; variables already set in the environment win.
# Set VERDANCE_NO_DOTENV=1 to skip this — for apps embedding verdance that manage their own env
# and don't want an implicit .env search from the current working directory.
if os.getenv("VERDANCE_NO_DOTENV", "").strip().lower() not in ("1", "true", "yes"):
    load_dotenv(find_dotenv(usecwd=True))

def get_gee_project_name() -> str:
    """Return the configured Earth Engine project name from the environment."""
    return os.getenv("GEE_PROJECT_NAME", "")


GEE_PROJECT_NAME = get_gee_project_name()


def _resolve_java_home() -> str:
    """Resolve the JDK location for Spark.

    JDK_HOME (from .env) may be an absolute JDK path, or just a JDK directory
    name installed under `<DATA_DIR parent>/.jdk/` or `~/.jdk/` (the layout
    used by the install-jdk package). Falls back to a pre-set JAVA_HOME.
    """
    jdk = os.getenv("JDK_HOME", "")
    if jdk:
        p = Path(jdk)
        if p.is_absolute():
            if p.exists():
                return str(p)
        else:
            candidates = [Path.home() / ".jdk" / jdk]
            data_dir = os.getenv("DATA_DIR", "")
            if data_dir:
                candidates.insert(0, Path(data_dir).parent / ".jdk" / jdk)
            for c in candidates:
                if c.exists():
                    return str(c)
    return os.getenv("JAVA_HOME", "")


def get_java_home() -> str:
    """Resolve the current JDK location from JDK_HOME / JAVA_HOME."""
    return _resolve_java_home()


JAVA_HOME = get_java_home()
