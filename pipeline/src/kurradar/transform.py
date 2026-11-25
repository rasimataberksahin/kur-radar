"""Run the dbt project in ``transform/`` against DATABASE_URL (one connection setting for all)."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

# src/kurradar/transform.py → repo root is three levels above the package
PROJECT_DIR = Path(__file__).resolve().parents[3] / "transform"


def dbt_env(database_url: str) -> dict[str, str]:
    """KURRADAR_DB_* variables read by transform/profiles.yml."""
    u = urlsplit(database_url)
    if u.scheme not in ("postgresql", "postgres") or not u.hostname or not u.path.strip("/"):
        raise ValueError("DATABASE_URL must look like postgresql://user:pass@host:port/dbname")
    return {
        "KURRADAR_DB_HOST": u.hostname,
        "KURRADAR_DB_PORT": str(u.port or 5432),
        "KURRADAR_DB_USER": unquote(u.username or ""),
        "KURRADAR_DB_PASSWORD": unquote(u.password or ""),
        "KURRADAR_DB_NAME": u.path.strip("/"),
    }


def run_dbt(args: list[str], database_url: str, project_dir: Path = PROJECT_DIR):
    """Invoke dbt in-process and return its ``dbtRunnerResult`` (``.success``, ``.result``)."""
    from dbt.cli.main import dbtRunner  # heavy import, only when needed

    os.environ.update(dbt_env(database_url))
    cli = [
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]
    return dbtRunner().invoke(cli)
