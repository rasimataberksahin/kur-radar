from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    raw_data_dir: Path
    tcmb_min_interval: float

    def __repr__(self) -> str:  # keep the DB password out of logs
        return f"Settings(database_url=<hidden>, raw_data_dir={self.raw_data_dir})"


def load_settings() -> Settings:
    env_file = find_dotenv(usecwd=True)
    load_dotenv(env_file)
    # relative paths are anchored at the repo root (where .env lives), not the cwd
    root = Path(env_file).parent if env_file else Path.cwd()
    raw = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
    return Settings(
        database_url=os.getenv("DATABASE_URL", ""),
        raw_data_dir=raw if raw.is_absolute() else root / raw,
        tcmb_min_interval=float(os.getenv("TCMB_MIN_INTERVAL_SECONDS", "0.25")),
    )
