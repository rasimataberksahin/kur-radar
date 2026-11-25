"""Raw landing zone: source bytes stored untouched and named by their SHA-256.

Layout: ``<raw_dir>/<source>/<yyyy>/<mm>/<source>_<yyyy-mm-dd>_<sha12>.<ext>``.
Same bytes → same file (never rewritten). A revised response → a new file.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_NAME_RE = re.compile(
    r"^(?P<source>[a-z0-9_]+)_(?P<day>\d{4}-\d{2}-\d{2})_(?P<sha>[0-9a-f]{12})\.(?P<ext>\w+)$"
)


@dataclass(frozen=True)
class LandedFile:
    path: Path
    source: str
    period_date: date
    sha256: str


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def land(raw_dir: Path, source: str, day: date, content: bytes, ext: str) -> LandedFile:
    sha = sha256_bytes(content)
    folder = raw_dir / source / f"{day:%Y}" / f"{day:%m}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{source}_{day.isoformat()}_{sha[:12]}.{ext}"
    if not path.exists():
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(content)
        tmp.replace(path)  # atomic on the same volume
    return LandedFile(path, source, day, sha)


def describe(path: Path) -> LandedFile:
    """Rebuild metadata from a landed file and verify its bytes still match the name."""
    m = _NAME_RE.match(path.name)
    if not m:
        raise ValueError(f"not a landed file name: {path.name}")
    sha = sha256_file(path)
    if not sha.startswith(m["sha"]):
        raise ValueError(f"{path.name}: content hash {sha[:12]} does not match the file name")
    return LandedFile(path, m["source"], date.fromisoformat(m["day"]), sha)


def iter_landed(raw_dir: Path, source: str) -> list[LandedFile]:
    base = raw_dir / source
    if not base.exists():
        return []
    files = [describe(p) for p in base.rglob(f"{source}_*") if not p.name.endswith(".tmp")]
    # oldest first, so a later revision of the same day wins on reload
    return sorted(files, key=lambda f: (f.period_date, f.path.stat().st_mtime_ns))
