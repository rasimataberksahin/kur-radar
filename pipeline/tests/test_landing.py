import hashlib
from datetime import date

import pytest

from kurradar.ingestion.landing import describe, iter_landed, land


def test_land_is_content_addressed_and_idempotent(tmp_path):
    a = land(tmp_path, "tcmb", date(2026, 11, 24), b"<x/>", "xml")
    mtime = a.path.stat().st_mtime_ns
    b = land(tmp_path, "tcmb", date(2026, 11, 24), b"<x/>", "xml")
    assert a == b and a.path.stat().st_mtime_ns == mtime
    assert a.sha256 == hashlib.sha256(b"<x/>").hexdigest()
    assert a.path.relative_to(tmp_path).as_posix() == (
        f"tcmb/2026/11/tcmb_2026-11-24_{a.sha256[:12]}.xml"
    )


def test_revision_lands_next_to_original(tmp_path):
    land(tmp_path, "tcmb", date(2026, 11, 24), b"v1", "xml")
    land(tmp_path, "tcmb", date(2026, 11, 24), b"v2", "xml")
    assert len(iter_landed(tmp_path, "tcmb")) == 2


def test_tampered_file_is_detected(tmp_path):
    f = land(tmp_path, "tcmb", date(2026, 11, 24), b"v1", "xml")
    assert describe(f.path) == f
    f.path.write_bytes(b"edited")
    with pytest.raises(ValueError, match="does not match"):
        describe(f.path)


def test_foreign_files_rejected(tmp_path):
    (tmp_path / "notes.txt").write_text("x")
    with pytest.raises(ValueError):
        describe(tmp_path / "notes.txt")
