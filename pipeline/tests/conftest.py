from __future__ import annotations

import os
from pathlib import Path

import pytest
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))
FIXTURES = Path(__file__).parent / "fixtures"


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def old_bulletin() -> bytes:
    return fixture_bytes("tcmb_2004-12-31.xml")


@pytest.fixture
def new_bulletin() -> bytes:
    return fixture_bytes("tcmb_2026-11-24.xml")


@pytest.fixture
def db_conn():
    import psycopg

    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set")
    try:
        conn = psycopg.connect(url, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"test database not reachable ({exc.__class__.__name__})")
    from kurradar.db import migrate

    conn.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    conn.commit()
    migrate(conn)
    yield conn
    conn.close()
