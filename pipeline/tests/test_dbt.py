"""Run the real dbt project on fixture data in the test database.

Proves the transformations on known numbers and that the data tests actually fail when
the data is wrong (a test that can't fail proves nothing).
"""

import os
from datetime import date
from decimal import Decimal

import pytest
from conftest import fixture_bytes

from kurradar.ingestion.gold.ounce import YahooChartProvider, load_ounce_history
from kurradar.ingestion.gold.truncgil import load_snapshot
from kurradar.ingestion.landing import land
from kurradar.ingestion.tcmb.loader import load_bulletin
from kurradar.transform import dbt_env, run_dbt

pytestmark = [pytest.mark.db, pytest.mark.dbt]
DBT_SCHEMAS = ["staging", "intermediate", "marts", "reference"]
OZ = Decimal("31.1034768")


@pytest.fixture
def loaded(db_conn, tmp_path):
    for schema in DBT_SCHEMAS:
        db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    db_conn.commit()
    for day, name in [
        (date(2004, 12, 31), "tcmb_2004-12-31.xml"),
        (date(2026, 11, 24), "tcmb_2026-11-24.xml"),
    ]:
        load_bulletin(db_conn, land(tmp_path, "tcmb", day, fixture_bytes(name), "xml"))
    board = fixture_bytes("truncgil_2026-11-25.json")
    load_snapshot(db_conn, land(tmp_path, "truncgil", date(2026, 11, 25), board, "json"))
    p = YahooChartProvider()
    chart = fixture_bytes("yahoo_gcf_2026-11-25.json")
    load_ounce_history(db_conn, p, land(tmp_path, p.source, date(2026, 11, 25), chart, "json"))
    return db_conn


def build(conn):
    conn.commit()  # dbt uses its own connection
    return run_dbt(["build", "--quiet"], os.environ["TEST_DATABASE_URL"])


def val(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


def failed_nodes(result) -> set[str]:
    return {r.node.name for r in result.result if r.status in ("fail", "error")}


def test_dbt_env_is_parsed_from_database_url():
    env = dbt_env("postgresql://u%40x:p%2Fw@db.local:6543/kur")
    assert env == {
        "KURRADAR_DB_HOST": "db.local",
        "KURRADAR_DB_PORT": "6543",
        "KURRADAR_DB_USER": "u@x",
        "KURRADAR_DB_PASSWORD": "p/w",
        "KURRADAR_DB_NAME": "kur",
    }
    with pytest.raises(ValueError):
        dbt_env("mysql://localhost/kur")


def test_build_transforms_fixture_data_exactly(loaded):
    result = build(loaded)
    assert result.success, failed_nodes(result)
    c = loaded

    # old lira ÷ 1,000,000 and JPY ÷ 100 (unit), forex mid
    q = "SELECT mid_try FROM marts.fct_fx_daily WHERE rate_date = %s AND currency_code = %s"
    assert val(c, q, date(2004, 12, 31), "USD") == Decimal("1.3395")
    assert val(c, q, date(2004, 12, 31), "JPY") == Decimal("0.013021")
    assert val(c, q, date(2026, 11, 24), "USD") == Decimal("50.5831")

    # gram gold = ounce × USD/TRY ÷ 31.1034768, USD taken as of the ounce date
    gram, age = c.execute(
        "SELECT gram_try, usd_rate_age_days FROM marts.fct_gold_gram_daily WHERE price_date = %s",
        (date(2026, 11, 25),),
    ).fetchone()
    assert age == 1  # no bulletin for the 25th in the fixtures → the 24th's is used
    assert round(gram, 6) == round(Decimal("3547.9") * Decimal("50.5831") / OZ, 6)

    # board: GRA premium over the international price uses the same-day ounce
    prem = val(
        c,
        "SELECT premium_over_intl_pct FROM marts.fct_gold_board_daily WHERE code = 'GRA'",
    )
    gra_mid = (Decimal("5721.38") + Decimal("5722.25")) / 2
    assert round(prem, 8) == round(gra_mid / (gram * Decimal("0.995")) - 1, 8)

    # labels only look forward, features only backward
    assert val(c, "SELECT count(*) FROM marts.fct_labels_daily WHERE label_date <= obs_date") == 0
    # older ounce bars have no TCMB rate within 10 days in the fixtures → not priced
    assert val(c, "SELECT count(*) FROM marts.fct_gold_gram_daily") == 2


def test_data_tests_fail_on_a_unit_error(loaded):
    # JPY quoted per 100 but loaded as per 1: a 100× error the per-unit test must catch
    loaded.execute("UPDATE raw.tcmb_rates SET unit = 1 WHERE currency_code = 'JPY'")
    result = build(loaded)
    assert not result.success
    assert "assert_per_unit_scaling" in failed_nodes(result)


def test_data_tests_fail_on_a_missed_redenomination(loaded):
    # a 2005 bulletin still in old lira must break the continuity test
    loaded.execute(
        """
        INSERT INTO raw.tcmb_rates (rate_date, currency_code, unit, forex_buying, forex_selling,
                                    source_sha256)
        SELECT '2005-01-03', currency_code, unit, forex_buying, forex_selling, source_sha256
        FROM raw.tcmb_rates WHERE rate_date = '2004-12-31'
        """
    )
    result = build(loaded)
    assert not result.success
    assert {"assert_redenomination_is_continuous", "assert_no_implausible_fx_jumps"} <= (
        failed_nodes(result)
    )
