from datetime import date
from decimal import Decimal

import pytest

from kurradar.ingestion.tcmb.parser import (
    EmptyBulletinError,
    TcmbPayloadError,
    parse_bulletin,
)


def test_parses_pre_2005_iso_8859_9_bulletin(old_bulletin):
    b = parse_bulletin(old_bulletin, expected_date=date(2004, 12, 31))
    assert b.rate_date == date(2004, 12, 31) and b.bulletin_no == "2004/251"
    by_code = {r["currency_code"]: r for r in b.rows}
    assert list(by_code) == ["CHF", "JPY", "USD"]  # sorted
    # old-lira values are kept exactly as published; dbt normalises later
    assert by_code["USD"]["forex_buying"] == Decimal("1336300")
    assert by_code["JPY"]["unit"] == 100
    assert by_code["CHF"]["name_tr"] == "İSVİÇRE FRANGI"  # Turkish chars decoded


def test_parses_current_utf8_bulletin_with_bom_and_blanks(new_bulletin):
    b = parse_bulletin(new_bulletin, expected_date=date(2026, 11, 24))
    by_code = {r["currency_code"]: r for r in b.rows}
    usd, eur, xdr = by_code["USD"], by_code["EUR"], by_code["XDR"]
    assert usd["forex_selling"] == Decimal("50.6286")
    assert usd["cross_rate_usd"] is None
    assert eur["cross_rate_other"] == Decimal("1.0958")
    assert xdr["forex_selling"] is None and xdr["forex_buying"] == Decimal("68.7716")
    assert xdr["name_tr"] == "ÖZEL ÇEKME HAKKI (SDR)"  # padding stripped


def test_rejects_wrong_date(new_bulletin):
    with pytest.raises(TcmbPayloadError, match="expected 2026-11-25"):
        parse_bulletin(new_bulletin, expected_date=date(2026, 11, 25))


def _doc(currencies: str, tarih: str = "24.11.2026") -> bytes:
    head = '<?xml version="1.0" encoding="UTF-8"?>'
    return f'{head}<Tarih_Date Tarih="{tarih}">{currencies}</Tarih_Date>'.encode()


USD = (
    '<Currency CurrencyCode="USD"><Unit>1</Unit><Isim>ABD DOLARI</Isim>'
    "<ForexBuying>{v}</ForexBuying></Currency>"
)


@pytest.mark.parametrize(
    "doc, msg",
    [
        (b"<html>maintenance</html>", "unexpected root"),
        (b"<Tarih_Date", "not well-formed"),
        (_doc(USD.format(v="1"), tarih="2026-11-24"), "Tarih"),
        (_doc(""), "lists no currencies"),
        (_doc(USD.format(v="1") * 2), "duplicate"),
        (_doc(USD.format(v="abc")), "not numeric"),
        (_doc(USD.format(v="-1")), "negative"),
        (_doc(USD.format(v="")), "no rate values"),
        (_doc(USD.format(v="1").replace("<Unit>1</Unit>", "<Unit>0</Unit>")), "positive"),
        (_doc(USD.format(v="1").replace('"USD"', '"US"')), "invalid currency"),
    ],
)
def test_rejects_malformed_bulletins(doc, msg):
    with pytest.raises(TcmbPayloadError, match=msg):
        parse_bulletin(doc)


def test_blocks_xml_entity_expansion():
    bomb = (
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]>'
        b'<Tarih_Date Tarih="24.11.2026">&b;</Tarih_Date>'
    )
    with pytest.raises(TcmbPayloadError, match="forbidden XML construct"):
        parse_bulletin(bomb)


# --- real-world quirks found in the 1996–2026 archive --------------------------------

LEGACY_DEM = (
    '<Currency CurrencyCode="DEM"><Unit/><Isim>ALMAN MARKI</Isim><ForexBuying/>'
    "<ForexSelling/><CrossRateEuro>1.95583</CrossRateEuro></Currency>"
)


def test_legacy_eurozone_currency_without_try_rate_is_skipped():
    b = parse_bulletin(_doc(USD.format(v="1.3383") + LEGACY_DEM))
    assert [r["currency_code"] for r in b.rows] == ["USD"]
    assert b.reference_only == ["DEM"]


def test_empty_unit_with_a_try_rate_is_still_an_error():
    doc = _doc(USD.format(v="1").replace("<Unit>1</Unit>", "<Unit/>"))
    with pytest.raises(TcmbPayloadError, match="Unit"):
        parse_bulletin(doc)


def test_blank_name_is_allowed():  # XDR in 2014–2016
    (row,) = parse_bulletin(_doc(USD.format(v="1").replace("ABD DOLARI", ""))).rows
    assert row["name_tr"] is None


def test_footnote_marker_becomes_null():  # EUR on 31.12.2001
    doc = _doc(
        USD.format(v="1")
        + USD.replace("USD", "EUR")
        .format(v="(*)")
        .replace("</Currency>", "<ForexSelling>1.2</ForexSelling></Currency>")
    )
    eur = [r for r in parse_bulletin(doc).rows if r["currency_code"] == "EUR"][0]
    assert eur["forex_buying"] is None and eur["forex_selling"] == Decimal("1.2")


@pytest.mark.parametrize("body", ["", LEGACY_DEM])
def test_bulletin_without_try_rates_is_empty(body):  # 17.08.1999
    with pytest.raises(EmptyBulletinError):
        parse_bulletin(_doc(body))


def test_earlier_dated_bulletin_is_stale_later_dated_is_invalid(new_bulletin):
    from kurradar.ingestion.tcmb.parser import StaleBulletinError

    with pytest.raises(StaleBulletinError):
        parse_bulletin(new_bulletin, expected_date=date(2026, 11, 25))
    with pytest.raises(TcmbPayloadError) as exc:
        parse_bulletin(new_bulletin, expected_date=date(2026, 11, 23))
    assert not isinstance(exc.value, StaleBulletinError)
