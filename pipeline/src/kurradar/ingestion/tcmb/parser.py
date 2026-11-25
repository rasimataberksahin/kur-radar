"""Parse a TCMB bulletin XML into validated rows (values kept exactly as published).

Handles both eras: ISO-8859-9 files with old-lira values before 2005
(e.g. USD 1336300) and UTF-8 files with new-lira values afterwards.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree.ElementTree import Element

from defusedxml import DefusedXmlException
from defusedxml import ElementTree as SafeET

_CODE_RE = re.compile(r"^[A-Z]{3}$")
_VALUE_FIELDS = {
    "ForexBuying": "forex_buying",
    "ForexSelling": "forex_selling",
    "BanknoteBuying": "banknote_buying",
    "BanknoteSelling": "banknote_selling",
    "CrossRateUSD": "cross_rate_usd",
    "CrossRateOther": "cross_rate_other",
}
COLUMNS = ["unit", "name_tr", "name_en", *_VALUE_FIELDS.values(), "bulletin_no"]


_TRY_COLUMNS = ["forex_buying", "forex_selling", "banknote_buying", "banknote_selling"]
# footnote markers TCMB occasionally prints instead of a value, e.g. EUR on 31.12.2001
_FOOTNOTE_RE = re.compile(r"^\(\*+\)$")


class TcmbPayloadError(ValueError):
    pass


class NoBulletinError(TcmbPayloadError):
    """The response is valid but is not a new bulletin for the requested day."""


class EmptyBulletinError(NoBulletinError):
    """A well-formed bulletin with no TRY rates (e.g. 17.08.1999, the Marmara earthquake)."""


class StaleBulletinError(NoBulletinError):
    """TCMB re-served an earlier day's bulletin (e.g. 28.01.1998 returns 27.01.1998)."""


@dataclass(frozen=True)
class Bulletin:
    rate_date: date
    bulletin_no: str | None
    rows: list[dict[str, Any]]
    reference_only: list[str] = field(default_factory=list)  # listed, but no TRY rate


def _text(el: Element, tag: str) -> str | None:
    child = el.find(tag)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def _decimal(raw: str | None, field: str, code: str) -> Decimal | None:
    if raw is None or _FOOTNOTE_RE.match(raw):
        return None
    try:
        value = Decimal(raw.replace(",", "."))
    except InvalidOperation:
        raise TcmbPayloadError(f"{code}.{field} is not numeric: {raw!r}") from None
    if value < 0:
        raise TcmbPayloadError(f"{code}.{field} is negative: {raw!r}")
    return value


def parse_bulletin(content: bytes, expected_date: date | None = None) -> Bulletin:
    try:
        root = SafeET.fromstring(content)
    except SafeET.ParseError as exc:
        raise TcmbPayloadError(f"not well-formed XML: {exc}") from None
    except DefusedXmlException as exc:  # DTD/entity tricks are never legit in a bulletin
        raise TcmbPayloadError(f"forbidden XML construct: {exc.__class__.__name__}") from None
    if root.tag != "Tarih_Date":
        raise TcmbPayloadError(f"unexpected root element <{root.tag}>")

    try:
        rate_date = datetime.strptime(root.attrib["Tarih"], "%d.%m.%Y").date()
    except (KeyError, ValueError):
        raise TcmbPayloadError("missing or malformed Tarih attribute") from None
    if expected_date is not None and rate_date != expected_date:
        if rate_date < expected_date:
            raise StaleBulletinError(f"bulletin is dated {rate_date}, expected {expected_date}")
        raise TcmbPayloadError(f"bulletin is dated {rate_date}, expected {expected_date}")
    bulletin_no = root.attrib.get("Bulten_No")

    rows: dict[str, dict[str, Any]] = {}
    reference_only: list[str] = []
    currencies = list(root.iter("Currency"))
    if not currencies:
        raise EmptyBulletinError(f"bulletin {rate_date} lists no currencies")

    for cur in currencies:
        code = (cur.attrib.get("CurrencyCode") or cur.attrib.get("Kod") or "").strip().upper()
        if not _CODE_RE.match(code):
            raise TcmbPayloadError(f"invalid currency code {code!r}")
        if code in rows or code in reference_only:
            raise TcmbPayloadError(f"duplicate currency {code}")

        values = {col: _decimal(_text(cur, tag), tag, code) for tag, col in _VALUE_FIELDS.items()}
        unit_raw = _text(cur, "Unit")
        if unit_raw is None and all(values[c] is None for c in _TRY_COLUMNS):
            # 2002–2008: legacy euro-zone currencies (DEM, FRF, IEP, …) are still listed
            # with an empty Unit and only their fixed CrossRateEuro — not a TRY rate.
            reference_only.append(code)
            continue
        try:
            unit = int(unit_raw or "")
        except ValueError:
            raise TcmbPayloadError(f"{code}: missing or non-integer Unit") from None
        if unit <= 0:
            raise TcmbPayloadError(f"{code}: Unit must be positive")
        if all(v is None for v in values.values()):
            raise TcmbPayloadError(f"{code}: no rate values at all")

        rows[code] = {
            "rate_date": rate_date,
            "currency_code": code,
            "unit": unit,
            # 2014–2016 bulletins publish XDR with an empty <Isim>; the name is optional
            "name_tr": _text(cur, "Isim"),
            "name_en": _text(cur, "CurrencyName"),
            "bulletin_no": bulletin_no,
            **values,
        }

    if not rows:
        raise EmptyBulletinError(f"bulletin {rate_date} has no TRY rates")
    return Bulletin(rate_date, bulletin_no, [rows[c] for c in sorted(rows)], sorted(reference_only))
