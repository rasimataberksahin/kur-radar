from datetime import date

import httpx
import pytest

from kurradar.ingestion.tcmb.client import TcmbClient, TcmbFetchError, bulletin_url


def make_client(handler, sleeps=None, **kw):
    return TcmbClient(
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://tcmb.test/kurlar",
        min_interval=0,
        sleep=(sleeps.append if sleeps is not None else lambda s: None),
        **kw,
    )


def test_url_format():
    assert bulletin_url(date(2026, 1, 5)) == "https://www.tcmb.gov.tr/kurlar/202601/05012026.xml"


def test_200_returns_bytes(new_bulletin):
    seen = []

    def handler(req):
        seen.append(str(req.url))
        return httpx.Response(200, content=new_bulletin)

    r = make_client(handler).fetch(date(2026, 11, 24))
    assert r.published and r.content == new_bulletin and r.http_status == 200
    assert seen == ["https://tcmb.test/kurlar/202611/24112026.xml"]


def test_404_means_not_published():
    r = make_client(lambda req: httpx.Response(404, text="<html>yok</html>")).fetch(
        date(2026, 11, 22)
    )
    assert not r.published and r.http_status == 404 and r.content is None


def test_5xx_and_network_errors_are_retried(new_bulletin):
    calls = iter(["boom", 503, 200])

    def handler(req):
        nxt = next(calls)
        if nxt == "boom":
            raise httpx.ConnectError("down")
        return httpx.Response(nxt, content=new_bulletin)

    sleeps = []
    r = make_client(handler, sleeps).fetch(date(2026, 11, 24))
    assert r.published and sleeps == [2, 4]  # exponential backoff


def test_gives_up_after_max_retries():
    client = make_client(lambda req: httpx.Response(503), max_retries=2)
    with pytest.raises(TcmbFetchError, match="503"):
        client.fetch(date(2026, 11, 24))


def test_html_served_with_200_is_rejected():
    client = make_client(lambda req: httpx.Response(200, text="<html>bakım</html>"))
    with pytest.raises(TcmbFetchError, match="not a TCMB XML"):
        client.fetch(date(2026, 11, 24))


def test_rate_limit_spaces_requests(new_bulletin):
    now = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        now[0] += s

    client = TcmbClient(
        http=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(404))),
        min_interval=0.5,
        sleep=sleep,
        clock=lambda: now[0],
    )
    for d in (1, 2, 3):
        client.fetch(date(2026, 11, d))
    assert sleeps == [0.5, 0.5]
