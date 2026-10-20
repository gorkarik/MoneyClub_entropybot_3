"""Ondo Perps as the hedge venue: names, lot/tick grid, signing, IOC
settlement (response / private ws / GET fallback), account reads, config
and menu. The exchange itself is faked: nothing here touches the network.

Run:  python3 -m pytest tests/
"""
import asyncio
import json
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import venue_ondo as vo  # noqa: E402
from entropy_arb.config import OndoCreds, VenueConf, load_config  # noqa: E402
from test_tickers import BASE  # noqa: E402

MARKETS = {"perps": {"tradingPairs": [
    {"market": "NVDA-USD.P", "baseIncrement": "0.01", "quoteIncrement": "0.01"},
    {"market": "XAU-USD.P", "baseIncrement": "0.001",
     "quoteIncrement": "0.05"}]}}
CONTRACTS = [{"market": "NVDA-USD.P", "disabled": False, "takerFee": "0.00025",
              "isClosed": False},
             {"market": "XAU-USD.P", "disabled": False, "takerFee": "0.00025",
              "isClosed": True}]


def venue(names=("NVDA",), fee=0.0, routes=None, creds=True):
    conf = VenueConf(key="hedge", kind="ondo", label="ONDO", symbol=names[0],
                     symbol_aliases=tuple(names), fee_bps=fee, cap_usd=100,
                     orders_per_min=30,
                     ondo_creds=OndoCreds("ondoKeyId_k", "ondoApiSecret_s")
                     if creds else None)
    v = vo.OndoVenue(conf, session=None, settle_timeout_sec=0.2)
    v.calls = []
    table = {("GET", "/v1/markets"): MARKETS,
             ("GET", "/v1/perps/contracts"): CONTRACTS, **(routes or {})}

    async def req(method, path, params=None, body=None, auth=True,
                  background=False):
        v.calls.append((method, path, params, body))
        r = table[(method, path)]
        if isinstance(r, Exception):
            raise r
        return r(body) if callable(r) else r
    v._req = req
    return v


def load(v):
    asyncio.run(v.load_market())
    return v


# ------------------------------------------------------------ names, grid

def test_market_names_and_step_decimals():
    assert vo.market_names(["xau", "GOLD", "XAU-USD.P"]) == (
        "XAU-USD.P", "GOLD-USD.P")
    assert vo.step_decimals(Decimal("0.01")) == 2
    assert vo.step_decimals(Decimal("0.001")) == 3
    assert vo.step_decimals(Decimal("1")) == 0
    # a lot of 0.5: whole units are on the grid, tenths are not
    assert vo.step_decimals(Decimal("0.5")) == 0
    assert vo.step_decimals(Decimal("0.25")) == 0


def test_load_market_picks_alias_tick_lot_and_fee():
    v = load(venue(("GOLD", "XAU")))
    assert v.market == "XAU-USD.P" and v.conf.symbol == "XAU-USD.P"
    assert (v.size_decimals, v.min_base) == (3, 0.001)
    assert v.fee_bps == pytest.approx(2.5)        # pair fee 0: exchange's
    assert v.is_closed
    v = load(venue(("NVDA",), fee=3.0))
    assert v.fee_bps == 3.0                         # pair fee kept


def test_load_market_not_found_or_disabled():
    with pytest.raises(RuntimeError, match="BTC-USD.P"):
        load(venue(("BTC",)))
    off = [{**CONTRACTS[0], "disabled": True}]
    with pytest.raises(RuntimeError, match="disabled"):
        load(venue(routes={("GET", "/v1/perps/contracts"): off}))


def test_price_and_size_on_the_grid():
    v = load(venue(("XAU",)))
    assert v.px_round(2400.03, True) == 2400.05
    assert v.px_round(2400.03, False) == 2400.0
    assert v._fmt(0.12345, v.lot) == "0.123"


# --------------------------------------------------------------- signing

class FakeResp:
    def __init__(self, status, payload):
        self.status, self.payload = status, payload

    async def json(self, content_type=None):
        return self.payload

    async def text(self):
        return json.dumps(self.payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeSession:
    def __init__(self, status=200, payload=None):
        self.status = status
        self.payload = payload or {"success": True, "result": {"ok": 1}}
        self.seen = []

    def request(self, method, url, data=None, headers=None, timeout=None):
        self.seen.append((method, url, data, headers))
        return FakeResp(self.status, self.payload)


def real_venue(session):
    conf = VenueConf(key="hedge", kind="ondo", label="ONDO", symbol="NVDA",
                     fee_bps=2.5, cap_usd=100, orders_per_min=30,
                     ondo_creds=OndoCreds("ondoKeyId_k", "ondoApiSecret_s"))
    return vo.OndoVenue(conf, session, 0.2, api_url="https://x")


def test_requests_are_signed_like_the_docs(monkeypatch):
    monkeypatch.setattr(vo, "REST_MIN_GAP_SEC", 0.0)
    s = FakeSession()
    v = real_venue(s)
    body = {"market": "NVDA-USD.P", "side": "buy"}
    asyncio.run(v._req("POST", "/v1/perps/orders", body=body))
    asyncio.run(v._req("GET", "/v1/perps/orders", params={"market": "N",
                                                          "limit": "5"}))
    (m1, u1, d1, h1), (m2, u2, d2, h2) = s.seen
    assert u1 == "https://x/v1/perps/orders" and d1 == json.dumps(
        body, separators=(",", ":"))
    assert h1["ONDO-KEY-ID"] == "ondoKeyId_k"
    assert h1["ONDO-SIGN"] == vo.sign("ondoApiSecret_s", h1["ONDO-TIMESTAMP"],
                                      "POST", "/v1/perps/orders", d1)
    # GET: the query string is signed, the body is empty
    assert u2 == "https://x/v1/perps/orders?market=N&limit=5" and d2 is None
    assert h2["ONDO-SIGN"] == vo.sign(
        "ondoApiSecret_s", h2["ONDO-TIMESTAMP"], "GET",
        "/v1/perps/orders?market=N&limit=5", "")
    import hashlib
    import hmac
    assert vo.sign("s", "1", "GET", "/p", "") == hmac.new(
        b"s", b"1GET/p", hashlib.sha256).hexdigest()


def test_error_envelope_raises_with_code(monkeypatch):
    monkeypatch.setattr(vo, "REST_MIN_GAP_SEC", 0.0)
    v = real_venue(FakeSession(400, {"success": False, "error": "no",
                                     "error_code": "insufficient_margin"}))
    with pytest.raises(vo.OndoError) as e:
        asyncio.run(v._req("GET", "/v1/perps/balance"))
    assert e.value.code == "insufficient_margin" and e.value.status == 400


# ------------------------------------------------------------ execution

def order(status, filled="0", cost="0", cid="x"):
    return {"orderId": "o1", "clientOrderId": cid, "status": status,
            "filledSize": filled, "filledCost": cost}


def test_ioc_final_in_the_response():
    sent = []

    def post(body):
        sent.append(body)
        return order("canceled", "0.30", "54.3", body["clientOrderId"])
    v = load(venue(routes={("POST", "/v1/perps/orders"): post}))
    r = asyncio.run(v.send_taker(is_buy=True, qty=0.3049, limit_px=181.004))
    assert r["status"] == "canceled" and r["filled_base"] == 0.3
    assert r["avg_px"] == pytest.approx(181.0) and not r["unresolved"]
    b = sent[0]
    assert (b["type"], b["timeInForce"], b["side"]) == ("limit", "IOC", "buy")
    assert b["size"] == "0.3" and b["price"] == "181.01"   # buy: tick up
    assert "reduceOnly" not in b
    asyncio.run(v.send_taker(is_buy=False, qty=0.3, limit_px=180.999,
                             reduce_only=True))
    assert sent[1]["price"] == "180.99" and sent[1]["reduceOnly"] is True


def test_ioc_settled_by_the_private_ws():
    async def go():
        v = venue(routes={("POST", "/v1/perps/orders"):
                          lambda b: order("open", cid=b["clientOrderId"])})
        await v.load_market()
        v.orders_feed = vo.OndoOrdersFeed("ONDO", "wss://x", v.market, "k", "s")

        async def push():
            await asyncio.sleep(0.02)
            cid = v.calls[-1][3]["clientOrderId"]
            v.orders_feed.on_frame({"type": "update", "channel": "ordersPerps",
                                    "data": [order("fullyfilled", "1", "180",
                                                   cid)]})
        asyncio.create_task(push())
        return await v.send_taker(is_buy=True, qty=1, limit_px=181)
    r = asyncio.run(go())
    assert r["status"] == "fullyfilled" and r["filled_base"] == 1.0
    assert r["avg_px"] == 180.0


def test_ioc_falls_back_to_get_by_client_id():
    async def go(final):
        v = venue(routes={
            ("POST", "/v1/perps/orders"): lambda b: order("pending"),
        })
        await v.load_market()
        v.orders_feed = vo.OndoOrdersFeed("ONDO", "wss://x", v.market, "k", "s")
        cid = f"mc{v._cid + 1}"
        v_table_get = order("canceled", "0.5", "90", cid) if final \
            else order("open")
        orig = v._req

        async def req(method, path, **kw):
            if method == "GET" and path == f"/v1/perps/orders/client:{cid}":
                v.calls.append((method, path, None, None))
                return v_table_get
            return await orig(method, path, **kw)
        v._req = req
        return v, await v.send_taker(is_buy=False, qty=0.5, limit_px=180)
    v, r = asyncio.run(go(True))
    assert r["filled_base"] == 0.5 and not r["unresolved"]
    assert v.calls[-1][1].startswith("/v1/perps/orders/client:mc")
    _, r = asyncio.run(go(False))
    assert r["status"] == "timeout" and r["unresolved"]


def test_rate_limit_is_reported():
    v = load(venue(routes={("POST", "/v1/perps/orders"):
                           vo.OndoError(429, "too_many_requests", "")}))
    r = asyncio.run(v.send_taker(is_buy=True, qty=1, limit_px=181))
    assert r["status"] == "send-failed" and r["err"].startswith("RATE_LIMITED")


# --------------------------------------------------------------- feeds

def test_book_feed_takes_full_snapshots():
    from entropy_arb.book import OrderBook
    book, n = OrderBook(), []
    f = vo.OndoBookFeed("ONDO", "wss://x", "NVDA-USD.P", book,
                        lambda: n.append(1))
    f.on_frame({"type": "update", "channel": "depthBooksPerps", "data": [
        {"market": "NVDA-USD.P", "bids": [["180.0", "5"], ["179.9", "2"]],
         "asks": [["180.1", "3"]]},
        {"market": "AAPL-USD.P", "bids": [["1", "1"]], "asks": []}]})
    assert book.ready and book.bids == {180.0: 5.0, 179.9: 2.0}
    f.on_frame({"type": "update", "channel": "depthBooksPerps", "data": [
        {"market": "NVDA-USD.P", "bids": [["180.05", "1"]],
         "asks": [["180.1", "0"], ["180.2", "4"]]}]})
    assert book.bids == {180.05: 1.0} and book.asks == {180.2: 4.0}
    assert len(n) == 2


def test_orders_feed_login_and_ready():
    f = vo.OndoOrdersFeed("ONDO", "wss://x", "NVDA-USD.P", "kid", "sec")
    m = json.loads(f.login_msg())
    a = m["args"]
    import hashlib
    import hmac
    assert m["op"] == "login" and a["key"] == "kid"
    assert a["sign"] == hmac.new(b"sec", (a["time"] + "ondo_perps_ws_login")
                                 .encode(), hashlib.sha256).hexdigest()
    assert not f.ready.is_set()
    f.on_frame({"type": "loggedIn", "msg": "Login successful"})
    assert f.ready.is_set()


# ------------------------------------------------------------- accounts

def test_position_balance_funding():
    pos = [{"market": "AAPL-USD.P", "direction": "long", "netQuantity": "9"},
           {"market": "NVDA-USD.P", "direction": "short",
            "netQuantity": "1.5"}]
    v = load(venue(routes={
        ("GET", "/v1/perps/positions"): pos,
        ("GET", "/v1/perps/balance"): {"marginBalance": "120.5",
                                       "availableMargin": "80"},
        ("GET", "/v1/perps/funding_fees"): [
            {"market": "NVDA-USD.P", "amount": "-0.02"},
            {"market": "NVDA-USD.P", "amount": "0.05"},
            {"market": "AAPL-USD.P", "amount": "9"}]}))
    assert asyncio.run(v.fetch_position()) == -1.5
    assert asyncio.run(v.fetch_equity()) == (120.5, 80.0)
    eq, _ = asyncio.run(v.fetch_risk_equity())
    assert eq == 120.5
    # equity + loss limit within 2 s: one request
    assert sum(1 for c in v.calls if c[1] == "/v1/perps/balance") == 1
    assert asyncio.run(v.fetch_funding(1000.0, 2000.0)) == pytest.approx(0.03)
    p = [c for c in v.calls if c[1] == "/v1/perps/funding_fees"][0][2]
    assert p["startTime"] == "1000000" and p["market"] == "NVDA-USD.P"
    v2 = load(venue(routes={("GET", "/v1/perps/positions"): [
        {"market": "NVDA-USD.P", "direction": "neutral", "netQuantity": "0"}]}))
    assert asyncio.run(v2.fetch_position()) == 0.0


def test_background_read_waits_outside_the_lock(monkeypatch):
    """A queued balance read must not delay an order behind it."""
    monkeypatch.setattr(vo, "REST_MIN_GAP_SEC", 0.0)
    monkeypatch.setattr(vo, "BACKGROUND_GAP_SEC", 0.3)
    s = FakeSession()
    v = real_venue(s)

    async def go():
        import time
        v._last_req = time.time()
        bg = asyncio.create_task(v._req("GET", "/v1/perps/balance",
                                        background=True))
        await asyncio.sleep(0.01)
        t0 = time.time()
        await v._req("POST", "/v1/perps/orders", body={"a": 1})
        took = time.time() - t0
        await bg
        return took
    assert asyncio.run(go()) < 0.1
    assert [x[0] for x in s.seen] == ["POST", "GET"]


# ------------------------------------------------------- config & menu

def _cfg(tmp_path, env=""):
    (tmp_path / "config.yaml").write_text(BASE)
    d = tmp_path / "tickers" / "ondo"
    d.mkdir(parents=True, exist_ok=True)
    (d / "NVDA.yaml").write_text(
        "ticker:\n  hedge_symbols: \"NVDA\"\nthresholds:\n  midline_bps: -3\n"
        "  upper_bps: 4\n  lower_bps: 4\nentropy:\n  dex: auto\n"
        "hedge:\n  taker_fee_bps: 2.5\n")
    (tmp_path / ".env").write_text(env)
    return str(tmp_path / "config.yaml"), str(tmp_path / ".env")


def test_config_builds_an_ondo_hedge(tmp_path, monkeypatch):
    for k in ("ONDO_API_KEY_ID", "ONDO_API_SECRET", "HL_PRIVATE_KEY"):
        monkeypatch.delenv(k, raising=False)
    c, e = _cfg(tmp_path)
    cfg = load_config(c, e, symbol="NVDA", hedge_venue="ondo")
    assert cfg.hedge.kind == "ondo" and cfg.hedge.label == "ONDO"
    assert cfg.hedge.fee_bps == 2.5 and not cfg.creds_complete
    c, e = _cfg(tmp_path, "HL_PRIVATE_KEY=0x" + "1" * 64 +
                "\nONDO_API_KEY_ID=ondoKeyId_a\nONDO_API_SECRET=ondoApiSecret_b\n")
    cfg = load_config(c, e, symbol="NVDA", hedge_venue="ondo")
    assert cfg.hedge.ondo_creds.complete and cfg.creds_complete


def test_engine_makes_an_ondo_venue(tmp_path, monkeypatch):
    from entropy_arb.engine import Engine
    c, e = _cfg(tmp_path)
    eng = Engine(load_config(c, e, symbol="NVDA", hedge_venue="ondo"))
    eng.session = None
    assert isinstance(eng._make_venue(eng.cfg.hedge), vo.OndoVenue)


@pytest.fixture
def club_dir(tmp_path, monkeypatch):
    import club
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    return club


def test_menu_seeds_ondo_pairs_and_reads_its_markets(club_dir, monkeypatch):
    club = club_dir
    assert "ondo" in club.VENUES and club.VENUE_TITLES["ondo"] == "Ondo"
    assert "NBIS" not in club.VENUE_TICKERS["ondo"]
    v = club.cfg_values(("ondo", "NVDA"))
    assert v["fee_hedge"] == 2.5 and not v["hedge_fee_checked"]
    monkeypatch.setattr(club, "_http_json", lambda url, *a, **k: {
        "success": True, "result": MARKETS})
    hed = club.ondo_markets()
    assert {"NVDA", "NVDA-USD.P", "XAU"} <= hed
    ent = {"io": {}, "main": {"NVDA": False}}
    ok, why, e_name, h_name, dex = club.market_status(("ondo", "NVDA"), ent,
                                                      hed)
    assert ok and (e_name, h_name, dex) == ("NVDA", "NVDA", "main")
    ok, why, *_ = club.market_status(("ondo", "AAPL"), {"io": {}, "main": {
        "AAPL": False}}, hed)
    assert not ok and why == "нет на Ondo"


def test_hedge_fee_edit_marks_it_checked(club_dir, monkeypatch):
    club = club_dir
    club.set_current_pair(("ondo", "NVDA"))
    monkeypatch.setattr(club, "header", lambda *a, **k: None)
    monkeypatch.setattr(club, "pause", lambda *a, **k: None)
    monkeypatch.setattr(club, "ask_fee", lambda *a, **k: 3.0)
    club.hedge_fee_edit(club.cfg_values(("ondo", "NVDA")))
    v = club.cfg_values(("ondo", "NVDA"))
    assert v["fee_hedge"] == 3.0 and v["hedge_fee_checked"]


def test_ondo_keys(club_dir):
    club = club_dir
    assert club.validate_key("ONDO_API_KEY_ID", " ondoKeyId_x ")[:2] == (
        "ondoKeyId_x", None)
    assert club.validate_key("ONDO_API_SECRET", "abc")[2]      # soft warning
    assert "ONDO_API_SECRET" in club.SECRET_KEYS
    assert club.mask("ONDO_API_SECRET", "ondoApiSecret_12345").startswith("••")
