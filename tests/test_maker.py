"""Maker order on Entropy: the quote price, the shadow mode (virtual order,
requote rules, trade-through fills, maker_shadow.csv), and the live mode
(post-only order, dead-man switch, hedge on fill, cancel, no double close)."""
import asyncio
import csv
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import entropy_arb.engine as engine_mod  # noqa: E402
from entropy_arb import maker  # noqa: E402
from entropy_arb.config import ConfigError, load_config  # noqa: E402
from test_risk import FakeVenue, make_engine  # noqa: E402
from test_tickers import BASE  # noqa: E402

ident = lambda px, up: px  # noqa: E731


def _book(v, bid, ask, sz=50):
    v.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                     [{"px": str(ask), "sz": str(sz)}]])


# ------------------------------------------------------------ pure parts

def test_quote_price_clears_hurdle_and_never_crosses():
    asks = [(100.0, 10)]
    px = maker.quote_price(True, asks, 1.0, 5.0, 99.0, 101.0, 2.0, ident)
    assert px == pytest.approx(101.0 * (1 - 2e-4))   # held near the best ask
    px = maker.quote_price(True, asks, 1.0, 5.0, 99.0, 100.02, 2.0, ident)
    assert px == pytest.approx(100.05)               # hurdle price
    # would cross the best bid: no quote
    assert maker.quote_price(True, asks, 1.0, 5.0, 100.06, 100.07, 2.0,
                             ident) is None
    bids = [(100.0, 10)]
    px = maker.quote_price(False, bids, 1.0, 5.0, 99.94, 100.2, 2.0, ident)
    assert px == pytest.approx(100.0 / 1.0005)
    # hurdle price above the best ask: it would cross, no quote
    assert maker.quote_price(False, bids, 1.0, 5.0, 99.93, 99.94, 2.0,
                             ident) is None


def test_edge_and_requote_rules():
    assert maker.edge_bps(True, 100.1, 100.0, 0, 0) == pytest.approx(10.0)
    assert maker.edge_bps(False, 99.9, 100.0, 0, 0) == pytest.approx(
        (100 / 99.9 - 1) * 1e4)
    q = maker.Quote("sell_entropy", "close", 100.0, 1.0, 0.0, 0.0)
    assert not maker.wants_move(q, 100.5, 1.0, 1.0, 3.0)    # too soon
    assert not maker.wants_move(q, 100.005, 5.0, 1.0, 3.0)  # too small
    assert maker.wants_move(q, 100.02, 5.0, 1.0, 3.0)
    assert not maker.shadow_filled(q, 100.1, 100.2, 0.3)    # not landed yet
    assert not maker.shadow_filled(q, 100.0, 100.2, 1.0)    # touch is not fill
    assert maker.shadow_filled(q, 100.01, 100.2, 1.0)


def test_summary():
    rows = [{"action": "close", "edge_bps": "2", "mark3_bps": "-1",
             "life_sec": "5"},
            {"action": "open", "edge_bps": "4", "mark3_bps": "",
             "life_sec": "9"}]
    s = maker.summary(rows)
    assert s["fills"] == 2 and s["opens"] == 1 and s["edge"] == 3.0
    assert s["mark3"] == -1.0 and maker.summary([]) is None


# ----------------------------------------------------------------- shadow

def test_shadow_quotes_fill_and_write_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(maker, "MARK_SEC", (0.01, 0.02))
    monkeypatch.setattr(maker, "PLACE_DELAY_SEC", 0.0)
    eng = make_engine(str(tmp_path))
    eng.risk_ready = True
    assert eng.cfg.maker_mode == "shadow"

    async def go():
        t = time.time()
        await eng._maker_step(t)
        q = eng._mq["sell_entropy"]
        assert q is not None and q.action == "open"
        assert q.px > eng.entropy.book.best_bid()
        assert eng._mq["buy_entropy"] is not None
        assert eng.entropy.sent == []                   # shadow sends nothing
        _book(eng.entropy, q.px + 0.01, q.px + 0.03)    # traded through
        await eng._maker_step(t + 1)
        assert eng._mq["sell_entropy"] is None
        await asyncio.sleep(0.05)
        return q
    q = asyncio.run(go())
    assert eng.maker_stats.fills == 1 and eng.maker_stats.placed >= 2
    p = os.path.join(os.path.dirname(eng.cfg.trades_csv), "maker_shadow.csv")
    row = list(csv.DictReader(open(p)))[-1]
    assert row["action"] == "open" and row["side"] == "sell"
    assert float(row["quote_px"]) == pytest.approx(q.px)
    assert row["edge_bps"] and row["mark3_bps"]


def test_shadow_requotes_only_by_the_rules(tmp_path):
    eng = make_engine(str(tmp_path))
    eng.risk_ready = True

    async def go():
        t = time.time()
        await eng._maker_step(t)
        q = eng._mq["sell_entropy"]
        px0 = q.px
        _book(eng.hedge, 100.04, 100.06)               # hedge moved ~5 bps
        await eng._maker_step(t + 1)                   # too soon to move
        assert q.px == px0 and q.requotes == 0
        await eng._maker_step(t + 4)
        assert q.px > px0 and q.requotes == 1
    asyncio.run(go())


def test_shadow_off_when_halted_or_paused(tmp_path):
    eng = make_engine(str(tmp_path))
    eng.risk_ready = True
    eng.halted = True
    asyncio.run(eng._maker_step(time.time()))
    assert not any(eng._mq.values())
    eng.halted = False
    eng.pauses.set("manual")
    asyncio.run(eng._maker_step(time.time()))
    assert not any(eng._mq.values())            # flat: everything is an open


# ------------------------------------------------------------------- live

class MakerVenue(FakeVenue):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.posted, self.canceled, self.deadman = [], [], []
        self.state = {}

    async def send_post_only(self, *, is_buy, qty, px, reduce_only=False):
        oid = len(self.posted) + 1
        self.posted.append((is_buy, qty, px, reduce_only))
        self.state[oid] = {"open": True, "filled": 0.0, "status": "open"}
        return {"status": "resting", "oid": oid}

    async def cancel_order(self, oid):
        self.canceled.append(oid)
        self.state[oid]["open"] = False
        return True

    async def schedule_cancel(self, at_ts):
        self.deadman.append(at_ts)
        return True

    async def order_state(self, oid):
        return dict(self.state[oid])


def live_engine(tmp, epos=0.5):
    eng = make_engine(tmp, epos=epos, hpos=-epos)
    eng.entropy = MakerVenue("entropy", "ENTROPY", pos=epos)
    eng.venues["entropy"] = eng.entropy
    eng.cfg.maker_mode = "live"
    eng.risk_ready = True
    return eng


def test_live_close_quote_hedged_on_fill(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_mod, "MAKER_POLL_SEC", 0.0)
    eng = live_engine(str(tmp_path))

    async def go():
        t = time.time()
        await eng._maker_step(t)
        e = eng.entropy
        assert e.deadman and e.deadman[0] > t           # dead-man first
        (is_buy, qty, px, ro), = e.posted
        assert not is_buy and ro and qty == pytest.approx(0.5)
        assert eng._mlive is not None
        # the taker path does not close this side meanwhile
        _book(eng.hedge, 98.99, 99.01)                # big taker edge
        eng.cfg.premium_persist_sec = 0.0
        eng._scan(t)
        best = eng._scan(t)
        assert best is None or best[1] is not eng.entropy
        _book(eng.hedge, 99.99, 100.01)
        e.state[1] = {"open": False, "filled": 0.5, "status": "filled"}
        await eng._maker_step(t + 0.1)
        return px
    px = asyncio.run(go())
    (h_buy, h_qty, _, _), = eng.hedge.sent
    assert h_buy and h_qty == pytest.approx(0.5)
    assert eng.entropy.position == pytest.approx(0.0)
    assert eng.hedge.position == pytest.approx(0.0)
    assert eng._mlive is None and eng.trades == 1
    rows = list(csv.DictReader(open(eng.cfg.trades_csv)))
    assert rows[-1]["strategy"] == "maker" and rows[-1]["action"] == "close"
    assert float(rows[-1]["sell_avg_px"]) == pytest.approx(px)


def test_live_never_opens_and_cancels_when_flat_or_halted(tmp_path):
    eng = live_engine(str(tmp_path), epos=0.0)
    asyncio.run(eng._maker_step(time.time()))
    assert eng.entropy.posted == []                 # flat: no live opening
    eng = live_engine(str(tmp_path))

    async def go():
        t = time.time()
        await eng._maker_step(t)
        assert eng._mlive is not None
        eng.halted = True
        await eng._maker_step(t + 1)
    asyncio.run(go())
    assert eng.entropy.canceled == [1] and eng._mlive is None
    assert eng.entropy.deadman[-1] is None          # dead-man removed


def test_live_requote_cancels_then_places(tmp_path):
    eng = live_engine(str(tmp_path))

    async def go():
        t = time.time()
        await eng._maker_step(t)
        _book(eng.hedge, 100.04, 100.06)
        await eng._maker_step(t + 4)
    asyncio.run(go())
    e = eng.entropy
    assert e.canceled == [1] and len(e.posted) == 2
    assert e.posted[1][2] > e.posted[0][2]
    assert None not in e.deadman                    # kept across the requote


def test_live_fill_racing_the_cancel_is_hedged(tmp_path):
    eng = live_engine(str(tmp_path))

    async def go():
        t = time.time()
        await eng._maker_step(t)
        eng.entropy.state[1] = {"open": True, "filled": 0.2, "status": "open"}
        assert await eng._maker_cancel_live("stop")
    asyncio.run(go())
    (h_buy, h_qty, _, _), = eng.hedge.sent
    assert h_buy and h_qty == pytest.approx(0.2)
    assert eng.entropy.position == pytest.approx(0.3)


def test_no_live_order_without_dead_man(tmp_path):
    eng = live_engine(str(tmp_path))

    async def refuse(at_ts):
        return False
    eng.entropy.schedule_cancel = refuse
    asyncio.run(eng._maker_step(time.time()))
    assert eng.entropy.posted == []


# ----------------------------------------------------------------- config

def test_maker_config(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(BASE)
    c = load_config(str(p), "/x", symbol="SNDK", hedge_venue="lighter-rh")
    assert c.maker_mode == "shadow" and c.maker_fee_bps == 1.5
    for bad in ("maker_mode: on", "maker_deadman_sec: 5",
                "maker_requote_bps: 0"):
        p.write_text(BASE + "execution:\n  " + bad + "\n")
        with pytest.raises(ConfigError):
            load_config(str(p), "/x", symbol="SNDK",
                        hedge_venue="lighter-rh")


def test_status_and_menu_summary(tmp_path, monkeypatch):
    from entropy_arb import telegram as tg
    eng = make_engine(str(tmp_path))
    assert "исполнений пока нет" in tg.maker_line(eng)
    eng.maker_stats.placed = 4
    eng.maker_stats.add_fill("close", 2.5)
    assert "исполнений 1 из 4" in tg.maker_line(eng)
    eng.cfg.maker_mode = "off"
    assert tg.maker_line(eng) == ""
    import club
    from entropy_arb import journal
    trades = os.path.join(str(tmp_path), "trades.csv")
    monkeypatch.setattr(club, "current_pair", lambda: ("lighter-rh", "SNDK"))
    monkeypatch.setattr(club, "cfg_values", lambda v=None: {
        "trades_csv": trades})
    assert "пока нет" in club.maker_summary_lines()[0]
    journal.append_row(journal.path_in(trades, "maker_shadow.csv"),
                       maker.SHADOW_HEADER,
                       ["1", "s", "SNDK", "lighter-rh", "close", "sell", "1",
                        "100", "0", "4", "0", "99.9", "3.1", "100", "0.5",
                        "-0.4"])
    lines = club.maker_summary_lines()
    assert "Теневых исполнений: 1" in lines[0] and "+3.10" in lines[1]
