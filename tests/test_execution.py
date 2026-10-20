"""Execution package: Entropy price limit from the trade's profit, hedge
repriced from a fresh book, book imbalance, pullback entry, fast-market
band."""
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import ConfigError, load_config  # noqa: E402
from test_guard import _arb_engine  # noqa: E402
from test_tickers import BASE  # noqa: E402


def _signal(eng):
    async def go():
        eng._scan(time.time())
        return eng._scan(time.time())
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(go())
    eng._scan(time.time())
    return eng._scan(time.time())


def _book(v, bids, asks):
    v.book.apply_hl([[{"px": str(p), "sz": str(s)} for p, s in bids],
                     [{"px": str(p), "sz": str(s)} for p, s in asks]])


# ------------------------------------------- Entropy limit from the profit

def test_profit_limit_tracks_spare_premium(tmp_path):
    eng = _arb_engine(tmp_path)
    buy, sell, plan = _signal(eng)
    cfg = eng.cfg
    hurdle = eng._eff_threshold(buy, sell) + buy.fee_bps + sell.fee_bps
    # the test books carry a huge edge: clamped at 3 × the fixed limit
    assert plan.marginal_premium_bps - hurdle > 15
    assert eng._ef_limit_bps(buy, sell, plan) == pytest.approx(
        3 * cfg.entropy_first_slip_bps)
    # a premium just over the hurdle keeps only the band share we give up
    plan.marginal_premium_bps = hurdle + 0.2
    band = eng._eff_threshold(buy, sell) - eng.mid_bps()
    cfg.entropy_first_keep = 0.5
    want = max(0.5, min(0.2 + max(band, 0) * 0.5,
                        3 * cfg.entropy_first_slip_bps))
    assert eng._ef_limit_bps(buy, sell, plan) == pytest.approx(want)
    cfg.entropy_first_keep = 1.0                 # keep the whole band
    assert eng._ef_limit_bps(buy, sell, plan) == pytest.approx(0.5)
    cfg.entropy_first_limit = "fixed"
    assert eng._ef_limit_bps(buy, sell, plan) == cfg.entropy_first_slip_bps


def test_profit_limit_sent_to_entropy(tmp_path):
    eng = _arb_engine(tmp_path)
    eng.cfg.exec_mode = "entropy_first"

    async def go():
        buy, sell, plan = _signal(eng)
        lim = eng._ef_limit_bps(buy, sell, plan)
        await eng._execute(buy, sell, plan)
        return plan, lim
    plan, lim = asyncio.run(go())
    (_b, _q, e_px, _), = eng.entropy.sent
    assert e_px == pytest.approx(plan.sell_limit * (1 - lim / 1e4))


# ------------------------------------------------ hedge from a fresh book

def test_hedge_repriced_when_its_book_ran_away(tmp_path):
    eng = _arb_engine(tmp_path)
    eng.cfg.exec_mode = "entropy_first"
    eng.cfg.leg_slippage_bps = 10.0
    orig = eng.entropy.send_taker

    async def entropy_then_move(**kw):
        info = await orig(**kw)
        # the hedge book (we buy there) jumps 1 % while Entropy fills
        _book(eng.hedge, [(99.98, 50)], [(100.0, 50)])
        return info
    eng.entropy.send_taker = entropy_then_move

    async def go():
        best = _signal(eng)
        await eng._execute(*best)
    asyncio.run(go())
    (h_buy, _q, h_px, _), = eng.hedge.sent
    assert h_buy and h_px == pytest.approx(100.0 * 1.001)


def test_hedge_bound_never_tighter_than_planned(tmp_path):
    eng = _arb_engine(tmp_path)
    h = eng.hedge
    # book moved in our favour: keep the planned (wider) bound
    assert eng._fresh_hedge_bound(h, True, 1.0, 105.0) == (105.0, False)
    bound, moved = eng._fresh_hedge_bound(h, True, 1.0, 98.0)
    assert moved and bound > 98.0
    bound, moved = eng._fresh_hedge_bound(h, False, 1.0, 100.0)
    assert moved and bound < 100.0


# ------------------------------------------------------- book imbalance

def test_imbalance_ratio_and_filter(tmp_path):
    eng = _arb_engine(tmp_path)
    e = eng.entropy
    # selling Entropy: heavy asks push the price down against us
    _book(e, [(100.0, 10)], [(100.02, 40)])
    buy, sell = eng.hedge, eng.entropy
    assert eng.imbalance(buy, sell) == pytest.approx(4.0, rel=1e-3)
    assert eng.imbalance(sell, buy) == pytest.approx(0.25, rel=1e-3)
    assert not eng._imbalance_blocks(buy, sell, "sell_entropy")  # 0: log only
    eng.cfg.imbalance_max = 3.0
    assert eng._imbalance_blocks(buy, sell, "sell_entropy")
    eng.cfg.imbalance_max = 5.0
    assert not eng._imbalance_blocks(buy, sell, "sell_entropy")


def test_imbalance_never_holds_a_close(tmp_path):
    eng = _arb_engine(tmp_path)
    _book(eng.entropy, [(100.0, 10)], [(100.02, 40)])
    eng.cfg.imbalance_max = 1.5
    eng.entropy.position, eng.hedge.position = 1.0, -1.0   # long: selling closes
    assert not eng._imbalance_blocks(eng.hedge, eng.entropy, "sell_entropy")


# ------------------------------------------------------- pullback entry

def test_retrace_waits_for_pullback(tmp_path):
    eng = _arb_engine(tmp_path)
    eng.cfg.retrace_bps = 2.0
    buy, sell, plan = _signal(eng) or (None, None, None)
    assert plan is None                        # first sight sets the peak
    peak = eng._peak["sell_entropy"]
    assert peak is not None
    assert eng._retrace_wait(eng.hedge, eng.entropy,
                             type("P", (), {"top_premium_bps": peak + 3})(),
                             "sell_entropy")   # still rising: wait
    assert not eng._retrace_wait(eng.hedge, eng.entropy,
                                 type("P", (), {"top_premium_bps": peak + 0.5})(),
                                 "sell_entropy")  # 2.5 below the new peak
    eng.cfg.retrace_bps = 0.0
    eng._peak["sell_entropy"] = None
    assert _signal(eng) is not None


def test_retrace_peak_resets_when_signal_gone(tmp_path):
    eng = _arb_engine(tmp_path)
    eng.cfg.retrace_bps = 2.0
    _signal(eng)
    assert eng._peak["sell_entropy"] is not None
    _book(eng.hedge, [(102.0, 50)], [(102.02, 50)])    # edge gone
    _signal(eng)
    assert eng._peak["sell_entropy"] is None


# --------------------------------------------------- fast-market band

def test_vol_band_widens_opening_hurdle_only(tmp_path):
    eng = _arb_engine(tmp_path)
    buy, sell = eng.hedge, eng.entropy
    base = eng._eff_threshold(buy, sell)
    eng.cfg.vol_k = 0.5
    t = time.time()
    eng._sample_vol(t - 10)
    _book(eng.entropy, [(100.99, 50)], [(101.01, 50)])  # +1 % in 10 s
    eng._sample_vol(t)
    assert eng.vol_bps() == pytest.approx(99.5, rel=0.01)
    assert eng._eff_threshold(buy, sell) == pytest.approx(
        base + 0.5 * eng.vol_bps())
    eng.entropy.position, eng.hedge.position = 1.0, -1.0   # now a close
    assert eng.vol_add_bps(buy, sell) == 0.0
    eng._sample_vol(t + 120)                   # old samples age out
    assert eng.vol_bps() == 0.0


# --------------------------------------------------------------- config

def test_execution_config_defaults_and_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(BASE)
    c = load_config(str(p), "/x", symbol="SNDK", hedge_venue="lighter-rh")
    assert (c.entropy_first_limit, c.entropy_first_keep, c.imbalance_max,
            c.retrace_bps, c.vol_k) == ("profit", 0.5, 0.0, 0.0, 0.0)
    for bad in ("entropy_first_limit: wide", "entropy_first_keep: 1.5",
                "imbalance_max: 1.0", "retrace_bps: -1", "vol_k: 9"):
        p.write_text(BASE + "execution:\n  " + bad + "\n")
        with pytest.raises(ConfigError):
            load_config(str(p), "/x", symbol="SNDK",
                        hedge_venue="lighter-rh")
