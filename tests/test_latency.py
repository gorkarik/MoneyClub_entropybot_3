"""Honest backtest: fills after a latency, strategy 2/3 misses, no look-
ahead, pullback entry and fast-market band, and the stricter autotune
(worst latency, most days)."""
import os
import sys
from dataclasses import replace

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import autotune  # noqa: E402
from entropy_arb import backtest as bt  # noqa: E402
from test_backtest import OFF, tick, wave  # noqa: E402


def P(band=3.0, **kw):
    return bt.Params(midline_bps=0, upper_bps=band, lower_bps=band,
                     cap_usd=20, auto=OFF, **kw)


def spike(prems, step=0.25):
    return [tick(i * step, x) for i, x in enumerate(prems)]


def test_latency_fills_at_the_later_snapshot():
    # premium 10 at the signal, 4 by the time the order lands 0.5 s later
    ticks = spike([0, 0, 10, 10, 4, 4, 4, 4])
    now = bt.run(ticks, P(latency_sec=0.0))
    late = bt.run(ticks, P(latency_sec=0.5))
    assert now.trades == late.trades == 1
    assert float(late.rows[0]["ts"]) == pytest.approx(1.0)
    assert late.trades_usd < now.trades_usd
    # the order is in flight: nothing else fires before it lands
    assert bt.run(spike([0, 10, 10, 10, 10, 10]), P(latency_sec=0.5)).trades == 1


def test_entropy_first_misses_when_entropy_ran_away():
    ticks = spike([0, 0, 12, 12, 0, 0, 0, 0])
    r = bt.run(ticks, P(mode="entropy_first", ef_slip_bps=2.0,
                        latency_sec=0.5))
    assert r.trades == 0 and r.misses == 1
    # simultaneous strategy fills anyway, at the worse price
    r = bt.run(ticks, P(latency_sec=0.5))
    assert r.trades == 1 and r.trades_usd < 0


def test_no_look_ahead():
    """Changing snapshots after k must not change any trade decided on a
    snapshot before k (beyond its fill delay)."""
    ticks = wave(n=2000, lo=-10, hi=10, step=0.25, period=80)
    k = 1200
    other = ticks[:k] + [tick(t.ts, 0.0) for t in ticks[k:]]
    for lat in (0.0, 0.6):
        a = bt.run(ticks, P(latency_sec=lat)).rows
        b = bt.run(other, P(latency_sec=lat)).rows
        cut = ticks[k].ts - 1.0
        a = [r for r in a[:-1] if float(r["ts"]) < cut]
        b = [r for r in b[:-1] if float(r["ts"]) < cut]
        assert a and a == b


def test_pullback_entry_waits_for_the_turn():
    ticks = spike([0, 6, 8, 10, 12, 11, 9, 9, 0])
    plain = bt.run(ticks, P())
    pull = bt.run(ticks, P(retrace_bps=2.0))
    assert float(plain.rows[0]["ts"]) == pytest.approx(0.25)
    assert float(pull.rows[0]["ts"]) == pytest.approx(1.5)   # 12 → 9


def test_fast_market_band_holds_new_positions():
    # both venues jump 30 bps (no premium), then an 8 bps premium: with
    # vol_k 0.5 the opening hurdle is 3 + 15 bps
    ticks = [tick(0, 0), tick(0.25, 0, h=100.3)] + \
        [tick(0.5 + 0.25 * i, 8, h=100.3) for i in range(3)]
    assert bt.run(ticks, P()).trades == 1
    assert bt.run(ticks, P(vol_k=0.5)).trades == 0
    calm = [replace(t, ts=t.ts + 120) for t in ticks[2:]]
    assert bt.run(ticks + calm, P(vol_k=0.5)).trades == 1   # jump aged out


def test_params_from_config_carry_the_new_options(tmp_path):
    from test_tickers import BASE
    from entropy_arb.config import load_config
    p = tmp_path / "c.yaml"
    p.write_text(BASE + "execution:\n  retrace_bps: 1.5\n  vol_k: 0.3\n"
                 "  mode: volume\n")
    c = load_config(str(p), "/x", symbol="SNDK", hedge_venue="lighter-rh")
    bp = bt.params_from_config(c)
    assert (bp.retrace_bps, bp.vol_k, bp.ef_limit, bp.latency_sec) == \
        (1.5, 0.3, "profit", 0.6)
    titles = [t for t, _ in bt.variants(bp)]
    assert "Задержка 0.3 с" in titles and "Без входа на откате" in titles
    assert "Цена Entropy фиксированная" in titles


# ------------------------------------------------------- stricter autotune

def _days(*segs):
    out = []
    for day, amp, period in segs:
        w = wave(n=2870, lo=-amp, hi=amp, step=30, period=period)
        out += [replace(t, ts=t.ts + day * 86400) for t in w]
    return out


def test_autotune_judges_by_the_worst_latency():
    ticks = _days((0, 30, 120))
    d = autotune.decide(ticks, P(3, slip_bps=1.0), 3, 3, max_drift_bps=2,
                        min_hours=20, min_gain_pct=10)
    for u, lo, _n, pnl in d.results:
        runs = [bt.run(ticks, P(u, slip_bps=1.0, latency_sec=x)).pnl
                for x in bt.LATENCIES]
        assert pnl == pytest.approx(min(runs))


def test_autotune_needs_most_days_not_one_lucky_day():
    # one wild day where the wider band earns much more, two quiet days
    # where it earns nothing: better in total, but only on 1 day of 3
    ticks = _days((0, 30, 120), (1, 4.2, 720), (2, 4.2, 720))
    d = autotune.decide(ticks, P(3, slip_bps=1.0), 3, 3, max_drift_bps=2,
                        min_hours=24, min_gain_pct=5)
    assert d.upper is None and d.days_won == (1, 3)
    assert "1 днях из 3" in d.reason
    d = autotune.decide(ticks[:2870], P(3, slip_bps=1.0), 3, 3,
                        max_drift_bps=2, min_hours=20, min_gain_pct=5)
    assert d.upper == 4 and d.days_won == (1, 1)
