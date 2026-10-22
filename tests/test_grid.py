"""Strategy 4 «Сетка»: config, the ladder thresholds, the centre, the scan,
and the tick backtest of tools/grid_backtest.py."""
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import autonomy  # noqa: E402
from entropy_arb.config import ConfigError, load_config  # noqa: E402
from entropy_arb.engine import GRID_NO_CENTRE_BPS, Engine  # noqa: E402
from entropy_arb.strategy import STRATEGIES, strategy_title  # noqa: E402
from entropy_arb.telegram import band_line  # noqa: E402
from test_engine import NO_ENV, StubVenue, run_scan  # noqa: E402


def _cfg(extra="", mode="grid"):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write("thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n"
            f"execution:\n  premium_persist_sec: 0.0\n  mode: {mode}\n"
            + extra)
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK",
                       hedge_venue="lighter-rh")


def _engine(extra="", centre=10.0):
    eng = Engine(_cfg(extra))
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    eng.entropy.set_book(99.99, 100.01)        # mid 100
    eng.hedge.set_book(99.99, 100.01)
    if centre is not None:
        for i in range(400):                   # ~67 minutes of samples
            eng.grid.add(i * autonomy.SAMPLE_SEC, centre)
        eng.centre = None                      # no 1 h fallback in tests
    return eng


def test_config_defaults_and_ranges():
    c = _cfg()
    assert c.exec_mode == "grid"
    assert c.grid_step_bps == 4.0 and c.grid_centre_hours == 24.0
    c = _cfg("  grid_step_bps: 6\n  grid_centre_hours: 12\n")
    assert c.grid_step_bps == 6.0 and c.grid_centre_hours == 12.0
    with pytest.raises(ConfigError):
        _cfg("  grid_step_bps: 0.5\n")
    with pytest.raises(ConfigError):
        _cfg("  grid_centre_hours: 1\n")
    assert STRATEGIES["grid"][0] == 4
    assert strategy_title("grid") == "Стратегия 4 · Сетка"


def test_grid_replaces_the_auto_corridor():
    eng = _engine()
    assert eng.grid is not None and eng.band is None
    other = Engine(_cfg(mode="simultaneous"))
    assert other.grid is None and other.band is not None


def test_no_centre_no_trades():
    eng = _engine(centre=None)
    eng.centre = None
    e, h = eng.entropy, eng.hedge
    assert eng.grid_levels() is None
    assert eng._eff_threshold(buy=h, sell=e) == GRID_NO_CENTRE_BPS
    assert eng._open_blocked(h, e, 0.0, "sell_entropy")


def test_ladder_flat_long_short():
    eng = _engine()
    e, h = eng.entropy, eng.hedge
    clip = eng.cfg.max_order_notional
    # flat: sell one clip at centre + step, buy one at centre − step
    assert eng.grid_levels() == pytest.approx((14.0, 6.0))
    assert eng._eff_threshold(buy=h, sell=e) == pytest.approx(14.0)
    assert eng._eff_threshold(buy=e, sell=h) == pytest.approx(-6.0)
    # two clips long: next buy two steps lower, sell one step below centre
    e.position = 2 * clip / 100.0
    assert eng.grid_level() == pytest.approx(2.0)
    assert eng.grid_levels() == pytest.approx((6.0, -2.0))
    # one clip short: mirror
    e.position = -clip / 100.0
    assert eng.grid_levels() == pytest.approx((18.0, 10.0))
    # a round trip always spans exactly one step: buy at L, then sell at L+s
    e.position = 0.0
    buy_at = eng.grid_levels()[1]
    e.position = clip / 100.0
    assert eng.grid_levels()[0] - buy_at == pytest.approx(4.0)


def test_levels_are_on_price_premium_fees_come_off():
    eng = _engine()
    e, h = eng.entropy, eng.hedge
    e.fee_bps = 0.86
    assert eng._eff_threshold(buy=h, sell=e) == pytest.approx(14.0 - 0.86)
    assert eng._eff_threshold(buy=e, sell=h) == pytest.approx(-6.0 - 0.86)


def test_no_inventory_surcharge_cooldown_or_jump_guard():
    eng = _engine()
    e, h = eng.entropy, eng.hedge
    eng.cfg.inventory_scale_bps, eng.cfg.inventory_floor_frac = 10.0, 0.0
    e.position = 50.0                                # half the cap long
    sell_at, buy_at = eng.grid_levels()
    assert eng._eff_threshold(buy=e, sell=h) == pytest.approx(-buy_at)
    eng.last_open_ts = 1e12                          # just opened
    assert not eng._open_blocked(e, h, 1e12, "buy_entropy")


def test_scan_trades_only_beyond_the_next_step():
    eng = _engine()
    e, h = eng.entropy, eng.hedge
    # premium ~+10 (the centre): between the steps, nothing to do
    e.set_book(100.09, 100.11)
    h.set_book(99.99, 100.01)
    assert run_scan(eng) is None
    # premium +16 (Entropy bid over hedge ask ≈ +15): sell one clip
    e.set_book(100.16, 100.18)
    best = run_scan(eng)
    assert best is not None and best[1].key == "entropy"
    assert best[2].buy_notional <= eng.cfg.max_order_notional + 1e-9
    # premium +4 (Entropy ask over hedge bid ≈ +5 ≤ 6): buy one clip
    e.set_book(100.03, 100.05)
    best = run_scan(eng)
    assert best is not None and best[0].key == "entropy"


def test_centre_follows_a_moved_premium():
    eng = _engine()
    t = 400 * autonomy.SAMPLE_SEC
    for i in range(int(24 * 3600 / autonomy.SAMPLE_SEC)):
        eng.grid.add(t + i * autonomy.SAMPLE_SEC, 60.0)
    assert eng.grid_centre() == pytest.approx(60.0)   # no leash, no wait


def test_status_line():
    eng = _engine()
    line = band_line(eng)
    assert line.startswith("Сетка: центр +10.0 bps (24 ч), шаг 4")
    assert "продажа Entropy от +14.0, покупка от +6.0" in line


def test_backtest_round_trips_earn_and_trend_is_capped():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
    import grid_backtest as gb
    import math
    rows = []
    for i in range(20000):                     # 2 s ticks, ~11 hours
        p = 10.0 + 12.0 * math.sin(i / 300.0)
        rows.append((i * 2.0, p + 0.5, p - 0.5, p))
    r = gb.simulate(rows, step=4.0, hours=1.0, clips=5, clip_usd=20.0,
                    fee_bps=0.86)
    assert r["trades"] > 20 and r["pnl"] > 0
    # a premium that only rises: the ladder sells to the cap and stops
    rows = [(i * 2.0, i * 0.05 + 0.5, i * 0.05 - 0.5, i * 0.05)
            for i in range(20000)]
    r = gb.simulate(rows, step=4.0, hours=1.0, clips=5, clip_usd=20.0,
                    fee_bps=0.86)
    assert r["max_clips"] == 5
