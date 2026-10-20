"""Auto-tuning of the band: off by default, small guarded steps, the leash
around the band set by hand, live change in the engine and the pair file,
and the menu/anchor plumbing."""
import asyncio
import csv
import os
import sys
import time

import pytest
import yaml

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import autotune  # noqa: E402
from entropy_arb import backtest as bt  # noqa: E402
from entropy_arb.config import ConfigError, load_config  # noqa: E402
from test_autocalib import _cfg, club_dir  # noqa: E402,F401
from test_backtest import OFF, wave  # noqa: E402
from test_risk import make_engine  # noqa: E402

GUARDS = dict(max_drift_bps=2.0, min_hours=24, min_gain_pct=20)
# premium swinging ±10 bps every hour, 30 h of books
TICKS = wave(n=3600, lo=-10, hi=10, step=30, period=120)


def params(band, slip=1.0):
    return bt.Params(midline_bps=0, upper_bps=band, lower_bps=band,
                     cap_usd=20, auto=OFF, slip_bps=slip)


# ------------------------------------------------------------- decide

def test_candidates_step_and_leash():
    assert autotune.candidates(4, 4, 4, 4, 2) == [(4, 4), (3, 3), (5, 5)]
    # at the edge of the leash: only back toward the manual band
    assert autotune.candidates(6, 6, 4, 4, 2) == [(6, 6), (5, 5)]
    # never narrower than 1.5 bps a side
    assert autotune.candidates(2, 3, 2, 3, 2) == [(2, 3), (3, 4)]


def test_switches_to_a_clearly_better_band():
    d = autotune.decide(TICKS, params(3), 3, 3, **GUARDS)
    assert (d.upper, d.lower) == (4, 4) and d.reason == ""
    assert [r[:2] for r in d.results] == [(3, 3), (2, 2), (4, 4)]


def test_small_gain_is_not_enough():
    # 7 → 8 earns ~16% more: below the 20% threshold
    d = autotune.decide(TICKS, params(7), 7, 7, **GUARDS)
    assert d.upper is None and "выигрыш мал" in d.reason
    d = autotune.decide(TICKS, params(7), 7, 7,
                        **{**GUARDS, "min_gain_pct": 10})
    assert d.upper == 8


def test_current_band_best_and_leash_respected():
    d = autotune.decide(TICKS, params(8), 8, 8, **GUARDS)
    assert d.upper is None and d.reason == "текущая полоса лучшая"
    # 5 → 6 would be better, but 6 is 3 bps from the manual 3
    d = autotune.decide(TICKS, params(5), 3, 3, **GUARDS)
    assert d.upper is None and (6, 6) not in [r[:2] for r in d.results]


def test_needs_data_trades_and_a_positive_result():
    d = autotune.decide(TICKS[:1000], params(3), 3, 3, **GUARDS)
    assert d.upper is None and "мало записанных" in d.reason
    d = autotune.decide(TICKS, params(3, slip=30), 3, 3, **GUARDS)
    assert d.upper is None and "в плюсе" in d.reason
    quiet = wave(n=3600, lo=-1, hi=1, step=30, period=120)
    d = autotune.decide(quiet, params(3), 3, 3, **GUARDS)
    assert d.upper is None and "мало сделок" in d.reason


def test_due_and_save_keep_comments(tmp_path):
    assert autotune.due(None, 24, 1000)
    assert not autotune.due(1000, 24, 1000 + 3600)
    assert autotune.due(1000, 24, 1000 + 24 * 3600)
    f = tmp_path / "SNDK.yaml"
    f.write_text("ticker:\n  fee_checked: true   # проверена\nthresholds:\n"
                 "  midline_bps: -7.0\n  upper_bps: 4   # полоса\n"
                 "  lower_bps: 4.5\n")
    autotune.save(str(f), 5.0, 5.5, 1234.0)
    text = f.read_text()
    assert "# полоса" in text and "# проверена" in text
    data = yaml.safe_load(text)
    assert data["thresholds"]["upper_bps"] == 5
    assert data["thresholds"]["lower_bps"] == 5.5
    assert data["ticker"]["autotune_last_ts"] == 1234


# --------------------------------------------------------------- config

def test_off_by_default_and_validated(tmp_path):
    c = load_config(_cfg(tmp_path), "/x", symbol="SNDK",
                    hedge_venue="lighter-rh")
    assert c.autotune_enabled is False and c.autotune_every_hours == 24
    assert c.upper_anchor_bps is None and c.autotune_last_ts is None
    c = load_config(_cfg(tmp_path, "autotune:\n  enabled: true\n",
                         "  upper_anchor_bps: 5\n  lower_anchor_bps: 6\n"),
                    "/x", symbol="SNDK", hedge_venue="lighter-rh")
    assert c.autotune_enabled and (c.upper_anchor_bps,
                                   c.lower_anchor_bps) == (5, 6)
    with pytest.raises(ConfigError, match="autotune"):
        load_config(_cfg(tmp_path, "autotune:\n  every_hours: 0\n"), "/x",
                    symbol="SNDK", hedge_venue="lighter-rh")


# --------------------------------------------------------------- engine

def _at_engine(tmp_path, monkeypatch, decision):
    eng = make_engine(str(tmp_path), epos=0.5, hpos=-0.5)   # position open
    pair = tmp_path / "SNDK.yaml"
    pair.write_text("ticker:\n  fee_checked: true\nthresholds:\n"
                    "  midline_bps: -7.0\n  upper_bps: 4.0\n  lower_bps: 4.5\n")
    eng.cfg.ticker_file = str(pair)
    eng.cfg.upper_bps, eng.cfg.lower_bps = 4.0, 4.5
    eng.autotune_on = True
    eng.autotune_anchor = (4.0, 4.5)
    monkeypatch.setattr(bt, "load_ticks", lambda *a, **k: [])
    monkeypatch.setattr(bt, "measured_slip", lambda *a, **k: (None, 0))
    seen = {}

    def decide(ticks, base, au, al, **kw):
        seen.update(base=base, anchor=(au, al), kw=kw)
        return decision
    monkeypatch.setattr(autotune, "decide", decide)
    return eng, pair, seen


def test_engine_changes_band_live_and_in_the_file(tmp_path, monkeypatch):
    d = autotune.Decision(5.0, 5.5, [(4.0, 4.5, 30, 0.5),
                                     (5.0, 5.5, 28, 0.9)], 72.0, "")
    eng, pair, seen = _at_engine(tmp_path, monkeypatch, d)
    now = time.time()
    asyncio.run(eng._autotune_once(now))     # does not wait for flat
    assert seen["anchor"] == (4.0, 4.5)
    assert seen["base"].slip_bps == bt.DEFAULT_SLIP_BPS
    assert (eng.cfg.upper_bps, eng.cfg.lower_bps) == (5.0, 5.5)
    data = yaml.safe_load(pair.read_text())
    assert data["thresholds"]["upper_bps"] == 5
    assert data["thresholds"]["midline_bps"] == -7.0     # centre untouched
    assert data["ticker"]["autotune_last_ts"] == int(round(now))
    rows = list(csv.DictReader(open(os.path.join(str(tmp_path), "logs",
                                                 "autotune.csv"))))
    assert rows[-1]["old_upper"] == "4" and rows[-1]["new_lower"] == "5.5"
    assert eng.autotune_last_ts == now


def test_engine_keeps_band_and_marks_the_run(tmp_path, monkeypatch):
    d = autotune.Decision(None, None, [], 30.0, "текущая полоса лучшая")
    eng, pair, _ = _at_engine(tmp_path, monkeypatch, d)
    asyncio.run(eng._autotune_once(5000.0))
    assert (eng.cfg.upper_bps, eng.cfg.lower_bps) == (4.0, 4.5)
    data = yaml.safe_load(pair.read_text())
    assert data["thresholds"]["upper_bps"] == 4.0
    assert data["ticker"]["autotune_last_ts"] == 5000
    assert not os.path.exists(os.path.join(str(tmp_path), "logs",
                                           "autotune.csv"))


def test_engine_off_by_default(tmp_path):
    assert not make_engine(str(tmp_path)).autotune_on


# ------------------------------------------------------------------ menu

def test_manual_band_becomes_the_anchor(club_dir):
    club = club_dir
    pair = ("lighter-rh", "SNDK")
    assert club.cfg_values(pair)["band_anchor"] is None
    before = time.time()
    club.save_config([("thresholds", "upper_bps", "6")], pair)
    v = club.cfg_values(pair)
    assert v["band_anchor"] == (6.0, v["lower_bps"])
    assert v["autotune_last_ts"] >= before - 1
    # the centre does not touch the band anchor
    club.save_config([("thresholds", "midline_bps", "-9")], pair)
    assert club.cfg_values(pair)["band_anchor"] == (6.0, v["lower_bps"])


def test_menu_toggle_goes_to_config_yaml(club_dir):
    club = club_dir
    pair = ("lighter-rh", "SNDK")
    assert not club.cfg_values(pair)["autotune"]
    club.save_config([("autotune", "enabled", "true")], pair)
    assert club.cfg_values(pair)["autotune"]
    assert yaml.safe_load(open("config.yaml"))["autotune"]["enabled"] is True
    assert load_config("config.yaml", "/x", symbol="SNDK",
                       hedge_venue="lighter-rh").autotune_enabled
