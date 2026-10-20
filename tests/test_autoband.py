"""Auto corridor: thresholds from the recent executable premiums, the cost
limit, the engine using them, and the "losing trades" pause being off."""
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import autonomy  # noqa: E402
from entropy_arb.autonomy import AutoBand, AutoBandConf  # noqa: E402
from entropy_arb.config import ConfigError, load_config  # noqa: E402
from test_engine import NO_ENV, make_engine  # noqa: E402

S = autonomy.SAMPLE_SEC


def _fill(ab, n, sell, buy, t0=0.0, slip=0.0):
    for i in range(n):
        s = sell(i) if callable(sell) else sell
        b = buy(i) if callable(buy) else buy
        ab.add(t0 + i * S, s, b, slip)
    return t0 + (n - 1) * S


def test_waits_for_enough_data_then_follows_the_quantile():
    ab = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                               max_cost_usd=50))
    _fill(ab, 30, 0.0, 0.0)                  # 5 minutes: not yet
    assert not ab.ready()
    # sell premiums 0..99 → the 70th percentile ≈ 69.3
    ab2 = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                                max_cost_usd=50))
    _fill(ab2, 100, lambda i: float(i), lambda i: -float(i))
    assert ab2.ready()
    assert ab2.sell == pytest.approx(69.3, abs=0.01)
    assert ab2.buy == pytest.approx(-29.7, abs=0.01)
    assert ab2.raised == 0.0


def test_moves_with_the_market_and_forgets_the_old_level():
    ab = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                               max_cost_usd=50))
    t = _fill(ab, 180, -200.0, 190.0)        # 30 min at one level
    assert ab.sell == pytest.approx(-200.0)
    _fill(ab, 180, -150.0, 140.0, t0=t + S)  # the premium moved 50 bps
    assert ab.sell == pytest.approx(-150.0)  # the corridor is there now
    assert ab.buy == pytest.approx(140.0)


def test_cost_limit_raises_both_thresholds_equally():
    # a round trip at the quantiles would cost 10 bps; allowed 2 × $1 = 2
    ab = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                               max_cost_usd=1.0))
    _fill(ab, 100, -5.0, -5.0)
    assert ab.raised == pytest.approx(4.0)
    assert ab.sell == pytest.approx(-1.0) and ab.buy == pytest.approx(-1.0)
    assert ab.round_trip_bps() == pytest.approx(-2.0)
    # measured slippage counts against the round trip too
    ab2 = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                                max_cost_usd=1.0))
    _fill(ab2, 100, -5.0, -5.0, slip=2.0)
    assert ab2.round_trip_bps() == pytest.approx(-2.0)
    assert ab2.sell == pytest.approx(0.0)


def test_engine_uses_the_auto_corridor_once_ready():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    e, h = eng.entropy, eng.hedge
    assert eng.band is not None                    # on by default
    assert eng._eff_threshold(buy=h, sell=e) == pytest.approx(9.0)  # file band
    eng.band.conf = AutoBandConf(max_cost_usd=50)
    _fill(eng.band, 100, 3.0, -1.0)
    assert eng.band.ready()
    assert eng._eff_threshold(buy=h, sell=e) == pytest.approx(3.0)
    assert eng._eff_threshold(buy=e, sell=h) == pytest.approx(-1.0)


def test_engine_samples_net_premiums_from_the_books():
    eng = make_engine()
    eng.entropy.fee_bps, eng.hedge.fee_bps = 0.0, 2.5
    eng.entropy.set_book(100.10, 100.12)
    eng.hedge.set_book(99.99, 100.01)
    sell, buy = eng.net_premiums()
    assert sell == pytest.approx((100.10 / (100.01 * 1.00025) - 1) * 1e4)
    assert buy == pytest.approx((99.99 * 0.99975 / 100.12 - 1) * 1e4)
    eng._feed_band(1000.0)
    assert len(eng.band.samples) == 1


def _cfg(text):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write("thresholds:\n  midline_bps: 0\n  upper_bps: 4\n"
            "  lower_bps: 4\n" + text)
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK",
                       hedge_venue="lighter-rh")


def test_config_section_and_ranges():
    c = _cfg("autoband:\n  enabled: false\n  share: 0.2\n  max_cost_usd: 3\n")
    assert c.autoband.enabled is False and c.autoband.share == 0.2
    assert c.autoband.max_cost_usd == 3.0
    with pytest.raises(ConfigError):
        _cfg("autoband:\n  share: 0.9\n")


def test_losing_trades_pause_is_off_even_with_old_settings():
    # an old config.yaml still has lowprofit_hours: 6 — it no longer pauses
    c = _cfg("guard:\n  lowprofit_hours: 6\n  lowprofit_min_trades: 1\n")
    assert c.lowprofit_on is False
    from entropy_arb.engine import Engine
    eng = Engine(c)
    assert eng.lowprofit.hours == 0.0
    for i in range(20):
        eng._watch_profit(-1.0, 20.0)
    assert not eng.pauses.active()
    c2 = _cfg("guard:\n  lowprofit_on: true\n  lowprofit_min_trades: 1\n")
    assert Engine(c2).lowprofit.hours == 6.0


def test_settings_menu_is_short_and_autoband_saves(tmp_path, monkeypatch):
    from rich.console import Console
    import club
    from test_tickers import BASE
    root = os.path.join(os.path.dirname(__file__), "..")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.example.yaml").write_text(
        open(os.path.join(root, "config.example.yaml"), encoding="utf-8").read())
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    rec = Console(record=True, width=140)
    monkeypatch.setattr(club, "console", rec)
    monkeypatch.setattr(club, "header", lambda *a, **k: None)
    monkeypatch.setattr(club, "ask", lambda *a, **k: "0")
    club.action_settings()
    out = rec.export_text()
    assert "Стратегия и автокоридор" in out
    assert "автокоридор «Баланс»" in out
    assert "Стоп по убытку — " in out
    # new installs: loss stop 3% per session by default
    import yaml
    ex = yaml.safe_load(open(os.path.join(root, "config.example.yaml"),
                             encoding="utf-8"))
    assert ex["risk"]["max_loss_pct"] == 3 and club.DEFAULT_MAX_LOSS_PCT == 3.0
    assert "Дополнительно" in out
    for gone in ("Комиссии", "Мейкер", "Присмотр", "Автоподбор"):
        assert gone not in out
    # the section is missing in an old config.yaml: saving appends it
    club.apply_changes([("autoband", "max_cost_usd", "2")])
    assert club.autoband_values()["max_cost_usd"] == 2.0
    assert club.autoband_values()["enabled"] is True
    assert club.autoband_profile(club.autoband_values()) is None   # «свой»
    # choosing «Доход» in the menu writes both numbers
    answers = iter(["3", "0"])
    monkeypatch.setattr(club, "ask", lambda *a, **k: next(answers))
    monkeypatch.setattr(club, "pause", lambda: None)
    club.autoband_menu()
    ab = club.autoband_values()
    assert (ab["share"], ab["max_cost_usd"]) == (0.2, -0.5)
    assert club.autoband_profile(ab) == "Доход"


def test_income_mode_demands_a_profitable_round_trip():
    # −0.5 $ per $10 000 → a round trip must earn at least +1 bps
    ab = AutoBand(AutoBandConf(window_min=30, min_min=10, share=0.3,
                               max_cost_usd=-0.5))
    _fill(ab, 100, -2.0, -2.0)
    assert ab.round_trip_bps() == pytest.approx(1.0)
    assert ab.sell == pytest.approx(0.5) and ab.buy == pytest.approx(0.5)


def test_strategy_menu_holds_the_autocorridor(tmp_path, monkeypatch):
    """Autocorridor sits next to the strategies: item 8 opens its modes."""
    import os
    from rich.console import Console
    import club
    from test_tickers import BASE
    root = os.path.join(os.path.dirname(__file__), "..")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.example.yaml").write_text(
        open(os.path.join(root, "config.example.yaml"), encoding="utf-8").read())
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    rec = Console(record=True, width=140)
    monkeypatch.setattr(club, "console", rec)
    monkeypatch.setattr(club, "header", lambda *a, **k: None)
    monkeypatch.setattr(club, "pause", lambda: None)
    answers = iter(["4", "3", "0", "0"])
    monkeypatch.setattr(club, "ask", lambda *a, **k: next(answers))
    club.strategy_menu()
    out = rec.export_text()
    assert "Автокоридор — когда входить" in out
    assert "Стратегия — как исполнять" in out
    assert club.autoband_profile(club.autoband_values()) == "Доход"
