"""Unattended running: alerts in Telegram, timed pauses of new positions
(/pause, losing hours, drawdown), /resume, heartbeat."""
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import journal, watch  # noqa: E402
from test_risk import make_engine  # noqa: E402


class FakeTG:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(text)
        return True


def tg_engine(tmp, **kw):
    eng = make_engine(tmp, **kw)
    eng.telegram = FakeTG()
    eng.cfg.trades_csv = os.path.join(tmp, "trades.csv")
    return eng


def run_alerts(eng, fn):
    async def go():
        fn()
        await asyncio.sleep(0)
        if eng._alert_tasks:
            await asyncio.gather(*eng._alert_tasks)
    asyncio.run(go())
    return eng.telegram.sent


def test_alerts_repeat_only_after_a_while():
    a = watch.Alerts(repeat_sec=600)
    assert a.due("x", 1000) and not a.due("x", 1100) and a.due("y", 1100)
    assert a.due("x", 1700)


def test_pauses_expire_and_clear():
    p = watch.Pauses()
    p.set("lowprofit", 2000)
    p.set("manual")
    assert p.active(1000) == ["lowprofit", "manual"]
    assert "ещё" in p.text(1000) and "/pause" in p.text(1000)
    assert p.active(2000) == ["manual"]
    assert p.clear("manual") == ["manual"] and p.active(2000) == []


def test_lowprofit_window():
    lp = watch.LowProfit(hours=1, min_trades=3)
    for i in range(2):
        lp.add(1000 + i, -0.01)
    assert lp.losing(1010) is None                    # too few trades
    lp.add(1010, 0.005)
    n, s = lp.losing(1011)
    assert n == 3 and abs(s + 0.015) < 1e-12
    assert lp.losing(1000 + 3700) is None             # all aged out


def test_lowprofit_tolerates_noise_loss():
    lp = watch.LowProfit(hours=6, min_trades=3, loss_bps=1.0)
    for i in range(3):                    # -0.0009 on $60: 0.15 bps, noise
        lp.add(1000 + i, -0.0003, 20.0)
    assert lp.losing(1010) is None
    lp2 = watch.LowProfit(hours=6, min_trades=3, loss_bps=1.0)
    for i in range(3):                    # -0.03 on $60: 5 bps, a real loss
        lp2.add(1000 + i, -0.01, 20.0)
    assert lp2.losing(1010) is not None


def test_equity_peak_spans_sessions(tmp_path):
    path = str(tmp_path / "equity.csv")
    now = time.time()
    for ts, tot, sid in ((now - 30 * 3600, 500, "a"), (now - 5 * 3600, 210, "a"),
                         (now - 3600, 200, "b")):
        journal.append_row(path, journal.EQUITY_HEADER,
                           [f"{ts}", sid, "", "", f"{tot}", "", "", "",
                            "SNDK", "lighter"])
    assert watch.equity_peak(path, now) == 210        # 30 h old row ignored
    assert abs(watch.drawdown_pct(210, 189) - 10.0) < 1e-9
    assert watch.equity_peak(str(tmp_path / "none.csv"), now) is None


def test_losing_hours_pause_new_positions_and_alert(tmp_path):
    eng = tg_engine(str(tmp_path))
    eng.lowprofit = watch.LowProfit(hours=6, min_trades=3)
    sent = run_alerts(eng, lambda: [eng._watch_profit(-0.01)
                                    for _ in range(3)])
    assert eng.pauses.active() == ["lowprofit"]
    assert len(sent) == 1 and "в минусе" in sent[0] and "/resume" in sent[0]
    # opening is held, closing is not
    eng.entropy.position, eng.hedge.position = 0.0, 0.0
    assert eng._open_blocked(eng.hedge, eng.entropy, time.time(), "k")


def test_drawdown_pause_and_auto_clear(tmp_path):
    eng = tg_engine(str(tmp_path))
    eng.cfg.drawdown_24h_pct = 5.0
    path = journal.path_in(eng.cfg.trades_csv, "equity.csv")
    journal.append_row(path, journal.EQUITY_HEADER,
                       [f"{time.time() - 600}", "s", "", "", "200", "", "",
                        "", "SNDK", "lighter"])
    sent = run_alerts(eng, lambda: eng._watch_drawdown(188.0))   # −6 %
    assert "drawdown" in eng.pauses.active() and "Просадка" in sent[0]
    eng._last_dd_check = 0
    eng._watch_drawdown(194.0)                    # −3 %: not yet under half of 5
    assert "drawdown" in eng.pauses.active()
    eng._last_dd_check = 0
    sent = run_alerts(eng, lambda: eng._watch_drawdown(199.0))
    assert eng.pauses.active() == [] and "разрешены" in sent[-1]


def test_drawdown_off_by_default(tmp_path):
    eng = tg_engine(str(tmp_path))
    assert eng.cfg.drawdown_24h_pct == 0
    eng._watch_drawdown(1.0)
    assert eng.pauses.active() == []


def test_telegram_pause_resume(tmp_path):
    eng = tg_engine(str(tmp_path))
    assert "/resume" in eng._tg_control("/pause")
    assert eng.pauses.active() == ["manual"]
    eng.error_pause_until = time.time() + 600
    assert "снята" in eng._tg_control("/resume")
    assert eng.pauses.active() == [] and eng.error_pause_until == 0
    assert "Пауз не было" in eng._tg_control("/resume")
    assert eng._tg_control("/status") is None


def test_halt_alerts(tmp_path):
    eng = tg_engine(str(tmp_path))
    sent = run_alerts(eng, lambda: eng._risk_halt("loss limit", "x"))
    assert "СТОП ПО УБЫТКУ" in sent[0]
    # the same event is not repeated straight away
    sent = run_alerts(eng, lambda: eng._risk_halt("loss limit", "x"))
    assert len(sent) == 1


def test_error_pause_blocks_evaluation(tmp_path):
    eng = tg_engine(str(tmp_path))
    eng.risk_ready = True
    eng.error_pause_until = time.time() + 60
    called = []
    eng._scan = lambda now: called.append(now)
    asyncio.run(eng._evaluate())
    assert called == []


# ------------------------------------------------------- auto-restart

def test_autorun_resume(tmp_path, monkeypatch):
    import club
    monkeypatch.setattr(club, "APP_DIR", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    launched, sent = [], []
    monkeypatch.setattr(club, "find_bots", lambda: [])
    monkeypatch.setattr(club, "launch_bot",
                        lambda record, pair: launched.append((record, pair))
                        or True)
    monkeypatch.setattr(club, "_tg_notify", lambda t: sent.append(t))
    assert club.resume_main() == 0 and launched == []      # never started
    club.autorun_save("lighter", "SNDK", False)
    assert club.resume_main() == 0
    assert launched == [(False, ("lighter", "SNDK"))] and "запущен снова" in sent[0]
    # crash loop: 3 restarts within an hour switch it off
    club.resume_main()
    club.resume_main()
    assert len(launched) == 3
    assert club.resume_main() == 1 and len(launched) == 3
    assert club.autorun_load() is None and "выключен" in sent[-1]
    # Stop from the menu clears it
    club.autorun_save("lighter", "SNDK", True)
    club.autorun_clear()
    assert club.autorun_load() is None


def test_resume_does_nothing_while_running(tmp_path, monkeypatch):
    import club
    monkeypatch.setattr(club, "APP_DIR", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    club.autorun_save("lighter", "SNDK", False)

    class B:
        ours = True
    monkeypatch.setattr(club, "find_bots", lambda: [B()])
    monkeypatch.setattr(club, "launch_bot", lambda *a: 1 / 0)
    assert club.resume_main() == 0


# --------------------------------------- losing-hours check ignores the level

def test_edge_vs_centre_removes_the_premium_level(tmp_path):
    eng = tg_engine(str(tmp_path))
    eng.centre = None
    eng.cfg.midline_bps = -250.0
    # selling $20 of Entropy at premium −245 earns 5 bps over the centre,
    # but the raw fill edge is −$0.49
    raw_sell = 20.0 * -245.0 / 1e4
    assert eng._edge_vs_centre("sell_entropy", raw_sell, 20.0) == \
        pytest.approx(20.0 * 5.0 / 1e4)
    # buying at −255 earns 5 bps; raw +$0.51
    raw_buy = 20.0 * 255.0 / 1e4
    assert eng._edge_vs_centre("buy_entropy", raw_buy, 20.0) == \
        pytest.approx(20.0 * 5.0 / 1e4)


def test_uneven_sells_and_buys_do_not_pause(tmp_path):
    """Twelve good sells and two good buys on a −250 bps pair: the raw sum
    is −$5.7 (what froze the bot), the sum from the centre is a profit."""
    eng = tg_engine(str(tmp_path))
    eng.centre = None
    eng.cfg.midline_bps = -250.0
    eng.lowprofit = watch.LowProfit(hours=6, min_trades=10)
    raw = 0.0
    for _ in range(12):
        e = 20.0 * -245.0 / 1e4
        raw += e
        eng._watch_profit(eng._edge_vs_centre("sell_entropy", e, 20.0))
    for _ in range(2):
        e = 20.0 * 255.0 / 1e4
        raw += e
        eng._watch_profit(eng._edge_vs_centre("buy_entropy", e, 20.0))
    assert raw < -4
    assert eng.pauses.active() == []


def test_really_losing_trades_still_pause(tmp_path):
    eng = tg_engine(str(tmp_path))
    eng.centre = None
    eng.cfg.midline_bps = -250.0
    eng.lowprofit = watch.LowProfit(hours=6, min_trades=10)
    for _ in range(12):      # sells at the centre, costs 3 bps each
        e = 20.0 * -253.0 / 1e4
        eng._watch_profit(eng._edge_vs_centre("sell_entropy", e, 20.0))
    assert eng.pauses.active() == ["lowprofit"]


# ------------------------------------------------ the midline follows the market

def _drifted_engine(tmp, market=-200.0, minutes=45):
    eng = tg_engine(str(tmp))
    eng.cfg.midline_bps = -266.0
    ticker = os.path.join(str(tmp), "ANTH.yaml")
    with open(ticker, "w") as fh:
        fh.write("thresholds:\n  midline_bps: -266\n  upper_bps: 5\n"
                 "  lower_bps: 5\nticker:\n  name: ANTH\n")
    eng.cfg.ticker_file = ticker
    from entropy_arb.autonomy import Centre
    eng.centre = Centre(eng.auto, manual=-266.0)
    t0 = time.time() - minutes * 60
    for i in range(minutes * 6):
        eng.centre.add(t0 + i * 10, market)
    return eng, ticker


def test_midline_moves_after_the_market_stays_away(tmp_path):
    eng, ticker = _drifted_engine(tmp_path)
    now = time.time()
    assert eng.centre.at_leash and eng.centre.live == -216.0   # stuck at the leash
    eng._leash_since = now - 31 * 60
    sent = run_alerts(eng, lambda: eng._maybe_recentre(now))
    assert eng.cfg.midline_bps == -200.0 and eng.centre.manual == -200.0
    assert eng.autocalib_anchor == -200.0
    assert eng.centre.live == -200.0 and not eng.centre.at_leash
    assert "midline_bps: -200" in open(ticker).read()
    assert len(sent) == 1 and "-266.0 → -200.0" in sent[0].replace("−", "-")


def test_midline_waits_out_the_time_and_a_fresh_jump(tmp_path):
    eng, ticker = _drifted_engine(tmp_path)
    now = time.time()
    eng._maybe_recentre(now)                       # starts the clock
    assert eng.cfg.midline_bps == -266.0 and eng._leash_since is not None
    eng._leash_since = now - 31 * 60
    eng.centre.recent = -150.0                     # premium still running
    eng._maybe_recentre(now)
    assert eng.cfg.midline_bps == -266.0
    eng.auto = type(eng.auto)(**{**eng.auto.__dict__, "recentre_min": 0.0})
    eng.centre.recent = -200.0
    eng._maybe_recentre(now)                       # switched off
    assert eng.cfg.midline_bps == -266.0


def test_leash_clock_resets_when_the_market_comes_back(tmp_path):
    eng, _ = _drifted_engine(tmp_path, market=-260.0)
    eng._leash_since = time.time() - 3600
    eng._maybe_recentre(time.time())
    assert eng._leash_since is None and eng.cfg.midline_bps == -266.0
