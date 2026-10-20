"""Menu and report usability: expected-vs-got line (engine, Telegram,
dashboard), no extra Enter after saving, the dashboard attach helper, and
the wording of the menus."""
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import telegram as tg  # noqa: E402
from test_guard import _arb_engine  # noqa: E402
from test_risk import make_engine  # noqa: E402
from test_tickers import BASE  # noqa: E402


# ------------------------------------------------- expected vs got

def test_engine_tracks_planned_vs_realized_edge(tmp_path):
    async def go():
        eng = _arb_engine(tmp_path)
        assert eng.edge_compare() is None            # no trades yet
        eng._scan(time.time())
        best = eng._scan(time.time())
        assert best is not None
        await eng._execute(*best)
        return eng, best[2]
    eng, plan = asyncio.run(go())
    n, exp, got = eng.edge_compare()
    assert n == 1 and exp == pytest.approx(plan.exp_edge_usd)
    # one number each, for the same trade
    assert got == pytest.approx(eng.total_fill_edge)


def test_telegram_shows_where_money_went(tmp_path):
    eng = make_engine(str(tmp_path))
    eng.cmp_n, eng.cmp_exp, eng.cmp_fill = 12, 1.50, 1.20
    eng.fees_usd = 0.25
    # the raw planned / filled edges carry the premium level: not shown
    assert tg.edge_line(eng.edge_compare()) == (
        "Проскальзывание (12 сделок): -$0.3000")
    line = tg.money_line(eng, 0.10)
    assert line == ("Куда ушли деньги: комиссии -$0.2500 · проскальзывание "
                    "-$0.3000. Без них результат был бы +$0.6500")
    assert "Куда ушли деньги" in tg.pnl_text(eng)
    assert "Куда ушли деньги" in tg.status_text(eng)
    assert "ожидалось" not in tg.pnl_text(eng)
    s = {"pnl": -0.2, "pnl_pct": -0.1, "turnover": 100, "trades": 12,
         "duration": 60, "reason": "user", "closed": True, "fees": 0.25,
         "edge_compare": eng.edge_compare()}
    assert ("Без них результат был бы +$0.3500"
            in tg.finish_text(eng, s))
    # before the first trade nothing is invented
    eng2 = make_engine(str(tmp_path))
    assert tg.edge_line(eng2.edge_compare()) is None
    assert tg.money_line(eng2, 0.0) is None


def test_dashboard_shows_where_money_went(tmp_path):
    from entropy_arb.dashboard import BufferLogHandler, Dashboard
    eng = make_engine(str(tmp_path))
    eng.cmp_n, eng.cmp_exp, eng.cmp_fill = 3, 0.50, 0.10
    eng.fees_usd = 0.05
    d = Dashboard(eng, BufferLogHandler(), "x.log", lang="ru")
    t = d._edge_line()
    assert "комиссии -$0.0500" in t.plain
    assert "проскальзывание -$0.4000" in t.plain
    assert "ожидалось" not in t.plain
    eng.cmp_n = 0
    assert d._edge_line() is None


# ------------------------------------------- menu: fewer Enters, wording

@pytest.fixture
def club_dir(tmp_path, monkeypatch):
    root = os.path.join(os.path.dirname(__file__), "..")
    import club
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.example.yaml").write_text(
        open(os.path.join(root, "config.example.yaml"), encoding="utf-8").read())
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    return club


def test_saving_a_setting_needs_no_extra_enter(club_dir, monkeypatch):
    club = club_dir
    asked = []
    monkeypatch.setattr(club, "ask", lambda p="": asked.append(p) or "")
    monkeypatch.setattr(club, "clear", lambda: None)
    club.apply_changes([("thresholds", "upper_bps", "5")])
    club.pause()                      # the usual call after a save
    assert asked == []                # no "Нажмите Enter…" prompt
    printed = []
    monkeypatch.setattr(club.console, "print",
                        lambda *a, **k: printed.append(str(a[0]) if a else ""))
    club.header("Настройки")          # the message is on the next screen
    assert any("Сохранено" in p for p in printed)
    club.pause()                      # a later, ordinary pause does ask
    assert len(asked) == 1


def test_failed_save_still_waits_for_enter(club_dir, monkeypatch):
    club = club_dir
    asked = []
    monkeypatch.setattr(club, "ask", lambda p="": asked.append(p) or "")
    club.apply_changes([("thresholds", "upper_bps", "-1")])   # rejected
    club.pause()
    assert len(asked) == 1


def test_no_menu_text_calls_defaults_the_authors(club_dir):
    src = open(os.path.join(os.path.dirname(__file__), "..", "club.py"),
               encoding="utf-8").read()
    ui = [l for l in src.splitlines() if "автор" in l
          and not l.lstrip().startswith("#")]
    assert ui == []


def test_pair_switch_items_explain_themselves():
    src = open(os.path.join(os.path.dirname(__file__), "..", "club.py"),
               encoding="utf-8").read()
    assert "выбрать другую" not in src
    assert "Сменить пару — другая пара для Настроек, Анализа и" in src
    assert "Сменить пару — другая пара для Анализа" in src


# --------------------------------------------------------- dashboard exit

def test_tmux_conf_binds_exit_keys_and_is_readable(club_dir):
    club = club_dir
    conf = club.TMUX_CONF_TEXT
    for k in ("q", "Q", "й", "Й", "C-c"):
        assert f"bind-key -n {k} detach-client" in conf
    # fg=white on the default (often white) background was invisible
    assert "bg=blue,fg=white" in conf and "нажмите Q" in conf


def test_real_tty_skips_dev_tty(club_dir, monkeypatch):
    club = club_dir
    names = {0: "/dev/tty", 1: "/dev/pts/7", 2: "/dev/pts/7"}

    def ttyname(fd):
        if fd not in names:
            raise OSError
        return names[fd]
    monkeypatch.setattr(club.os, "ttyname", ttyname)
    assert club.real_tty() == "/dev/pts/7"
    names.update({1: "/dev/tty", 2: "/dev/tty"})
    assert club.real_tty() is None


def test_dashboard_attach_reloads_conf_and_reports_errors(club_dir,
                                                         monkeypatch):
    club = club_dir
    calls = []

    class R:
        returncode = 1
        stderr = b"open terminal failed: not a terminal\n"

    monkeypatch.setattr(club, "real_tty", lambda: None)
    monkeypatch.setattr(club.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(club, "tmux", lambda *a, **k: calls.append(a) or R())
    ok, err = club.attach_dashboard()
    assert not ok and "not a terminal" in err
    assert calls[0][0] == "source-file"           # the q binding is reloaded
    assert calls[1][0] == "attach-session"


# ---------------------------------------------- Stop screen: expected/got

def test_stop_report_shows_expected_and_got(club_dir):
    club = club_dir
    row = {"symbol": "SNDK", "hedge_venue": "lighter-rh", "pnl_usd": "-0.01",
           "exp_edge_usd": "0.0500", "fill_edge_usd": "-0.0200",
           "duration_sec": "60"}
    text = "\n".join(club.session_report_lines(row))
    assert "ожидалось +$0.0500" in text and "-$0.0200" in text
    assert "разница -$0.0700" in text
    # rows written before this version have no such columns: no line
    old = {k: v for k, v in row.items() if "edge" not in k}
    assert "ожидалось" not in "\n".join(club.session_report_lines(old))


def test_old_sessions_file_upgraded_in_place(tmp_path):
    from entropy_arb import journal
    p = tmp_path / "sessions.csv"
    old_header = journal.SESSIONS_HEADER[:-3]
    p.write_text(",".join(old_header) + "\n" + ",".join(["x"] * len(
        old_header)) + "\n")
    journal.append_row(str(p), journal.SESSIONS_HEADER,
                       ["y"] * len(journal.SESSIONS_HEADER))
    rows = journal.read_rows(str(p))
    assert len(rows) == 2                         # the old row is kept
    assert rows[0]["exp_edge_usd"] == "" and rows[1]["exp_edge_usd"] == "y"
    assert not list(tmp_path.glob("sessions.csv.old-*"))
