"""Управление из Telegram: слушатель отвечает только своему чату, Старт,
Стоп и смена пары — после «Да», настройки пишутся в config.yaml, а торговый
бот отдаёт /status и /pnl через файл и берёт /pause из файла."""
import asyncio
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_tickers import BASE  # noqa: E402


class FakeApi:
    def __init__(self):
        self.chat_id = "123"
        self.sent = []

    def send(self, text, buttons=None, html_text=False, new=False):
        self.sent.append((text, buttons or []))

    def last(self):
        return self.sent[-1][0]

    def datas(self):
        return [d for row in self.sent[-1][1] for _t, d in row]


class FakeBot:
    def __init__(self, ticker="OAI", record=False):
        self.ours, self.ticker, self.venue = True, ticker, "lighter-rh"
        self.record, self.pid = record, 1


def uncalibrate(club, pair):
    import re
    path = club.pair_path(pair)
    text = open(path, encoding="utf-8").read()
    text = re.sub(r"(?m)^(\s*midline_bps:).*$", r"\1 0", text)
    open(path, "w", encoding="utf-8").write(text)
    assert not club.is_calibrated(club.cfg_values(pair))


@pytest.fixture
def ctl(tmp_path, monkeypatch):
    import club
    import tgcontrol
    root = os.path.join(os.path.dirname(__file__), "..")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.example.yaml").write_text(
        open(os.path.join(root, "config.example.yaml"), encoding="utf-8").read())
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    club.set_current_pair(("lighter-rh", "SNDK"))
    calls = {"launch": [], "stop": 0}

    def launch(record=False, pair=None):
        calls["launch"].append((record, pair))
        return True

    def stop(bots):
        calls["stop"] += 1
        monkeypatch.setattr(club, "find_bots", lambda: [])
        return True
    monkeypatch.setattr(club, "launch_bot", launch)
    monkeypatch.setattr(club, "stop_bots", stop)
    c = tgcontrol.Control(FakeApi())
    c.calls = calls
    return c


def test_only_own_chat_is_read():
    import tgcontrol
    msg = {"update_id": 1, "message": {"chat": {"id": 999}, "text": "/stop"}}
    assert tgcontrol.parse(msg, "123") is None
    msg["message"]["chat"]["id"] = 123
    assert tgcontrol.parse(msg, "123") == ("text", "/stop", None, None)
    btn = {"update_id": 2, "callback_query": {
        "id": "q", "data": "stop!",
        "message": {"chat": {"id": 999}, "message_id": 7}}}
    assert tgcontrol.parse(btn, "123") is None
    btn["callback_query"]["message"]["chat"]["id"] = 123
    assert tgcontrol.parse(btn, "123") == ("button", "stop!", "q", 7)


def test_menu_has_the_buttons(ctl):
    ctl.handle("text", "/menu")
    d = ctl.api.datas()
    for want in ("status", "run", "record", "fix", "mode", "pair",
                 "limits", "stoploss", "report", "help"):
        assert want in d
    text = ctl.api.last()
    assert "остановлен" in text.lower() and "SNDK" in text
    labels = [t for row in ctl.api.sent[-1][1] for t, _d in row]
    assert labels[0] == "💰 Торговля (реальные деньги)"
    assert "🔁 Сменить пару" in labels and "🛠 Настроить пару" in labels


def test_stop_and_start_ask_first(ctl, monkeypatch):
    import club
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot()])
    ctl.handle("text", "/stop")
    assert "stop!" in ctl.api.datas() and ctl.calls["stop"] == 0
    ctl.handle("button", "stop!")
    assert ctl.calls["stop"] == 1
    monkeypatch.setattr(club, "find_bots", lambda: [])
    ctl.handle("text", "/run")
    assert "run!" in ctl.api.datas() and ctl.calls["launch"] == []


def test_start_needs_keys_and_never_asks_for_them(ctl, monkeypatch):
    import club
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, False))
    ctl.handle("button", "run!")
    assert ctl.calls["launch"] == []
    assert "только на сервере" in ctl.api.last()


def test_start_launches_when_ready(ctl, monkeypatch):
    import club
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    ctl.handle("button", "run!")
    assert ctl.calls["launch"] == [(False, ("lighter-rh", "SNDK"))]


def test_trading_without_data_records_then_goes_live_itself(ctl, monkeypatch):
    """No centre and no recording: «Торговля» records prices first and the
    listener switches to trading by itself once there is enough data."""
    import club
    import tgcontrol
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    pair = ("lighter-rh", "SNDK")
    uncalibrate(club, pair)
    ctl.handle("button", "run")
    assert "run!" in ctl.api.datas() and "РЕАЛЬНЫЕ" in ctl.api.last()
    ctl.handle("button", "run!")
    assert ctl.calls["launch"] == [(True, pair)]
    assert tgcontrol.golive_load()[0] == pair
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot("SNDK", True)])
    ctl.tick()                                   # not enough data yet
    assert ctl.calls["stop"] == 0
    monkeypatch.setattr(tgcontrol, "recorded_median",
                        lambda p, **k: (-190.0, 15))
    ctl.tick()
    assert ctl.calls["stop"] == 1
    assert ctl.calls["launch"][-1] == (False, pair)
    assert club.cfg_values(pair)["midline_bps"] == -190.0
    assert tgcontrol.golive_load() is None


def test_stopping_the_recording_cancels_going_live(ctl, monkeypatch):
    import club
    import tgcontrol
    tgcontrol.golive_save(("lighter-rh", "SNDK"))
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot("SNDK", True)])
    ctl.handle("button", "stop!")
    assert tgcontrol.golive_load() is None
    tgcontrol.golive_save(("lighter-rh", "SNDK"))
    ctl.tick()                                   # bot gone: forget it
    assert tgcontrol.golive_load() is None


def test_fix_pair_sets_centre_thresholds_and_autoband(ctl, monkeypatch):
    import club
    import tgcontrol
    pair = ("lighter-rh", "SNDK")
    ent = {"io": {"SNDK": False}, "main": {}}
    monkeypatch.setattr(club, "fetch_markets",
                        lambda v: (ent, {"SNDK"}, None))
    monkeypatch.setattr(tgcontrol, "recorded_median",
                        lambda p, **k: (-33.0, 120))
    club.save_config([("thresholds", "upper_bps", "9"),
                      ("autoband", "enabled", "false")], pair)
    ctl.handle("button", "fix")
    assert "fix!" in ctl.api.datas()
    ctl.handle("button", "fix!")
    v = club.cfg_values(pair)
    assert v["midline_bps"] == -33.0
    assert v["upper_bps"] == club.DEFAULT_THRESHOLDS["upper_bps"]
    assert club.autoband_values()["enabled"]
    assert "настроена" in ctl.api.last()


def test_uncalibrated_pair_gets_centre_from_recording(ctl, monkeypatch):
    import club
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    pair = ("lighter-rh", "SNDK")
    uncalibrate(club, pair)
    csv = club.cfg_values(pair)["csv"]
    os.makedirs(os.path.dirname(csv) or ".", exist_ok=True)
    open(csv, "w").write("ts\n")
    import tgcontrol
    monkeypatch.setattr(tgcontrol, "recorded_median",
                        lambda p, **k: (-12.5, 900))
    ctl.handle("button", "run!")
    assert club.cfg_values(pair)["midline_bps"] == -12.5
    assert ctl.calls["launch"] == [(False, pair)]


def test_mode_and_strategy_saved(ctl):
    import club
    ctl.handle("text", "/mode")
    d = ctl.api.datas()
    assert "ab:Доход" in d and "st:entropy_first" in d
    ctl.handle("button", "ab:Доход")
    assert club.autoband_profile(club.autoband_values()) == "Доход"
    ctl.handle("button", "st:entropy_first")
    assert club.cfg_values()["exec_mode"] == "entropy_first"
    ctl.handle("button", "st:volume")              # not offered — ignored
    assert club.cfg_values()["exec_mode"] == "entropy_first"


def test_change_while_running_offers_restart(ctl, monkeypatch):
    import club
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot("SNDK")])
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    ctl.handle("button", "sl:2")
    assert club.cfg_values()["max_loss_pct"] == 2.0
    assert "restart!" in ctl.api.datas() and ctl.calls["stop"] == 0
    ctl.handle("button", "restart!")
    assert ctl.calls["stop"] == 1
    assert ctl.calls["launch"] == [(False, ("lighter-rh", "SNDK"))]


def test_stoploss_and_limits(ctl):
    import club
    ctl.handle("text", "/stoploss 4")
    assert club.cfg_values()["max_loss_pct"] == 4.0
    ctl.handle("text", "/stoploss 0")
    assert club.cfg_values()["max_loss_pct"] == 0.0
    ctl.handle("text", "/limits 150 25")
    v = club.cfg_values()
    assert v["pos_entropy"] == 150 and v["order"] == 25
    ctl.handle("text", "/limits 20 50")              # order above the cap
    assert club.cfg_values()["order"] == 25
    ctl.handle("text", "/limits")
    assert "/limits 150 25" in ctl.api.last()


def test_pair_only_lighter_rh_and_live_markets(ctl, monkeypatch):
    import club
    ent = {"io": {"SNDK": False, "OAI": False}, "main": {}}
    monkeypatch.setattr(club, "fetch_markets",
                        lambda v: (ent, {"SNDK", "OPENAI", "OAI"}, None))
    ctl.handle("text", "/pair")
    d = ctl.api.datas()
    assert "pair:SNDK" in d and "pair:OAI" in d and "pair:ANTH" not in d
    # bot stopped: the pair is chosen at once, then trade or record
    ctl.handle("button", "pair:SNDK")
    assert club.current_pair() == ("lighter-rh", "SNDK")
    assert {"goT:SNDK", "goR:SNDK"} <= set(ctl.api.datas())
    # bot running: asks first, then stops and starts on the new pair
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot("OAI")])
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    club.set_current_pair(("lighter-rh", "OAI"))
    ctl.handle("button", "pair:SNDK")
    assert "swT:SNDK" in ctl.api.datas() and ctl.calls["stop"] == 0
    ctl.handle("button", "swT:SNDK")
    assert ctl.calls["stop"] == 1
    assert club.current_pair() == ("lighter-rh", "SNDK")
    assert ctl.calls["launch"] == [(False, ("lighter-rh", "SNDK"))]


def test_new_ticker_offers_only_recording_and_switches_to_it(ctl, monkeypatch):
    import club
    ent = {"io": {"SNDK": False, "OAI": False}, "main": {}}
    monkeypatch.setattr(club, "fetch_markets",
                        lambda v: (ent, {"SNDK", "OAI"}, None))
    uncalibrate(club, ("lighter-rh", "OAI"))
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot("SNDK")])
    ctl.handle("button", "pair:OAI")
    d = ctl.api.datas()
    assert "swR:OAI" in d and "swT:OAI" in d
    assert "нет данных" in ctl.api.last()
    monkeypatch.setattr(club, "keys_state", lambda venue=None: ({}, True))
    monkeypatch.setattr(club, "live_deps_ok", lambda: True)
    ctl.handle("button", "swT:OAI")
    assert ctl.calls["launch"] == [(True, ("lighter-rh", "OAI"))]
    import tgcontrol
    assert tgcontrol.golive_load()[0] == ("lighter-rh", "OAI")


def test_menu_has_test_recording_and_limit_buttons(ctl):
    import club
    ctl.handle("text", "/menu")
    assert "record" in ctl.api.datas()
    ctl.handle("button", "record")
    assert "record!" in ctl.api.datas() and ctl.calls["launch"] == []
    ctl.handle("button", "limits")
    assert "lim:100:25" in ctl.api.datas()
    ctl.handle("button", "lim:100:25")
    v = club.cfg_values()
    assert v["pos_entropy"] == 100 and v["order"] == 25


def test_bot_session_lookup_is_exact(monkeypatch):
    """tmux matches a session name by prefix: «moneyclub3» found the
    listener «moneyclub3-tg», and Stop killed the listener itself."""
    import club
    seen = []
    monkeypatch.setattr(club, "tmux", lambda *a, **k: seen.append(a) or
                        type("R", (), {"returncode": 1})())
    club.tmux_has_session()
    assert seen[-1][:3] == ("has-session", "-t", "=" + club.TMUX_SESSION)


def test_status_from_engine_snapshot(ctl, monkeypatch):
    import club
    import tgcontrol
    ctl.handle("text", "/status")
    assert "не запущен" in ctl.api.last()
    monkeypatch.setattr(club, "find_bots", lambda: [FakeBot()])
    os.makedirs(club.LOG_DIR, exist_ok=True)
    with open(tgcontrol.SNAPSHOT, "w", encoding="utf-8") as fh:
        json.dump({"ts": time.time(), "status": "📊 ok", "pnl": "💰 ok"}, fh)
    ctl.handle("text", "/pnl")
    assert ctl.api.last() == "💰 ok"
    ctl.handle("text", "/pause")
    assert json.load(open(tgcontrol.CONTROL))["cmd"] == "/pause"


def test_engine_bridge_writes_snapshot_and_takes_pause(tmp_path):
    from test_risk import make_engine
    from entropy_arb.engine import TG_CONTROL, TG_SNAPSHOT
    from entropy_arb import journal

    async def go():
        eng = make_engine(str(tmp_path))
        eng.record_only = False
        sent = []

        async def send(text):
            sent.append(text)
        eng._tg_send = send
        snap, ctl = eng._tg_paths()
        os.makedirs(os.path.dirname(ctl), exist_ok=True)
        with open(ctl, "w") as fh:
            json.dump({"cmd": "/pause", "ts": time.time()}, fh)
        task = asyncio.create_task(eng._tg_bridge_loop())
        for _ in range(200):
            if sent and os.path.exists(snap):
                break
            await asyncio.sleep(0.01)
        eng.stop.set()
        await asyncio.wait_for(task, 3)
        return eng, sent, snap, ctl
    eng, sent, snap, ctl = asyncio.run(go())
    assert sent and "/resume" in sent[0]
    assert not os.path.exists(ctl)
    d = json.load(open(snap, encoding="utf-8"))
    assert d["pnl"].startswith("💰") and d["status"]
    assert snap.endswith(TG_SNAPSHOT) and ctl.endswith(TG_CONTROL)


def test_recorded_median_needs_only_15_minutes(tmp_path):
    """The menu's analysis wants 6 hours for its 24 h median; trading from
    Telegram must start after 15 minutes of recording."""
    import tgcontrol
    path = tmp_path / "m.csv"
    now = time.time()
    rows = ["minute_ts,samples,premium_close_bps"]
    for i in range(14):
        rows.append(f"{now - 60 * (i + 1):.0f},30,{-190 - i % 3}")
    rows.append(f"{now - 60 * 40:.0f},2,-500")           # too few samples
    rows.append(f"{now - 3600 * 30:.0f},30,-500")        # older than a day
    path.write_text("\n".join(rows) + "\n")
    assert tgcontrol.recorded_median(str(path)) == (None, 14)
    with open(path, "a") as fh:
        fh.write(f"{now - 60 * 31:.0f},30,-191\n")
    med, n = tgcontrol.recorded_median(str(path))
    assert n == 15 and med == -191.0


def test_api_edits_the_pressed_message_and_keeps_the_header():
    import tgcontrol
    calls = []
    api = tgcontrol.Api("t", "123")
    api.call = lambda m, p, timeout=15.0: calls.append((m, p)) or {
        "ok": True, "result": {"message_id": 5}}
    api.send("a < b", [[("x", "y")]])
    assert calls[-1][0] == "sendMessage" and api.msg_id == 5
    text = calls[-1][1]["text"]
    assert text.startswith(tgcontrol.HEADER) and "a &lt; b" in text
    api.send("next")
    assert calls[-1][0] == "editMessageText"
    assert calls[-1][1]["message_id"] == 5
