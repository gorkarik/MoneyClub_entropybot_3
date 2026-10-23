"""Data export: period filter, day files, no keys, parts under the limit."""
import gzip
import os
import sys
import tarfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import export  # noqa: E402

DAY = 86400.0
T0 = 1_760_000_000.0            # 2025-10-09 UTC


def _app(tmp_path):
    app = tmp_path / "app"
    logs = app / "logs"
    (logs / "ticks").mkdir(parents=True)
    (logs / "flows").mkdir()
    (app / "tickers").mkdir()
    (logs / "trades.csv").write_text(
        "ts,direction,fill\n"
        f"{T0 - 5 * DAY},sell,1\n{T0 - DAY / 2},buy,2\n#marker,,\n")
    (logs / "sessions.csv").write_text(
        f"session_id,start_ts,end_ts\nold,{T0 - 9 * DAY},{T0 - 8 * DAY}\n")
    (logs / "notime.csv").write_text("a,b\n1,2\n")
    for d in (export._day(T0 - 5 * DAY), export._day(T0)):
        with gzip.open(logs / "ticks" / f"lighter-rh_ANTH_{d}.csv.gz",
                       "wt") as fh:
            fh.write("ts\n1\n")
        (logs / "flows" / f"lighter-rh_ANTH_{d}.csv").write_text("ts\n1\n")
    (logs / "engine.log").write_text("log line\n")
    (logs / "slip_x.json").write_text("{}")
    (app / ".env").write_text("HL_PRIVATE_KEY=secret\n")
    (app / "config.yaml").write_text("x: 1\n")
    (app / "tickers" / "ANTH.yaml").write_text("t: 1\n")
    return app


def _names(paths):
    out = {}
    for p in paths:
        with tarfile.open(p) as t:
            for m in t.getmembers():
                out[m.name] = t.extractfile(m).read()
    return out


def test_all_for_two_days(tmp_path):
    app = _app(tmp_path)
    parts = export.build(str(app), "logs", T0 - 2 * DAY,
                         str(app / "exports"), "all", "2 дня", now=T0)
    assert len(parts) == 1 and parts[0].endswith(".tar.gz")
    n = _names(parts)
    assert "README.txt" in n and "config.yaml" in n
    assert "tickers/ANTH.yaml" in n and "slip_x.json" in n
    assert "engine.log" in n and "notime.csv" in n
    # only rows of the period, the marker kept, the old session dropped
    assert n["trades.csv"].decode().splitlines() == [
        "ts,direction,fill", f"{T0 - DAY / 2},buy,2", "#marker,,"]
    assert "sessions.csv" not in n
    day = export._day(T0)
    assert f"ticks/lighter-rh_ANTH_{day}.csv.gz" in n
    assert f"flows/lighter-rh_ANTH_{day}.csv" in n
    assert not any("ANTH_" + export._day(T0 - 5 * DAY) in k for k in n)
    assert not any(".env" in k or b"secret" in v for k, v in n.items())


def test_flows_only_and_all_time(tmp_path):
    app = _app(tmp_path)
    parts = export.build(str(app), "logs", 0.0, str(app / "exports"),
                         "flows", now=T0)
    n = _names(parts)
    assert {k for k in n if k != "README.txt"} == {
        f"flows/lighter-rh_ANTH_{export._day(T0 - 5 * DAY)}.csv",
        f"flows/lighter-rh_ANTH_{export._day(T0)}.csv"}


def test_nothing_in_period(tmp_path):
    app = tmp_path / "empty"
    (app / "logs").mkdir(parents=True)
    assert export.build(str(app), "logs", T0, str(app / "exports"),
                        "flows", now=T0) == []
    assert os.listdir(app / "exports") == []


def test_split_into_parts(tmp_path):
    app = _app(tmp_path)
    big = os.urandom(300_000)
    for i in range(4):
        p = app / "logs" / "ticks" / \
            f"lighter-rh_X{i}_{export._day(T0)}.csv.gz"
        p.write_bytes(big)
    parts = export.build(str(app), "logs", 0.0, str(app / "exports"), "all",
                         now=T0, limit=700_000)
    assert len(parts) >= 2
    assert all("_part" in p for p in parts)
    assert all(os.path.getsize(p) < 1_100_000 for p in parts)
    n = _names(parts)
    assert sum(1 for k in n if "_X" in k) == 4


def test_old_exports_removed(tmp_path):
    out = tmp_path / "exports"
    out.mkdir()
    old = out / "mriya_all_old.tar.gz"
    old.write_bytes(b"x")
    os.utime(old, (T0 - 4 * DAY, T0 - 4 * DAY))
    export.cleanup(str(out), T0)
    assert not old.exists()


def test_menu_builds_and_prints_scp(tmp_path, monkeypatch):
    from rich.console import Console
    import club
    app = _app(tmp_path)
    monkeypatch.setattr(club, "APP_DIR", str(app))
    monkeypatch.chdir(app)
    monkeypatch.setattr(club, "cfg_values",
                        lambda pair=None: {"trades_csv": "logs/trades.csv"})
    monkeypatch.setattr(club, "read_env", lambda: {})
    rec = Console(record=True, width=160)
    monkeypatch.setattr(club, "console", rec)
    monkeypatch.setattr(club, "header", lambda *a, **k: None)
    monkeypatch.setattr(club, "pause", lambda: None)
    answers = iter(["1", "5"])
    monkeypatch.setattr(club, "menu", lambda *a, **k: next(answers))
    printed = []
    monkeypatch.setattr("builtins.print", lambda *a, **k: printed.append(
        " ".join(map(str, a))))
    club.action_export()
    out = rec.export_text()
    assert "Готово: 1 файл" in out
    assert any(line.strip().startswith("scp ") and "exports/mriya_all_" in line
               for line in printed)
