"""Data export for analysis on a computer: one or more .tar.gz archives with
everything the bot recorded over a period.

What goes in ("all"):
  * logs/*.csv with a time column (trades, sessions, hedges, exec_diag,
    equity, minutes of every pair, autocalib, latency, maker_shadow …) —
    only the rows of the period;
  * logs/ticks/ and logs/flows/ — the day files of the period;
  * logs/engine.log and its older parts changed during the period;
  * logs/*.json (measured slippage), config.yaml, tickers/*.yaml;
  * README.txt — what every file is.
"flows" packs only logs/flows/.

Never packed: .env (keys), venv, the exports themselves.

Archives are split so that each one stays under PART_LIMIT bytes (Telegram
takes files up to 50 MB); each part is a complete archive on its own.
"""
from __future__ import annotations

import csv
import glob
import io
import os
import re
import tarfile
import time
from typing import Iterable, List, Optional, Tuple

PART_LIMIT = 45 * 1024 * 1024
KEEP_EXPORTS_SEC = 3 * 86400       # old archives are removed after 3 days
TIME_COLUMNS = ("ts", "minute_ts", "end_ts", "start_ts")
DAY_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.csv(\.gz)?$")
NEVER = (".env",)

README = """Mriya — данные для анализа
=========================

Период: {period}. Собрано: {built} UTC. Время в файлах — UTC (ts — секунды
Unix, time_utc — дата и время).

trades.csv        сделки бота: обе ноги, план и исполнение, fill edge
sessions.csv      сессии Старт…Стоп: баланс, результат, оборот
hedges.csv        добор хеджа и закрытия
exec_diag.csv     каждый сигнал: возраст стаканов, премия до и после
equity.csv        балансы бирж раз в несколько секунд
minutes_*.csv     минутные свечи премии по каждой паре
ticks/            стаканы обеих бирж раз в 0.5 с (по дням, .csv.gz)
flows/            крупные сделки на Entropy (заявки от $20 000)
engine.log*       журнал бота
slip_*.json       измеренное проскальзывание по парам
config.yaml       настройки (ключей здесь нет — они в .env, его в архиве нет)
tickers/          настройки пар

Из CSV взяты только строки за период; файлы по дням — дни периода.
"""


def _day(ts: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def filtered_csv(path: str, since: float) -> Optional[bytes]:
    """The rows of a CSV from `since` on (all rows when it has no time
    column or since is 0). Rows whose time does not parse (markers) stay.
    None: nothing left but the header."""
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        rows = list(csv.reader(fh))
    if not rows:
        return None
    head = rows[0]
    col = next((head.index(c) for c in TIME_COLUMNS if c in head), None)
    keep = rows[1:]
    if since > 0 and col is not None:
        out = []
        for r in keep:
            try:
                if float(r[col]) < since:
                    continue
            except (IndexError, ValueError):
                pass
            out.append(r)
        keep = out
    if not keep:
        return None
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(head)
    w.writerows(keep)
    return buf.getvalue().encode("utf-8")


def _day_files(directory: str, since: float) -> List[str]:
    out = []
    first = _day(since) if since > 0 else ""
    for p in sorted(glob.glob(os.path.join(directory, "*.csv*"))):
        m = DAY_RE.search(os.path.basename(p))
        if m and m.group(1) >= first:
            out.append(p)
    return out


def collect(app_dir: str, log_dir: str, since: float,
            what: str = "all") -> List[Tuple[str, str]]:
    """(source path, name in the archive) of everything to pack."""
    log_dir = os.path.join(app_dir, log_dir) if not os.path.isabs(log_dir) \
        else log_dir
    items: List[Tuple[str, str]] = []
    for p in _day_files(os.path.join(log_dir, "flows"), since):
        items.append((p, "flows/" + os.path.basename(p)))
    if what == "flows":
        return items
    for p in _day_files(os.path.join(log_dir, "ticks"), since):
        items.append((p, "ticks/" + os.path.basename(p)))
    for p in sorted(glob.glob(os.path.join(log_dir, "*.csv"))):
        items.append((p, os.path.basename(p)))
    for p in sorted(glob.glob(os.path.join(log_dir, "*.json"))):
        items.append((p, os.path.basename(p)))
    for p in sorted(glob.glob(os.path.join(log_dir, "engine.log*"))):
        try:
            if since <= 0 or os.path.getmtime(p) >= since:
                items.append((p, os.path.basename(p)))
        except OSError:
            continue
    cfg = os.path.join(app_dir, "config.yaml")
    if os.path.exists(cfg):
        items.append((cfg, "config.yaml"))
    for p in sorted(glob.glob(os.path.join(app_dir, "tickers", "*.yaml"))):
        items.append((p, "tickers/" + os.path.basename(p)))
    return [(p, n) for p, n in items
            if not any(os.path.basename(p).startswith(x) for x in NEVER)]


def _add(tar: tarfile.TarFile, name: str, data: bytes, mtime: float) -> None:
    ti = tarfile.TarInfo(name)
    ti.size = len(data)
    ti.mtime = int(mtime)
    tar.addfile(ti, io.BytesIO(data))


def build(app_dir: str, log_dir: str, since: float, out_dir: str,
          what: str = "all", period: str = "", now: Optional[float] = None,
          limit: int = PART_LIMIT) -> List[str]:
    """Pack the data; returns the archive paths (one or more parts).
    Empty list: nothing recorded in the period."""
    now = time.time() if now is None else now
    items = collect(app_dir, log_dir, since, what)
    os.makedirs(out_dir, exist_ok=True)
    cleanup(out_dir, now)
    stamp = time.strftime("%Y%m%d-%H%M", time.gmtime(now))
    base = os.path.join(out_dir, f"mriya_{what}_{stamp}")
    parts: List[str] = []
    raw = tar = None
    added = 0

    def open_part():
        nonlocal raw, tar
        path = f"{base}_part{len(parts) + 1}.tar.gz"
        parts.append(path)
        raw = open(path, "wb")
        tar = tarfile.open(fileobj=raw, mode="w:gz")
        _add(tar, "README.txt", README.format(
            period=period or "всё время",
            built=time.strftime("%Y-%m-%d %H:%M", time.gmtime(now))
        ).encode("utf-8"), now)

    def close_part():
        nonlocal raw, tar
        if tar is not None:
            tar.close()
            raw.close()
        raw = tar = None

    for src, name in items:
        try:
            if "/" not in name and name.endswith(".csv"):
                data = filtered_csv(src, since)
            else:
                with open(src, "rb") as fh:
                    data = fh.read()
            mtime = os.path.getmtime(src)
        except OSError:
            continue
        if not data:
            continue
        # what the file will add to the archive: .gz is not compressed again
        est = len(data) if name.endswith(".gz") else len(data) // 6
        if tar is None:
            open_part()
        elif raw.tell() + est > limit:
            close_part()
            open_part()
        _add(tar, name, data, mtime)
        added += 1
    close_part()
    if added == 0:
        for p in parts:
            try:
                os.remove(p)
            except OSError:
                pass
        return []
    if len(parts) == 1:
        single = base + ".tar.gz"
        os.replace(parts[0], single)
        parts = [single]
    return parts


def cleanup(out_dir: str, now: float) -> None:
    for p in glob.glob(os.path.join(out_dir, "mriya_*.tar.gz")):
        try:
            if now - os.path.getmtime(p) > KEEP_EXPORTS_SEC:
                os.remove(p)
        except OSError:
            continue


def human_size(n: float) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024 or unit == "ГБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} ГБ"


def tg_send_document(token: str, chat_id: str, path: str,
                     caption: str = "", timeout: float = 300.0) -> dict:
    """Telegram sendDocument (multipart, standard library only)."""
    import json
    import urllib.error
    import urllib.request
    import uuid
    boundary = "mriya" + uuid.uuid4().hex
    name = os.path.basename(path)
    with open(path, "rb") as fh:
        body_file = fh.read()

    def field(k: str, v: str) -> bytes:
        return (f"--{boundary}\r\nContent-Disposition: form-data; "
                f"name=\"{k}\"\r\n\r\n{v}\r\n").encode("utf-8")

    body = (field("chat_id", str(chat_id)) + field("caption", caption[:1000])
            + (f"--{boundary}\r\nContent-Disposition: form-data; "
               f"name=\"document\"; filename=\"{name}\"\r\nContent-Type: "
               f"application/gzip\r\n\r\n").encode("utf-8")
            + body_file + f"\r\n--{boundary}--\r\n".encode("utf-8"))
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendDocument", data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                 "User-Agent": "mriya"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}"}
    except Exception as e:
        return {"ok": False, "description": str(e)}


def server_address() -> str:
    """IP of this server as the SSH client sees it (for the scp line)."""
    conn = os.environ.get("SSH_CONNECTION", "").split()
    if len(conn) >= 3:
        return conn[2]
    try:
        import subprocess
        out = subprocess.run(["hostname", "-I"], capture_output=True,
                             text=True, timeout=3).stdout.split()
        if out:
            return out[0]
    except Exception:
        pass
    return "IP-сервера"


def scp_line(paths: Iterable[str]) -> str:
    import getpass
    user = getpass.getuser()
    host = server_address()
    paths = list(paths)
    if len(paths) == 1:
        return f"scp {user}@{host}:{paths[0]} ."
    folder = os.path.dirname(paths[0])
    stem = os.path.basename(paths[0]).rsplit("_part", 1)[0]
    return f"scp {user}@{host}:{folder}/{stem}_part*.tar.gz ."
