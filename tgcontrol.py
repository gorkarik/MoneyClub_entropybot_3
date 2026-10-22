"""Управление ботом из Telegram: постоянный слушатель чата.

Запускается сам (cron через `moneyclub --resume`, после перезагрузки сервера
и при установке) в своей фоновой tmux-сессии: `moneyclub --telegram`.
Работает и когда торговый бот остановлен — поэтому из чата можно его
запустить.

Всё управление — кнопками одного «живого» сообщения: нажатие меняет это же
сообщение, чат не засоряется. Сверху каждого сообщения — шапка MONEY CLUB.
Из команд нужна только /menu (остальные старые команды тоже работают).

Отвечает только чату из .env (TELEGRAM_CHAT_ID), всё остальное
игнорирует. Ключи бирж через Telegram не вводятся и не показываются.
Торговля, Стоп, смена пары и перезапуск — только после кнопки «Да».

Торговый бот сам Telegram не опрашивает (так запросы не конфликтуют):
состояние и результат он раз в 10 с пишет в logs/tg_snapshot.json, а паузу
берёт из logs/tg_control.json.
"""
from __future__ import annotations

import html
import json
import os
import subprocess
import time
import traceback
from typing import List, Optional, Tuple

import club

VENUE = "lighter-rh"            # из Telegram — только Entropy ↔ Lighter RH
SNAPSHOT = os.path.join(club.LOG_DIR, "tg_snapshot.json")
CONTROL = os.path.join(club.LOG_DIR, "tg_control.json")
GOLIVE = os.path.join(club.LOG_DIR, ".tg_golive.json")
LOCK = os.path.join(club.LOG_DIR, ".tglisten.lock")
SNAPSHOT_FRESH_SEC = 60.0
STOPLOSS_CHOICES = (1.0, 2.0, 3.0, 5.0)
LIMIT_CHOICES = ((50, 20), (100, 25), (150, 25), (200, 50))
# столько минут записи цен — и центр коридора ставится сам. Дальше центр
# плавает за рынком (медиана часа), а пороги ведёт автокоридор
CALIB_MIN_MINUTES = 15
GOLIVE_MAX_SEC = 12 * 3600      # «запись, потом торговля» — не дольше
REPORT_PERIODS = {"24": (24, "24 часа"), "168": (168, "неделю"),
                  "0": (0, "всё время")}
MAX_TEXT = 3800                 # предел сообщения Telegram — 4096

HEADER = "💚 <b>MONEY CLUB</b> · версия 3\n━━━━━━━━━━━━━━━━━━\n"
MENU_BTN = ("« Меню", "menu")

HELP = (
    "Как пользоваться\n\n"
    "💰 Торговля (реальные деньги) — бот торгует на ваших счетах. Если по "
    "паре ещё нет данных, он сначала "
    f"{CALIB_MIN_MINUTES} минут записывает цены без денег, потом сам "
    "включает торговлю и пишет сюда.\n"
    "🧪 Тест без денег — только запись цен обеих бирж, сделок нет.\n"
    "⏹ Стоп — бот закрывает позиции на обеих биржах и останавливается.\n"
    "⏸ Пауза — новые позиции не открываются, закрытие идёт как обычно.\n\n"
    "🔁 Сменить пару — тикер для Entropy ↔ Lighter RH. Все кнопки меню "
    "работают с парой, которая указана в шапке.\n"
    "🛠 Настроить пару — если что-то сбилось: проверю пару на биржах, "
    "поставлю центр коридора по записи цен, верну пороги и включу "
    "автокоридор.\n"
    "⚙️ Режим и стратегия — когда входить (автокоридор) и как исполнять.\n"
    "📏 Лимиты — размер позиции и сделки.  🛑 Стоп-лосс — стоп по "
    "убытку за сессию.\n"
    "📊 Статус · 💰 Результат · 📄 Отчёт — что происходит и сколько "
    "заработано.\n\n"
    "Меню всегда можно открыть командой /menu.\n"
    "Ключи бирж меняются только на сервере (команда moneyclub).")

Buttons = Optional[List[List[Tuple[str, str]]]]


# ------------------------------------------------------------- Telegram API

class Api:
    """Одно «живое» сообщение: send() меняет сообщение, на котором нажали
    кнопку (или последнее отправленное в этом ответе), а если это нельзя —
    присылает новое."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self.offset: Optional[int] = None
        self.msg_id: Optional[int] = None

    def call(self, method: str, payload: dict, timeout: float = 15.0) -> dict:
        import urllib.error
        url = club.TG_API.format(token=self.token, method=method)
        try:
            return club._http_json(url, payload, timeout=timeout)
        except urllib.error.HTTPError as e:
            try:
                return json.loads(e.read().decode("utf-8"))
            except Exception:
                return {"ok": False, "description": f"HTTP {e.code}"}

    def send(self, text: str, buttons: Buttons = None, html_text: bool = False,
             new: bool = False) -> None:
        body = text if html_text else html.escape(text[:MAX_TEXT], quote=False)
        payload = {"chat_id": self.chat_id, "text": HEADER + body,
                   "parse_mode": "HTML", "disable_web_page_preview": True,
                   "reply_markup": {"inline_keyboard": [
                       [{"text": t, "callback_data": d} for t, d in row]
                       for row in (buttons or [])]}}
        if self.msg_id is not None and not new:
            d = self.call("editMessageText",
                          dict(payload, message_id=self.msg_id))
            if d.get("ok") or "not modified" in str(d.get("description")):
                return
            log(f"edit refused: {d.get('description')}")
        d = self.call("sendMessage", payload)
        if d.get("ok"):
            self.msg_id = (d.get("result") or {}).get("message_id")
        else:
            log(f"send refused: {d.get('description')}")

    def answer(self, callback_id: str) -> None:
        self.call("answerCallbackQuery", {"callback_query_id": callback_id})

    def set_commands(self) -> None:
        """В списке команд Telegram — только /menu."""
        self.call("setMyCommands", {"commands": [
            {"command": "menu", "description": "Меню MoneyClub"}]})

    def updates(self, timeout: int = 25) -> list:
        payload = {"timeout": timeout,
                   "allowed_updates": ["message", "callback_query"]}
        if self.offset is not None:
            payload["offset"] = self.offset
        d = self.call("getUpdates", payload, timeout=timeout + 10)
        if not d.get("ok"):
            raise RuntimeError(d.get("description") or "getUpdates failed")
        res = d.get("result") or []
        for u in res:
            self.offset = int(u["update_id"]) + 1
        return res

    def skip_backlog(self) -> None:
        """Команды, присланные, пока слушатель не работал, не выполняются
        задним числом (например, старый /stop)."""
        d = self.call("getUpdates", {"offset": -1, "timeout": 0})
        res = d.get("result") or []
        if res:
            self.offset = int(res[-1]["update_id"]) + 1


def parse(update: dict, chat_id: str):
    """(вид, текст, id_кнопки, id_сообщения) для нашего чата; None для
    чужих."""
    if "callback_query" in update:
        q = update["callback_query"]
        msg = q.get("message") or {}
        chat = (msg.get("chat") or {}).get("id")
        if str(chat) != str(chat_id):
            return None
        return "button", str(q.get("data") or ""), q.get("id"), \
            msg.get("message_id")
    msg = update.get("message") or {}
    if str((msg.get("chat") or {}).get("id", "")) != str(chat_id):
        return None
    text = (msg.get("text") or "").strip()
    if not text:
        return None
    return "text", text, None, None


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S ") + msg, flush=True)


# ------------------------------------------------------------- состояние

def running():
    """(бот этой папки или None, запущен ли чужой бот)."""
    bots = club.find_bots()
    ours = [b for b in bots if b.ours]
    return (ours[0] if ours else None), any(not b.ours for b in bots)


def snapshot() -> Optional[dict]:
    try:
        with open(SNAPSHOT, encoding="utf-8") as fh:
            d = json.load(fh)
    except Exception:
        return None
    if time.time() - float(d.get("ts") or 0) > SNAPSHOT_FRESH_SEC:
        return None
    return d


def settings_line(pair=None) -> str:
    try:
        v = club.cfg_values(pair)
        ab = club.autoband_values()
        lim = v["max_loss_pct"]
        return (f"{club.strategy_label(v['exec_mode'])} · "
                f"{club.autoband_short(ab)}\n"
                f"Лимит позиции {club.usd(v['pos_entropy'])}, сделка до "
                f"{club.usd(v['order'])} · стоп по убытку "
                + (f"{club.fmt(lim, True)}%" if lim > 0 else "выключен"))
    except Exception as e:
        return f"Настройки не читаются: {e}"


def recorded_median(path: str, hours: float = 24.0, min_samples: int = 10):
    """(медиана премии за последние сутки или None, минут записи) прямо по
    минутному файлу записи цен. Анализ в меню сервера ждёт 6 часов данных —
    для старта торговли хватает CALIB_MIN_MINUTES: дальше центр плавает за
    рынком, а коридор ведёт автокоридор."""
    import csv
    import statistics
    cutoff = time.time() - hours * 3600
    prem = []
    try:
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    if float(r["minute_ts"]) < cutoff:
                        continue
                    if int(r["samples"]) < min_samples:
                        continue
                    prem.append(float(r["premium_close_bps"]))
                except (KeyError, ValueError, TypeError):
                    continue
    except FileNotFoundError:
        return None, 0
    if len(prem) < CALIB_MIN_MINUTES:
        return None, len(prem)
    return round(statistics.median(prem), 1), len(prem)


def golive_load():
    """Пара, на которой после записи цен надо самому включить торговлю."""
    try:
        with open(GOLIVE, encoding="utf-8") as fh:
            d = json.load(fh)
        return tuple(d["pair"]), float(d["ts"])
    except Exception:
        return None


def golive_save(pair) -> None:
    os.makedirs(club.LOG_DIR, exist_ok=True)
    club._atomic_write(GOLIVE, json.dumps({"pair": list(pair),
                                           "ts": time.time()}))


def golive_clear() -> None:
    try:
        os.remove(GOLIVE)
    except OSError:
        pass


# ------------------------------------------------------------- действия

class Control:
    def __init__(self, api: Api):
        self.api = api

    @staticmethod
    def pair():
        """Пара, с которой работает меню: у запущенного бота — его пара,
        иначе выбранная."""
        return club.current_pair()

    def minutes_left(self, pair) -> int:
        _med, n = self.recorded_minutes(pair)
        return max(CALIB_MIN_MINUTES - n, 1)

    # -- меню
    def menu(self, note: str = "") -> None:
        bot, foreign = running()
        pair = self.pair()
        e = lambda s: html.escape(str(s), quote=False)  # noqa: E731
        lines = []
        if note:
            lines += [e(note), ""]
        lines.append(f"Пара: <b>{e(pair[1])}</b> · "
                     f"{e(club.pair_label(pair).split(' · ', 1)[-1])}")
        gl = golive_load()
        if bot is not None and not bot.record:
            paused = (snapshot() or {}).get("paused")
            lines.append("⏸ <b>Торговля на паузе</b> (новые позиции не "
                         "открываются)" if paused else
                         "🟢 <b>Торгует на реальные деньги</b>")
        elif bot is not None:
            s = "🧪 <b>Тестовая запись</b> (без денег, без сделок)"
            if gl is not None and gl[0] == tuple(pair):
                s += (f"\n⏳ Торговля включится сама примерно через "
                      f"{self.minutes_left(pair)} мин")
            lines.append(s)
        elif foreign:
            lines.append("⚠ На сервере работает другой бот — этот не "
                         "запущен")
        else:
            s = "⏹ <b>Остановлен</b>"
            if club.autorun_load() is not None:
                s += " · автоперезапуск ждёт (раз в 5 минут)"
            lines.append(s)
        lines.append(e(settings_line(pair)))
        if bot is None:
            top = [[("💰 Торговля (реальные деньги)", "run")],
                   [("🧪 Тест без денег", "record"),
                    ("🛠 Настроить пару", "fix")]]
        elif bot.record:
            top = [[("💰 Включить торговлю (реальные деньги)", "run")],
                   [("⏹ Стоп", "stop"), ("🛠 Настроить пару", "fix")]]
        else:
            snap = snapshot() or {}
            top = [[("⏹ Стоп", "stop"),
                    ("▶️ Снять паузу", "resume") if snap.get("paused")
                    else ("⏸ Пауза", "pause")],
                   [("💰 Результат", "pnl"), ("🛠 Настроить пару", "fix")]]
        rows = top + [
            [("🔁 Сменить пару", "pair"), ("⚙️ Режим и стратегия", "mode")],
            [("📏 Лимиты", "limits"), ("🛑 Стоп-лосс", "stoploss")],
            [("📊 Статус", "status"), ("📄 Отчёт", "report"),
             ("❓ Помощь", "help")],
        ]
        self.api.send("\n".join(lines), rows, html_text=True)

    def status(self, kind: str) -> None:
        bot, _ = running()
        if bot is None:
            self.menu("Бот не запущен — состояния и результата сессии нет.")
            return
        snap = snapshot()
        if snap is None:
            text = ("Бот запущен, но ещё не прислал состояние (первые "
                    "секунды после старта). Нажмите «Обновить» через "
                    "полминуты.")
        else:
            text = snap.get(kind) or "—"
        self.api.send(text, [[("🔄 Обновить", kind), MENU_BTN]])

    def pause(self, cmd: str) -> None:
        bot, _ = running()
        if bot is None or bot.record:
            self.menu("Пауза нужна только во время торговли.")
            return
        club._atomic_write(CONTROL, json.dumps({"cmd": cmd,
                                                "ts": time.time()}))
        self.menu("⏸ Передал боту: пауза — ответ придёт отдельным "
                  "сообщением." if cmd == "/pause" else
                  "▶️ Передал боту: снять паузу — ответ придёт отдельным "
                  "сообщением.")

    # -- торговля / запись / стоп
    def ask_run(self) -> None:
        bot, foreign = running()
        if foreign:
            self.menu("На сервере работает другой бот — второй запускать "
                      "нельзя.")
            return
        if bot is not None and not bot.record:
            self.menu("Бот уже торгует.")
            return
        pair = self.pair()
        extra = ""
        if not self.ready_to_trade(pair):
            extra = (f"\n\nПо {pair[1]} ещё нет данных. Сначала "
                     f"~{self.minutes_left(pair)} мин записываю цены без "
                     f"денег, потом сам включаю торговлю и пишу сюда.")
        self.api.send(f"💰 Запустить ТОРГОВЛЮ НА РЕАЛЬНЫЕ ДЕНЬГИ?\n\n"
                      f"{club.pair_label(pair)}\n{settings_line(pair)}"
                      + extra,
                      [[("✅ Да, торговать", "run!"), ("Отмена", "menu")]])

    def run(self, record: bool = False) -> None:
        bot, foreign = running()
        pair = self.pair()
        if foreign or (bot is not None and (record or not bot.record)):
            self.menu("Бот уже работает — сначала Стоп.")
            return
        if not record:
            why = self.cannot_trade(pair)
            if why:
                self.api.send(why[0], why[1])
                return
            if not club.is_calibrated(club.cfg_values(pair)):
                # данных ещё нет: запись цен, потом торговля сама
                golive_save(pair)
                if bot is None:
                    self.launch(pair, record=True,
                                done=f"🧪 Записываю цены {pair[1]} без денег."
                                     f" Примерно через "
                                     f"{self.minutes_left(pair)} мин сам "
                                     f"включу торговлю и напишу сюда.")
                else:
                    self.menu(f"Запись уже идёт. Примерно через "
                              f"{self.minutes_left(pair)} мин сам включу "
                              f"торговлю и напишу сюда.")
                return
            if bot is not None:          # запись → торговля
                self.restart(pair, record=False)
                return
        golive_clear()
        self.launch(pair, record=record)

    def launch(self, pair, record: bool, done: str = "") -> None:
        self.api.send("Запускаю…")
        ok = club.launch_bot(record=record, pair=pair)
        if not ok:
            golive_clear()
            self.menu("✘ Бот не запустился. Подробности — на сервере "
                      "(команда moneyclub → Старт) или в logs/stderr.log.")
            return
        self.menu(done or ("✅ Запущена тестовая запись (без денег)." if record
                           else "✅ Запущена торговля на реальные деньги."))

    def cannot_trade(self, pair):
        """(текст, кнопки), если торговать по паре сейчас нельзя. Центр
        коридора ставит сам, если хватает записи цен."""
        from entropy_arb import journal
        _, keys_ok = club.keys_state(pair[0])
        if not keys_ok:
            return ("✘ Ключи для этой биржи не заполнены. Ключи вводятся "
                    "только на сервере: команда moneyclub → Настройки → "
                    "Ключи.", [[MENU_BTN]])
        if not club.live_deps_ok():
            return ("✘ Не установлены библиотеки боевого режима. Запустите "
                    "установку ещё раз той же командой.", [[MENU_BTN]])
        v = club.cfg_values(pair)
        if not club.is_calibrated(v):
            self.auto_calibrate(pair)
        path = v["trades_csv"]
        marker = journal.read_marker(path)
        last = journal.last_session(path)
        if marker is not None or (last is not None
                                  and last.get("positions_closed") == "0"):
            return ("⚠ Прошлый запуск мог оставить открытые позиции. "
                    "Проверьте позиции на обеих биржах и подтвердите.",
                    [[("Позиции проверил, запускать", "clearmark!"),
                      ("Отмена", "menu")]])
        return None

    def recorded_minutes(self, pair):
        """(медиана премии за сутки или None, минут записи)."""
        return recorded_median(club.cfg_values(pair)["csv"])

    def ready_to_trade(self, pair) -> bool:
        """Центр есть или его можно поставить по записи."""
        if club.is_calibrated(club.cfg_values(pair)):
            return True
        med, n = self.recorded_minutes(pair)
        return med is not None and n >= CALIB_MIN_MINUTES

    def auto_calibrate(self, pair) -> Optional[float]:
        """Центр коридора по медиане премии из записи цен; None — мало
        данных."""
        med, n = self.recorded_minutes(pair)
        if med is None or n < CALIB_MIN_MINUTES:
            return None
        club.save_config([("thresholds", "midline_bps", club.fmt(med))], pair)
        log(f"центр {pair[1]}: {med} по {n} мин записи")
        return med

    def ask_record(self) -> None:
        bot, foreign = running()
        if bot is not None or foreign:
            self.menu("Бот уже работает — сначала Стоп.")
            return
        pair = self.pair()
        _med, n = self.recorded_minutes(pair)
        self.api.send(f"🧪 Запустить тест без денег: {club.pair_label(pair)}?"
                      f"\n\nБот только записывает цены обеих бирж, сделок "
                      f"нет. Записано за сутки: {n} мин.",
                      [[("✅ Да, тест", "record!"), ("Отмена", "menu")]])

    def clear_marker(self) -> None:
        from entropy_arb import journal
        journal.clear_marker(club.cfg_values(self.pair())["trades_csv"])
        self.run()

    def ask_stop(self) -> None:
        bot, _ = running()
        golive = golive_load() is not None
        if bot is None:
            golive_clear()
            if club.autorun_load() is not None:
                club.autorun_clear()
                self.menu("Бот не запущен (похоже, остановился сам). "
                          "Автоперезапуск выключен.")
            else:
                self.menu("Бот и так остановлен.")
            return
        if bot.record:
            text = ("Остановить тестовую запись?" +
                    (" Торговля после неё тоже не включится." if golive
                     else ""))
        else:
            text = ("⏹ Остановить торговлю? Бот закроет позиции на обеих "
                    "биржах и пришлёт итог сессии.")
        self.api.send(text, [[("✅ Да, остановить", "stop!"),
                              ("Отмена", "menu")]])

    def stop(self, show: bool = True) -> Optional[bool]:
        """Останавливает; вернёт режим (True — запись), если бот работал."""
        golive_clear()
        bot, _ = running()
        if bot is None:
            if show:
                self.menu("Бот и так остановлен.")
            return None
        self.api.send("Останавливаю" + (
            "…" if bot.record else ", закрываю позиции на обеих биржах — "
            "это занимает до минуты…"))
        record = bot.record
        ok = club.stop_bots([b for b in club.find_bots() if b.ours])
        if not ok:
            self.menu("✘ Не удалось остановить бота — проверьте на сервере "
                      "(команда moneyclub → Стоп).")
            return None
        if show:
            self.menu("⏹ Бот остановлен.")
        return record

    def restart(self, pair=None, record=None) -> None:
        was = self.stop(show=False)
        if was is None:
            self.menu("Бот не работал — ничего не перезапускаю.")
            return
        if pair is not None:
            club.set_current_pair(pair)
        self.run(record=was if record is None else record)

    def tick(self) -> None:
        """Раз в цикл слушателя: запись цен набрала данных — включить
        торговлю, о которой просили."""
        gl = golive_load()
        if gl is None:
            return
        pair, ts = gl
        bot, _ = running()
        if (time.time() - ts > GOLIVE_MAX_SEC or bot is None
                or not bot.record or (bot.venue, bot.ticker) != pair):
            golive_clear()               # остановили или сменили пару
            return
        if not self.ready_to_trade(pair):
            return
        self.api.msg_id = None           # новое сообщение — с уведомлением
        self.auto_calibrate(pair)
        log(f"запись {pair[1]} готова — включаю торговлю")
        self.restart(pair, record=False)

    # -- настройки
    def change(self, changes, what: str) -> None:
        """Сохраняет для пары из шапки. Если бот работает — предлагает
        перезапуск (настройки бот подхватывает только при старте)."""
        try:
            club.save_config(changes, self.pair())
        except Exception as e:
            self.menu(f"✘ Не сохранено: {e}")
            return
        bot, _ = running()
        if bot is None:
            self.menu(f"✔ {what}. Применится при следующем запуске.")
            return
        self.api.send(f"✔ {what}.\n\nБот подхватит это после перезапуска"
                      + ("" if bot.record else
                         " (при остановке он закроет позиции)") + ".",
                      [[("🔄 Перезапустить сейчас", "restart!"),
                        ("Позже", "menu")]])

    def mode_screen(self) -> None:
        pair = self.pair()
        v = club.cfg_values(pair)
        ab = club.autoband_values()
        cur = club.autoband_profile(ab) if ab["enabled"] else None
        rows = [[(("● " if name == cur else "") + name, f"ab:{name}")
                 for name, _sh, _c, _n in club.AUTOBAND_PROFILES]]
        smode = v["exec_mode"]
        rows.append([(("● " if smode == "simultaneous" else "")
                      + "Стратегия 1", "st:simultaneous"),
                     (("● " if smode == "entropy_first" else "")
                      + "Стратегия 2", "st:entropy_first"),
                     (("● " if smode == "grid" else "")
                      + "Стратегия 4", "st:grid")])
        rows.append([MENU_BTN])
        notes = "\n".join(f"• {n}: {t}" for n, _s, _c, t
                          in club.AUTOBAND_PROFILES)
        self.api.send(
            "⚙️ Режим и стратегия\n\n"
            "Автокоридор — КОГДА входить:\n" + notes +
            "\n\nСтратегия — КАК исполнять:\n"
            "• 1: обе ноги сразу\n"
            "• 2: сначала Entropy с пределом цены, потом хедж\n"
            "• 4: сетка — лесенка вокруг средней премии, сама решает, когда "
            "входить (автокоридор в ней не действует)\n\n"
            f"Сейчас: {club.strategy_label(smode)} · "
            f"{club.autoband_short(ab)}", rows)

    def set_mode(self, name: str) -> None:
        prof = next((p for p in club.AUTOBAND_PROFILES if p[0] == name), None)
        if prof is None:
            return
        _n, share, cost, _t = prof
        self.change([("autoband", "share", club.fmt(share, True)),
                     ("autoband", "max_cost_usd", club.fmt(cost, True)),
                     ("autoband", "enabled", "true")],
                    f"Автокоридор: режим «{name}»")

    def set_strategy(self, mode: str) -> None:
        if mode not in ("simultaneous", "entropy_first", "grid"):
            return
        self.change([("execution", "mode", mode)],
                    club.strategy_label(mode))

    # -- пара
    def pair_screen(self) -> None:
        ent, hed, err = club.fetch_markets(VENUE)
        rows, row = [], []
        cur = self.pair()
        for t in club.pair_list(VENUE):
            if err is None:
                try:
                    ok, _why, _e, _h, _dex = club.market_status((VENUE, t),
                                                                ent, hed)
                except Exception:
                    ok = False
                if not ok:
                    continue
            mark = "● " if cur == (VENUE, t) else ""
            row.append((f"{mark}{t}", f"pair:{t}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([MENU_BTN])
        note = ("" if err is None else
                "\n(Биржи сейчас не ответили — показываю все тикеры.)")
        self.api.send(f"🔁 Сменить пару (Entropy ↔ Lighter RH)\n\nСейчас: "
                      f"{cur[1]}. Выберите тикер:{note}", rows)

    def pair_buttons(self, pair, prefix: str):
        t = pair[1]
        return [[("💰 Торговля (реальные деньги)", f"{prefix}T:{t}")],
                [("🧪 Тест без денег", f"{prefix}R:{t}"), ("Отмена", "menu")]]

    def check_market(self, pair) -> Optional[str]:
        """Почему пару сейчас выбрать нельзя (None — можно); запоминает,
        где торгуется первая нога."""
        ent, hed, err = club.fetch_markets(VENUE)
        if err is not None:
            return None
        ok, why, _e, _h, dex = club.market_status(pair, ent, hed)
        if not ok:
            return why
        club.remember_dex(pair, dex)
        return None

    def ask_pair(self, ticker: str) -> None:
        if ticker not in club.pair_list(VENUE):
            return
        pair = (VENUE, ticker)
        why = self.check_market(pair)
        if why:
            self.menu(f"✘ {ticker}: {why} — выбрать нельзя.")
            return
        note = ("" if self.ready_to_trade(pair) else
                f"\n\nПо {ticker} ещё нет данных: при Торговле бот сначала "
                f"~{self.minutes_left(pair)} мин запишет цены без денег, "
                f"потом сам включит торговлю.")
        bot, _ = running()
        if bot is None:
            club.set_current_pair(pair)
            self.api.send(f"✔ Пара: {club.pair_label(pair)}.\nЧто "
                          f"запустить?" + note, self.pair_buttons(pair, "go"))
            return
        if (bot.venue, bot.ticker) == pair:
            self.menu(f"Бот уже работает с {ticker}.")
            return
        self.api.send(f"Сейчас бот работает с {bot.ticker}. Остановить его"
                      + ("" if bot.record else " (с закрытием позиций)") +
                      f" и запустить {ticker}?" + note,
                      self.pair_buttons(pair, "sw"))

    def go_pair(self, mode: str, ticker: str, switch: bool) -> None:
        """mode: T — торговля, R — тест без денег."""
        if ticker not in club.pair_list(VENUE):
            return
        pair = (VENUE, ticker)
        record = mode == "R"
        if switch and running()[0] is not None:
            self.restart(pair, record=record)
            return
        club.set_current_pair(pair)
        self.run(record=record)

    # -- починка пары
    def ask_fix(self) -> None:
        pair = self.pair()
        self.api.send(f"🛠 Настроить пару {pair[1]}?\n\n"
                      "• проверю, что пара есть на обеих биржах;\n"
                      "• поставлю центр коридора по записи цен за сутки;\n"
                      "• верну запасные пороги к стандартным;\n"
                      "• включу автокоридор.\n\n"
                      "Лимиты, стоп-лосс и стратегия не меняются.",
                      [[("✅ Да, настроить", "fix!"), ("Отмена", "menu")]])

    def fix(self) -> None:
        pair = self.pair()
        why = self.check_market(pair)
        if why:
            self.menu(f"✘ {pair[1]}: {why}. Выберите другую пару.")
            return
        done = ["✔ пара есть на обеих биржах"]
        d = club.DEFAULT_THRESHOLDS
        changes = [("thresholds", "upper_bps", club.fmt(d["upper_bps"])),
                   ("thresholds", "lower_bps", club.fmt(d["lower_bps"]))]
        med, n = self.recorded_minutes(pair)
        cur = club.cfg_values(pair)["midline_bps"]
        if med is not None:
            changes.append(("thresholds", "midline_bps", club.fmt(med)))
            done.append(f"✔ центр коридора {club.fmt(med)} bps (по {n} мин "
                        f"записи)")
        elif club.is_calibrated(club.cfg_values(pair)):
            done.append(f"• центр оставил {club.fmt(cur)} bps — записи цен "
                        f"мало ({n} мин)")
        else:
            done.append(f"• центра пока нет: нажмите «Торговля» — бот сам "
                        f"запишет цены ~{self.minutes_left(pair)} мин и "
                        f"включит торговлю")
        done.append(f"✔ запасные пороги +{club.fmt(d['upper_bps'])} / "
                    f"−{club.fmt(d['lower_bps'])} bps")
        try:
            club.save_config(changes, pair)
            if not club.autoband_values()["enabled"]:
                club.save_config([("autoband", "enabled", "true")], pair)
            done.append("✔ автокоридор включён")
        except Exception as e:
            self.menu(f"✘ Не сохранено: {e}")
            return
        bot, _ = running()
        text = f"🛠 {pair[1]} настроена:\n" + "\n".join(done)
        if bot is None:
            self.menu(text)
            return
        self.api.send(text + "\n\nБот подхватит это после перезапуска.",
                      [[("🔄 Перезапустить сейчас", "restart!"),
                        ("Позже", "menu")]])

    # -- лимиты, стоп-лосс, отчёт
    def limits(self, args: List[str]) -> None:
        pair = self.pair()
        v = club.cfg_values(pair)
        if len(args) != 2:
            cur = (v["pos_entropy"], v["order"])
            btn = lambda p, o: ((("● " if cur == (p, o) else "")  # noqa: E731
                                 + f"${p} / ${o}"), f"lim:{p}:{o}")
            rows = [[btn(p, o) for p, o in LIMIT_CHOICES[:2]],
                    [btn(p, o) for p, o in LIMIT_CHOICES[2:]],
                    [MENU_BTN]]
            self.api.send(f"📏 Лимиты {pair[1]}: позиция / сделка\n\n"
                          f"Сейчас: {club.usd(v['pos_entropy'])} / "
                          f"{club.usd(v['order'])} (минимум сделки "
                          f"{club.usd(v['min_order'])}).\n\nВыберите кнопку "
                          f"или напишите свои числа: /limits 150 25", rows)
            return
        try:
            pos, order = (float(a.replace(",", ".").lstrip("$")) for a in args)
        except ValueError:
            self.menu("Нужны два числа, например /limits 150 25")
            return
        if not (0 < order <= pos <= 100000) or order < v["min_order"]:
            self.menu(f"Сделка должна быть не меньше "
                      f"{club.usd(v['min_order'])} и не больше лимита "
                      f"позиции.")
            return
        self.change([("entropy", "max_position_usd", club.fmt(pos, True)),
                     ("hedge", "max_position_usd", club.fmt(pos, True)),
                     ("sizing", "max_order_notional_usd",
                      club.fmt(order, True))],
                    f"Лимит позиции {club.usd(pos)}, сделка до "
                    f"{club.usd(order)}")

    def stoploss_screen(self) -> None:
        lim = club.cfg_values(self.pair())["max_loss_pct"]
        rows = [[((("● " if abs(lim - p) < 1e-9 else "") + f"{club.fmt(p)}%"),
                  f"sl:{club.fmt(p)}") for p in STOPLOSS_CHOICES],
                [(("● " if lim <= 0 else "") + "Выключить", "sl:0"),
                 MENU_BTN]]
        self.api.send("🛑 Стоп по убытку\n\nБот останавливается и закрывает "
                      "позиции, если за сессию потерял столько процентов "
                      "баланса.\nСейчас: "
                      + (f"{club.fmt(lim, True)}%" if lim > 0 else "выключен")
                      + "\nДругое число: /stoploss 4", rows)

    def set_stoploss(self, raw: str) -> None:
        try:
            pct = float(raw.replace(",", ".").rstrip("%"))
        except ValueError:
            self.menu("Нужно число процентов, например /stoploss 3")
            return
        if not 0 <= pct < 100:
            self.menu("От 0 до 99 процентов (0 — выключить).")
            return
        self.change([("risk", "max_loss_pct", club.fmt(pct))],
                    "Стоп по убытку: " + (f"{club.fmt(pct)}% за сессию"
                                          if pct > 0 else "выключен"))

    def report_screen(self) -> None:
        self.api.send(f"📄 Отчёт по сделкам {self.pair()[1]} за период:", [
            [(label.capitalize(), f"rep:{key}")
             for key, (_h, label) in REPORT_PERIODS.items()],
            [MENU_BTN]])

    def report(self, key: str) -> None:
        if key not in REPORT_PERIODS:
            return
        hours, label = REPORT_PERIODS[key]
        v = club.cfg_values(self.pair())
        self.api.send("Собираю отчёт…")
        try:
            r = subprocess.run(
                [club.PY, "tools/report.py", "--dir",
                 os.path.dirname(v["trades_csv"]) or club.LOG_DIR,
                 "--config", club.CONFIG, "--hours", str(hours), "--summary",
                 *club.report_args(v)],
                capture_output=True, text=True, timeout=120)
            out = r.stdout.strip() or r.stderr.strip()
        except Exception as e:
            out = f"Отчёт не собрался: {e}"
        self.api.send(f"📄 {club.pair_label(v['pair'])} · за {label}\n\n"
                      + (out or "Сделок за период нет."),
                      [[("Другой период", "report"), MENU_BTN]])

    # -- разбор
    def handle(self, kind: str, text: str) -> None:
        if kind == "text":
            parts = text.split()
            cmd = parts[0].split("@")[0].lower()
            args = parts[1:]
        else:
            cmd, args = text, []
        if cmd in ("/start", "/menu", "menu", "меню"):
            self.menu()
        elif cmd in ("/help", "help"):
            self.api.send(HELP, [[MENU_BTN]])
        elif cmd in ("/status", "status"):
            self.status("status")
        elif cmd in ("/pnl", "pnl"):
            self.status("pnl")
        elif cmd in ("/pause", "pause"):
            self.pause("/pause")
        elif cmd in ("/resume", "resume"):
            self.pause("/resume")
        elif cmd in ("/run", "run"):
            self.ask_run()
        elif cmd == "run!":
            self.run()
        elif cmd in ("/record", "record"):
            self.ask_record()
        elif cmd == "record!":
            self.run(record=True)
        elif cmd == "clearmark!":
            self.clear_marker()
        elif cmd in ("/stop", "stop"):
            self.ask_stop()
        elif cmd == "stop!":
            self.stop()
        elif cmd == "restart!":
            self.restart()
        elif cmd in ("/fix", "fix"):
            self.ask_fix()
        elif cmd == "fix!":
            self.fix()
        elif cmd in ("/mode", "mode"):
            self.mode_screen()
        elif cmd.startswith("ab:"):
            self.set_mode(cmd[3:])
        elif cmd.startswith("st:"):
            self.set_strategy(cmd[3:])
        elif cmd in ("/pair", "pair"):
            if args:
                self.ask_pair(args[0].upper())
            else:
                self.pair_screen()
        elif cmd[:4] in ("goT:", "goR:"):
            self.go_pair(cmd[2], cmd[4:], switch=False)
        elif cmd[:4] in ("swT:", "swR:"):
            self.go_pair(cmd[2], cmd[4:], switch=True)
        elif cmd.startswith("pair:"):
            self.ask_pair(cmd[5:])
        elif cmd in ("/limits", "limits"):
            self.limits(args)
        elif cmd.startswith("lim:"):
            self.limits(cmd[4:].split(":"))
        elif cmd in ("/stoploss", "stoploss"):
            if args:
                self.set_stoploss(args[0])
            else:
                self.stoploss_screen()
        elif cmd.startswith("sl:"):
            self.set_stoploss(cmd[3:])
        elif cmd in ("/report", "report"):
            self.report_screen()
        elif cmd.startswith("rep:"):
            self.report(cmd[4:])
        elif kind == "text":
            self.menu()                  # любое другое сообщение — меню


# ------------------------------------------------------------- запуск

def creds() -> Tuple[str, str]:
    env = club.read_env()
    return ((env.get("TELEGRAM_BOT_TOKEN") or "").strip(),
            (env.get("TELEGRAM_CHAT_ID") or "").strip())


def code_stamp():
    """Время изменения файлов слушателя: после обновления бота слушатель
    перезапускается сам."""
    out = []
    for f in ("tgcontrol.py", "club.py"):
        try:
            out.append(os.path.getmtime(os.path.join(club.APP_DIR, f)))
        except OSError:
            out.append(0.0)
    return tuple(out)


def listen() -> int:
    """`moneyclub --telegram`: слушать чат, пока Telegram настроен."""
    import fcntl
    os.chdir(club.APP_DIR)
    os.makedirs(club.LOG_DIR, exist_ok=True)
    lock = open(LOCK, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("уже работает другой слушатель")
        return 0
    token, chat = creds()
    if not token or not chat:
        log("Telegram не настроен — выхожу")
        return 0
    api = Api(token, chat)
    ctl = Control(api)
    try:
        api.set_commands()
    except Exception as e:
        log(f"set commands: {e!r}")
    try:
        api.skip_backlog()
    except Exception as e:
        log(f"skip backlog: {e!r}")
    log("слушаю Telegram")
    code = code_stamp()
    while True:
        if creds() != (token, chat):
            log("ключи Telegram изменились — перезапуск")
            return 0
        if code_stamp() != code:
            # бота обновили: перезапускаю себя с новым кодом
            log("код обновлён — перезапуск слушателя")
            lock.close()
            os.execv(club.PY, [club.PY, "club.py", "--telegram"])
        try:
            ctl.tick()
        except Exception:
            log("tick: " + traceback.format_exc())
        try:
            updates = api.updates(25)
        except Exception as e:
            log(f"poll: {e!r}")
            time.sleep(5)
            continue
        for u in updates:
            p = parse(u, chat)
            if p is None:
                continue
            kind, text, cb, msg_id = p
            api.msg_id = msg_id          # кнопка — меняем её сообщение
            if cb:
                try:
                    api.answer(cb)
                except Exception:
                    pass
            log(f"команда: {text[:60]}")
            try:
                ctl.handle(kind, text)
            except Exception as e:
                log("ошибка: " + traceback.format_exc())
                try:
                    api.send(f"✘ Ошибка: {e}", [[MENU_BTN]])
                except Exception:
                    pass


TMUX_SESSION_TG = club.TMUX_SESSION + "-tg"
TG_LOG = os.path.join(club.LOG_DIR, "telegram.log")


def listener_running() -> bool:
    return club.tmux("has-session", "-t", "=" + TMUX_SESSION_TG,
                     capture_output=True).returncode == 0


def ensure_listener(restart: bool = False) -> bool:
    """Поднять слушатель в фоне, если Telegram настроен. True — работает."""
    token, chat = creds()
    if restart and listener_running():
        club.tmux("kill-session", "-t", "=" + TMUX_SESSION_TG, capture_output=True)
    if not token or not chat:
        return False
    if listener_running():
        return True
    os.makedirs(club.LOG_DIR, exist_ok=True)
    club.trim_log(TG_LOG)
    q = club.shlex.quote
    cmd = (f"cd {q(club.APP_DIR)} && exec {q(club.PY)} club.py --telegram "
           f">>{q(TG_LOG)} 2>&1")
    r = club.tmux("new-session", "-d", "-s", TMUX_SESSION_TG, "-x", "120",
                  "-y", "30", cmd, capture_output=True, text=True)
    return r.returncode == 0
