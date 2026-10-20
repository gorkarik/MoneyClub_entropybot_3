"""Unattended running: alerts, timed pauses of new positions, and the
checks that set them.

* Alerts: one Telegram message per event, the same event at most once per
  ALERT_REPEAT_SEC, so a flapping venue does not flood the chat.
* Pauses: named reasons that hold NEW positions (closing toward the centre
  always continues), each until a time or until cleared:
    - "manual"    — /pause in Telegram, until /resume;
    - "lowprofit" — the last `hours` of trades lost money in sum;
    - "drawdown"  — equity fell more than `pct` below its 24 h peak.
* Error pause (in the engine): after a run of execution errors trading
  stops for `error_pause_min` minutes, and only after `pauses_before_halt`
  such pauses in a row does it halt for good.

Everything here is pure bookkeeping; the engine decides what to do.
"""
from __future__ import annotations

import csv
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

ALERT_REPEAT_SEC = 600.0
REASON_TEXT = {
    "manual": "пауза по команде /pause",
    "lowprofit": "сделки последних часов в минусе",
    "drawdown": "просадка баланса за сутки",
}


class Alerts:
    def __init__(self, repeat_sec: float = ALERT_REPEAT_SEC) -> None:
        self.repeat = repeat_sec
        self._last: Dict[str, float] = {}

    def due(self, key: str, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        last = self._last.get(key)
        if last is not None and now - last < self.repeat:
            return False
        self._last[key] = now
        return True


class Pauses:
    """Reasons that hold new positions. until=None: until cleared."""

    def __init__(self) -> None:
        self._until: Dict[str, Optional[float]] = {}

    def set(self, reason: str, until: Optional[float] = None) -> None:
        self._until[reason] = until

    def clear(self, reason: Optional[str] = None) -> List[str]:
        if reason is None:
            gone = list(self._until)
            self._until.clear()
            return gone
        return [reason] if self._until.pop(reason, 0) != 0 else []

    def active(self, now: Optional[float] = None) -> List[str]:
        now = time.time() if now is None else now
        for r, u in list(self._until.items()):
            if u is not None and now >= u:
                del self._until[r]
        return sorted(self._until)

    def until(self, reason: str) -> Optional[float]:
        return self._until.get(reason)

    def text(self, now: Optional[float] = None) -> str:
        now = time.time() if now is None else now
        out = []
        for r in self.active(now):
            u = self._until[r]
            t = REASON_TEXT.get(r, r)
            if u is not None:
                t += f" (ещё {max(0, u - now) / 60:.0f} мин)"
            out.append(t)
        return "; ".join(out)


class LowProfit:
    """Sum of fill edges (fees and slippage included) of the trades in the
    last `hours`. Negative over at least `min_trades` trades: the market
    pays less than the bot spends, stop opening for a while.

    `loss_bps`: a loss smaller than this many bps of the window's turnover
    is noise, not a loss (a volume bot sits at about zero by design)."""

    def __init__(self, hours: float, min_trades: int,
                 loss_bps: float = 0.0) -> None:
        self.hours = hours
        self.min_trades = min_trades
        self.loss_bps = loss_bps
        self._rows: Deque[Tuple[float, float, float]] = deque()

    def add(self, ts: float, fill_edge: float, notional: float = 0.0) -> None:
        self._rows.append((ts, fill_edge, notional))

    def window(self, now: float) -> Tuple[int, float]:
        cut = now - self.hours * 3600.0
        while self._rows and self._rows[0][0] < cut:
            self._rows.popleft()
        return len(self._rows), sum(r[1] for r in self._rows)

    def losing(self, now: float) -> Optional[Tuple[int, float]]:
        if self.hours <= 0:
            return None
        n, s = self.window(now)
        turnover = sum(r[2] for r in self._rows)
        if n >= self.min_trades and s < -self.loss_bps / 1e4 * turnover:
            return n, s
        return None

    def reset(self) -> None:
        self._rows.clear()


def equity_peak(path: str, now: float, hours: float = 24.0) -> Optional[float]:
    """Highest total equity in equity.csv over the last `hours` (all
    sessions: a restart does not reset it). None if no rows."""
    cut = now - hours * 3600.0
    peak = None
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = float(row.get("ts") or 0)
                    tot = float(row.get("total_equity") or "nan")
                except ValueError:
                    continue
                if ts < cut or tot != tot:
                    continue
                peak = tot if peak is None else max(peak, tot)
    except FileNotFoundError:
        return None
    return peak


def drawdown_pct(peak: Optional[float], total: Optional[float]) -> Optional[float]:
    if peak is None or total is None or peak <= 0:
        return None
    return max(0.0, (peak - total) / peak * 100.0)
