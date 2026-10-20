"""Auto-tuning of the band (автоподбор полосы). OFF by default.

When the user turns it on, once every `every_hours` the bot replays the
last `days` of its recorded books (logs/ticks) through the backtester with
the band as it is, one step narrower and one step wider, and keeps the
best. Only thresholds.upper_bps / lower_bps move, by the same step on both
sides; the centre is the autonomy's job. Guards:

  * enough data: at least `min_hours` of recorded books in the window and
    at least MIN_TRADES simulated trades with the current band;
  * honest fills: every band is replayed with the fill 0.3, 0.6 and 0.9 s
    after the signal (backtest.LATENCIES) and judged by its WORST result;
  * a real difference: the better band must earn at least `min_gain_pct`
    more than the current one, and at least MIN_GAIN_USD, and be positive;
  * not one lucky day: it must also beat the current band on most of the
    recorded days (each day of at least MIN_DAY_HOURS, replayed alone);
  * small steps: STEP_BPS per run;
  * a leash: never further than `max_drift_bps` from the ANCHOR — the band
    the user last set by hand — and never narrower than MIN_SIDE_BPS.

The backtest's absolute result is optimistic (fills at the recorded $20
price); comparing bands with each other on the same data is what it is
good for, and all that is used here.

Every change is written to the pair file, to logs/autotune.csv and to the
log, and announced in Telegram. Setting the band by hand resets the anchor
and the timer.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

from . import backtest as bt
from . import yamledit

STEP_BPS = 1.0
MIN_SIDE_BPS = 1.5          # a side narrower than this is all noise
MIN_TRADES = 20
MIN_GAIN_USD = 0.05
MIN_DAY_HOURS = 6.0         # shorter days are not counted in the day vote


@dataclass
class Decision:
    upper: Optional[float]          # new band (None: leave as is)
    lower: Optional[float]
    results: List[Tuple[float, float, int, float]]   # (upper, lower, trades, pnl)
    hours: float                    # hours of recorded books used
    reason: str
    days_won: Optional[Tuple[int, int]] = None   # best vs current: (won, days)


def split_days(ticks: List[bt.Tick]) -> List[List[bt.Tick]]:
    """Ticks by UTC day, only days with at least MIN_DAY_HOURS of books."""
    days, cur, key = [], [], None
    for t in ticks:
        k = int(t.ts // 86400)
        if k != key and cur:
            days.append(cur)
            cur = []
        key = k
        cur.append(t)
    if cur:
        days.append(cur)
    return [d for d in days
            if len(d) > 1 and d[-1].ts - d[0].ts >= MIN_DAY_HOURS * 3600]


def worst_run(ticks: List[bt.Tick], p: bt.Params) -> Tuple[int, float]:
    """(trades at the middle latency, worst pnl over backtest.LATENCIES)."""
    runs = [bt.run(ticks, replace(p, latency_sec=lat)) for lat in bt.LATENCIES]
    return runs[len(runs) // 2].trades, min(r.pnl for r in runs)


def candidates(upper: float, lower: float, anchor_upper: float,
               anchor_lower: float, max_drift_bps: float) -> List[Tuple[float,
                                                                        float]]:
    """Current band first, then one step narrower and one wider — within
    the leash around the manual band."""
    out = [(upper, lower)]
    for d in (-STEP_BPS, STEP_BPS):
        u, lo = round(upper + d, 2), round(lower + d, 2)
        if min(u, lo) < MIN_SIDE_BPS:
            continue
        if abs(u - anchor_upper) > max_drift_bps + 1e-9 or \
                abs(lo - anchor_lower) > max_drift_bps + 1e-9:
            continue
        out.append((u, lo))
    return out


def decide(ticks: List[bt.Tick], base: bt.Params, anchor_upper: float,
           anchor_lower: float, *, max_drift_bps: float, min_hours: float,
           min_gain_pct: float) -> Decision:
    hours = (ticks[-1].ts - ticks[0].ts) / 3600.0 if len(ticks) > 1 else 0.0
    if hours < min_hours:
        return Decision(None, None, [], hours,
                        f"мало записанных стаканов: {hours:.0f} ч из "
                        f"{min_hours:g}")
    results = []
    for u, lo in candidates(base.upper_bps, base.lower_bps, anchor_upper,
                            anchor_lower, max_drift_bps):
        n, pnl = worst_run(ticks, replace(base, upper_bps=u, lower_bps=lo))
        results.append((u, lo, n, pnl))
    cur = results[0]
    if cur[2] < MIN_TRADES:
        return Decision(None, None, results, hours,
                        f"мало сделок в бэктесте: {cur[2]} из {MIN_TRADES}")
    best = max(results, key=lambda x: x[3])
    if best is cur:
        return Decision(None, None, results, hours, "текущая полоса лучшая")
    gain = best[3] - cur[3]
    if best[3] <= 0:
        return Decision(None, None, results, hours,
                        "ни один вариант не в плюсе")
    if gain < MIN_GAIN_USD or (cur[3] > 0 and
                               gain < cur[3] * min_gain_pct / 100.0):
        return Decision(None, None, results, hours,
                        f"выигрыш мал: +${gain:.2f}")
    days = split_days(ticks)
    if days:
        mid = replace(base, latency_sec=bt.LATENCIES[len(bt.LATENCIES) // 2])
        won = sum(
            bt.run(d, replace(mid, upper_bps=best[0], lower_bps=best[1])).pnl
            > bt.run(d, replace(mid, upper_bps=cur[0], lower_bps=cur[1])).pnl
            for d in days)
        if won * 2 <= len(days):
            return Decision(None, None, results, hours,
                            f"лучше лишь в {won} днях из {len(days)}",
                            (won, len(days)))
        return Decision(best[0], best[1], results, hours, "",
                        (won, len(days)))
    return Decision(best[0], best[1], results, hours, "")


def save(ticker_file: str, upper: float, lower: float, now: float) -> None:
    with open(ticker_file, encoding="utf-8") as fh:
        text = fh.read()
    text = yamledit.set_value(text, "thresholds", "upper_bps", f"{upper:g}")
    text = yamledit.set_value(text, "thresholds", "lower_bps", f"{lower:g}")
    text = yamledit.set_value(text, "ticker", "autotune_last_ts", f"{now:.0f}")
    tmp = ticker_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, ticker_file)


def mark_run(ticker_file: Optional[str], now: float) -> None:
    if not ticker_file:
        return
    with open(ticker_file, encoding="utf-8") as fh:
        text = fh.read()
    text = yamledit.set_value(text, "ticker", "autotune_last_ts", f"{now:.0f}")
    tmp = ticker_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, ticker_file)


def due(last_ts: Optional[float], every_hours: float, now: float) -> bool:
    return last_ts is None or now - last_ts >= every_hours * 3600.0
