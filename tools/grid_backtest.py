#!/usr/bin/env python3
"""Strategy 4 «Сетка» on the recorded book snapshots (logs/ticks): what the
grid would have earned, hour by hour and day by day. Nothing here changes
settings and nothing is sent to an exchange.

Бэктест сетки по записанным стаканам пары. Ничего не меняет и не торгует.

Model (the same rules as the engine):
  * centre = median of the mid premium over the last --hours (minute
    medians, past minutes only; needs one hour of data);
  * with n clips long Entropy: buy the next clip when Entropy ask over
    hedge bid (VWAP for the recorded notional) ≤ centre − (n+1)·step,
    sell one when Entropy bid over hedge ask ≥ centre − (n−1)·step;
  * the signal must still hold on the next snapshot, which is the fill;
  * Entropy taker fee per trade (rebated later in real life, not here);
  * at most --clips clips each way (the position cap);
  * result marked to market (what closing everything would give).
Not modelled: latency beyond one snapshot, misses, funding.

Usage:
    python3 tools/grid_backtest.py --symbol ANTH --hedge-venue lighter-rh
    python3 tools/grid_backtest.py ... --step 3 4 5 --hours 24 --clips 5
"""
from __future__ import annotations

import argparse
import bisect
import os
import statistics
import sys
import time
from typing import Dict, List, Sequence, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

GAP_SEC = 30.0          # a longer gap between snapshots: no fill across it
MIN_CENTRE_MIN = 60     # minutes of data before the centre is used

Row = Tuple[float, float, float, float]   # ts, buy prem, sell prem, mid prem


def rows_from_ticks(ticks) -> List[Row]:
    out = []
    for t in ticks:
        out.append((t.ts, (t.e_buy / t.h_sell - 1.0) * 1e4,
                    (t.e_sell / t.h_buy - 1.0) * 1e4,
                    ((t.e_bid + t.e_ask) / (t.h_bid + t.h_ask) - 1.0) * 1e4))
    return out


def centres(rows: Sequence[Row], hours: float) -> List[float]:
    """Centre for every row: median of the minute medians of the window
    before this minute (nan while there is less than an hour)."""
    by_min: Dict[int, List[float]] = {}
    for ts, _b, _s, m in rows:
        by_min.setdefault(int(ts // 60), []).append(m)
    mins = sorted(by_min)
    med = [statistics.median(by_min[m]) for m in mins]
    k = int(hours * 60)
    need = min(MIN_CENTRE_MIN, k)
    cmap: Dict[int, float] = {}
    for i, m in enumerate(mins):
        lo = bisect.bisect_left(mins, m - k)
        cmap[m] = (statistics.median(med[lo:i]) if i - lo >= need
                   else float("nan"))
    return [cmap[int(ts // 60)] for ts, _b, _s, _m in rows]


def simulate(rows: Sequence[Row], step: float, hours: float, clips: int,
             clip_usd: float, fee_bps: float) -> dict:
    c_all = centres(rows, hours)
    n = 0
    cash = 0.0             # bps of one clip
    trades = 0
    max_clips = 0
    hourly: Dict[int, float] = {}
    for i in range(len(rows) - 1):
        c = c_all[i]
        ts, pb, ps, _m = rows[i]
        nts, npb, nps, _nm = rows[i + 1]
        if c != c or nts - ts > GAP_SEC:
            continue
        buy_at = c - (n + 1) * step
        sell_at = c - (n - 1) * step
        if n < clips and pb <= buy_at and npb <= buy_at:
            cash -= npb + fee_bps
            n += 1
            trades += 1
        elif n > -clips and ps >= sell_at and nps >= sell_at:
            cash += nps - fee_bps
            n -= 1
            trades += 1
        max_clips = max(max_clips, abs(n))
        mark = n * (nps if n > 0 else npb)
        hourly[int(ts // 3600)] = (cash + mark) * clip_usd / 1e4
    hs = sorted(hourly)
    eq = [hourly[h] for h in hs]
    deltas = [b - a for a, b in zip([0.0] + eq[:-1], eq)]
    peak, dd = 0.0, 0.0
    for v in eq:
        peak = max(peak, v)
        dd = max(dd, peak - v)
    days: Dict[str, float] = {}
    prev = 0.0
    for h, v in zip(hs, eq):
        d = time.strftime("%Y-%m-%d", time.gmtime(h * 3600))
        days[d] = days.get(d, 0.0) + v - prev
        prev = v
    return {"pnl": eq[-1] if eq else 0.0, "trades": trades,
            "volume": trades * clip_usd * 2, "hours": len(hs),
            "plus_hours": (sum(1 for d in deltas if d > 0) / len(deltas)
                           if deltas else 0.0),
            "worst_hour": min(deltas) if deltas else 0.0,
            "max_dd": dd, "max_clips": max_clips, "open_clips": n,
            "days": days}


def main() -> None:
    from entropy_arb import backtest as bt
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--hedge-venue", default="lighter-rh")
    ap.add_argument("--ticks-dir", default="logs/ticks")
    ap.add_argument("--days", type=float, default=0.0,
                    help="last N days only (0 = all files)")
    ap.add_argument("--step", type=float, nargs="+", default=[3, 4, 5, 6])
    ap.add_argument("--hours", type=float, nargs="+", default=[24])
    ap.add_argument("--clips", type=int, default=5,
                    help="position cap in clips (лимит позиции / порция)")
    ap.add_argument("--clip-usd", type=float, default=20.0,
                    help="clip = sizing.max_order_notional_usd")
    ap.add_argument("--fee-bps", type=float, default=0.86,
                    help="Entropy taker fee per trade")
    a = ap.parse_args()
    ticks = bt.load_ticks(a.ticks_dir, a.hedge_venue, a.symbol, a.days)
    if len(ticks) < 1000:
        sys.exit(f"мало данных: {len(ticks)} снимков в {a.ticks_dir}")
    rows = rows_from_ticks(ticks)
    span = (rows[-1][0] - rows[0][0]) / 3600
    print(f"{a.symbol}: {len(rows)} снимков, {span:.0f} ч · порция "
          f"${a.clip_usd:g}, до {a.clips} порций, комиссия {a.fee_bps:g} bps")
    print(f"{'шаг':>4} {'центр':>6} {'итог $':>8} {'сделок':>7} "
          f"{'объём $':>9} {'+часов':>7} {'худший ч':>9} {'просадка':>9}")
    best = None
    for h in a.hours:
        for s in a.step:
            r = simulate(rows, s, h, a.clips, a.clip_usd, a.fee_bps)
            print(f"{s:>4g} {h:>5g}ч {r['pnl']:>8.2f} {r['trades']:>7d} "
                  f"{r['volume']:>9.0f} {r['plus_hours']:>6.0%} "
                  f"{r['worst_hour']:>9.2f} {r['max_dd']:>9.2f}")
            if best is None or r["pnl"] > best[2]["pnl"]:
                best = (s, h, r)
    s, h, r = best
    print(f"\nПо дням (шаг {s:g}, центр {h:g} ч):")
    for d, v in sorted(r["days"].items()):
        print(f"  {d}  {v:+.2f} $")
    print("\nЭто история, не обещание: прошлые колебания премии могут не "
          "повториться.")


if __name__ == "__main__":
    main()
