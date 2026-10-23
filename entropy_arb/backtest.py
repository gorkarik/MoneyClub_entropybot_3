"""Backtest: replay recorded book snapshots (logs/ticks) through the entry
rules with candidate settings, and see what they would have done.

What is modelled, the same way the engine does it:
  * the band: centre + upper to sell Entropy, centre − lower to buy it
    (in premium terms; the buy side is turned around exactly, as the engine),
    both venues' taker fees on top (book.plan_arb's inequality);
  * autonomy: floating centre (median of the premium sampled every 10 s),
    exit relief for trades that reduce the position, opening cooldown and
    the jump pause;
  * strategy 3 narrowing, the inventory ladder, the position cap, the trade
    cooldown; closing trades never exceed the position.

  * latency (`latency_sec`): the signal is decided on one snapshot and
    filled at the first snapshot `latency_sec` later, at that snapshot's
    prices; strategies 2/3 miss (no trade) when Entropy has moved past the
    order's price limit by then; nothing else fires while an order travels;
  * pullback entry, the fast-market band, the Entropy limit from profit.

What is not: depth beyond the $20 the snapshot measured (every fill is at
the recorded $20 VWAP), the race between the two legs. The execution loss
the bot really has is charged per trade as `slip_bps` of the Entropy
notional — take it from the bot's own trades.

No look-ahead: whether a signal fires is decided only from snapshots up to
the signal; later snapshots only price its fill (tests check this).

Бэктест: прогон записанных стаканов через правила входа с другими
настройками. Результат — оценка, не обещание.
"""
from __future__ import annotations

import csv
import glob
import gzip
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Iterable, Iterator, List, Optional

from . import autonomy, holding
from .autonomy import AutonomyConf, Centre
from .book import reverse_bps
from .strategy import volume_narrow_bps

DEFAULT_SLIP_BPS = 1.0     # execution loss when the pair has too few trades
LATENCIES = (0.3, 0.6, 0.9)   # seconds from signal to fill tried by autotune
VOL_WINDOW_SEC = 60.0         # as the engine's fast-market band
MAX_AGE_MS = 3000.0        # a side older than this is stale: tick skipped
GAP_SEC = 60.0             # longer gaps (bot off) are not counted as time


@dataclass(frozen=True)
class Tick:
    ts: float
    e_bid: float
    e_ask: float
    h_bid: float
    h_ask: float
    e_buy: float           # $20 VWAP buying on Entropy
    e_sell: float
    h_buy: float
    h_sell: float


@dataclass(frozen=True)
class Params:
    midline_bps: float
    upper_bps: float
    lower_bps: float
    entropy_fee_bps: float = 0.0
    hedge_fee_bps: float = 0.0
    order_usd: float = 20.0
    min_order_usd: float = 10.0
    cap_usd: float = 150.0
    cooldown_sec: float = 0.0
    inventory_scale_bps: float = 0.0
    inventory_floor_frac: float = 0.5
    mode: str = "simultaneous"
    slip_bps: float = 0.0
    auto: AutonomyConf = field(default_factory=AutonomyConf)
    latency_sec: float = 0.0
    retrace_bps: float = 0.0
    vol_k: float = 0.0
    ef_limit: str = "fixed"         # strategies 2/3: profit | fixed
    ef_slip_bps: float = 5.0
    ef_keep: float = 0.5


@dataclass
class Result:
    trades: int = 0
    opens: int = 0
    closes: int = 0
    e_volume: float = 0.0
    trades_usd: float = 0.0        # sum of fill edges (fees, slip included)
    final_usd: float = 0.0         # closing what is left at the last tick
    hours: float = 0.0
    hours_at_cap: float = 0.0
    max_pos_usd: float = 0.0
    jump_pauses: int = 0
    misses: int = 0                # strategies 2/3: Entropy moved past the limit
    rows: List[dict] = field(default_factory=list)

    @property
    def pnl(self) -> float:
        return self.trades_usd + self.final_usd

    @property
    def pnl_bps(self) -> Optional[float]:
        return self.pnl / self.e_volume * 1e4 if self.e_volume > 0 else None

    def hold(self) -> holding.Holding:
        return holding.holding(self.rows)


def _f(x) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def tick_files(ticks_dir: str, venue: str, symbol: str) -> List[str]:
    pat = os.path.join(ticks_dir, f"{venue}_{symbol}_*.csv*")
    return sorted(glob.glob(pat))


def read_ticks(paths: Iterable[str], since: float = 0.0) -> Iterator[Tick]:
    for p in paths:
        opener = gzip.open if p.endswith(".gz") else open
        try:
            with opener(p, "rt", newline="") as fh:
                for r in csv.DictReader(fh):
                    ts = _f(r.get("ts"))
                    if ts is None or ts < since:
                        continue
                    if (_f(r.get("e_age_ms")) or 0) > MAX_AGE_MS or \
                            (_f(r.get("h_age_ms")) or 0) > MAX_AGE_MS:
                        continue
                    v = [_f(r.get(k)) for k in (
                        "e_bid", "e_ask", "h_bid", "h_ask", "e_buy_px",
                        "e_sell_px", "h_buy_px", "h_sell_px")]
                    if None in v:
                        continue
                    yield Tick(ts, *v)
        except (OSError, EOFError):
            continue


def load_ticks(ticks_dir: str, venue: str, symbol: str,
               days: float = 0.0) -> List[Tick]:
    since = time.time() - days * 86400 if days > 0 else 0.0
    ticks = list(read_ticks(tick_files(ticks_dir, venue, symbol), since))
    ticks.sort(key=lambda t: t.ts)
    return ticks


def _row(ts: float, ent_buy: bool, qty: float, fill: float) -> dict:
    """A trades.csv-like row, so holding.py can read the simulated trades."""
    return {"ts": str(ts), "session_id": "bt", "ok": "1",
            "buy_venue": "ENTROPY" if ent_buy else "HEDGE",
            "sell_venue": "HEDGE" if ent_buy else "ENTROPY",
            "buy_fill": str(qty), "sell_fill": str(qty),
            "fill_edge_usd": str(fill)}


def _fill_index(ticks: List[Tick], i: int, latency: float) -> Optional[int]:
    """Index of the snapshot the signal at i fills at: the first one at
    least `latency` seconds later (i itself with no latency). None when the
    recording stops or has a gap first."""
    if latency <= 0:
        return i
    t0 = ticks[i].ts
    for j in range(i + 1, len(ticks)):
        if ticks[j].ts - ticks[j - 1].ts > GAP_SEC:
            return None
        if ticks[j].ts >= t0 + latency:
            return j
    return None


def run(ticks: List[Tick], p: Params) -> Result:
    res = Result()
    auto = p.auto
    centre = Centre(auto, manual=p.midline_bps) if auto.enabled else None
    narrow = (volume_narrow_bps(p.upper_bps, p.lower_bps)
              if p.mode == "volume" else 0.0)
    ef = p.mode in ("entropy_first", "volume")
    fe, fh = p.entropy_fee_bps / 1e4, p.hedge_fee_bps / 1e4
    fees_bps = p.entropy_fee_bps + p.hedge_fee_bps
    pos = 0.0                       # Entropy qty, hedge holds −pos
    last_sample = last_trade = last_open = -1e18
    busy_until = -1e18              # an order is travelling until then
    open_ts: Optional[float] = None
    jump_on = False
    prev: Optional[Tick] = None
    peak = {True: None, False: None}     # pullback entry, per direction
    vol_px: deque = deque()
    for i, t in enumerate(ticks):
        ref = (t.e_bid + t.e_ask) / 2.0
        h_mid = (t.h_bid + t.h_ask) / 2.0
        pos_usd = pos * ref
        if prev is not None:
            dt = t.ts - prev.ts
            if dt <= GAP_SEC:
                res.hours += dt / 3600.0
                if p.cap_usd - abs(pos_usd) < p.min_order_usd:
                    res.hours_at_cap += dt / 3600.0
        prev = t
        if centre is not None and t.ts - last_sample >= autonomy.SAMPLE_SEC:
            centre.add(t.ts, (ref / h_mid - 1.0) * 1e4)
            last_sample = t.ts
        vol = 0.0
        if p.vol_k > 0:
            vol_px.append((t.ts, ref))
            while vol_px[0][0] < t.ts - VOL_WINDOW_SEC:
                vol_px.popleft()
            pxs = [x for _, x in vol_px]
            vol = (max(pxs) - min(pxs)) / ref * 1e4
        if t.ts < busy_until or t.ts - last_trade < p.cooldown_sec:
            continue
        c = centre.live if centre is not None and centre.live is not None \
            else p.midline_bps
        u = min(abs(pos_usd) / p.cap_usd, 1.0) if p.cap_usd > 0 else 1.0
        age = t.ts - open_ts if open_ts is not None else None
        jumped = centre is not None and centre.jumped()
        if jumped and not jump_on:
            res.jump_pauses += 1
        jump_on = jumped

        for sell_entropy in (True, False):
            opens = pos <= 0 if sell_entropy else pos >= 0
            band = p.upper_bps if sell_entropy else p.lower_bps
            # distance from the centre the premium must reach (as the engine)
            dist = band
            if auto.enabled and not opens:
                dist -= autonomy.exit_relief_bps(auto, band, age, u)
            dist -= narrow
            if opens and p.inventory_scale_bps > 0:
                floor = min(max(p.inventory_floor_frac, 0.0), 0.99)
                if u > floor:
                    dist += p.inventory_scale_bps * (u - floor) / (1 - floor)
            if opens:
                dist += p.vol_k * vol
            # buying Entropy is checked as the hedge over Entropy: the level
            # is turned around exactly (reverse_bps), not by a sign
            hurdle = (c + dist) if sell_entropy else reverse_bps(c - dist)
            if sell_entropy:
                ok = t.e_sell * (1 - fe) >= t.h_buy * (1 + fh) * \
                    (1 + hurdle / 1e4)
                prem = (t.e_sell / t.h_buy - 1.0) * 1e4
            else:
                ok = t.h_sell * (1 - fh) >= t.e_buy * (1 + fe) * \
                    (1 + hurdle / 1e4)
                prem = (t.h_sell / t.e_buy - 1.0) * 1e4
            if not ok:
                peak[sell_entropy] = None
                continue
            if opens and p.retrace_bps > 0:
                pk = peak[sell_entropy]
                pk = prem if pk is None else max(pk, prem)
                peak[sell_entropy] = pk
                if prem > pk - p.retrace_bps:
                    continue
            if opens:
                if auto.enabled and (t.ts - last_open < auto.open_cooldown_sec
                                     or jumped):
                    continue
                notional = min(p.order_usd, p.cap_usd - abs(pos_usd))
                if notional < p.min_order_usd:
                    continue
                qty = notional / ref
            else:            # never more than the position: no flip
                qty = min(p.order_usd / ref, abs(pos))
                if (abs(pos) - qty) * ref < p.min_order_usd:
                    qty = abs(pos)   # no remainder below the minimum order
                if qty <= 0:
                    continue
                notional = qty * ref
            j = _fill_index(ticks, i, p.latency_sec)
            if j is None:
                continue
            f = ticks[j]
            peak[sell_entropy] = None
            if ef:
                lim = p.ef_slip_bps
                if p.ef_limit == "profit":
                    band_eff = dist
                    spare = max(0.0, prem - hurdle - fees_bps)
                    lim = min(max(spare + max(0.0, band_eff)
                                  * (1.0 - p.ef_keep), 0.5),
                              3.0 * p.ef_slip_bps)
                missed = (f.e_sell < t.e_sell * (1 - lim / 1e4)
                          if sell_entropy else
                          f.e_buy > t.e_buy * (1 + lim / 1e4))
                if missed:
                    res.misses += 1
                    busy_until = f.ts if j > i else busy_until
                    break
            if sell_entropy:
                fill = qty * (f.e_sell * (1 - fe) - f.h_buy * (1 + fh))
                pos -= qty
            else:
                fill = qty * (f.h_sell * (1 - fh) - f.e_buy * (1 + fe))
                pos += qty
            if abs(pos) * ref < 1e-6:
                pos = 0.0    # float dust after a full close
            fill -= notional * p.slip_bps / 1e4
            res.trades += 1
            res.opens += opens
            res.closes += not opens
            res.e_volume += notional
            res.trades_usd += fill
            res.rows.append(_row(f.ts, not sell_entropy, qty, fill))
            last_trade = f.ts
            if j > i:
                busy_until = f.ts
            if opens:
                last_open = f.ts
            was_flat = abs(pos_usd) < 1.0
            pos_usd = pos * ref
            res.max_pos_usd = max(res.max_pos_usd, abs(pos_usd))
            if abs(pos_usd) < 1.0:
                open_ts = None
            elif was_flat:
                open_ts = f.ts
            break
    if prev is not None and abs(pos) > 0:
        t = prev
        if pos > 0:          # sell Entropy, buy back the hedge
            res.final_usd = pos * (t.e_sell * (1 - fe) - t.h_buy * (1 + fh))
        else:
            res.final_usd = -pos * (t.h_sell * (1 - fh) - t.e_buy * (1 + fe))
        res.rows.append(_row(t.ts, pos < 0, abs(pos), res.final_usd))
    return res


def params_from_config(cfg) -> Params:
    """Params of a loaded Config (the pair's current settings)."""
    return Params(
        midline_bps=cfg.midline_bps, upper_bps=cfg.upper_bps,
        lower_bps=cfg.lower_bps, entropy_fee_bps=cfg.entropy.fee_bps,
        hedge_fee_bps=cfg.hedge.fee_bps,
        order_usd=cfg.max_order_notional,
        min_order_usd=cfg.min_order_notional,
        cap_usd=min(cfg.entropy.cap_usd, cfg.hedge.cap_usd),
        cooldown_sec=cfg.cooldown_sec,
        inventory_scale_bps=cfg.inventory_scale_bps,
        inventory_floor_frac=cfg.inventory_floor_frac,
        mode=cfg.exec_mode, auto=cfg.autonomy,
        latency_sec=LATENCIES[1],
        retrace_bps=getattr(cfg, "retrace_bps", 0.0),
        vol_k=getattr(cfg, "vol_k", 0.0),
        ef_limit=getattr(cfg, "entropy_first_limit", "fixed"),
        ef_slip_bps=getattr(cfg, "entropy_first_slip_bps", 5.0),
        ef_keep=getattr(cfg, "entropy_first_keep", 0.5))


def variants(base: Params) -> List[tuple]:
    """(title, params) of the comparison the menu shows."""
    out = [("Текущие настройки", base)]
    if base.auto.enabled:
        out.append(("Без автономности", replace(
            base, auto=replace(base.auto, enabled=False))))
        for w in (30.0, 120.0):
            if w != base.auto.window_min:
                a = replace(base.auto, window_min=w,
                            min_min=min(base.auto.min_min, w))
                out.append((f"Центр по {w:g} мин", replace(base, auto=a)))
    else:
        out.append(("С автономностью", replace(
            base, auto=replace(base.auto, enabled=True))))
    out.append(("Полоса шире на 1 bps", replace(
        base, upper_bps=base.upper_bps + 1, lower_bps=base.lower_bps + 1)))
    if min(base.upper_bps, base.lower_bps) > 1.5:
        out.append(("Полоса уже на 1 bps", replace(
            base, upper_bps=base.upper_bps - 1, lower_bps=base.lower_bps - 1)))
    for lat in LATENCIES:
        if lat != base.latency_sec:
            out.append((f"Задержка {lat:g} с", replace(base, latency_sec=lat)))
    if base.retrace_bps > 0:
        out.append(("Без входа на откате", replace(base, retrace_bps=0.0)))
    else:
        out.append(("Вход на откате 1 bps", replace(base, retrace_bps=1.0)))
    if base.vol_k > 0:
        out.append(("Без поправки на быстрый рынок", replace(base, vol_k=0.0)))
    else:
        out.append(("Быстрый рынок ×0.5", replace(base, vol_k=0.5)))
    if base.mode in ("entropy_first", "volume"):
        other_lim = "fixed" if base.ef_limit == "profit" else "profit"
        out.append(("Цена Entropy " + ("фиксированная" if other_lim == "fixed"
                                       else "из прибыли"),
                    replace(base, ef_limit=other_lim)))
    other = "volume" if base.mode != "volume" else "entropy_first"
    out.append(("Стратегия 3 (полоса уже)" if other == "volume"
                else "Без сужения стратегии 3", replace(base, mode=other)))
    return out


def measured_slip(trades_csv: str, symbol: str, venue: str,
                  days: float = 7.0):
    """(average execution loss per trade, bps of notional; trades) from the
    bot's own trades of this pair: plan − fact. None when too few."""
    since = time.time() - days * 86400
    vals = []
    try:
        with open(trades_csv, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("ok") != "1" or (r.get("symbol") or "") != symbol or \
                        (r.get("hedge_venue") or "") != venue:
                    continue
                try:
                    if float(r["ts"]) < since:
                        continue
                    exp = float(r.get("exp_edge_usd") or 0)
                    fill = float(r.get("fill_edge_usd") or 0)
                    avg = (float(r.get("buy_notional") or 0) +
                           float(r.get("sell_notional") or 0)) / 2
                except (KeyError, ValueError):
                    continue
                if avg > 0 and float(r.get("buy_fill") or 0) > 0 and \
                        float(r.get("sell_fill") or 0) > 0:
                    vals.append((exp - fill) / avg * 1e4)
    except FileNotFoundError:
        pass
    if len(vals) < 10:
        return None, len(vals)
    return max(sum(vals) / len(vals), 0.0), len(vals)
