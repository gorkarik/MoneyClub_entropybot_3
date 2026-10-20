"""Autonomy: keep the bot trading without manual re-centering (ON by default).

Three rules, all settings in config.yaml `autonomy:`.

  * Floating centre. The engine samples the mid-to-mid premium every
    SAMPLE_SEC and uses the median of the last `window_min` minutes as the
    live centre of the band, also while a position is open. It never moves
    further than `max_drift_bps` from the midline set in the pair file (a
    leash against bad data). The pair file is never written: the live centre
    exists only while the bot runs. With too little data (`min_min` minutes)
    the pair's midline is used.

  * Exit relief. The hurdle of a trade that REDUCES the position falls from
    the full band (centre + upper / centre − lower) toward
    centre ± `exit_floor_bps` as the position ages (linear to `exit_hours`)
    and, up to `size_weight` of the way, as it fills the second half of
    the cap. A position the
    market left behind is closed near break-even instead of being held for
    days at the cap.

  * Calm opening. Trades that ADD to the position are spaced by at least
    `open_cooldown_sec`, and paused while the premium of the last
    `jump_window_min` minutes is more than `jump_bps` away from the centre
    (a level shift: the centre has not caught up yet). Closing trades are
    never held back.
"""
from __future__ import annotations

import csv
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple

SAMPLE_SEC = 10.0          # how often the engine samples the premium


@dataclass(frozen=True)
class AutonomyConf:
    enabled: bool = True
    window_min: float = 60.0
    min_min: float = 20.0
    max_drift_bps: float = 50.0
    exit_hours: float = 6.0
    exit_floor_bps: float = 0.0
    size_weight: float = 0.5
    open_cooldown_sec: float = 30.0
    jump_bps: float = 20.0
    jump_window_min: float = 5.0
    # the centre may not leave the pair midline by more than max_drift_bps.
    # If the market stays outside that for recentre_min minutes, the pair
    # midline itself is moved to the market median (0 = never)
    recentre_min: float = 30.0


def median(vals: List[float]) -> float:
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


class Centre:
    """Rolling premium samples → live centre and level-shift detector."""

    def __init__(self, conf: AutonomyConf, manual: float) -> None:
        self.conf = conf
        self.manual = manual               # pair-file midline (the leash)
        self.samples: Deque[Tuple[float, float]] = deque()
        self.live: Optional[float] = None  # None: not enough data yet
        self.recent: Optional[float] = None
        self.at_leash = False

    def seed(self, rows: List[Tuple[float, float]]) -> None:
        """Older (ts, premium) rows, e.g. minutes of the pair file."""
        for ts, p in sorted(rows):
            self.samples.append((ts, p))

    def add(self, ts: float, premium: float) -> None:
        self.samples.append((ts, premium))
        self.update(ts)

    def update(self, now: float) -> None:
        c = self.conf
        cut = now - c.window_min * 60.0
        while self.samples and self.samples[0][0] < cut:
            self.samples.popleft()
        if not self.samples or \
                now - self.samples[0][0] < c.min_min * 60.0 - SAMPLE_SEC:
            self.live = self.recent = None
            self.at_leash = False
            return
        med = median([p for _, p in self.samples])
        lo, hi = self.manual - c.max_drift_bps, self.manual + c.max_drift_bps
        self.at_leash = not lo <= med <= hi
        self.live = round(max(lo, min(hi, med)), 2)
        rcut = now - c.jump_window_min * 60.0
        recent = [p for t, p in self.samples if t >= rcut]
        self.recent = median(recent) if recent else None

    def median_now(self) -> Optional[float]:
        """Unclamped median of the window (None: not enough data)."""
        if self.live is None or not self.samples:
            return None
        return median([p for _, p in self.samples])

    def jumped(self) -> bool:
        if self.live is None or self.recent is None or self.conf.jump_bps <= 0:
            return False
        return abs(self.recent - self.live) > self.conf.jump_bps


def seed_rows(csv_path: str, window_min: float, now: float,
              min_samples: int = 10) -> List[Tuple[float, float]]:
    """(ts, premium) of the pair's recorded minutes inside the window, so a
    restart does not wait `min_min` minutes for the centre."""
    cut = now - window_min * 60.0
    out: List[Tuple[float, float]] = []
    try:
        with open(csv_path, newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    ts = float(r["minute_ts"]) + 30.0     # mid of the minute
                    if ts < cut or ts > now:
                        continue
                    if int(r.get("samples") or 0) < min_samples:
                        continue
                    out.append((ts, float(r["premium_mean_bps"])))
                except (KeyError, ValueError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    return out


def exit_fraction(conf: AutonomyConf, age_sec: Optional[float],
                  cap_frac: float) -> float:
    """0..1: how far a reducing trade's hurdle moves from the full band
    toward the exit floor."""
    f_age = 0.0
    if age_sec is not None and conf.exit_hours > 0:
        f_age = min(max(age_sec, 0.0) / (conf.exit_hours * 3600.0), 1.0)
    # the size part starts at half the cap, like the inventory ladder
    u = min(max(cap_frac, 0.0), 1.0)
    f_size = conf.size_weight * max(0.0, (u - 0.5) / 0.5)
    return max(f_age, f_size)


def exit_relief_bps(conf: AutonomyConf, band_bps: float, age_sec: Optional[float],
                    cap_frac: float) -> float:
    """How many bps to take off a reducing trade's hurdle (never below
    the floor: relief ≤ band − floor)."""
    room = band_bps - conf.exit_floor_bps
    if room <= 0:
        return 0.0
    return room * exit_fraction(conf, age_sec, cap_frac)


# ------------------------------------------------------------ auto corridor

@dataclass(frozen=True)
class AutoBandConf:
    """Auto corridor (ON by default): the entry thresholds follow the
    market instead of fixed bands. See AutoBand."""
    enabled: bool = True
    window_min: float = 30.0     # look-back of the executable premiums
    min_min: float = 10.0        # less data: the pair's own bands are used
    share: float = 0.3           # trade in the best `share` of the window
    # what a round trip may cost: $ per $10 000 of Entropy volume (a round
    # trip of notional N makes 2N of Entropy volume, so the limit on the
    # sum of the two thresholds is −2 × max_cost_usd bps of N). Below 0 a
    # round trip must EARN that much: −0.5 → at least +1 bps
    max_cost_usd: float = 0.0


def quantile(vals: List[float], q: float) -> float:
    """Linear-interpolated quantile, q in 0..1 (vals non-empty)."""
    s = sorted(vals)
    if len(s) == 1:
        return s[0]
    pos = min(max(q, 0.0), 1.0) * (len(s) - 1)
    i = int(pos)
    j = min(i + 1, len(s) - 1)
    return s[i] + (s[j] - s[i]) * (pos - i)


class AutoBand:
    """Thresholds from what the market actually offered lately.

    Every SAMPLE_SEC the engine adds the two executable premiums net of
    both taker fees (bps): sell = selling Entropy against the hedge
    (Entropy bid over hedge ask), buy = buying Entropy (hedge bid over
    Entropy ask). The threshold of a direction is the (1 − share) quantile
    of its own samples: the bot trades in the best `share` of the recent
    window on each side, whatever the level of the premium. So it never
    stands for hours after the premium has moved: within minutes the
    corridor sits where the premium is now.

    A round trip at both thresholds earns about sell + buy (minus
    slippage). When that is below −2 × max_cost_usd, both thresholds are
    raised by the same amount until it is not: more volume, but never
    more expensive than allowed."""

    def __init__(self, conf: AutoBandConf) -> None:
        self.conf = conf
        self.samples: Deque[Tuple[float, float, float]] = deque()
        self.sell: Optional[float] = None    # threshold, bps (None: no data)
        self.buy: Optional[float] = None
        self.raised = 0.0                    # added to each by the cost cap
        self.slip_rt = 0.0                   # slippage counted per round trip

    def add(self, ts: float, sell_net: float, buy_net: float,
            slip_rt: float = 0.0) -> None:
        self.samples.append((ts, sell_net, buy_net))
        self.slip_rt = max(0.0, slip_rt)
        self.update(ts)

    def ready(self) -> bool:
        return self.sell is not None and self.buy is not None

    def update(self, now: float) -> None:
        c = self.conf
        cut = now - c.window_min * 60.0
        while self.samples and self.samples[0][0] < cut:
            self.samples.popleft()
        if not self.samples or \
                now - self.samples[0][0] < c.min_min * 60.0 - SAMPLE_SEC:
            self.sell = self.buy = None
            self.raised = 0.0
            return
        q = 1.0 - min(max(c.share, 0.01), 0.5)
        s = quantile([x for _, x, _ in self.samples], q)
        b = quantile([y for _, _, y in self.samples], q)
        floor = -2.0 * c.max_cost_usd
        rt = s + b - self.slip_rt
        self.raised = max(0.0, (floor - rt) / 2.0)
        self.sell = round(s + self.raised, 2)
        self.buy = round(b + self.raised, 2)

    def round_trip_bps(self) -> Optional[float]:
        """Expected result of one round trip at the thresholds, bps of the
        trade (fees and measured slippage included)."""
        if not self.ready():
            return None
        return self.sell + self.buy - self.slip_rt

    def minutes(self, now: float) -> float:
        return (now - self.samples[0][0]) / 60.0 if self.samples else 0.0
