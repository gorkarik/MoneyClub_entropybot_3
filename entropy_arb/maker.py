"""Maker order on Entropy (мейкер-ордер на Entropy).

Instead of taking the Entropy book, the bot rests a post-only (ALO) limit
order on Entropy at the worst price that still clears the band against the
hedge book as it is now; when it fills, the hedge goes out as a taker for
the filled quantity. A maker fill has no Entropy slippage and pays the
lower maker fee — the opposite of the bot's main loss today.

Modes (execution.maker_mode):
  * "off";
  * "shadow" (default): no orders at all. The bot keeps a virtual order
    where it would rest, moves it by the requote rules, and counts it filled
    only when the Entropy book trades THROUGH its price (best bid above our
    ask, or best ask below our bid) at least PLACE_DELAY_SEC after it was
    placed. Each fill goes to logs/maker_shadow.csv with the hedge price at
    that moment and where Entropy traded 1 s and 3 s later (adverse
    selection). Opening and closing quotes are both recorded;
  * "live": real ALO orders, for closing trades only (toward the centre),
    with a dead-man switch (Hyperliquid scheduleCancel) refreshed while an
    order rests. While a live closing quote rests, the taker path does not
    close (no double close).

Requote rules: the order is moved only when the wanted price differs by at
least `requote_bps`, and not more often than every `requote_sec`; it never
sits more than `inside_bps` better than the best price on its side (a lone
order far inside the spread is the one that gets picked off) — that only
ever moves it to a MORE profitable price.

Everything here is pure; the engine owns the loop, the venues and money.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .book import walk_depth

PLACE_DELAY_SEC = 0.6          # a new or moved order is not live before this
MARK_SEC = (1.0, 3.0)
SHADOW_HEADER = ["ts", "session_id", "symbol", "hedge_venue", "action",
                 "side", "qty", "quote_px", "placed_ts", "life_sec",
                 "requotes", "hedge_px", "edge_bps", "e_mid", "mark1_bps",
                 "mark3_bps"]
MODES = ("off", "shadow", "live")


def quote_price(sell_entropy: bool, hedge_levels: List[Tuple[float, float]],
                qty: float, hurdle_bps: float, e_best_bid: Optional[float],
                e_best_ask: Optional[float], inside_bps: float,
                px_round) -> Optional[float]:
    """Price of a post-only Entropy order that clears `hurdle_bps` (band
    and both fees) against taking `qty` from the hedge book now.

    sell_entropy: ask at hedge_ask × (1 + hurdle), never at or below the
    best bid (it would cross) and never more than inside_bps under the best
    ask. Buying: the mirror image. None when there is no sensible price."""
    if not hedge_levels or qty <= 0 or not e_best_bid or not e_best_ask:
        return None
    h_px, _ = walk_depth(hedge_levels, qty)
    k = hurdle_bps / 1e4
    if sell_entropy:
        px = px_round(h_px * (1 + k), True)
        px = max(px, px_round(e_best_ask * (1 - inside_bps / 1e4), True))
        return px if px > e_best_bid else None
    px = px_round(h_px / (1 + k), False)
    px = min(px, px_round(e_best_bid * (1 + inside_bps / 1e4), False))
    return px if px < e_best_ask else None


def edge_bps(sell_entropy: bool, e_px: float, h_px: float, e_fee_bps: float,
             h_fee_bps: float) -> float:
    """Net premium of a filled maker trade: Entropy at e_px (maker fee),
    hedge taken at h_px (taker fee), bps."""
    fe, fh = e_fee_bps / 1e4, h_fee_bps / 1e4
    if sell_entropy:
        return (e_px * (1 - fe) / (h_px * (1 + fh)) - 1.0) * 1e4
    return (h_px * (1 - fh) / (e_px * (1 + fe)) - 1.0) * 1e4


@dataclass
class Quote:
    key: str                        # "sell_entropy" / "buy_entropy"
    action: str                     # open / close
    px: float
    qty: float
    placed_ts: float                # first placed
    moved_ts: float                 # last placed or moved
    requotes: int = 0
    oid: Optional[int] = None       # live orders
    filled: float = 0.0             # live orders: base filled so far

    @property
    def sell(self) -> bool:
        return self.key == "sell_entropy"


def wants_move(q: Quote, px: float, now: float, requote_bps: float,
               requote_sec: float) -> bool:
    if now - q.moved_ts < requote_sec:
        return False
    return abs(px / q.px - 1.0) * 1e4 >= requote_bps


def shadow_filled(q: Quote, e_best_bid: Optional[float],
                  e_best_ask: Optional[float], now: float) -> bool:
    """Counted only when the book trades through our price after the
    order has had time to land: certain, not optimistic."""
    if now - q.moved_ts < PLACE_DELAY_SEC:
        return False
    if q.sell:
        return bool(e_best_bid) and e_best_bid > q.px
    return bool(e_best_ask) and e_best_ask < q.px


@dataclass
class Stats:
    placed: int = 0
    requotes: int = 0
    fills: int = 0
    edge_sum: float = 0.0
    mark3_sum: float = 0.0
    mark3_n: int = 0
    by_action: Dict[str, int] = field(default_factory=dict)

    def add_fill(self, action: str, edge: float) -> None:
        self.fills += 1
        self.edge_sum += edge
        self.by_action[action] = self.by_action.get(action, 0) + 1

    def avg_edge(self) -> Optional[float]:
        return self.edge_sum / self.fills if self.fills else None

    def avg_mark3(self) -> Optional[float]:
        return self.mark3_sum / self.mark3_n if self.mark3_n else None


def summary(rows: List[dict]) -> Optional[dict]:
    """maker_shadow.csv rows -> {fills, opens, closes, edge, mark3, life}."""
    def num(r, k):
        try:
            return float(r.get(k) or "nan")
        except ValueError:
            return float("nan")
    if not rows:
        return None
    edges = [num(r, "edge_bps") for r in rows]
    edges = [x for x in edges if x == x]
    marks = [num(r, "mark3_bps") for r in rows]
    marks = [x for x in marks if x == x]
    lives = sorted(x for x in (num(r, "life_sec") for r in rows) if x == x)
    return {"fills": len(rows),
            "opens": sum(r.get("action") == "open" for r in rows),
            "closes": sum(r.get("action") == "close" for r in rows),
            "edge": sum(edges) / len(edges) if edges else None,
            "mark3": sum(marks) / len(marks) if marks else None,
            "life": lives[len(lives) // 2] if lives else None}
