"""Recording of large trades on Entropy (Hyperliquid `trades` feed).

Record only: nothing here changes a decision of the bot. Each large taker
order (fills of one transaction hash are added up) is written to
logs/flows/<venue>_<SYMBOL>_<day>.csv, next to the tick files of the pair,
so that after a week it can be checked what the premium did after big sells,
TWAP sells (the same address selling again and again) and liquidations.

The same public data is behind the «Entropy Flows» Telegram channel; reading
it from Hyperliquid directly is faster and needs no third party.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
from typing import Dict, List

from .feeds import ws_connect

log = logging.getLogger("flows")

HEADER = ["ts", "time_utc", "taker_side", "notional_usd", "avg_px", "fills",
          "buyer", "seller", "hash"]
FLUSH_SEC = 1.0          # fills of one hash arrive within this time


class Aggregator:
    """Adds up the fills of one transaction into one row (pure, testable)."""

    def __init__(self, min_usd: float) -> None:
        self.min_usd = min_usd
        self.open: Dict[str, dict] = {}

    def add(self, fill: dict, now: float) -> None:
        try:
            px, sz = float(fill["px"]), float(fill["sz"])
        except (KeyError, TypeError, ValueError):
            return
        key = str(fill.get("hash") or fill.get("tid") or "")
        users = fill.get("users") or ["", ""]
        row = self.open.get(key)
        if row is None:
            row = self.open[key] = {
                "ts": float(fill.get("time", now * 1000)) / 1000.0,
                "seen": now, "side": str(fill.get("side", "")),
                "usd": 0.0, "base": 0.0, "fills": 0,
                "buyer": str(users[0]) if len(users) > 0 else "",
                "seller": str(users[1]) if len(users) > 1 else "",
                "hash": key}
        row["usd"] += px * sz
        row["base"] += sz
        row["fills"] += 1
        row["seen"] = now

    def take_done(self, now: float) -> List[dict]:
        """Rows no new fill has joined for FLUSH_SEC and that are large."""
        out = []
        for key in [k for k, r in self.open.items()
                    if now - r["seen"] >= FLUSH_SEC]:
            r = self.open.pop(key)
            if r["usd"] >= self.min_usd and r["base"] > 0:
                out.append(r)
        return sorted(out, key=lambda r: r["ts"])


def row_for_csv(r: dict) -> list:
    return [f"{r['ts']:.3f}",
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(r["ts"])),
            "buy" if r["side"] == "B" else "sell",
            f"{r['usd']:.2f}", f"{r['usd'] / r['base']:.6g}", r["fills"],
            r["buyer"], r["seller"], r["hash"]]


class FlowRecorder:
    def __init__(self, directory: str, venue: str, symbol: str, ws_url: str,
                 coin: str, min_usd: float) -> None:
        self.dir = directory
        self.prefix = f"{venue}_{symbol}_"
        self.ws_url = ws_url
        self.coin = coin
        self.agg = Aggregator(min_usd)
        self.rows_written = 0

    def _write(self, rows: List[dict]) -> None:
        if not rows:
            return
        os.makedirs(self.dir, exist_ok=True)
        by_day: Dict[str, List[dict]] = {}
        for r in rows:
            by_day.setdefault(time.strftime("%Y-%m-%d", time.gmtime(r["ts"])),
                              []).append(r)
        for day, rs in by_day.items():
            path = os.path.join(self.dir, f"{self.prefix}{day}.csv")
            new = not os.path.exists(path) or os.path.getsize(path) == 0
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(HEADER)
                for r in rs:
                    w.writerow(row_for_csv(r))
        self.rows_written += len(rows)

    async def _flusher(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(FLUSH_SEC)
            try:
                self._write(self.agg.take_done(time.time()))
            except Exception:
                log.exception("[flows] write failed")

    async def run(self, stop: asyncio.Event) -> None:
        ft = asyncio.create_task(self._flusher(stop))
        backoff = 1.0
        try:
            while not stop.is_set():
                try:
                    async with ws_connect(self.ws_url, max_size=2**23,
                                          open_timeout=10, ping_interval=15,
                                          ping_timeout=15) as ws:
                        log.info("[flows] recording trades >= $%.0f of %s",
                                 self.agg.min_usd, self.coin)
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "trades",
                                             "coin": self.coin}}))
                        async for raw in ws:
                            backoff = 1.0
                            msg = json.loads(raw)
                            if msg.get("channel") == "trades":
                                now = time.time()
                                for f in msg.get("data") or []:
                                    if f.get("coin") == self.coin:
                                        self.agg.add(f, now)
                            if stop.is_set():
                                break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[flows] ws error: %s — reconnect in %.0fs",
                                e, backoff)
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
        finally:
            ft.cancel()
            try:
                self._write(self.agg.take_done(time.time() + 10 * FLUSH_SEC))
            except Exception:
                pass
