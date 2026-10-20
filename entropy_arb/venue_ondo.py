"""Ondo Perps venue adapter (hedge leg).

Written from the public API docs (docs.ondoperps.xyz), NOT verified on the
live exchange from here. What the docs leave open is handled defensively:

* REST is limited to 1 request per second per key by default. Every REST
  call goes through one throttle; background reads (balance, position,
  keepalive) are spaced out further so an order rarely waits behind them.
* Orders are IOC limit orders (POST /v1/perps/orders). The docs do not say
  whether the create response already carries the final IOC state, so the
  result is taken from the response when it is final, else from the private
  `ordersPerps` websocket, else from one GET by clientOrderId.
* `depthBooksPerps` messages are treated as full snapshots of the top
  levels (the schema is named BookSnapshot; spot books are documented as
  replacement snapshots).
* Position size comes unsigned with a `direction`; both forms are read.

send_taker() returns the same shape as the other venues:
{status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from collections import OrderedDict
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Optional, Tuple
from urllib.parse import urlencode

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import ONDO_API_URL, ONDO_WS_URL, VenueConf

log = logging.getLogger("ondo")

REST_TIMEOUT = 10.0
REST_MIN_GAP_SEC = 1.05      # 1 request/second per key (docs, July 2026)
BACKGROUND_GAP_SEC = 2.2     # reads leave room for an order in between
KEEPALIVE_IDLE_SEC = 60.0    # warm_http pings only after this much silence
WS_PING_SEC = 30.0           # server closes idle sockets after 180 s
BOOK_LEVELS = 20
BALANCE_CACHE_SEC = 2.0
FINAL = {"fullyfilled", "canceled", "cancelled"}
SUFFIX = "-USD.P"


def market_names(names) -> Tuple[str, ...]:
    """Ticker names -> Ondo perp markets: NVDA -> NVDA-USD.P."""
    out = []
    for n in names:
        n = (n or "").strip().upper()
        if not n:
            continue
        m = n if n.endswith(".P") else n + SUFFIX
        if m not in out:
            out.append(m)
    return tuple(out)


def step_decimals(inc: Decimal) -> int:
    """Decimals d such that every multiple of 10**-d is a multiple of `inc`
    (so the engine's power-of-ten size grid stays on Ondo's lot grid)."""
    digits = max(0, -inc.normalize().as_tuple().exponent)
    for d in range(digits, -1, -1):
        if (Decimal(10) ** -d) % inc == 0:
            return d
    return 0


def sign(secret: str, ts: str, method: str, path: str, body: str) -> str:
    return hmac.new(secret.encode(), (ts + method + path + body).encode(),
                    hashlib.sha256).hexdigest()


def order_info(o: dict) -> dict:
    fb = float(o.get("filledSize") or 0.0)
    fq = float(o.get("filledCost") or 0.0)
    return {"status": str(o.get("status") or "").lower(), "filled_base": fb,
            "avg_px": (fq / fb) if fb > 0 else None,
            "cancel_reason": o.get("cancelReason") or ""}


class OndoBookFeed:
    """Public depthBooksPerps stream for one market."""

    def __init__(self, name: str, ws_url: str, market: str, book: OrderBook,
                 notify) -> None:
        self.name, self.ws_url, self.market = name, ws_url, market
        self.book, self.notify = book, notify

    def on_frame(self, msg: dict) -> None:
        self.book.touch()
        if msg.get("type") != "update" or \
                msg.get("channel") != "depthBooksPerps":
            return
        data = msg.get("data")
        for snap in (data if isinstance(data, list) else [data]):
            if isinstance(snap, dict) and snap.get("market") == self.market:
                self.book.apply_levels(snap.get("bids"), snap.get("asks"))
                self.notify()

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            ping = None
            try:
                async with ws_connect(self.ws_url, max_size=2**22,
                                      open_timeout=10, ping_interval=None) as ws:
                    log.info("[%s] book connected (%s)", self.name, self.ws_url)
                    self.book.clear()
                    await ws.send(json.dumps({
                        "op": "subscribe", "channel": "depthBooksPerps",
                        "markets": [self.market], "limit": BOOK_LEVELS}))
                    ping = asyncio.create_task(_pinger(ws))
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        if msg.get("type") == "error":
                            log.warning("[%s] book ws error: %s", self.name,
                                        msg.get("msg"))
                        self.on_frame(msg)
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] book ws error: %s — reconnect in %.0fs",
                            self.name, e, backoff)
            finally:
                if ping is not None:
                    ping.cancel()
            self.book.ready = False
            self.notify()
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def _pinger(ws) -> None:
    while True:
        await asyncio.sleep(WS_PING_SEC)
        await ws.send(json.dumps({"op": "ping"}))


class OndoOrdersFeed:
    """Private ordersPerps stream (login with the API key): settles our IOC
    orders by clientOrderId."""

    def __init__(self, name: str, ws_url: str, market: str, key_id: str,
                 secret: str) -> None:
        self.name, self.ws_url, self.market = name, ws_url, market
        self.key_id, self.secret = key_id, secret
        self.ready = asyncio.Event()
        self._pending = {}
        self._terminal: "OrderedDict[str, dict]" = OrderedDict()

    def watch(self, cid: str) -> asyncio.Future:
        fut = asyncio.get_running_loop().create_future()
        if cid in self._terminal:
            fut.set_result(self._terminal[cid])
        else:
            self._pending[cid] = fut
        return fut

    def unwatch(self, cid: str) -> None:
        self._pending.pop(cid, None)

    def on_frame(self, msg: dict) -> None:
        t = msg.get("type")
        if t == "loggedIn":
            self.ready.set()
            return
        if t != "update" or msg.get("channel") != "ordersPerps":
            return
        data = msg.get("data")
        for o in (data if isinstance(data, list) else [data]):
            if not isinstance(o, dict):
                continue
            info = order_info(o)
            cid = o.get("clientOrderId")
            if not cid or info["status"] not in FINAL:
                continue
            self._terminal[cid] = info
            while len(self._terminal) > 512:
                self._terminal.popitem(last=False)
            fut = self._pending.pop(cid, None)
            if fut is not None and not fut.done():
                fut.set_result(info)

    def login_msg(self) -> str:
        ts = str(int(time.time() * 1000))
        sig = hmac.new(self.secret.encode(),
                       (ts + "ondo_perps_ws_login").encode(),
                       hashlib.sha256).hexdigest()
        return json.dumps({"op": "login", "args": {"key": self.key_id,
                                                   "time": ts, "sign": sig}})

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            ping = None
            try:
                async with ws_connect(self.ws_url, max_size=2**22,
                                      open_timeout=10, ping_interval=None) as ws:
                    await ws.send(self.login_msg())
                    await ws.send(json.dumps({
                        "op": "subscribe", "channel": "ordersPerps",
                        "markets": [self.market]}))
                    ping = asyncio.create_task(_pinger(ws))
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        if msg.get("type") == "error":
                            log.warning("[%s] orders ws error: %s", self.name,
                                        msg.get("msg"))
                        elif msg.get("type") == "loggedIn":
                            log.info("[%s] orders ws logged in", self.name)
                        self.on_frame(msg)
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] orders ws error: %s — reconnect in %.0fs",
                            self.name, e, backoff)
            finally:
                if ping is not None:
                    ping.cancel()
            self.ready.clear()
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


class OndoError(RuntimeError):
    def __init__(self, status: int, code: str, msg: str) -> None:
        super().__init__(f"HTTP {status} {code}: {msg}")
        self.status, self.code = status, code


class OndoVenue:
    kind = "ondo"

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float, api_url: str = ONDO_API_URL,
                 ws_url: str = ONDO_WS_URL) -> None:
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.api_url, self.ws_url = api_url, ws_url
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0
        self.equity = None
        self.free = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market = ""
        self.tick = Decimal("0.01")
        self.lot = Decimal("0.01")
        self.price_decimals = 2
        self.size_decimals = 2
        self.min_base = 0.01
        self.min_quote = 0.0
        self.is_closed = False
        self.signer = None              # set by init_signer (the credentials)
        self.orders_feed: Optional[OndoOrdersFeed] = None
        self._lock = asyncio.Lock()
        self._last_req = 0.0
        self._cid = int(time.time() * 1000)
        self._bal: Optional[Tuple[float, dict]] = None

    # ------------------------------------------------------------------ REST

    async def _req(self, method: str, path: str, params: Optional[dict] = None,
                   body: Optional[dict] = None, auth: bool = True,
                   background: bool = False):
        if params:
            path = path + "?" + urlencode(params)
        data = json.dumps(body, separators=(",", ":")) if body is not None \
            else ""
        if background:
            # wait outside the lock, so an order never queues behind a read
            while True:
                wait = self._last_req + BACKGROUND_GAP_SEC - time.time()
                if wait <= 0:
                    break
                await asyncio.sleep(wait)
        async with self._lock:
            wait = self._last_req + REST_MIN_GAP_SEC - time.time()
            if wait > 0:
                await asyncio.sleep(wait)
            headers = {"Content-Type": "application/json"} if data else {}
            if auth:
                c = self.conf.ondo_creds
                if c is None or not c.complete:
                    raise RuntimeError(f"[{self.name}] no Ondo API key")
                ts = str(int(time.time() * 1000))
                headers.update({"ONDO-KEY-ID": c.key_id, "ONDO-TIMESTAMP": ts,
                                "ONDO-SIGN": sign(c.secret, ts, method, path,
                                                  data)})
            try:
                async with self.session.request(
                        method, self.api_url + path, data=data or None,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
                    status = r.status
                    try:
                        payload = await r.json(content_type=None)
                    except Exception:
                        payload = {"success": False,
                                   "error": (await r.text())[:200]}
            finally:
                self._last_req = time.time()
        if not isinstance(payload, dict) or status >= 400 or \
                payload.get("success") is False:
            p = payload if isinstance(payload, dict) else {}
            code = str(p.get("error_code") or ("too_many_requests"
                                               if status == 429 else ""))
            raise OndoError(status, code, str(p.get("error") or "")[:200])
        return payload.get("result")

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        res = await self._req("GET", "/v1/markets", auth=False)
        pairs = {p.get("market"): p
                 for p in ((res or {}).get("perps") or {}).get(
                     "tradingPairs") or []}
        names = market_names(self.conf.symbol_aliases or (self.conf.symbol,))
        found = next((n for n in names if n in pairs), None)
        if found is None:
            raise RuntimeError(f"[{self.name}] {'/'.join(names)} not found on "
                               f"Ondo Perps")
        p = pairs[found]
        self.market = found
        self.conf.symbol = found
        self.tick = Decimal(str(p.get("quoteIncrement") or "0.01"))
        self.lot = Decimal(str(p.get("baseIncrement") or "0.01"))
        self.price_decimals = max(0, -self.tick.normalize().as_tuple().exponent)
        self.size_decimals = step_decimals(self.lot)
        self.min_base = float(max(self.lot,
                                  Decimal(10) ** -self.size_decimals))
        taker = None
        try:
            for c in await self._req("GET", "/v1/perps/contracts",
                                     auth=False) or []:
                if c.get("market") != found:
                    continue
                if c.get("disabled"):
                    raise RuntimeError(f"[{self.name}] {found} is disabled")
                self.is_closed = bool(c.get("isClosed"))
                if c.get("takerFee") not in (None, ""):
                    taker = float(c["takerFee"]) * 1e4
        except OndoError as e:
            log.warning("[%s] contracts not read: %s", self.name, e)
        if self.fee_bps <= 0 and taker:
            self.fee_bps = taker
        log.info("[%s] %s tick=%s lot=%s sz_dec=%d taker_fee=%s bps "
                 "(using %.2f)%s", self.name, found, self.tick, self.lot,
                 self.size_decimals, f"{taker:g}" if taker else "?",
                 self.fee_bps,
                 " — underlying market closed now (weekend/holiday pricing)"
                 if self.is_closed else "")

    def init_signer(self) -> None:
        c = self.conf.ondo_creds
        assert c is not None and c.complete, f"[{self.name}] missing API key"
        self.signer = c
        log.info("[%s] API key ready", self.name)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            OndoBookFeed(self.name, self.ws_url, self.market, self.book,
                         notify).run(stop), name=f"book-{self.key}")]
        if live:
            c = self.conf.ondo_creds
            self.orders_feed = OndoOrdersFeed(self.name, self.ws_url,
                                              self.market, c.key_id, c.secret)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return self.orders_feed is not None and self.orders_feed.ready.is_set()

    async def warm_http(self) -> None:
        # every request costs one of the 1/s budget: ping only when idle
        if time.time() - self._last_req < KEEPALIVE_IDLE_SEC:
            return
        try:
            await self._req("GET", "/status", auth=False, background=True)
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        q = Decimal(repr(px)) / self.tick
        q = q.to_integral_value(ROUND_CEILING if round_up else ROUND_FLOOR)
        return float(q * self.tick)

    def _fmt(self, v: float, inc: Decimal, up: bool = False) -> str:
        q = (Decimal(repr(v)) / inc).to_integral_value(
            ROUND_CEILING if up else ROUND_FLOOR)
        return format((q * inc).normalize(), "f")

    # ------------------------------------------------------------- execution

    def _next_cid(self) -> str:
        self._cid += 1
        return f"mc{self._cid}"

    async def _order_by_cid(self, cid: str) -> Optional[dict]:
        try:
            return order_info(await self._req(
                "GET", f"/v1/perps/orders/client:{cid}"))
        except Exception as e:
            log.warning("[%s] order %s not read: %r", self.name, cid, e)
            return None

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False) -> dict:
        """IOC limit order at limit_px; result from the response, the
        private ws, or one GET by clientOrderId."""
        cid = self._next_cid()
        size = self._fmt(qty + 1e-12, self.lot)
        if Decimal(size) <= 0:
            return {"status": "send-failed", "filled_base": 0.0,
                    "avg_px": None, "err": f"size {qty} below lot {self.lot}",
                    "unresolved": False}
        body = {"market": self.market, "side": "buy" if is_buy else "sell",
                "type": "limit", "timeInForce": "IOC",
                "price": self._fmt(limit_px, self.tick, up=is_buy),
                "size": size, "clientOrderId": cid}
        if reduce_only:
            body["reduceOnly"] = True
        feed = self.orders_feed
        fut = feed.watch(cid) if feed is not None else None
        try:
            res = await self._req("POST", "/v1/perps/orders", body=body)
        except Exception as e:
            if fut is not None:
                feed.unwatch(cid)
            msg = f"{type(e).__name__}: {e}"
            if getattr(e, "status", None) == 429 or \
                    getattr(e, "code", "") == "too_many_requests":
                msg = "RATE_LIMITED: " + msg
            elif isinstance(e, asyncio.TimeoutError) or \
                    isinstance(e, aiohttp.ClientError):
                # the order may have reached the exchange
                got = await self._order_by_cid(cid)
                if got is not None and got["status"] in FINAL:
                    return {**got, "err": None, "unresolved": False}
                return {"status": "send-unknown", "filled_base": 0.0,
                        "avg_px": None, "err": msg, "unresolved": True}
            return {"status": "send-failed", "filled_base": 0.0,
                    "avg_px": None, "err": msg, "unresolved": False}
        info = order_info(res or {})
        if info["status"] in FINAL:
            if fut is not None:
                feed.unwatch(cid)
            return {**info, "err": None, "unresolved": False}
        if fut is not None:
            try:
                info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
                return {**info, "err": None, "unresolved": False}
            except asyncio.TimeoutError:
                feed.unwatch(cid)
        got = await self._order_by_cid(cid)
        if got is not None and got["status"] in FINAL:
            return {**got, "err": None, "unresolved": False}
        log.warning("[%s] no final state for %s in %.1fs", self.name, cid,
                    self.settle_timeout)
        return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                "err": None, "unresolved": True}

    # -------------------------------------------------------------- accounts

    async def _balance(self) -> dict:
        # the equity poll and the loss limit often ask within a second
        if self._bal is not None and time.time() - self._bal[0] < BALANCE_CACHE_SEC:
            return self._bal[1]
        b = await self._req("GET", "/v1/perps/balance", background=True)
        if not isinstance(b, dict):
            raise RuntimeError(f"[{self.name}] balance missing")
        self._bal = (time.time(), b)
        return b

    async def fetch_equity(self):
        if self.conf.ondo_creds is None or not self.conf.ondo_creds.complete:
            return None
        b = await self._balance()
        return (float(b.get("marginBalance") or 0.0),
                float(b.get("availableMargin") or 0.0))

    async def fetch_risk_equity(self):
        b = await self._balance()
        v = b.get("marginBalance")
        if v in (None, ""):
            raise RuntimeError(f"[{self.name}] marginBalance missing")
        return float(v), time.time()

    async def fetch_funding(self, start_ts: float, end_ts: float):
        """Funding received (+) / paid (-) in USDC on this market between the
        two unix times. Raises on failure."""
        if self.conf.ondo_creds is None or not self.conf.ondo_creds.complete:
            return None
        # hourly funding: 1000 rows cover 41 days, more than any session
        res = await self._req("GET", "/v1/perps/funding_fees", params={
            "market": self.market, "limit": "1000",
            "startTime": str(int(start_ts * 1000)),
            "endTime": str(int(end_ts * 1000) + 1000)}, background=True)
        total = 0.0
        for it in res if isinstance(res, list) else []:
            if it.get("market") == self.market:
                total += float(it.get("amount") or 0.0)
        return total

    async def fetch_position(self) -> float:
        for p in await self._req("GET", "/v1/perps/positions",
                                 background=True) or []:
            if p.get("market") != self.market:
                continue
            q = abs(float(p.get("netQuantity") or 0.0))
            d = str(p.get("direction") or "").lower()
            if d == "short" or float(p.get("netQuantity") or 0.0) < 0:
                return -q
            return 0.0 if d == "neutral" else q
        return 0.0

    async def close(self) -> None:
        return None
