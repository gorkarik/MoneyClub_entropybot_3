"""Two-venue arbitrage engine: Entropy vs one hedge venue.

The signal is a fixed band around a configured midline (config.yaml):

    SELL entropy / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY entropy / SELL hedge  when executable premium <= midline - lower (+fees)

Around the signal: per-direction persistence arming,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation. There is no paper mode: the
bot either trades live or runs --record-only (data collection, no strategy).
Both venues' books are recorded to 1-minute CSV bars throughout.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp

from .book import ArbPlan, floor_step, plan_arb, walk_depth
from .config import Config
from .recorder import MinuteRecorder
from .slipgate import SlipModel, leg_slip_bps, slip_file_name
from .strategy import VolumeCost, strategy_title, tight_entropy_first
from .ticks import TickRecorder
from . import autocalib, autotune
from . import backtest as bt
from . import autonomy
from . import journal
from . import maker
from . import watch
from . import telegram as tgmsg
from .telegram import TelegramBot
from .risk import NOISE, PENDING, TRIP, WAIT, LossGuard, Sample
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue
from .venue_ondo import OndoVenue

TG_SNAPSHOT = "tg_snapshot.json"     # /status and /pnl texts for the listener
TG_CONTROL = "tg_control.json"       # /pause, /resume from the listener
TG_SNAPSHOT_SEC = 10.0

log = logging.getLogger("engine")

CSV_HEADER = journal.TRADES_HEADER   # kept for compatibility
BALANCE_POLL_SEC = 30.0
RISK_MIN_GAP_SEC = 3.0     # never read equity for the loss limit more often
RISK_READ_TIMEOUT_SEC = 10.0
SHUTDOWN_FLATTEN_WAIT_SEC = 25.0  # an emergency close in flight at Stop
STOP_FLATTEN_BUDGET_SEC = 45.0    # closing positions at Stop (menu waits 90s)
EQUITY_LOG_SEC = 60.0             # equity.csv cadence
# Hyperliquid address-based action budget (10k + 1 per USDC traded). Over
# the cap the exchange accepts about one action per 10 s: an arb leg sent
# into a busy slot is rejected while the other venue's leg fills, leaving
# a one-sided position to be closed at market. So below MIN headroom the
# bot sends at most one action per SLOW_GAP on that venue — never a leg
# that the exchange would refuse.
PREM_WINDOW_SEC = 3600.0          # premium range shown for the last hour
REQ_BUDGET_POLL_SEC = 15.0
REQ_BUDGET_MIN_HEADROOM = 2
REQ_SLOW_GAP_SEC = 11.0
# the "below hedgeable minimum" note repeats on every reconcile (15 s):
# log it when the remainder changes, otherwise at most this often
DUST_LOG_SEC = 300.0
DIAG_MARKS_SEC = (1.0, 3.0)        # premium / Entropy price re-read after a signal
DIAG_HEADER = ["ts", "session_id", "symbol", "hedge_venue", "mode",
               "direction", "outcome", "signal_age_ms", "e_age_ms", "h_age_ms",
               "top_prem_bps", "mid_prem0_bps", "mid_prem1_bps",
               "mid_prem3_bps", "e_fill_px", "e_slip_bps", "e_mark1_bps",
               "e_mark3_bps", "e_ms", "h_ms", "imbalance", "e_limit_bps",
               "h_repriced"]
IMB_LEVELS = 5                    # book levels counted for the imbalance ratio
VOL_WINDOW_SEC = 60.0             # Entropy price range window of the fast-market band
VOL_SAMPLE_SEC = 0.5
MAKER_STEP_SEC = 0.25             # maker quote loop
MAKER_POLL_SEC = 0.5              # live maker order: fill check (info request)
AUTOCALIB_POLL_SEC = 60.0         # how often the auto-calibration checks it is due
AUTOCALIB_HEADER = ["ts", "session_id", "symbol", "hedge_venue", "old_midline",
                    "new_midline", "target", "minutes", "window_hours",
                    "anchor", "note"]
AUTOTUNE_HEADER = ["ts", "session_id", "symbol", "hedge_venue", "old_upper",
                   "old_lower", "new_upper", "new_lower", "hours", "slip_bps",
                   "results", "note"]
FUNDING_READ_TIMEOUT_SEC = 8.0    # funding history read at the end of a session


class Engine:
    def __init__(self, cfg: Config, record_only: bool = False) -> None:
        self.cfg = cfg
        self.record_only = record_only
        self.session: Optional[aiohttp.ClientSession] = None
        self.entropy = None
        self.hedge = None
        self.venues: Dict[str, object] = {}
        self.recorder: Optional[MinuteRecorder] = None
        self.ticks: Optional[TickRecorder] = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: Dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self._diag_tasks: set = set()
        # strategy choices: entropy-first misses, signals skipped as "too good
        # to be real", the age of the signal that fired
        self.ef_attempts = 0
        self.ef_misses = 0
        self.excess_skips = 0
        self._excess_on: Dict[str, bool] = {}
        self._signal_age: Dict[str, float] = {}
        # realized slippage per venue (always measured; used as an entry gate
        # only when enabled) — one file per pair, survives restarts
        self.slip: Optional[SlipModel] = None
        if not record_only and getattr(cfg, "trades_csv", None):
            self.slip = SlipModel(
                journal.path_in(cfg.trades_csv, slip_file_name(
                    cfg.hedge_venue, cfg.symbol,
                    getattr(cfg, "exec_mode", "simultaneous"))),
                getattr(cfg, "slipgate_lookback_hours", 48.0),
                getattr(cfg, "slipgate_min_fills", 5),
                getattr(cfg, "slipgate_max_samples", 50))
        # strategy 3 «Объём»: realized cost of Entropy volume (this pair)
        self.vol_cost: Optional[VolumeCost] = None
        if (not record_only and getattr(cfg, "exec_mode", "") == "volume"
                and getattr(cfg, "trades_csv", None)):
            self.vol_cost = VolumeCost()
            try:
                self.vol_cost.seed_csv(cfg.trades_csv, cfg.symbol,
                                       cfg.hedge_venue)
            except Exception:
                log.exception("volume cost: trades.csv unreadable")
        self.halted = False
        self.halt_reason = ""
        self.consec_errors = 0
        # unattended running (watch.py)
        self.alerts = watch.Alerts()
        self.pauses = watch.Pauses()
        # the "losing trades" pause holds volume back: only when asked for
        self.lowprofit = watch.LowProfit(
            float(getattr(cfg, "lowprofit_hours", 0.0))
            if getattr(cfg, "lowprofit_on", False) else 0.0,
            int(getattr(cfg, "lowprofit_min_trades", 10)),
            float(getattr(cfg, "lowprofit_loss_bps", 0.0)))
        self.error_pause_until = 0.0
        self.error_pauses = 0
        self._alert_tasks: set = set()
        self._last_dd_check = 0.0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        # planned vs realized edge of the SAME trades (the arbitrage legs
        # only; hedges, funding and fees of hedges are not in it)
        self.cmp_n = 0
        self.cmp_exp = 0.0
        self.cmp_fill = 0.0
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: Optional[float] = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: Dict[str, Optional[float]] = {"sell_entropy": None,
                                                   "buy_entropy": None}
        # retracement entry: best premium seen since the direction armed
        self._peak: Dict[str, Optional[float]] = {"sell_entropy": None,
                                                  "buy_entropy": None}
        self._vol_px: deque = deque()     # (ts, Entropy mid) for vol_bps
        self.imb_skips = 0
        self.retrace_waits = 0
        # maker order on Entropy (maker.py)
        self._mq: Dict[str, Optional[maker.Quote]] = {}   # shadow quotes
        self._mlive: Optional[maker.Quote] = None          # live closing quote
        self.maker_stats = maker.Stats()
        self._deadman_next = 0.0
        self._deadman_on = False
        self._mlive_poll_ts = 0.0
        self._maker_tasks: set = set()
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: Optional[float] = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: Dict[str, deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: Dict[str, float] = {}
        # Hyperliquid action budget per venue key (see REQ_BUDGET_*):
        # {"used", "cap", "surplus", "headroom", "ts"}; absent = not known
        self.req_budget: Dict[str, dict] = {}
        self._last_action_ts: Dict[str, float] = {}
        self._req_evt = asyncio.Event()
        self._req_state_logged: Dict[str, bool] = {}
        # both legs on ONE Hyperliquid address (Entropy + trade.xyz with the
        # same keys): one request budget for the two venue keys
        self.hl_shared = False
        self._last_dust_log: Optional[tuple] = None   # (net, ts)
        # auto-calibration of the midline (off unless enabled in config)
        self.autocalib_on = (not record_only
                             and getattr(cfg, "autocalib_enabled", False)
                             and bool(getattr(cfg, "ticker_file", None)))
        anchor = getattr(cfg, "midline_anchor_bps", None)
        self.autocalib_anchor = cfg.midline_bps if anchor is None else anchor
        self.autocalib_last_ts = getattr(cfg, "autocalib_last_ts", None)
        self.autocalib_last: Optional[autocalib.Decision] = None
        self.autocalib_waiting = False      # due, but a position is open
        # auto-tuning of the band (off unless enabled in config)
        self.autotune_on = (not record_only
                            and getattr(cfg, "autotune_enabled", False)
                            and bool(getattr(cfg, "ticker_file", None)))
        ua = getattr(cfg, "upper_anchor_bps", None)
        la = getattr(cfg, "lower_anchor_bps", None)
        self.autotune_anchor = (cfg.upper_bps if ua is None else ua,
                                cfg.lower_bps if la is None else la)
        self.autotune_last_ts = getattr(cfg, "autotune_last_ts", None)
        self.autotune_last: Optional[autotune.Decision] = None
        # autonomy (autonomy.py): floating centre, exit relief, calm opening
        conf = getattr(cfg, "autonomy", None)
        self.auto = conf if conf is not None else autonomy.AutonomyConf(
            enabled=False)
        self.centre: Optional[autonomy.Centre] = (
            autonomy.Centre(self.auto, cfg.midline_bps)
            if self.auto.enabled else None)
        # auto corridor (autonomy.AutoBand): thresholds follow the market
        bconf = getattr(cfg, "autoband", None)
        self.band: Optional[autonomy.AutoBand] = (
            autonomy.AutoBand(bconf)
            if bconf is not None and bconf.enabled and not record_only
            else None)
        self.last_open_ts = 0.0
        self.pos_open_ts: Optional[float] = None    # when the position opened
        self.jump_skips = 0
        self._jump_on = False
        self._leash_logged = False
        self._leash_since: Optional[float] = None
        # unhedged remainder (net of both legs) — max $ seen this session,
        # and what flatten could not close at Stop (below venue minimums)
        self.max_unhedged_usd = 0.0
        self.dust_left_usd = 0.0
        # funding of the session per venue key (+ received, - paid, None =
        # unreadable); read once, when the session ends
        self.funding: Dict[str, Optional[float]] = {}
        # premium samples (ts, bps) at ~1 Hz for the last PREM_WINDOW_SEC —
        # dashboard (test recording) and Telegram /status
        self._prem_hist: deque = deque()
        # Telegram (optional): start / session summary / read-only commands
        tg = TelegramBot(getattr(cfg, "telegram_token", None),
                         getattr(cfg, "telegram_chat_id", None))
        self.telegram: Optional[TelegramBot] = tg if tg.enabled else None
        self._session_summary: Optional[dict] = None
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: Dict[str, float] = {}
        self._venue_probe_at: Dict[str, float] = {}
        self._venue_fetch_fails: Dict[str, int] = {}
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        # loss limit (risk section). Trading waits for risk_ready, so it
        # can never start before the baseline is settled.
        self.risk_enabled = (not record_only) and cfg.max_loss_pct > 0
        self.risk_ready = not self.risk_enabled
        self.risk_blind = False          # equity unreadable: trading paused
        self.risk_guard: Optional[LossGuard] = None
        self.risk_last_sample: Optional[Sample] = None
        # session = this run, Start .. Stop
        self.session_id = time.strftime("%Y%m%d-%H%M%S")
        self.session_start_ts = time.time()
        self.session_base: Optional[Dict[str, float]] = None
        self.session_base_total: Optional[float] = None
        self.last_equity: Dict[str, float] = {}
        self._last_equity_log = 0.0
        self.fees_usd = 0.0              # estimate: fills × fee rates in force
        self.flattening = False
        self.flatten_failed = False
        self.stopping = False
        self.done = False                # set when run() has fully finished
        self._risk_evt = asyncio.Event()
        self._risk_misses = 0
        self._risk_last_read = 0.0

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v) -> bool:
        """True while the venue is under its max_orders_per_min (sliding 60s)."""
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _budget_keys(self, v) -> List[str]:
        """Venue keys that spend the same Hyperliquid request budget as v
        (the budget belongs to an address, not to a dex)."""
        if self.hl_shared and getattr(v, "kind", "") == "hl":
            return [k for k, o in self.venues.items()
                    if getattr(o, "kind", "") == "hl"]
        return [v.key]

    def _record_send(self, v) -> None:
        now = time.time()
        self._sends.setdefault(v.key, deque()).append(now)
        for k in self._budget_keys(v):
            self._last_action_ts[k] = now
        b = self.req_budget.get(v.key)
        if b is not None:
            # local count until the next budget read; peers on one address
            # share this very dict, so one send counts once for both
            b["headroom"] -= 1

    def req_limited(self, v) -> bool:
        """True while this venue's Hyperliquid action budget is exhausted."""
        b = self.req_budget.get(v.key)
        return b is not None and b["headroom"] < REQ_BUDGET_MIN_HEADROOM

    def _req_slot_wait(self, v) -> float:
        """Seconds until an action may be sent on v without being refused
        by the exchange's over-budget pacing (0 = now)."""
        if not self.req_limited(v):
            return 0.0
        since = time.time() - self._last_action_ts.get(v.key, 0.0)
        return max(0.0, REQ_SLOW_GAP_SEC - since)

    def _on_rate_limited(self, v) -> None:
        """The exchange refused an action for the address budget: pause the
        venue and switch it to paced sending until a read says otherwise."""
        self._mark_limited(v)
        if getattr(v, "kind", "") == "hl":
            b = self.req_budget.setdefault(
                v.key, {"used": None, "cap": None, "surplus": 0,
                        "headroom": 0, "ts": time.time()})
            b["headroom"] = min(b.get("headroom", 0), 0)
            for k in self._budget_keys(v):
                self.req_budget[k] = b
            self._req_evt.set()

    def shared_budget_blocks_arb(self, buy, sell) -> bool:
        """True when both legs would spend ONE Hyperliquid address budget
        and it cannot take two more actions. Over the cap the exchange takes
        about one action per 10 s, so the second leg would be refused while
        the first fills: no arbitrage then (single-leg hedges and closing
        still go out, paced)."""
        if not self.hl_shared or buy.key == sell.key:
            return False
        if self._budget_keys(buy) != self._budget_keys(sell) \
                or len(self._budget_keys(buy)) < 2:
            return False
        b = self.req_budget.get(buy.key)
        return b is not None and \
            b["headroom"] < REQ_BUDGET_MIN_HEADROOM + 1

    def unhedged(self):
        """(net base, $ value or None, hedgeable) when the two legs do not
        cancel out beyond net_tolerance_base; None when they do. Not
        hedgeable = below what an order may be on the venues (a remainder
        the bot shows and closes at Stop instead of trading on it)."""
        net = sum(v.position for v in self.venues.values())
        if abs(net) <= self.cfg.net_tolerance_base:
            return None
        px = None
        for v in self.venues.values():
            px = self._ref_px(v)
            if px is not None:
                break
        if px is None:
            return net, None, True
        usd = abs(net) * px
        need = max([self.cfg.min_order_notional]
                   + [getattr(v, "min_quote", 0.0) or 0.0
                      for v in self.venues.values()])
        return net, usd, usd >= need

    def _note_unhedged(self) -> None:
        u = self.unhedged()
        if u is not None and u[1] is not None:
            self.max_unhedged_usd = max(self.max_unhedged_usd, u[1])

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()
        self._risk_evt.set()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            keepalive_timeout=75.0, ttl_dns_cache=300))
        try:
            await self._run_inner()
        finally:
            self.done = True
            await self.session.close()

    def _make_venue(self, vc):
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec)
        if vc.kind == "ondo":
            return OndoVenue(vc, self.session, self.cfg.settle_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec)

    async def _run_inner(self) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"entropy": self.entropy, "hedge": self.hedge}
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True

        live = not self.record_only
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example; Lighter core reads LIGHTER_CORE_*, "
                    "Lighter RH reads LIGHTER_*, Ondo ONDO_API_*); use --record-only to run "
                    "without them / для режима торговли нужны ключи обеих бирж "
                    "в .env (Lighter core — LIGHTER_CORE_*, Lighter RH — "
                    "LIGHTER_*, Ondo — ONDO_API_*); без ключей работает только --record-only")
            self.entropy.init_signer()
            self.hedge.init_signer()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
        if (self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False  # shared account: count once
            self.hl_shared = True
            log.warning("both legs trade from ONE Hyperliquid address — one "
                        "request budget for both / обе ноги на одном адресе "
                        "Hyperliquid: лимит запросов общий")

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair ENTROPY(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, self.entropy.fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")
            self._log_strategy()
            await self._reconcile_positions(hedge=False, strict=True)
            try:
                journal.ensure_header(cfg.trades_csv, journal.TRADES_HEADER)
            except Exception:
                log.exception("trades.csv upgrade failed")
            journal.write_marker(cfg.trades_csv, {
                "session_id": self.session_id, "start_ts": self.session_start_ts,
                "symbol": cfg.symbol, "hedge": cfg.hedge_venue,
                "hedge_symbol": self.hedge_symbol()})
            log.info("starting positions: %s (net %+.6g)",
                     " ".join(f"{v.name}={v.position:+.6g}"
                              for v in self.venues.values()),
                     sum(v.position for v in self.venues.values()))

        tasks: List[asyncio.Task] = []
        for v in self.venues.values():
            tasks += v.start_tasks(self.stop, self._update_evt.set, live)
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(cfg.recorder_csv, self.entropy.book,
                                           self.hedge.book, cfg.staleness_sec)
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
        if cfg.ticks_sec > 0:
            self.ticks = TickRecorder(
                cfg.ticks_dir, cfg.hedge_venue, cfg.symbol, self.entropy.book,
                self.hedge.book, cfg.staleness_sec, cfg.ticks_sec,
                cfg.max_order_notional)
            tasks.append(asyncio.create_task(self.ticks.run(self.stop),
                                             name="ticks"))
        if not self.record_only:
            tasks.append(asyncio.create_task(self._strategy_loop(),
                                             name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                             name="keepalive"))
            if getattr(cfg, "maker_mode", "off") != "off":
                tasks.append(asyncio.create_task(self._maker_loop(),
                                                 name="maker"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if self.centre is not None:
            try:
                rows = autonomy.seed_rows(cfg.recorder_csv,
                                          self.auto.window_min, time.time())
                self.centre.seed(rows)
                self.centre.update(time.time())
                log.info("[autonomy] centre seeded from %d minutes: %s",
                         len(rows), f"{self.centre.live:+.1f} bps"
                         if self.centre.live is not None else "not yet")
            except Exception:
                log.exception("[autonomy] centre seed failed")
        tasks.append(asyncio.create_task(self._premium_sampler(),
                                         name="premium"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(),
                                             name="reconcile"))
            tasks.append(asyncio.create_task(self._request_budget_loop(),
                                             name="request-budget"))
            # session balance (PnL, report) always; loss limit if set
            tasks.append(asyncio.create_task(self._risk_loop(), name="risk"))
            if self.autocalib_on:
                tasks.append(asyncio.create_task(self._autocalib_loop(),
                                                 name="autocalib"))
            if self.autotune_on:
                tasks.append(asyncio.create_task(self._autotune_loop(),
                                                 name="autotune"))

        if self.telegram is not None:
            # Telegram is polled by the always-on listener (club.py
            # --telegram): it answers even while the bot is stopped. The
            # engine only hands it /status and /pnl and takes /pause, /resume.
            tasks.append(asyncio.create_task(self._tg_bridge_loop(),
                                             name="telegram"))
            if float(getattr(cfg, "heartbeat_hours", 0.0)) > 0:
                tasks.append(asyncio.create_task(self._heartbeat_loop(),
                                                 name="heartbeat"))
            await self._tg_send(tgmsg.start_text(self))

        await self.stop.wait()
        self.stopping = True
        if self._exec_tasks:  # let in-flight executions settle, never cancel
            log.info("waiting for %d in-flight execution(s) to settle",
                     len(self._exec_tasks))
            wait = cfg.settle_timeout_sec + 2.0
            if self.flattening:  # an emergency close must not be cut short
                wait = max(wait, SHUTDOWN_FLATTEN_WAIT_SEC)
            await asyncio.wait(self._exec_tasks, timeout=wait)
        if live:
            # a session ends flat: close both legs before exiting
            log.warning("[STOP] closing positions before shutdown / закрываю "
                        "позиции перед остановкой")
            try:
                closed = await asyncio.wait_for(self._flatten("stop"),
                                                STOP_FLATTEN_BUDGET_SEC)
            except asyncio.TimeoutError:
                closed = False
                self.flatten_failed = True
                log.critical("[STOP] / ОСТАНОВКА — closing did not finish in "
                             "%.0fs: CHECK POSITIONS / закрытие не успело "
                             "завершиться — проверьте позиции на биржах",
                             STOP_FLATTEN_BUDGET_SEC)
            except Exception:
                closed = False
                self.flatten_failed = True
                log.exception("[STOP] closing positions failed")
            try:
                await asyncio.wait_for(self._finish_session(closed), 20.0)
            except Exception:
                log.exception("session summary failed")
        if self.telegram is not None:
            if self.record_only:
                await self._tg_send(tgmsg.record_finish_text(self))
            elif self._session_summary is not None:
                await self._tg_send(tgmsg.finish_text(
                    self, self._session_summary))
        for t in list(tasks) + list(self._diag_tasks):
            t.cancel()
        await asyncio.gather(*tasks, *self._diag_tasks, return_exceptions=True)
        for v in self.venues.values():
            await v.close()
        if self.telegram is not None:
            await self.telegram.close()
        log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                 "fill edge $%.4f", self.trades, self.hedges,
                 self.total_exp_edge, self.total_fill_edge)

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell) -> float:
        """Net hurdle (bps, on top of fees) for the direction buy->sell.

        selling entropy: executable premium must clear midline + upper;
        buying entropy: the reverse premium must clear lower - midline."""
        ab = self.band
        if ab is not None and ab.ready():
            # auto corridor: the threshold already sits where the market is
            # and already carries the cost limit — no exit relief, no
            # strategy-3 narrowing on top of it
            base = ab.sell if sell.key == "entropy" else ab.buy
            return (base + self.vol_add_bps(buy, sell)
                    + self._inv_add_bps(buy, sell)
                    + self.slip_charge_bps(buy, sell)
                    + self.volume_charge_bps(buy, sell))
        mid = self.mid_bps()
        if sell.key == "entropy":
            band = self.cfg.upper_bps
            base = mid + band
        else:
            band = self.cfg.lower_bps
            base = band - mid
        if self.auto.enabled and not self._opens(buy, sell):
            base -= self.exit_relief_bps(band)
        return (base - self.volume_narrow_bps()
                + self.vol_add_bps(buy, sell)
                + self._inv_add_bps(buy, sell)
                + self.slip_charge_bps(buy, sell)
                + self.volume_charge_bps(buy, sell))

    # ------------------------------------------------ fast market, imbalance

    def _sample_vol(self, now: float) -> None:
        e = self.entropy
        m = e.book.mid() if e is not None else None
        if not m:
            return
        q = self._vol_px
        if not q or now - q[-1][0] >= VOL_SAMPLE_SEC:
            q.append((now, m))
        while q and q[0][0] < now - VOL_WINDOW_SEC:
            q.popleft()

    def vol_bps(self) -> float:
        """Entropy price range (high − low over the mid) of the last
        VOL_WINDOW_SEC, bps. 0 with fewer than two samples."""
        q = self._vol_px
        if len(q) < 2:
            return 0.0
        pxs = [px for _, px in q]
        lo, hi = min(pxs), max(pxs)
        return (hi - lo) / ((hi + lo) / 2.0) * 1e4

    def vol_add_bps(self, buy=None, sell=None) -> float:
        """Fast-market widening of the opening hurdle: vol_k × vol_bps.
        Closing trades are never widened."""
        k = getattr(self.cfg, "vol_k", 0.0)
        if k <= 0 or (buy is not None and not self._opens(buy, sell)):
            return 0.0
        return k * self.vol_bps()

    def imbalance(self, buy, sell) -> Optional[float]:
        """Book pressure against the trade on Entropy: depth on the side
        that pushes the price away from us over the depth we take (top
        IMB_LEVELS). Buying Entropy: bids / asks; selling: asks / bids.
        Above 1 the price is more likely to move against the order."""
        e = self.entropy
        if e is None:
            return None
        bids = e.book.sorted_bids()[:IMB_LEVELS]
        asks = e.book.sorted_asks()[:IMB_LEVELS]
        b = sum(px * sz for px, sz in bids)
        a = sum(px * sz for px, sz in asks)
        if b <= 0 or a <= 0:
            return None
        return b / a if buy.key == e.key else a / b

    def _ef_limit_bps(self, buy, sell, plan: ArbPlan) -> float:
        """How much worse than the plan the Entropy IOC may fill
        (strategies 2/3). "profit": the plan's spare premium over the full
        hurdle plus the part of the band we agree to give up
        (1 − entropy_first_keep), clamped to 0.5 .. 3 × the fixed limit;
        "fixed": entropy_first_slip_bps."""
        cfg = self.cfg
        fixed = cfg.entropy_first_slip_bps
        if getattr(cfg, "entropy_first_limit", "fixed") != "profit":
            return fixed
        eff = self._eff_threshold(buy, sell)
        hurdle = eff + buy.fee_bps + sell.fee_bps
        mid = self.mid_bps()
        band = eff - (mid if sell.key == "entropy" else -mid)
        spare = max(0.0, plan.marginal_premium_bps - hurdle)
        give = max(0.0, band) * (1.0 - cfg.entropy_first_keep)
        return min(max(spare + give, 0.5), 3.0 * fixed)

    def mid_bps(self) -> float:
        """Centre of the band in use: the floating centre when autonomy is
        on and has enough data, else the pair's midline."""
        c = self.centre
        if c is not None and c.live is not None:
            return c.live
        return self.cfg.midline_bps

    def hold_age_sec(self, now: Optional[float] = None) -> Optional[float]:
        """Seconds since the Entropy position last opened from flat (None:
        flat). Positions below the venue minimum count as flat."""
        now = time.time() if now is None else now
        e = self.entropy
        ref = e.book.mid() if e is not None else None
        flat = (e is None or not ref
                or abs(e.position) * ref < max(self._min_notional, 1.0))
        if flat:
            self.pos_open_ts = None
            return None
        if self.pos_open_ts is None:
            self.pos_open_ts = now
        return now - self.pos_open_ts

    def cap_frac(self) -> float:
        e = self.entropy
        ref = e.book.mid() if e is not None else None
        if not ref or not e.cap_usd:
            return 0.0
        return min(abs(e.position) * ref / e.cap_usd, 1.0)

    def exit_relief_bps(self, band: float) -> float:
        return autonomy.exit_relief_bps(self.auto, band, self.hold_age_sec(),
                                        self.cap_frac())

    def _open_blocked(self, buy, sell, now: float, dkey: str) -> bool:
        """Calm opening: space out trades that add to the position and hold
        them while the premium has jumped away from the centre. Pauses
        (Telegram /pause, losing hours, drawdown) hold new positions too."""
        if self._opens(buy, sell):
            held = self.pauses.active(now)
            if held:
                self._skiplog("%s held: %s", dkey, ", ".join(held))
                return True
        if not self.auto.enabled or not self._opens(buy, sell):
            return False
        gap = self.auto.open_cooldown_sec
        if gap > 0 and now - self.last_open_ts < gap:
            self._schedule_poke(gap - (now - self.last_open_ts))
            return True
        c = self.centre
        if c is not None and c.jumped():
            if not self._jump_on:
                self._jump_on = True
                self.jump_skips += 1
                log.warning("[autonomy] premium jumped: last %.0f min %+.1f vs "
                            "centre %+.1f bps — no new positions until the "
                            "centre catches up (closing still allowed)",
                            self.auto.jump_window_min, c.recent, c.live)
            self._skiplog("%s held: premium jump (%+.1f vs centre %+.1f)",
                          dkey, c.recent, c.live)
            return True
        if self._jump_on:
            self._jump_on = False
            log.warning("[autonomy] premium back near the centre — opening "
                        "allowed again")
        return False

    def _feed_centre(self, now: float) -> None:
        c = self.centre
        if c is None:
            return
        self.hold_age_sec(now)
        c.manual = self.cfg.midline_bps       # autocalib / menu may move it
        st = self.cfg.staleness_sec
        p = None
        if (self.entropy is not None and self.hedge is not None
                and self.entropy.book.is_fresh(st)
                and self.hedge.book.is_fresh(st)):
            p = self.premium_bps()
        if p is None:
            c.update(now)
        else:
            c.add(now, p)
        if c.at_leash and not self._leash_logged:
            log.warning("[autonomy] premium is more than %.0f bps from the "
                        "pair midline %+.1f — the centre stops at %+.1f; "
                        "re-calibrate the pair", self.auto.max_drift_bps,
                        self.cfg.midline_bps, c.live)
        self._leash_logged = c.at_leash
        self._maybe_recentre(now)

    def net_premiums(self):
        """(sell, buy) executable premiums at the top of both books net of
        both taker fees, bps — exactly what plan_arb compares with the
        threshold. None when a book is empty or stale."""
        e, h = self.entropy, self.hedge
        st = self.cfg.staleness_sec
        if e is None or h is None or not (e.book.is_fresh(st)
                                          and h.book.is_fresh(st)):
            return None
        eb, ea = e.book.best_bid(), e.book.best_ask()
        hb, ha = h.book.best_bid(), h.book.best_ask()
        if not (eb and ea and hb and ha):
            return None
        fe, fh = e.fee_bps / 1e4, h.fee_bps / 1e4
        sell = (eb * (1 - fe) / (ha * (1 + fh)) - 1.0) * 1e4
        buy = (hb * (1 - fh) / (ea * (1 + fe)) - 1.0) * 1e4
        return sell, buy

    def _slip_round_trip_bps(self) -> float:
        """Measured slippage of a round trip (two trades, both venues), bps;
        0 until the slippage model has enough fills."""
        if self.slip is None:
            return 0.0
        tot = 0.0
        for key in ("entropy", "hedge"):
            try:
                m, _ = self.slip.median(key)
            except Exception:
                m = None
            if m is not None and m > 0:
                tot += m
        return 2.0 * tot

    def _feed_band(self, now: float) -> None:
        ab = self.band
        if ab is None:
            return
        was = ab.ready()
        net = self.net_premiums()
        if net is None:
            ab.update(now)
        else:
            ab.add(now, net[0], net[1], self._slip_round_trip_bps())
        if ab.ready() and not was:
            log.warning("[autoband] auto corridor on: sell Entropy from "
                        "%+.1f bps, buy from %+.1f bps (round trip %+.1f bps) "
                        "/ автокоридор включился", ab.sell, ab.buy,
                        ab.round_trip_bps())
            self._update_evt.set()

    def _maybe_recentre(self, now: float) -> None:
        """The market has stayed beyond max_drift_bps of the pair midline for
        recentre_min minutes: move the midline (the leash's anchor) to the
        market median, in memory and in the pair file, and say so. A jump in
        the last minutes (premium not settled) waits."""
        c = self.centre
        wait = float(getattr(self.auto, "recentre_min", 0.0)) * 60.0
        if c is None or not c.at_leash:
            self._leash_since = None
            return
        if self._leash_since is None:
            self._leash_since = now
        if wait <= 0 or now - self._leash_since < wait:
            return
        med = c.median_now()
        if med is None or c.recent is None \
                or abs(c.recent - med) > self.auto.jump_bps:
            return
        old = self.cfg.midline_bps
        new = round(med, 2) or 0.01
        self.cfg.midline_bps = c.manual = new
        self.autocalib_anchor = new       # a hand-set value for autocalib too
        self._leash_since = None
        self._leash_logged = False
        c.update(now)
        log.warning("[autonomy] market stayed %.0f+ bps from the midline for "
                    "%.0f min — midline moved %+.1f → %+.1f / центр пары "
                    "перенесён за рынком", self.auto.max_drift_bps,
                    wait / 60.0, old, new)
        try:
            autocalib.save(self.cfg.ticker_file, new, now)
        except Exception:
            log.exception("[autonomy] pair file not updated — the new midline "
                          "holds until the bot restarts")
        self._alert("recentre", (
            f"🧭 Рынок {wait / 60:.0f} мин держался дальше "
            f"{self.auto.max_drift_bps:g} bps от центра пары. Центр перенесён: "
            f"{old:+.1f} → {new:+.1f} bps. Полоса прежняя."))

    def _log_strategy(self) -> None:
        cfg = self.cfg
        mode = getattr(cfg, "exec_mode", "simultaneous")
        extra = ""
        if tight_entropy_first(mode):
            if getattr(cfg, "entropy_first_limit", "fixed") == "profit":
                extra += (f" · worst Entropy price from the trade's profit "
                          f"(keeps {cfg.entropy_first_keep:.0%} of the band, "
                          f"0.5..{3 * cfg.entropy_first_slip_bps:g} bps)")
            else:
                extra += (f" · worst Entropy price "
                          f"{cfg.entropy_first_slip_bps:g} bps")
        for on, txt in ((getattr(cfg, "retrace_bps", 0) > 0,
                         f"pullback entry {getattr(cfg, 'retrace_bps', 0):g} bps"),
                        (getattr(cfg, "imbalance_max", 0) > 0,
                         f"book imbalance ≤ {getattr(cfg, 'imbalance_max', 0):g}×"),
                        (getattr(cfg, "vol_k", 0) > 0,
                         f"fast-market band ×{getattr(cfg, 'vol_k', 0):g}")):
            if on:
                extra += " · " + txt
        if mode == "volume":
            cost, n = self.volume_cost()
            extra += (f" · band −{cfg.volume_narrow_bps:g} bps each side · "
                      f"price of volume ≤ ${cfg.volume_max_cost_usd:g} per "
                      f"$10k Entropy volume (now "
                      + (f"${cost:.2f} over {n} trades)" if cost is not None
                         else f"not measured, {n} trades)"))
        guards = []
        if getattr(cfg, "max_excess_bps", 0) > 0:
            guards.append(f"skip signals > hurdle + {cfg.max_excess_bps:g} bps")
        if getattr(cfg, "slipgate_enabled", False):
            guards.append("execution-loss correction")
        log.warning("[strategy] %s%s%s", self.strategy_name, extra,
                    (" · protection: " + ", ".join(guards)) if guards else "")

    @property
    def leg1_title(self) -> str:
        """'Entropy' or 'Hyperliquid' (main exchange) — for people."""
        from .config import leg1_title
        conf = getattr(self.entropy, "conf", None) or self.cfg.entropy
        return leg1_title(getattr(conf, "hl_dex", "io"))

    @property
    def strategy_name(self) -> str:
        """'Стратегия 2 · Сначала Entropy' — dashboard, Telegram, logs."""
        return strategy_title(getattr(self.cfg, "exec_mode", "simultaneous"))

    def volume_narrow_bps(self) -> float:
        """Strategy 3: the entry band is this much narrower on each side."""
        if getattr(self.cfg, "exec_mode", "") != "volume":
            return 0.0
        return float(getattr(self.cfg, "volume_narrow_bps", 0.0))

    def volume_cost(self):
        """(cost of $10 000 of Entropy volume, trades counted) — strategy 3
        only; (None, 0) otherwise or while too few trades."""
        if self.vol_cost is None:
            return None, 0
        return self.vol_cost.cost_bps()

    def volume_charge_bps(self, buy=None, sell=None) -> float:
        """Strategy 3: while volume costs more than allowed, opening trades
        need that much more premium. Closing trades are never held back."""
        if self.vol_cost is None:
            return 0.0
        if buy is not None and not self._opens(buy, sell):
            return 0.0
        return self.vol_cost.charge_bps(
            float(getattr(self.cfg, "volume_max_cost_usd", 2.0)))

    def _opens(self, buy, sell) -> bool:
        """True when the trade adds to (or opens) the Entropy position;
        closing / reducing trades are never charged for slippage."""
        e = self.entropy.position if self.entropy is not None else 0.0
        return e <= 0 if sell.key == "entropy" else e >= 0

    def slip_charge_bps(self, buy=None, sell=None) -> float:
        """Realized-slippage charge on the entry threshold (0 when the gate
        is off, the samples are too few, or the trade reduces a position)."""
        if not getattr(self.cfg, "slipgate_enabled", False) or self.slip is None:
            return 0.0
        if buy is not None and not self._opens(buy, sell):
            return 0.0
        return self.slip.charge_bps(list(self.venues.keys()),
                                    self.cfg.slipgate_weight)

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    def _plan(self, buy, sell, cap_notional: float):
        return plan_arb(
            buy.book, sell.book,
            threshold_bps=self._eff_threshold(buy, sell),
            buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
        )

    # -------------------------------------------------------------- strategy

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted:
            return
        if not self.risk_ready or self.risk_blind:
            return  # loss limit not armed yet / equity unreadable
        now = time.time()
        if now < self.error_pause_until:
            self._schedule_poke(self.error_pause_until - now)
            return
        if now - self.last_trade_ts < cfg.cooldown_sec:
            self._schedule_poke(cfg.cooldown_sec - (now - self.last_trade_ts))
            return
        best = self._scan(now)
        if best is None:
            return
        buy, sell, plan = best
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(buy, sell, plan))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(self, buy, sell, plan: ArbPlan) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        unresolved = False
        try:
            unresolved = await self._execute(buy, sell, plan)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
        finally:
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        if unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        self._risk_evt.set()    # re-check the loss limit after every trade
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Evaluate both directions; returns the best executable
        (buy, sell, plan), or None."""
        cfg = self.cfg
        best = None
        if getattr(cfg, "vol_k", 0.0) > 0:
            self._sample_vol(now)
        for buy, sell, dkey in ((self.hedge, self.entropy, "sell_entropy"),
                                (self.entropy, self.hedge, "buy_entropy")):
            if not (buy.book.is_fresh(cfg.staleness_sec)
                    and sell.book.is_fresh(cfg.staleness_sec)):
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                continue
            if self._venue_down:
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                continue  # reactive 429 exclusion
            if self.shared_budget_blocks_arb(buy, sell):
                self._skiplog("%s blocked: both legs on one Hyperliquid "
                              "address and its request budget is exhausted "
                              "— two orders cannot go out together", dkey)
                continue
            wait = max(self._req_slot_wait(buy), self._req_slot_wait(sell))
            if wait > 0:
                # Hyperliquid budget exhausted: the exchange takes ~1 action
                # per 10 s — send nothing until the slot is free, so a leg is
                # never refused while the other venue's leg fills
                self._skiplog("%s deferred %.0fs: Hyperliquid request budget "
                              "exhausted (1 order per %.0fs)", dkey, wait,
                              REQ_SLOW_GAP_SEC)
                self._schedule_poke(wait + 0.05)
                continue
            if not (self._venue_rate_ok(buy) and self._venue_rate_ok(sell)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                continue
            plan, reason = self._plan(buy, sell, cfg.max_order_notional)
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                self._armed[dkey] = None
                self._peak[dkey] = None
                continue
            armed = self._armed.get(dkey)
            if armed is None:
                # premium persistence: only fire if the edge survives
                # premium_persist_sec (filters one-tick phantoms)
                self._armed[dkey] = now
                self._schedule_poke(cfg.premium_persist_sec)
                continue
            if now - armed < cfg.premium_persist_sec:
                self._schedule_poke(cfg.premium_persist_sec - (now - armed))
                continue
            if plan is None:
                continue
            if self._open_blocked(buy, sell, now, dkey):
                continue
            if self._mlive is not None and self._mlive.key == dkey:
                continue   # a live maker order is closing this side
            if self._retrace_wait(buy, sell, plan, dkey):
                continue
            if self._imbalance_blocks(buy, sell, dkey):
                continue
            if (self.auto.enabled or self.band is not None) \
                    and not self._opens(buy, sell):
                # a relaxed closing hurdle must never open the other side:
                # a closing trade is at most the position it closes
                pos_usd = abs(self.entropy.position) * plan.buy_limit
                if plan.buy_notional > pos_usd + 1e-9:
                    plan, _ = self._plan(buy, sell,
                                         min(cfg.max_order_notional, pos_usd))
                    if plan is None:
                        continue
            if self._too_good(buy, sell, plan, dkey):
                continue
            headroom = self._headroom(buy, sell, plan.buy_limit)
            if headroom < plan.buy_notional:
                plan, _ = self._plan(buy, sell,
                                     min(cfg.max_order_notional, headroom))
                if plan is None:
                    self._skiplog("%s blocked by position caps (headroom $%.0f)",
                                  dkey, max(headroom, 0.0))
                    continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan)
                self._signal_age[dkey] = now - armed
        return best

    def _retrace_wait(self, buy, sell, plan, dkey: str) -> bool:
        """Retracement entry for new positions: fire only once the premium
        has pulled back retrace_bps from its best since the signal armed
        (still above the hurdle, or there would be no plan). A premium that
        is still running is let run: the Hyperliquid book then moves less
        while our order travels."""
        r = getattr(self.cfg, "retrace_bps", 0.0)
        if r <= 0 or not self._opens(buy, sell):
            return False
        prem = plan.top_premium_bps
        peak = self._peak.get(dkey)
        if peak is None:
            self.retrace_waits += 1
        peak = prem if peak is None else max(peak, prem)
        self._peak[dkey] = peak
        if prem <= peak - r:
            return False
        self._skiplog("%s waits for a pullback: premium %.1f, peak %.1f, "
                      "fires at <= %.1f bps", dkey, prem, peak, peak - r)
        return True

    def _imbalance_blocks(self, buy, sell, dkey: str) -> bool:
        lim = getattr(self.cfg, "imbalance_max", 0.0)
        if lim <= 0 or not self._opens(buy, sell):
            return False
        imb = self.imbalance(buy, sell)
        if imb is None or imb <= lim:
            return False
        self.imb_skips += 1
        self._skiplog("%s skipped: Entropy book %.1f× heavier against the "
                      "trade (limit %.1f)", dkey, imb, lim)
        return True

    def _too_good(self, buy, sell, plan, dkey: str) -> bool:
        """Signal ceiling: a premium that beats its own entry hurdle by more
        than max_excess_bps is most often a book that has not caught up yet
        (it is gone by the time the order lands). Skip it, disarmed."""
        lim = getattr(self.cfg, "max_excess_bps", 0.0)
        if lim <= 0:
            return False
        excess = plan.top_premium_bps - (self._eff_threshold(buy, sell)
                                         + buy.fee_bps + sell.fee_bps)
        if excess <= lim:
            self._excess_on[dkey] = False
            return False
        self._armed[dkey] = None
        if not self._excess_on.get(dkey):
            self._excess_on[dkey] = True
            self.excess_skips += 1
        self._skiplog("%s skipped: premium %.1f bps beats the entry hurdle by "
                      "%.1f > %.1f — too good to be real (stale book?)", dkey,
                      plan.top_premium_bps, excess, lim)
        return True

    # ------------------------------------------------------------- execution

    async def _execute(self, buy, sell, plan: ArbPlan) -> bool:
        """Send both legs and settle the fills. Both venue locks are held by
        the caller. Returns True when an outcome is unresolved and the caller
        must escalate to reconcile."""
        if self.halted:
            return False
        cfg = self.cfg
        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        # open = adds to (or starts) the Entropy position; close = reduces it
        action = "open" if self._opens(buy, sell) else "close"
        self.last_trade_ts = time.time()
        if action == "open":
            self.last_open_ts = self.last_trade_ts
        imb = self.imbalance(buy, sell)
        log.info("[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f | imb %s",
                 direction, buy.name, plan.qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd,
                 "-" if imb is None else f"{imb:.2f}")
        slip = cfg.leg_slippage_bps / 1e4
        buy_bound = buy.px_round(plan.buy_limit * (1 + slip), round_up=False)
        sell_bound = sell.px_round(plan.sell_limit * (1 - slip), round_up=True)
        spreads = {}
        for v in (buy, sell):
            bb, ba = v.book.best_bid(), v.book.best_ask()
            spreads[v.key] = (ba / bb - 1) * 1e4 if (bb and ba) else None
        diag = self._diag_start(direction, plan)
        diag["imbalance"] = imb
        self._peak[direction] = None     # the next entry waits for its own pullback
        missed = False
        if tight_entropy_first(getattr(cfg, "exec_mode", "simultaneous")):
            binfo, sinfo, timing, missed = await self._send_entropy_first(
                buy, sell, plan, buy_bound, sell_bound, diag)
            timing["spreads"] = (spreads[buy.key], spreads[sell.key])
        else:
            self._record_send(buy)
            self._record_send(sell)
            t0 = time.time()
            done_at: Dict[str, float] = {}

            async def leg(v, coro):
                try:
                    return await coro
                finally:
                    done_at[v.key] = time.time()

            res = await asyncio.gather(
                leg(buy, buy.send_taker(is_buy=True, qty=plan.qty,
                                        limit_px=buy_bound)),
                leg(sell, sell.send_taker(is_buy=False, qty=plan.qty,
                                          limit_px=sell_bound)),
                return_exceptions=True)
            timing = {
                "buy_ms": (done_at.get(buy.key, t0) - t0) * 1e3,
                "sell_ms": (done_at.get(sell.key, t0) - t0) * 1e3,
                "spreads": (spreads[buy.key], spreads[sell.key]),
            }
            timing["gap_ms"] = abs(timing["buy_ms"] - timing["sell_ms"])
            binfo, sinfo = (r if isinstance(r, dict) else
                            {"status": "send-failed", "filled_base": 0.0,
                             "avg_px": None, "err": repr(r),
                             "unresolved": False}
                            for r in res)
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = binfo["filled_base"]
        sfill = sinfo["filled_base"]
        buy.position += bfill
        sell.position -= sfill
        if bfill:
            bpx = binfo.get("avg_px") or plan.buy_limit
            buy.cash -= bfill * bpx * (1 + plan.buy_fee)
            buy.volume_usd += bfill * bpx
            self.fees_usd += bfill * bpx * plan.buy_fee
        if sfill:
            spx = sinfo.get("avg_px") or plan.sell_limit
            sell.cash += sfill * spx * (1 - plan.sell_fee)
            sell.volume_usd += sfill * spx
            self.fees_usd += sfill * spx * plan.sell_fee

        matched = min(bfill, sfill)
        fill_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (sinfo["avg_px"] * (1 - plan.sell_fee)
                                   - binfo["avg_px"] * (1 + plan.buy_fee))
            self.total_fill_edge += fill_edge
            e_px = (binfo if buy.key == "entropy" else sinfo)["avg_px"]
            self._watch_profit(self._edge_vs_centre(
                direction, fill_edge, matched * e_px), matched * e_px)
        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f", direction,
                 buy.name, binfo["status"], bfill, plan.qty,
                 sell.name, sinfo["status"], sfill, plan.qty, matched, fill_edge)
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = binfo.get("unresolved") or sinfo.get("unresolved")
        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._on_rate_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        sent_ok = not hard_err and not unresolved
        if sent_ok:
            self.consec_errors = 0
            self.error_pauses = 0
        elif not rate_limited:
            self.consec_errors += 1
            limit_pauses = int(getattr(cfg, "error_pauses_before_halt", 1))
            pause_min = float(getattr(cfg, "error_pause_min", 0.0))
            if self.consec_errors >= cfg.max_consecutive_errors \
                    and not self.halted and pause_min > 0 \
                    and self.error_pauses + 1 < limit_pauses:
                self.error_pauses += 1
                self.error_pause_until = time.time() + pause_min * 60.0
                log.critical("PAUSED %.0f min after %d consecutive execution "
                             "problems (pause %d of %d before a full halt) / "
                             "пауза после серии ошибок", pause_min,
                             self.consec_errors, self.error_pauses,
                             limit_pauses - 1)
                self._alert("errpause", (
                    f"Пауза {pause_min:.0f} мин: {self.consec_errors} ошибки "
                    f"исполнения подряд. Потом бот попробует снова. Пауза "
                    f"{self.error_pauses} из {limit_pauses - 1}; следующая "
                    f"серия ошибок после них — полная остановка."))
                self.consec_errors = 0
            elif self.consec_errors >= cfg.max_consecutive_errors \
                    and not self.halted:
                self.halted = True
                self.halt_reason = (f"{self.consec_errors} consecutive "
                                    f"execution errors")
                log.critical("HALTED after %d consecutive execution problems "
                             "— flatten manually and restart / несколько ошибок подряд, "
                             "бот остановлен: закройте позиции вручную и перезапустите", self.consec_errors)
                self._alert("halt", (
                    f"🛑 Торговля ОСТАНОВЛЕНА: {self.consec_errors} ошибки "
                    f"исполнения подряд после {self.error_pauses} пауз. "
                    + ("Бот закрывает позиции." if cfg.flatten_on_halt else
                       "Позиции НЕ закрыты: проверьте биржи и перезапустите "
                       "бота.")))
                if cfg.flatten_on_halt:
                    self._spawn_exec(self._flatten("errors"))
        if sent_ok and not missed:
            self.trades += 1
            self.total_exp_edge += plan.exp_edge_usd
            if matched > 0 and plan.qty:
                # the plan for the part that actually traded on both legs
                self.cmp_n += 1
                self.cmp_exp += plan.exp_edge_usd * min(matched / plan.qty, 1.0)
                self.cmp_fill += fill_edge
                if self.vol_cost is not None and bfill > 0 and sfill > 0:
                    e_not = (plan.buy_notional if buy.key == "entropy"
                             else plan.sell_notional)
                    self.vol_cost.add(e_not * min(matched / plan.qty, 1.0),
                                      fill_edge)
        self._learn_slippage(buy, sell, plan, binfo, sinfo, bfill, sfill)
        self._diag_finish(diag, binfo, sinfo, timing, missed, unresolved)
        sent_ok = sent_ok and not missed
        self._record_trade(direction, plan,
                           None if unresolved else fill_edge,
                           f"{binfo['status']}/{sinfo['status']}", sent_ok)
        self._log_csv(direction, buy, sell, plan, sent_ok, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps,
                      binfo.get("avg_px"), sinfo.get("avg_px"), timing,
                      action)
        self.last_trade_ts = time.time()
        self._req_evt.set()   # re-read the request budget after a trade
        self._note_unhedged()
        return bool(unresolved)

    # ------------------------------------------------------- maker order

    async def _maker_loop(self) -> None:
        mode = self.cfg.maker_mode
        log.info("[maker] %s — %s", mode,
                 "records where a post-only Entropy order would rest and "
                 "whether it would fill (logs/maker_shadow.csv), sends nothing"
                 if mode == "shadow" else
                 "REAL post-only Entropy orders for closing trades")
        while not self.stop.is_set():
            try:
                await self._maker_step(time.time())
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[maker] step failed")
            await asyncio.sleep(MAKER_STEP_SEC)

    def _maker_qty(self, buy, sell, opens: bool) -> Optional[float]:
        e = self.entropy
        ref = e.book.mid()
        if not ref:
            return None
        cfg = self.cfg
        if opens:
            notional = min(cfg.max_order_notional,
                           self._headroom(buy, sell, ref))
            qty = notional / ref
        else:
            qty = min(cfg.max_order_notional / ref, abs(e.position))
        qty = floor_step(qty, self._step)
        if qty < self._min_base or qty * ref < self._min_notional:
            return None
        return qty

    def _maker_price(self, buy, sell, qty: float) -> Optional[float]:
        e, h = self.entropy, self.hedge
        sell_e = sell.key == "entropy"
        hurdle = (self._eff_threshold(buy, sell) + self.cfg.maker_fee_bps
                  + h.fee_bps)
        levels = h.book.sorted_asks() if sell_e else h.book.sorted_bids()
        return maker.quote_price(sell_e, levels, qty, hurdle,
                                 e.book.best_bid(), e.book.best_ask(),
                                 self.cfg.maker_inside_bps, e.px_round)

    def _maker_ok(self) -> bool:
        cfg = self.cfg
        return (not self.halted and self.risk_ready and not self.risk_blind
                and not self._venue_down and not self.flattening
                and not self.stopping and time.time() >= self.error_pause_until
                and self.entropy.book.is_fresh(cfg.staleness_sec)
                and self.hedge.book.is_fresh(cfg.staleness_sec))

    async def _maker_step(self, now: float) -> None:
        cfg = self.cfg
        e, h = self.entropy, self.hedge
        if cfg.maker_mode == "live":
            await self._maker_live_step(now)
            return
        if not self._maker_ok():
            self._mq.clear()
            return
        bb, ba = e.book.best_bid(), e.book.best_ask()
        for key, buy, sell in (("sell_entropy", h, e), ("buy_entropy", e, h)):
            opens = self._opens(buy, sell)
            action = "open" if opens else "close"
            q = self._mq.get(key)
            if q is not None and maker.shadow_filled(q, bb, ba, now):
                self._maker_shadow_fill(q, now)
                self._mq[key] = None
                continue
            qty = self._maker_qty(buy, sell, opens)
            if opens and qty and self.pauses.active(now):
                qty = None
            want = self._maker_price(buy, sell, qty) if qty else None
            if want is None or (q is not None and q.action != action):
                self._mq[key] = None
                continue
            if q is None:
                self._mq[key] = maker.Quote(key, action, want, qty, now, now)
                self.maker_stats.placed += 1
            elif maker.wants_move(q, want, now, cfg.maker_requote_bps,
                                  cfg.maker_requote_sec):
                q.px, q.qty, q.moved_ts = want, qty, now
                q.requotes += 1
                self.maker_stats.requotes += 1

    def _maker_shadow_fill(self, q: "maker.Quote", now: float) -> None:
        h = self.hedge
        levels = h.book.sorted_asks() if q.sell else h.book.sorted_bids()
        if not levels:
            return
        h_px, _ = walk_depth(levels, q.qty)
        edge = maker.edge_bps(q.sell, q.px, h_px, self.cfg.maker_fee_bps,
                              h.fee_bps)
        self.maker_stats.add_fill(q.action, edge)
        log.info("[maker/shadow] %s %s %.6g @ %.6g would have filled after "
                 "%.1fs (%d moves) — hedge %.6g, edge %+.2f bps", q.action,
                 "sell" if q.sell else "buy", q.qty, q.px, now - q.placed_ts,
                 q.requotes, h_px, edge)
        row = {"ts": now, "q": q, "h_px": h_px, "edge": edge,
               "e_mid": self.entropy.book.mid()}
        try:
            t = asyncio.get_running_loop().create_task(self._maker_marks(row))
        except RuntimeError:
            return
        self._maker_tasks.add(t)
        t.add_done_callback(self._maker_tasks.discard)

    async def _maker_marks(self, row: dict) -> None:
        q = row["q"]
        marks, waited = [], 0.0
        for at in maker.MARK_SEC:
            await asyncio.sleep(at - waited)
            waited = at
            m = self.entropy.book.mid()
            # positive = Entropy later moved in our favour after the fill
            marks.append("" if not m else
                         f"{((q.px / m - 1) if q.sell else (m / q.px - 1)) * 1e4:.2f}")
        try:
            m3 = float(marks[-1])
            self.maker_stats.mark3_sum += m3
            self.maker_stats.mark3_n += 1
        except ValueError:
            pass
        cfg = self.cfg
        try:
            journal.append_row(
                journal.path_in(cfg.trades_csv, "maker_shadow.csv"),
                maker.SHADOW_HEADER,
                [f"{row['ts']:.3f}", self.session_id, cfg.symbol,
                 cfg.hedge_venue, q.action, "sell" if q.sell else "buy",
                 f"{q.qty:.8g}", f"{q.px:.10g}", f"{q.placed_ts:.3f}",
                 f"{row['ts'] - q.placed_ts:.1f}", q.requotes,
                 f"{row['h_px']:.10g}", f"{row['edge']:.2f}",
                 "" if not row["e_mid"] else f"{row['e_mid']:.10g}",
                 *marks])
        except Exception:
            log.exception("maker_shadow.csv write failed")

    # live

    def _maker_close_side(self):
        """(key, buy, sell) of the trade that closes the Entropy position,
        or None when flat."""
        e, h = self.entropy, self.hedge
        if e.position > 0:
            return "sell_entropy", h, e
        if e.position < 0:
            return "buy_entropy", e, h
        return None

    async def _maker_live_step(self, now: float) -> None:
        cfg = self.cfg
        e = self.entropy
        q = self._mlive
        if q is not None and now - self._mlive_poll_ts >= MAKER_POLL_SEC:
            self._mlive_poll_ts = now
            await self._maker_poll(q)
            q = self._mlive
        side = self._maker_close_side()
        want = qty = None
        if side is not None and self._maker_ok() \
                and not self._vlock(e.key).locked():
            key, buy, sell = side
            qty = self._maker_qty(buy, sell, False)
            want = self._maker_price(buy, sell, qty) if qty else None
        if q is not None:
            gone = (want is None or side is None or side[0] != q.key)
            if gone or maker.wants_move(q, want, now, cfg.maker_requote_bps,
                                        cfg.maker_requote_sec) \
                    or abs(q.qty - q.filled - (qty or 0)) > self._step / 2 \
                    and now - q.moved_ts >= cfg.maker_requote_sec:
                if not await self._maker_cancel_live("requote" if not gone
                                                     else "no quote"):
                    return
                q = None
                if gone:
                    return
            else:
                await self._maker_deadman(now)
                return
        if want is None or self._req_slot_wait(e) > 0 \
                or not self._venue_rate_ok(e):
            if q is None and self._deadman_on:
                self._deadman_on = False
                await e.schedule_cancel(None)
            return
        key = side[0]
        await self._maker_deadman(now, force=True)
        if not self._deadman_on:
            return            # never a live order without the dead-man switch
        self._record_send(e)
        r = await e.send_post_only(is_buy=key == "buy_entropy", qty=qty,
                                   px=want, reduce_only=True)
        if r.get("status") == "resting":
            self._mlive = maker.Quote(key, "close", want, qty, now, now,
                                      oid=r["oid"])
            self.maker_stats.placed += 1
            log.info("[maker] resting %s %.6g @ %.6g (oid %s)",
                     "sell" if key == "sell_entropy" else "buy", qty, want,
                     r["oid"])
        elif r.get("status") == "filled":
            q = maker.Quote(key, "close", want, qty, now, now,
                            oid=r.get("oid"))
            await self._maker_hedge(q, float(r.get("filled_base") or 0.0))
        else:
            log.info("[maker] post-only refused: %s", r.get("err"))

    async def _maker_deadman(self, now: float, force: bool = False) -> None:
        """Keep Hyperliquid's cancel-all timer ahead of us while a live
        order rests: if the bot dies, its order dies with it."""
        if not force and now < self._deadman_next:
            return
        if force and self._deadman_on and now < self._deadman_next:
            return
        dm = self.cfg.maker_deadman_sec
        if await self.entropy.schedule_cancel(now + dm):
            self._deadman_on = True
            self._deadman_next = now + dm / 3.0
        else:
            self._deadman_on = False
            log.warning("[maker] dead-man switch not set — no live order "
                        "without it")
            if self._mlive is not None:
                await self._maker_cancel_live("no dead-man switch")

    async def _maker_poll(self, q: "maker.Quote") -> None:
        st = await self.entropy.order_state(q.oid)
        if st is None:
            return
        if st["filled"] > q.filled + 1e-12:
            delta = st["filled"] - q.filled
            q.filled = st["filled"]
            await self._maker_hedge(q, delta)
        if not st["open"]:
            self._mlive = None

    async def _maker_cancel_live(self, why: str) -> bool:
        """Cancel the live maker order (hedging anything that filled before
        the cancel landed). True when no live order is left."""
        q = self._mlive
        if q is None or q.oid is None:
            self._mlive = None
            return True
        e = self.entropy
        self._record_send(e)
        ok = await e.cancel_order(q.oid)
        await self._maker_poll(q)          # a fill that beat the cancel
        if ok:
            self._mlive = None
            log.info("[maker] canceled (%s)", why)
            if why != "requote" and self._deadman_on:
                self._deadman_on = False
                await e.schedule_cancel(None)
            return True
        log.warning("[maker] cancel failed (%s) — the dead-man switch "
                    "removes it if the bot stops", why)
        return False

    async def _maker_hedge(self, q: "maker.Quote", delta: float) -> None:
        """A live maker fill on Entropy: hedge the filled base with a taker
        on the hedge venue at once, and book it like any trade."""
        if delta <= 0:
            return
        e, h = self.entropy, self.hedge
        locks = [self._vlock(e.key), self._vlock(h.key)]
        for lk in locks:
            await lk.acquire()
        try:
            if q.sell:
                e.position -= delta
                e.cash += delta * q.px * (1 - self.cfg.maker_fee_bps / 1e4)
            else:
                e.position += delta
                e.cash -= delta * q.px * (1 + self.cfg.maker_fee_bps / 1e4)
            e.volume_usd += delta * q.px
            self.fees_usd += delta * q.px * self.cfg.maker_fee_bps / 1e4
            hq = floor_step(delta, self._step)
            h_buy = q.sell
            ref = h.book.best_ask() if h_buy else h.book.best_bid()
            bound, _ = self._fresh_hedge_bound(
                h, h_buy, hq, h.px_round(ref * (1 + (1 if h_buy else -1)
                                                * self.cfg.leg_slippage_bps
                                                / 1e4), not h_buy)
                if ref else 0.0)
            hinfo = {"status": "skipped", "filled_base": 0.0,
                     "avg_px": None, "err": None, "unresolved": False}
            if hq >= h.min_base and hq * (ref or 0) >= self._min_notional:
                self._record_send(h)
                try:
                    hinfo = await h.send_taker(is_buy=h_buy, qty=hq,
                                               limit_px=bound)
                except Exception as ex:   # noqa: BLE001
                    hinfo = {"status": "send-failed", "filled_base": 0.0,
                             "avg_px": None, "err": repr(ex),
                             "unresolved": False}
            hfill = hinfo.get("filled_base") or 0.0
            hpx = hinfo.get("avg_px") or ref or q.px
            hf = h.fee_bps / 1e4
            h.position += hfill if h_buy else -hfill
            if hfill:
                h.cash += (-1 if h_buy else 1) * hfill * hpx * \
                    (1 + hf if h_buy else 1 - hf)
                h.volume_usd += hfill * hpx
                self.fees_usd += hfill * hpx * hf
            matched = min(delta, hfill)
            edge = 0.0
            if matched > 0:
                mf = self.cfg.maker_fee_bps / 1e4
                edge = matched * ((q.px * (1 - mf) - hpx * (1 + hf)) if q.sell
                                  else (hpx * (1 - hf) - q.px * (1 + mf)))
                self.total_fill_edge += edge
                self._watch_profit(self._edge_vs_centre(
                    q.key, edge, matched * q.px), matched * q.px)
                self.trades += 1
                if self.vol_cost is not None:
                    self.vol_cost.add(matched * q.px, edge)
            self.maker_stats.add_fill("close", maker.edge_bps(
                q.sell, q.px, hpx, self.cfg.maker_fee_bps, h.fee_bps))
            direction = q.key
            buy, sell = (h, e) if q.sell else (e, h)
            bpx, spx = (hpx, q.px) if q.sell else (q.px, hpx)
            plan = ArbPlan(qty=delta, buy_limit=bpx, sell_limit=spx,
                           buy_notional=delta * bpx, sell_notional=delta * spx,
                           q_max=delta, q_max_notional=delta * bpx,
                           top_premium_bps=(spx / bpx - 1) * 1e4,
                           marginal_premium_bps=(spx / bpx - 1) * 1e4,
                           buy_fee=(hf if q.sell else
                                    self.cfg.maker_fee_bps / 1e4),
                           sell_fee=(self.cfg.maker_fee_bps / 1e4 if q.sell
                                     else hf))
            ok = hinfo.get("err") is None and not hinfo.get("unresolved")
            log.info("[maker] filled %s %.6g @ %.6g on Entropy, hedge %s "
                     "%.6g @ %s — edge $%.4f", "sell" if q.sell else "buy",
                     delta, q.px, hinfo["status"], hfill,
                     f"{hpx:.6g}" if hfill else "-", edge)
            if hinfo.get("err"):
                log.error("[%s] maker hedge: %s", h.name, hinfo["err"])
            self._record_trade(direction, plan, edge, f"maker/{hinfo['status']}",
                               ok)
            self._log_csv(direction, buy, sell, plan, ok,
                          delta if not q.sell else hfill,
                          hfill if not q.sell else delta,
                          "maker" if not q.sell else hinfo["status"],
                          hinfo["status"] if not q.sell else "maker", edge,
                          0.0, bpx, spx, None, "close", mode="maker")
            e.last_traded_ts = h.last_traded_ts = time.time()
            self.last_trade_ts = time.time()
            unresolved = bool(hinfo.get("unresolved"))
        finally:
            for lk in locks:
                lk.release()
        if unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        self._note_unhedged()
        self._risk_evt.set()

    def _fresh_hedge_bound(self, h, is_buy: bool, qty: float,
                           old: float) -> tuple:
        """Hedge price bound re-read from the hedge book as it is now (after
        the Entropy fill), with the usual leg_slippage pad. The more
        permissive of this and the bound planned before the Entropy order:
        the hedge must fill. Returns (bound, repriced)."""
        levels = h.book.sorted_asks() if is_buy else h.book.sorted_bids()
        if not levels or qty <= 0:
            return old, False
        px, _ = walk_depth(levels, qty)
        pad = self.cfg.leg_slippage_bps / 1e4
        if is_buy:
            new = h.px_round(px * (1 + pad), round_up=False)
            return (new, True) if new > old else (old, False)
        new = h.px_round(px * (1 - pad), round_up=True)
        return (new, True) if new < old else (old, False)

    async def _send_entropy_first(self, buy, sell, plan: ArbPlan,
                                  buy_bound: float, sell_bound: float,
                                  diag: Optional[dict] = None):
        """Strategies 2 and 3 ("entropy_first", "volume"): the
        Entropy IOC goes alone, limited to _ef_limit_bps worse than the plan
        (from the trade's profit, or a fixed entropy_first_slip_bps); the
        hedge is sent only for the quantity that filled, priced from its
        book as it is after the fill. A signal that has already vanished
        simply does not fill — no hedge, no one-legged position, no
        slippage. Returns (buy_info, sell_info, timing, missed)."""
        e = self.entropy
        e_is_buy = buy.key == e.key
        h = sell if e_is_buy else buy
        lim_bps = self._ef_limit_bps(buy, sell, plan)
        if diag is not None:
            diag["e_limit_bps"] = lim_bps
        tight = lim_bps / 1e4
        if e_is_buy:
            e_bound = e.px_round(plan.buy_limit * (1 + tight), round_up=False)
            h_bound = sell_bound
        else:
            e_bound = e.px_round(plan.sell_limit * (1 - tight), round_up=True)
            h_bound = buy_bound

        def failed(ex) -> dict:
            return {"status": "send-failed", "filled_base": 0.0,
                    "avg_px": None, "err": repr(ex), "unresolved": False}
        hinfo = {"status": "skipped", "filled_base": 0.0, "avg_px": None,
                 "err": None, "unresolved": False}
        self.ef_attempts += 1
        t0 = time.time()
        self._record_send(e)
        try:
            einfo = await e.send_taker(is_buy=e_is_buy, qty=plan.qty,
                                       limit_px=e_bound)
        except Exception as ex:   # noqa: BLE001 — reported like any leg
            einfo = failed(ex)
        t_e = time.time()
        efill = einfo.get("filled_base") or 0.0
        missed = False
        h_ms = 0.0
        if einfo.get("unresolved"):
            log.warning("[ENTROPY-FIRST] Entropy outcome unknown — no hedge "
                        "sent; reconcile will decide / исход ноги Entropy "
                        "неизвестен, хедж не отправлен")
        elif efill <= 0:
            missed = einfo.get("err") is None
            if missed:
                self.ef_misses += 1
                log.info("[ENTROPY-FIRST] Entropy %s did not fill within "
                         "%.1f bps of the plan — nothing sent to %s",
                         "buy" if e_is_buy else "sell", lim_bps, h.name)
        else:
            hq = floor_step(efill, self._step)
            h_bound, repriced = self._fresh_hedge_bound(
                h, not e_is_buy, hq, h_bound)
            if diag is not None:
                diag["h_repriced"] = repriced
            if repriced:
                log.info("[ENTROPY-FIRST] %s book moved during the Entropy "
                         "order — hedge limit widened to %.6g", h.name, h_bound)
            if hq < h.min_base or hq * h_bound < self._min_notional:
                log.warning("[ENTROPY-FIRST] Entropy filled %.6g — too small "
                            "to hedge now; the net-delta hedge handles it",
                            efill)
            else:
                self._record_send(h)
                try:
                    hinfo = await h.send_taker(is_buy=not e_is_buy, qty=hq,
                                               limit_px=h_bound)
                except Exception as ex:   # noqa: BLE001
                    hinfo = failed(ex)
                h_ms = (time.time() - t_e) * 1e3
        e_ms = (t_e - t0) * 1e3
        binfo, sinfo = (einfo, hinfo) if e_is_buy else (hinfo, einfo)
        timing = {"buy_ms": e_ms if e_is_buy else h_ms,
                  "sell_ms": h_ms if e_is_buy else e_ms,
                  "gap_ms": h_ms}
        return binfo, sinfo, timing, missed

    def _learn_slippage(self, buy, sell, plan, binfo, sinfo, bfill,
                        sfill) -> None:
        """One realized-slippage sample per filled leg (planned average price
        of the plan vs the fill's average price)."""
        if self.slip is None or not plan.qty:
            return
        try:
            if bfill and binfo.get("avg_px"):
                self.slip.add(buy.key, leg_slip_bps(
                    True, binfo["avg_px"], plan.buy_notional / plan.qty))
            if sfill and sinfo.get("avg_px"):
                self.slip.add(sell.key, leg_slip_bps(
                    False, sinfo["avg_px"], plan.sell_notional / plan.qty))
        except Exception:
            log.exception("slippage sample failed")

    # ---------------------------------------------------------- diagnostics

    @staticmethod
    def _mid_prem(e_mid, h_mid):
        if not e_mid or not h_mid:
            return None
        return (e_mid / h_mid - 1.0) * 1e4

    def _diag_start(self, direction: str, plan: ArbPlan) -> dict:
        """What the books looked like when the signal fired."""
        now = time.time()
        e, h = self.entropy, self.hedge

        def age(v):
            ts = getattr(v.book, "last_update_ts", 0) or 0
            return (now - ts) * 1e3 if ts else None
        return {"ts": now, "direction": direction,
                "signal_age_ms": self._signal_age.get(direction, 0.0) * 1e3,
                "e_age_ms": age(e), "h_age_ms": age(h),
                "top_prem": plan.top_premium_bps,
                "prem0": self._mid_prem(e.book.mid(), h.book.mid()),
                # the Entropy leg's planned average price
                "planned_px": ((plan.buy_notional if direction == "buy_entropy"
                                else plan.sell_notional) / plan.qty
                               if plan.qty else None)}

    def _diag_finish(self, diag: dict, binfo, sinfo, timing, missed: bool,
                     unresolved: bool) -> None:
        """Re-read the premium and the Entropy price 1 s and 3 s later in the
        background and write one row to logs/exec_diag.csv: did the signal
        survive, and where did Entropy trade afterwards (markout)."""
        e_buy = diag["direction"] == "buy_entropy"
        einfo = binfo if e_buy else sinfo
        efill = einfo.get("filled_base") or 0.0
        if unresolved:
            outcome = "unknown"
        elif missed:
            outcome = "missed"
        elif efill <= 0:
            outcome = "failed"
        else:
            hinfo = sinfo if e_buy else binfo
            hfill = hinfo.get("filled_base") or 0.0
            outcome = "filled" if abs(hfill - efill) < 1e-12 else "uneven"
        diag.update(outcome=outcome, e_buy=e_buy,
                    e_px=einfo.get("avg_px") if efill else None,
                    e_ms=timing.get("buy_ms" if e_buy else "sell_ms"),
                    h_ms=timing.get("sell_ms" if e_buy else "buy_ms"))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        t = loop.create_task(self._diag_marks(diag))
        self._diag_tasks.add(t)
        t.add_done_callback(self._diag_tasks.discard)

    async def _diag_marks(self, d: dict) -> None:
        marks = {}
        waited = 0.0
        for at in DIAG_MARKS_SEC:
            await asyncio.sleep(at - waited)
            waited = at
            e_mid = self.entropy.book.mid()
            marks[at] = (self._mid_prem(e_mid, self.hedge.book.mid()), e_mid)
        cfg = self.cfg

        def mark(at):
            e_mid = marks[at][1]
            if not d.get("e_px") or not e_mid:
                return ""
            # positive = Entropy later traded in our favour
            m = (e_mid / d["e_px"] - 1.0) if d["e_buy"] else \
                (d["e_px"] / e_mid - 1.0)
            return f"{m * 1e4:.2f}"

        def num(x, n=2):
            return "" if x is None else f"{x:.{n}f}"
        e_slip = ""
        if d.get("e_px") and d.get("planned_px"):
            e_slip = f"{leg_slip_bps(d['e_buy'], d['e_px'], d['planned_px']):.2f}"
        row = [f"{d['ts']:.3f}", self.session_id, cfg.symbol, cfg.hedge_venue,
               getattr(cfg, "exec_mode", "simultaneous"), d["direction"],
               d["outcome"], num(d["signal_age_ms"], 0), num(d["e_age_ms"], 0),
               num(d["h_age_ms"], 0), num(d["top_prem"]), num(d["prem0"]),
               num(marks[DIAG_MARKS_SEC[0]][0]),
               num(marks[DIAG_MARKS_SEC[-1]][0]),
               "" if not d.get("e_px") else f"{d['e_px']:.10g}", e_slip,
               mark(DIAG_MARKS_SEC[0]), mark(DIAG_MARKS_SEC[-1]),
               num(d.get("e_ms"), 0), num(d.get("h_ms"), 0),
               num(d.get("imbalance")), num(d.get("e_limit_bps")),
               "" if d.get("h_repriced") is None else
               ("1" if d["h_repriced"] else "0")]
        try:
            journal.append_row(journal.path_in(cfg.trades_csv, "exec_diag.csv"),
                               DIAG_HEADER, row)
        except Exception:
            log.exception("exec_diag.csv write failed")

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool) -> None:
        self.recent_trades.append({
            "ts": time.time(), "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd, "fill": fill_edge, "status": status,
            "ok": ok})

    async def _maybe_hedge(self) -> None:
        net = sum(v.position for v in self.venues.values())
        if abs(net) > self.cfg.net_tolerance_base:
            await self._hedge(net)

    async def _hedge(self, net: float) -> None:
        """Reduce the venue that carries the imbalance back toward net zero
        (reduce-only taker with hedge_slippage_bps price protection)."""
        cfg = self.cfg
        is_sell = net > 0
        sgn = 1.0 if net > 0 else -1.0
        slip = cfg.hedge_slippage_bps / 1e4
        for v in sorted(self.venues.values(),
                        key=lambda x: (self._venue_limited(x)
                                       or self._req_slot_wait(x) > 0,
                                       -x.position * sgn)):
            if v.position * sgn <= 0:
                continue
            if v.key in self._venue_down \
                    or not v.book.is_fresh(cfg.staleness_sec):
                continue  # unreachable or blind: cannot hedge here
            lk = self._vlock(v.key)
            if lk.locked():
                continue
            qty = floor_step(min(abs(net), abs(v.position)), self._step)
            if qty < v.min_base:
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            limit = v.px_round(ref * (1 - slip), False) if is_sell \
                else v.px_round(ref * (1 + slip), True)
            if qty * limit < max(cfg.min_order_notional, v.min_quote):
                continue
            await lk.acquire()  # verified free, no awaits since: fast path
            try:
                log.warning("[HEDGE] net %+.6g — %s %.6g on %s @%.6g",
                            net, "SELL" if is_sell else "BUY", qty, v.name, limit)
                self.hedges += 1
                self._record_send(v)  # counts toward the budget, never blocked
                info = await v.send_taker(is_buy=not is_sell, qty=qty,
                                          limit_px=limit, reduce_only=True)
                try:
                    journal.append_row(
                        journal.path_in(cfg.trades_csv, "hedges.csv"),
                        journal.HEDGES_HEADER,
                        [f"{time.time():.3f}", self.session_id, "hedge",
                         v.name, "SELL" if is_sell else "BUY", f"{qty:.8g}",
                         f"{float(info.get('filled_base') or 0):.8g}",
                         journal.fmt(info.get("avg_px"), 8),
                         journal.fmt(limit, 8), info.get("status", ""),
                         info.get("err") or "", cfg.symbol,
                         cfg.hedge_venue])
                except Exception:
                    log.exception("hedges.csv write failed")
                if info.get("err") or info.get("unresolved"):
                    log.error("[HEDGE] %s: %s", v.name,
                              info.get("err") or "unresolved")
                    if str(info.get("err", "")).startswith("RATE_LIMITED"):
                        self._on_rate_limited(v)
                    self._reconcile_evt.set()
                else:
                    fill = info["filled_base"]
                    v.position += -fill if is_sell else fill
                    if fill:
                        px = info.get("avg_px") or limit
                        fee = v.fee_bps / 1e4
                        v.cash += fill * px * (1 - fee) if is_sell \
                            else -fill * px * (1 + fee)
                        v.volume_usd += fill * px
                        self.fees_usd += fill * px * fee
                    log.info("[HEDGE SETTLED] %s %s %.6g/%.6g",
                             v.name, info["status"], fill, qty)
                v.last_traded_ts = time.time()
            finally:
                lk.release()
            return
        self._note_unhedged()
        now = time.time()
        last = self._last_dust_log
        if last is None or abs(last[0] - net) > 1e-12 \
                or now - last[1] >= DUST_LOG_SEC:
            self._last_dust_log = (net, now)
            log.warning("[HEDGE] net %+.6g below hedgeable minimum — carrying "
                        "(next reconcile retries; closed at Stop) / остаток "
                        "ниже минимальной сделки — закроется при Стопе", net)

    # ------------------------------------------- session, loss limit, flatten

    def _spawn_exec(self, coro) -> asyncio.Task:
        """Run coro as a tracked execution: shutdown waits for it instead of
        cancelling it (an emergency close must not be cut in half)."""
        t = asyncio.create_task(coro)
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        return t

    def _ref_px(self, v) -> Optional[float]:
        m = v.book.mid()
        if m is not None:
            return m
        for o in self.venues.values():
            m = o.book.mid()
            if m is not None:
                return m
        return None

    def _is_dust(self, v, pos: float) -> bool:
        """Below what the venue itself can order: not an open position for
        flatten / flat checks (it is still reported, with its $ value)."""
        if pos == 0:
            return True
        if floor_step(abs(pos), self._step) < max(v.min_base, 1e-12):
            return True
        px = self._ref_px(v)
        return px is not None and abs(pos) * px < v.min_quote

    def _pos_desc(self, v, pos: Optional[float]) -> str:
        if pos is None:
            return f"{v.name} позиция неизвестна (не читается)"
        px = self._ref_px(v)
        usd = f" (~${abs(pos) * px:.2f})" if px else ""
        side = "LONG" if pos > 0 else "SHORT"
        return f"{v.name} {side} {abs(pos):.6g}{usd}"

    async def _fetch_positions(self) -> Dict[str, Optional[float]]:
        vs = list(self.venues.values())
        got = await asyncio.gather(
            *(asyncio.wait_for(v.fetch_position(), RISK_READ_TIMEOUT_SEC)
              for v in vs), return_exceptions=True)
        out: Dict[str, Optional[float]] = {}
        for v, r in zip(vs, got):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, BaseException):
                log.warning("[%s] position read failed: %r", v.name, r)
                out[v.key] = None
            else:
                out[v.key] = float(r)
        return out

    async def _read_risk_sample(self):
        """Both venues' equity read in parallel, from the exchanges.
        Returns (Sample, {venue: equity}) or None (a miss): any failure, or
        responses further apart than max_read_skew_sec."""
        vs = list(self.venues.values())

        async def one(v):
            r = await asyncio.wait_for(v.fetch_risk_equity(),
                                       RISK_READ_TIMEOUT_SEC)
            return r, time.time()

        got = await asyncio.gather(*(one(v) for v in vs),
                                   return_exceptions=True)
        vals: Dict[str, float] = {}
        as_of: Dict[str, float] = {}
        done: List[float] = []
        for v, r in zip(vs, got):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, BaseException):
                log.warning("[risk] %s equity read failed: %r", v.name, r)
                return None
            (val, ts), t_done = r
            vals[v.key], as_of[v.key] = float(val), float(ts)
            done.append(t_done)
        skew = max(done) - min(done)
        if skew > self.cfg.risk_max_read_skew_sec:
            log.warning("[risk] equity reads %.1fs apart (> %.1fs) — sample "
                        "discarded", skew, self.cfg.risk_max_read_skew_sec)
            return None
        return Sample(total=sum(vals.values()), as_of=as_of,
                      wall=time.time()), vals

    # ------------------------------------------------------------- watch

    def _alert(self, key: str, text: str) -> None:
        """One Telegram message per event (repeats of the same event are
        held back for a while). Never blocks the caller."""
        if self.telegram is None or not self.alerts.due(key):
            return
        try:
            t = asyncio.get_running_loop().create_task(self._tg_send(text))
        except RuntimeError:
            return
        self._alert_tasks.add(t)
        t.add_done_callback(self._alert_tasks.discard)

    def _edge_vs_centre(self, direction: str, fill_edge: float,
                        notional: float) -> float:
        """A trade's fill edge measured from the centre of the band.

        A raw fill edge contains the premium LEVEL: on a pair whose premium
        sits at −250 bps, every sell of Entropy shows −$0.5 and every buy
        +$0.5, and only the round trip is a profit. Summed over a window of
        uneven sells and buys the raw edges say "losing" when the trades
        are fine. Subtracting the centre leaves what the trade earned over
        its own entry point: sell: (premium − centre), buy: (centre −
        premium), times the notional."""
        c = self.mid_bps() / 1e4 * notional
        return fill_edge - c if direction == "sell_entropy" else fill_edge + c

    def _watch_profit(self, fill_edge: float, notional: float = 0.0) -> None:
        """fill_edge here is already measured from the centre
        (_edge_vs_centre)."""
        now = time.time()
        self.lowprofit.add(now, fill_edge, notional)
        bad = self.lowprofit.losing(now)
        if bad is None or "lowprofit" in self.pauses.active(now):
            return
        hours = float(getattr(self.cfg, "lowprofit_pause_hours", 2.0))
        self.pauses.set("lowprofit", now + hours * 3600.0)
        self.lowprofit.reset()
        n, total = bad
        log.warning("[watch] last %.0f h: %d trades, $%.4f in sum — no new "
                    "positions for %.1f h (closing continues) / сделки в "
                    "минусе, новые позиции на паузе", self.lowprofit.hours,
                    n, total, hours)
        self._alert("lowprofit", (
            f"⏸ Сделки последних {self.lowprofit.hours:g} ч в минусе: {n} "
            f"сделок, {tgmsg.money(total)}. Новые позиции на паузе "
            f"{hours:g} ч, закрытие идёт как обычно. Команда /resume снимет "
            f"паузу."))

    def _watch_drawdown(self, total: Optional[float]) -> None:
        pct = float(getattr(self.cfg, "drawdown_24h_pct", 0.0))
        now = time.time()
        if pct <= 0 or total is None or now - self._last_dd_check < 60.0:
            return
        self._last_dd_check = now
        try:
            peak = watch.equity_peak(
                journal.path_in(self.cfg.trades_csv, "equity.csv"), now)
        except Exception:
            log.exception("[watch] equity.csv unreadable")
            return
        peak = total if peak is None else max(peak, total)
        dd = watch.drawdown_pct(peak, total)
        if dd is None:
            return
        held = "drawdown" in self.pauses.active(now)
        if dd >= pct and not held:
            self.pauses.set("drawdown")
            log.warning("[watch] equity %.2f is %.2f%% below its 24h peak "
                        "%.2f — no new positions / просадка за сутки",
                        total, dd, peak)
            self._alert("drawdown", (
                f"⏸ Просадка за сутки {dd:.1f}% (баланс ${total:.2f}, "
                f"максимум ${peak:.2f}, порог {pct:g}%). Новые позиции на "
                f"паузе, закрытие идёт. Пауза снимется сама при просадке "
                f"меньше {pct / 2:g}% или командой /resume."))
        elif held and dd < pct / 2.0:
            self.pauses.clear("drawdown")
            log.warning("[watch] drawdown back to %.2f%% — opening allowed",
                        dd)
            self._alert("drawdown-ok", f"▶️ Просадка уменьшилась до "
                        f"{dd:.1f}% — новые позиции снова разрешены.")

    async def _heartbeat_loop(self) -> None:
        hours = float(getattr(self.cfg, "heartbeat_hours", 0.0))
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), hours * 3600.0)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._tg_send("💓 Бот работает\n" + tgmsg.status_text(self))
            except Exception:
                log.exception("heartbeat failed")

    def _tg_control(self, cmd: str) -> Optional[str]:
        """/pause and /resume from the owner's chat; None for others."""
        if cmd == "/pause":
            self.pauses.set("manual")
            log.warning("[telegram] /pause — no new positions until /resume")
            return ("⏸ Новые позиции на паузе. Закрытие идёт как обычно. "
                    "Снять паузу: /resume")
        if cmd == "/resume":
            gone = self.pauses.clear()
            was_err = self.error_pause_until > time.time()
            self.error_pause_until = 0.0
            self.lowprofit.reset()
            log.warning("[telegram] /resume — pauses cleared: %s",
                        ", ".join(gone) or "none")
            self._update_evt.set()
            if not gone and not was_err:
                return "Пауз не было — бот торгует." + (
                    " (Торговля ОСТАНОВЛЕНА: " + self.halt_reason +
                    " — нужен перезапуск.)" if self.halted else "")
            return "▶️ Пауза снята, бот снова открывает позиции." + (
                " (Но торговля ОСТАНОВЛЕНА: " + self.halt_reason +
                " — нужен перезапуск.)" if self.halted else "")
        return None

    def _risk_halt(self, reason: str, msg: str, *args) -> None:
        self.halted = True
        self.halt_reason = reason
        log.critical(msg, *args)
        text = {"loss limit": "🛑 СТОП ПО УБЫТКУ: торговля остановлена, бот "
                              "закрывает позиции.",
                "risk init failed": "🛑 Стоп по убытку не запустился "
                                    "(баланс не прочитан): торговля "
                                    "выключена."}.get(reason,
                                                      f"🛑 Торговля остановлена: "
                                                      f"{reason}.")
        self._alert("halt", text)

    def _take_sample(self, sample: Sample, vals: Dict[str, float]) -> None:
        self.risk_last_sample = sample
        self._watch_drawdown(sample.total)
        self.last_equity = dict(vals)
        for k, val in vals.items():
            v = self.venues[k]
            # a hedge sharing the HL account reports 0 (counted on entropy)
            if getattr(v, "include_core_equity", True):
                v.equity = val
        now = time.time()
        if now - self._last_equity_log >= EQUITY_LOG_SEC:
            self._last_equity_log = now
            try:
                journal.append_row(
                    journal.path_in(self.cfg.trades_csv, "equity.csv"),
                    journal.EQUITY_HEADER,
                    [f"{now:.3f}", self.session_id,
                     journal.fmt(vals.get("entropy")),
                     journal.fmt(vals.get("hedge")),
                     journal.fmt(sample.total),
                     journal.fmt(self.session_result()),
                     f"{self.entropy.position:.8g}",
                     f"{self.hedge.position:.8g}", self.cfg.symbol,
                     self.cfg.hedge_venue])
            except Exception:
                log.exception("equity.csv write failed")

    def session_result(self) -> Optional[float]:
        """Session PnL from real balances: Σ equity now − Σ equity at Start."""
        s = self.risk_last_sample
        if s is None or self.session_base_total is None:
            return None
        return s.total - self.session_base_total

    def risk_loss(self) -> Optional[float]:
        r = self.session_result()
        return None if r is None else -r

    def turnover_usd(self) -> float:
        return sum(v.volume_usd for v in self.venues.values())

    def edge_compare(self):
        """(trades, planned $, realized $) of this session's arbitrage
        trades: what the bot expected to earn on them at the moment of the
        decision vs what the fills actually gave. None before the first
        trade. The gap is the loss to slippage and latency."""
        if not self.cmp_n:
            return None
        return self.cmp_n, self.cmp_exp, self.cmp_fill

    async def _risk_init(self) -> bool:
        """Session baseline: both venues' equity at Start, from the exchanges.
        With a loss limit set, trading waits for it."""
        cfg = self.cfg
        deadline = time.time() + 30.0
        while not self.stop.is_set() and time.time() < deadline and not all(
                v.book.is_fresh(cfg.staleness_sec)
                for v in self.venues.values()):
            await asyncio.sleep(0.5)
        while not self.stop.is_set():
            res = await self._read_risk_sample()
            if res is not None:
                break
            log.warning("[session] start balance not read yet%s — retry in "
                        "%.0fs", " — trading waits" if self.risk_enabled
                        else "", cfg.risk_check_sec)
            try:
                await asyncio.wait_for(self.stop.wait(), cfg.risk_check_sec)
            except asyncio.TimeoutError:
                pass
        else:
            return False
        sample, vals = res
        self.session_base = dict(vals)
        self.session_base_total = sample.total
        self._take_sample(sample, vals)
        log.warning("[session] start balance $%.2f (%s)", sample.total,
                    " + ".join(f"{self.venues[k].name} ${val:.2f}"
                               for k, val in vals.items()))
        if self.risk_enabled:
            self.risk_guard = LossGuard(sample.total, cfg.max_loss_pct,
                                        cfg.risk_confirm_interval_sec)
            log.warning("[risk] loss limit for this session: %.2f%% = $%.4f "
                        "/ стоп по убытку при −$%.2f", cfg.max_loss_pct,
                        self.risk_guard.limit_usd, self.risk_guard.limit_usd)
        else:
            log.warning("[risk] loss limit OFF (risk.max_loss_pct = 0) / стоп "
                        "по убытку выключен")
        self.risk_ready = True
        self._risk_evt.set()
        self._update_evt.set()
        return True

    async def _risk_loop(self) -> None:
        cfg = self.cfg
        try:
            if self.session_base_total is None and not await self._risk_init():
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[session] start balance failed")
            if self.risk_enabled:
                self._risk_halt("risk init failed",
                                "HALTED (risk): loss limit could not start — "
                                "trading off / стоп по убытку не запустился")
            return
        g = self.risk_guard
        while not self.stop.is_set():
            pending = g is not None and g.pending is not None
            timeout = (cfg.risk_confirm_interval_sec if pending
                       else cfg.risk_check_sec)
            try:
                await asyncio.wait_for(self._risk_evt.wait(), timeout)
            except asyncio.TimeoutError:
                pass
            self._risk_evt.clear()
            if self.stop.is_set():
                break
            gap = time.time() - self._risk_last_read
            if gap < RISK_MIN_GAP_SEC:
                await asyncio.sleep(RISK_MIN_GAP_SEC - gap)
            self._risk_last_read = time.time()
            try:
                res = await self._read_risk_sample()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[risk] read failed: %r", e)
                res = None
            if res is None:
                self._risk_misses += 1
                if (self.risk_enabled and not self.risk_blind and
                        self._risk_misses >= cfg.risk_max_equity_misses):
                    self.risk_blind = True
                    log.critical("[risk] equity unreadable %d times in a row "
                                 "— trading PAUSED (no flatten: the loss cannot "
                                 "be judged blind); resumes when readings "
                                 "return / баланс не читается — торговля на "
                                 "паузе", self._risk_misses)
                    self._alert("blind", "⏸ Баланс бирж не читается — "
                                "торговля на паузе, позиции не тронуты. "
                                "Продолжится сама, когда баланс прочитается.")
                continue
            sample, vals = res
            self._risk_misses = 0
            if self.risk_blind:
                self.risk_blind = False
                log.warning("[risk] equity readable again — trading RESUMED")
                self._alert("blind-ok", "▶️ Баланс снова читается — "
                            "торговля продолжена.")
                self._update_evt.set()
            self._take_sample(sample, vals)
            if g is None or self.halt_reason == "loss limit" or self.stopping:
                continue  # no limit / already tripped / closing at Stop
            act = g.observe(sample)
            loss = g.last_loss
            if act == PENDING:
                log.warning("[risk] loss $%.4f ≥ limit $%.4f — confirming with "
                            "a second reading in %.0fs", loss, g.limit_usd,
                            cfg.risk_confirm_interval_sec)
            elif act == WAIT:
                ages = ", ".join(f"{self.venues[k].name} {time.time() - t:.0f}s"
                                 for k, t in sample.as_of.items())
                log.info("[risk] still over the limit ($%.4f), waiting for a "
                         "newer reading (data age: %s)", loss, ages)
            elif act == NOISE:
                log.warning("[risk] single dip: loss back under the limit "
                            "($%.4f < $%.4f) — possible measurement noise, stop "
                            "NOT triggered / одиночная просадка, возможно шум",
                            loss, g.limit_usd)
            elif act == TRIP:
                await self._trip(loss)

    async def _trip(self, loss: float) -> None:
        g = self.risk_guard
        self._risk_halt(
            "loss limit",
            "HALTED (loss limit) / СТОП ПО УБЫТКУ: loss $%.4f ≥ limit $%.4f "
            "(%.2f%% of $%.2f), confirmed twice — closing both legs / "
            "закрываю позиции", loss, g.limit_usd, self.cfg.max_loss_pct,
            g.base_total)
        await asyncio.shield(self._spawn_exec(self._flatten("loss limit")))

    async def _flatten(self, why: str) -> bool:
        """Close both legs from the positions the EXCHANGES report, with
        reduce-only takers, then verify. True = both legs closed (a remainder
        below the venue's own minimum order aside, which is reported)."""
        self.flattening = True
        try:
            await self._maker_cancel_live(why)
            me = asyncio.current_task()
            others = {t for t in self._exec_tasks if t is not me}
            if others:  # let in-flight executions settle first
                await asyncio.wait(others,
                                   timeout=self.cfg.settle_timeout_sec + 2.0)
            locks = [self._vlock(v.key) for v in self.venues.values()]
            for lk in locks:  # reconcile/hedge must not interleave
                await lk.acquire()
            try:
                ok = await self._flatten_locked(why)
            finally:
                for lk in locks:
                    lk.release()
            self.flatten_failed = not ok
            return ok
        finally:
            self.flattening = False

    def _adopt_position(self, v, pos: float) -> None:
        delta = pos - v.position
        if abs(delta) > 1e-12:
            mid = v.book.mid()
            if mid is not None:
                v.cash -= delta * mid
            v.position = pos

    def _log_single(self, reason: str, v, is_sell: bool, qty: float,
                    info: dict, limit: float) -> None:
        """One single-leg order (hedge / flatten) into hedges.csv, and its
        fill into turnover and the fee estimate."""
        fill = float(info.get("filled_base") or 0.0)
        px = info.get("avg_px") or limit
        if fill:
            v.volume_usd += fill * px
            self.fees_usd += fill * px * v.fee_bps / 1e4
        try:
            journal.append_row(
                journal.path_in(self.cfg.trades_csv, "hedges.csv"),
                journal.HEDGES_HEADER,
                [f"{time.time():.3f}", self.session_id, reason, v.name,
                 "SELL" if is_sell else "BUY", f"{qty:.8g}", f"{fill:.8g}",
                 journal.fmt(info.get("avg_px"), 8), journal.fmt(limit, 8),
                 info.get("status", ""), info.get("err") or "",
                 self.cfg.symbol, self.cfg.hedge_venue])
        except Exception:
            log.exception("hedges.csv write failed")

    def hedge_symbol(self) -> str:
        """Market name actually used on the hedge venue (may differ from the
        menu ticker, e.g. ANTH vs ANTHROPIC)."""
        conf = getattr(self.hedge, "conf", None)
        return getattr(conf, "symbol", None) or self.cfg.symbol

    def _flatten_tags(self, why: str):
        if why == "stop":
            return "[STOP] / ОСТАНОВКА", "[STOP] / ОСТАНОВКА"
        if why == "loss limit":
            return ("HALTED (loss limit) / СТОП ПО УБЫТКУ",
                    "HALTED (loss limit) / СТОП ПО УБЫТКУ")
        return f"HALTED ({why})", f"HALTED ({why})"

    async def _flatten_locked(self, why: str) -> bool:
        cfg = self.cfg
        slip = cfg.flatten_slippage_bps / 1e4
        ok_tag, bad_tag = self._flatten_tags(why)
        for attempt in range(1, cfg.flatten_attempts + 1):
            pos = await self._fetch_positions()
            for k, p in pos.items():
                if p is not None:
                    self._adopt_position(self.venues[k], p)
            open_ = {k: p for k, p in pos.items()
                     if p is None or not self._is_dust(self.venues[k], p)}
            orderable = {k: p for k, p in pos.items()
                         if p and floor_step(abs(p), self._step)
                         >= max(self.venues[k].min_base, 1e-12)}
            # done when nothing real is open; a sub-minimum remainder gets
            # one try on the first attempt only
            if not open_ and (attempt > 1 or not orderable):
                break
            log.warning("[FLATTEN] %s — attempt %d/%d: %s", why, attempt,
                        cfg.flatten_attempts,
                        "; ".join(self._pos_desc(self.venues[k], p)
                                  for k, p in pos.items() if p != 0))
            sends = []
            # every orderable remainder, including ones under the venue's
            # min order value (a reduce-only close may still be accepted);
            # unknown sizes are never sent blind
            for k, p in pos.items():
                if p is None or p == 0:
                    continue
                v = self.venues[k]
                is_sell = p > 0
                qty = floor_step(abs(p), self._step)
                if qty < max(v.min_base, 1e-12):
                    continue
                ref = (v.book.best_bid() if is_sell else v.book.best_ask()) \
                    or v.book.mid()
                if ref is None:
                    log.error("[FLATTEN] %s: no price to close against", v.name)
                    continue
                if not v.book.is_fresh(cfg.staleness_sec):
                    log.warning("[FLATTEN] %s: book is stale, closing anyway "
                                "(slippage cap %.0f bps)", v.name,
                                cfg.flatten_slippage_bps)
                limit = v.px_round(ref * (1 - slip), False) if is_sell \
                    else v.px_round(ref * (1 + slip), True)
                self._record_send(v)
                sends.append((v, is_sell, qty, limit, v.send_taker(
                    is_buy=not is_sell, qty=qty, limit_px=limit,
                    reduce_only=True)))
            got = await asyncio.gather(*(s[4] for s in sends),
                                       return_exceptions=True)
            for (v, is_sell, qty, limit, _), r in zip(sends, got):
                v.last_traded_ts = time.time()
                if isinstance(r, BaseException):
                    log.error("[FLATTEN] %s %s %.6g: %r", v.name,
                              "SELL" if is_sell else "BUY", qty, r)
                    r = {"status": "exception", "filled_base": 0.0,
                         "avg_px": None, "err": repr(r)}
                if str(r.get("err", "")).startswith("RATE_LIMITED"):
                    self._on_rate_limited(v)
                self._log_single(f"flatten:{why}", v, is_sell, qty, r, limit)
                log.warning("[FLATTEN] %s %s %.6g reduce-only: %s filled %.6g%s",
                            v.name, "SELL" if is_sell else "BUY", qty,
                            r.get("status"), r.get("filled_base") or 0.0,
                            f" err={r.get('err')}" if r.get("err") else "")
            # Lighter's REST lags its settlements (see RECONCILE_GRACE_SEC)
            await asyncio.sleep(self.RECONCILE_GRACE_SEC)
        pos = await self._fetch_positions()
        for k, p in pos.items():
            if p is not None:
                self._adopt_position(self.venues[k], p)
        left = {k: p for k, p in pos.items()
                if p is None or not self._is_dust(self.venues[k], p)}
        dust = {k: p for k, p in pos.items() if p and k not in left}
        self.dust_left_usd = 0.0
        for k, p in dust.items():
            px = self._ref_px(self.venues[k])
            if px is not None:
                self.dust_left_usd += abs(p) * px
        if not left:
            tail = ""
            if dust:
                tail = (" · remainder below the venue's minimum order, could "
                        "not be closed by the bot: " + "; ".join(
                            self._pos_desc(self.venues[k], p)
                            for k, p in dust.items())
                        + " / остаток ниже минимального ордера биржи")
            log.critical("%s — both legs CLOSED / обе ноги закрыты%s",
                         ok_tag, tail)
            if why != "stop":
                self._alert("flat-ok", "Позиции на обеих биржах закрыты.")
            return True
        log.critical("%s — FLATTEN FAILED, POSITION OPEN: %s — close it "
                     "manually / НЕ ЗАКРЫЛОСЬ, позиция висит: закройте "
                     "вручную", bad_tag,
                     "; ".join(self._pos_desc(self.venues[k], p)
                               for k, p in left.items()))
        self._alert("flat-fail", "🚨 Позиции НЕ закрылись: " + "; ".join(
            self._pos_desc(self.venues[k], p) for k, p in left.items())
            + ". Закройте их вручную на биржах.")
        return False

    def _stop_reason(self) -> str:
        if not self.halted:
            return "manual"
        if self.halt_reason == "loss limit":
            return "loss_limit"
        if "execution errors" in self.halt_reason:
            return "errors"
        return "halt: " + self.halt_reason

    async def _read_funding(self, start: float, end: float):
        """Funding of the session on each venue, from the exchanges' own
        history, in parallel. A venue that cannot answer gives None — shown
        as unknown, never as 0."""
        vs = list(self.venues.values())

        async def one(v):
            fn = getattr(v, "fetch_funding", None)
            if fn is None:
                return None
            return await asyncio.wait_for(fn(start, end),
                                          FUNDING_READ_TIMEOUT_SEC)
        got = await asyncio.gather(*(one(v) for v in vs),
                                   return_exceptions=True)
        out: Dict[str, Optional[float]] = {}
        for v, r in zip(vs, got):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, BaseException):
                log.warning("[%s] funding history not read: %r", v.name, r)
                out[v.key] = None
            else:
                out[v.key] = None if r is None else float(r)
        return out

    async def _finish_session(self, closed: Optional[bool]) -> None:
        """Final balance read, sessions.csv row, Russian summary in the log,
        open-session marker removed."""
        cfg = self.cfg
        res = None
        fund_task = asyncio.create_task(
            self._read_funding(self.session_start_ts, time.time()))
        for _ in range(3):
            res = await self._read_risk_sample()
            if res is not None:
                break
            await asyncio.sleep(1.0)
        if res is not None:
            self._take_sample(*res)
        try:
            self.funding = await fund_task
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("funding read failed")
            self.funding = {}
        end = time.time()
        f_ent = self.funding.get("entropy")
        f_hed = self.funding.get("hedge")
        start_eq = self.session_base_total
        end_eq = self.risk_last_sample.total if self.risk_last_sample \
            else None
        pnl = self.session_result()
        turnover = self.turnover_usd()
        pnl_pct = (pnl / start_eq * 100) if (pnl is not None and start_eq) \
            else None
        pnl_bps = (pnl / turnover * 1e4) if (pnl is not None and turnover) \
            else None
        row = [self.session_id, f"{self.session_start_ts:.0f}", f"{end:.0f}",
               f"{end - self.session_start_ts:.0f}", cfg.symbol,
               self.entropy.name, cfg.hedge_venue, journal.fmt(start_eq),
               journal.fmt(end_eq), journal.fmt(pnl), journal.fmt(pnl_pct),
               journal.fmt(turnover, 2), self.trades, self.hedges,
               journal.fmt(self.fees_usd), journal.fmt(pnl_bps, 2),
               self._stop_reason(),
               "" if closed is None else int(bool(closed)),
               cfg.max_loss_pct, self.entropy.fee_bps, self.hedge.fee_bps,
               cfg.midline_bps, cfg.upper_bps, cfg.lower_bps,
               self.hedge_symbol(), journal.fmt(self.max_unhedged_usd, 2),
               journal.fmt(self.dust_left_usd, 2),
               journal.fmt(f_ent), journal.fmt(f_hed),
               journal.fmt(self.cmp_exp) if self.cmp_n else "",
               journal.fmt(self.cmp_fill) if self.cmp_n else "",
               getattr(cfg, "exec_mode", "simultaneous")]
        try:
            journal.append_row(journal.path_in(cfg.trades_csv, "sessions.csv"),
                               journal.SESSIONS_HEADER, row)
        except Exception:
            log.exception("sessions.csv write failed")
        dur = int(end - self.session_start_ts)
        self._session_summary = {
            "pnl": pnl, "pnl_pct": pnl_pct, "turnover": turnover,
            "trades": self.trades, "duration": dur,
            "reason": self._stop_reason(), "closed": closed,
            "dust_left": self.dust_left_usd,
            "edge_compare": self.edge_compare(),
            "fees": self.fees_usd,
            "funding_entropy": f_ent, "funding_hedge": f_hed,
            "strategy": self.strategy_name,
            "volume_cost": self.volume_cost()}
        log.warning("[SESSION] %s · %s ENTROPY ↔ %s · %d:%02d:%02d · баланс "
                    "$%s → $%s · результат %s (%s) · оборот $%.2f · сделок %d · "
                    "комиссии ≈ $%.4f · funding %s · позиции %s",
                    self.session_id, cfg.symbol, self.hedge.name, dur // 3600,
                    dur % 3600 // 60, dur % 60,
                    f"{start_eq:.2f}" if start_eq is not None else "?",
                    f"{end_eq:.2f}" if end_eq is not None else "?",
                    f"${pnl:+.4f}" if pnl is not None else "?",
                    f"{pnl_pct:+.2f}%" if pnl_pct is not None else "?",
                    turnover, self.trades, self.fees_usd,
                    " / ".join(
                        f"{n} {'—' if f is None else f'${f:+.4f}'}"
                        for n, f in ((self.entropy.name, f_ent),
                                     (self.hedge.name, f_hed))),
                    {None: "—", True: "закрыты", False: "НЕ ЗАКРЫТЫ"}[closed])
        if closed is not False:
            journal.clear_marker(cfg.trades_csv)

    # --------------------------------------------------- reconcile / status

    # Lighter's REST account state lags its ws settlements; overwriting a
    # venue that traded seconds ago "restores" stale positions and triggers
    # phantom hedge oscillations. Grace-guard + venue lock prevent that.
    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool,
                                   strict: bool = False) -> None:
        now = time.time()
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue  # just traded: chain read would be stale
            if v is self.entropy and self._mlive is not None:
                continue  # a resting maker order may fill at any moment:
                          # its fills are booked by the maker poll
            if v.key in self._venue_down \
                    and now < self._venue_probe_at.get(v.key, 0.0):
                continue  # down venue: probe only every venue_probe_sec
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs),
            return_exceptions=True)
        for r in got:
            if isinstance(r, BaseException):
                raise r  # strict startup: fail loudly
        if hedge:
            await self._maybe_hedge()

    async def _reconcile_venue(self, v, strict: bool) -> None:
        async with self._vlock(v.key):
            now = time.time()
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return  # traded while waiting for the lock
            try:
                r = await v.fetch_position()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}")
                # exchange unreachable (e.g. scheduled maintenance): pause
                # trading and keep probing until it answers again
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical("[%s] API unreachable (%d attempts) — "
                                 "trading PAUSED; probing every %.0fs until "
                                 "it recovers", v.name, n,
                                 self.cfg.venue_probe_sec)
                    self._alert(f"down-{v.key}", f"⏸ {v.name}: биржа не "
                                f"отвечает — торговля на паузе, бот "
                                f"проверяет её каждые "
                                f"{self.cfg.venue_probe_sec:.0f} с.")
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r",
                                v.name, n, e)
                return
            if v.key in self._venue_down:
                gone = now - self._venue_down.pop(v.key)
                log.warning("[%s] API recovered after %.0fs outage — "
                            "trading RESUMED", v.name, gone)
                self._alert(f"up-{v.key}", f"▶️ {v.name} снова отвечает "
                            f"(не было {gone / 60:.0f} мин) — торговля "
                            f"продолжена.")
                self._update_evt.set()
            self._venue_fetch_fails[v.key] = 0
            delta = r - v.position
            if abs(delta) > 1e-12:
                if abs(delta) > self.cfg.net_tolerance_base:
                    log.warning("[%s] reconcile: chain %+.6g vs local %+.6g "
                                "— adopting chain", v.name, r, v.position)
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(),
                                       timeout=self.cfg.reconcile_sec)
                self._reconcile_evt.clear()
                await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            for v in self.venues.values():
                try:
                    got = await v.fetch_equity()
                    if got is not None:
                        v.equity, v.free = got
                        if v.start_equity is None:
                            v.start_equity = v.equity
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.debug("[%s] equity poll failed: %r", v.name, e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------ auto-calibration

    def is_flat(self) -> bool:
        """No open position on either venue (a sub-minimum remainder aside)
        and no execution in flight."""
        if self._exec_tasks or self.flattening:
            return False
        return all(self._is_dust(v, v.position) for v in self.venues.values())

    def autocalib_next_ts(self) -> Optional[float]:
        if not self.autocalib_on:
            return None
        if self.autocalib_last_ts is None:
            return time.time()
        return self.autocalib_last_ts + self.cfg.autocalib_every_hours * 3600.0

    async def _autocalib_loop(self) -> None:
        cfg = self.cfg
        log.warning("[autocalib] ON — midline follows the premium median of "
                    "the last %s, at most %g bps per %g h, within ±%g of the "
                    "manual %+.1f / автокалибровка центра включена",
                    autocalib.window_label(cfg.autocalib_window_hours),
                    cfg.autocalib_max_step_bps, cfg.autocalib_every_hours,
                    cfg.autocalib_max_drift_bps, self.autocalib_anchor)
        while not self.stop.is_set():
            try:
                now = time.time()
                if (autocalib.due(self.autocalib_last_ts,
                                  cfg.autocalib_every_hours, now)
                        and self.risk_ready and not self.halted
                        and not self.stopping):
                    if not self.is_flat():
                        if not self.autocalib_waiting:
                            log.info("[autocalib] due — waits until no "
                                     "position is open / ждёт, пока позиция "
                                     "не закроется")
                        self.autocalib_waiting = True
                    else:
                        self.autocalib_waiting = False
                        await self._autocalib_once(now)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[autocalib] check failed")
                self.autocalib_last_ts = time.time()   # retry next period
            try:
                await asyncio.wait_for(self.stop.wait(), AUTOCALIB_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def _autocalib_once(self, now: float) -> None:
        cfg = self.cfg
        prems = await asyncio.to_thread(
            autocalib.window_premiums, cfg.recorder_csv,
            cfg.autocalib_window_hours, now)
        d = autocalib.decide(cfg.midline_bps, self.autocalib_anchor, prems,
                             min_hours=cfg.autocalib_min_hours,
                             max_step_bps=cfg.autocalib_max_step_bps,
                             max_drift_bps=cfg.autocalib_max_drift_bps)
        self.autocalib_last = d
        self.autocalib_last_ts = now
        win = autocalib.window_label(cfg.autocalib_window_hours)
        if d.new_midline is None:
            log.info("[autocalib] midline stays %+.1f (median over %s: %s; "
                     "%s)", cfg.midline_bps, win,
                     "—" if d.target is None else f"{d.target:+.1f}",
                     d.reason)
            try:
                await asyncio.to_thread(autocalib.mark_run, cfg.ticker_file,
                                        now)
            except Exception:
                log.exception("[autocalib] pair file not updated")
            return
        old = cfg.midline_bps
        cfg.midline_bps = d.new_midline          # the strategy reads it live
        log.warning("[autocalib] midline %+.1f → %+.1f (median over %s "
                    "%+.1f, %d min%s) / автокалибровка: центр %+.1f → %+.1f",
                    old, d.new_midline, win, d.target, d.minutes,
                    f"; {d.reason}" if d.reason else "", old, d.new_midline)
        try:
            await asyncio.to_thread(autocalib.save, cfg.ticker_file,
                                    d.new_midline, now)
        except Exception:
            log.exception("[autocalib] pair file not updated — the new "
                          "midline holds until the bot restarts")
        try:
            journal.append_row(
                journal.path_in(cfg.trades_csv, "autocalib.csv"),
                AUTOCALIB_HEADER,
                [f"{now:.0f}", self.session_id, cfg.symbol, cfg.hedge_venue,
                 f"{old:g}", f"{d.new_midline:g}", f"{d.target:g}", d.minutes,
                 f"{cfg.autocalib_window_hours:g}",
                 f"{self.autocalib_anchor:g}", d.reason])
        except Exception:
            log.exception("autocalib.csv write failed")
        await self._tg_send(tgmsg.autocalib_text(self, old, d))

    async def _autotune_loop(self) -> None:
        cfg = self.cfg
        log.warning("[autotune] ON — every %g h the band is compared with one "
                    "step narrower and wider on the last %g days of recorded "
                    "books, within ±%g of the manual +%g/−%g / автоподбор "
                    "полосы включён", cfg.autotune_every_hours,
                    cfg.autotune_days, cfg.autotune_max_drift_bps,
                    *self.autotune_anchor)
        while not self.stop.is_set():
            try:
                now = time.time()
                if (autotune.due(self.autotune_last_ts,
                                 cfg.autotune_every_hours, now)
                        and self.risk_ready and not self.halted
                        and not self.stopping):
                    await self._autotune_once(now)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[autotune] run failed")
                self.autotune_last_ts = time.time()    # retry next period
            try:
                await asyncio.wait_for(self.stop.wait(), AUTOCALIB_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    def _autotune_params(self, slip_bps: float) -> "bt.Params":
        from dataclasses import replace as dc_replace
        p = bt.params_from_config(self.cfg)
        return dc_replace(p, entropy_fee_bps=self.entropy.fee_bps,
                          hedge_fee_bps=self.hedge.fee_bps,
                          slip_bps=round(slip_bps, 2))

    async def _autotune_once(self, now: float) -> None:
        cfg = self.cfg
        slip, _n = await asyncio.to_thread(bt.measured_slip, cfg.trades_csv,
                                           cfg.symbol, cfg.hedge_venue)
        slip = bt.DEFAULT_SLIP_BPS if slip is None else slip
        ticks = await asyncio.to_thread(bt.load_ticks, cfg.ticks_dir,
                                        cfg.hedge_venue, cfg.symbol,
                                        cfg.autotune_days)
        base = self._autotune_params(slip)
        d = await asyncio.to_thread(
            autotune.decide, ticks, base, *self.autotune_anchor,
            max_drift_bps=cfg.autotune_max_drift_bps,
            min_hours=cfg.autotune_min_hours,
            min_gain_pct=cfg.autotune_min_gain_pct)
        self.autotune_last = d
        self.autotune_last_ts = now
        res = "; ".join(f"+{u:g}/−{lo:g}: {n} trades ${p:+.2f}"
                        for u, lo, n, p in d.results)
        if d.upper is None:
            log.info("[autotune] band stays +%g/−%g (%s) %s", cfg.upper_bps,
                     cfg.lower_bps, d.reason, res)
            try:
                await asyncio.to_thread(autotune.mark_run, cfg.ticker_file,
                                        now)
            except Exception:
                log.exception("[autotune] pair file not updated")
            return
        old = (cfg.upper_bps, cfg.lower_bps)
        cfg.upper_bps, cfg.lower_bps = d.upper, d.lower   # read live
        log.warning("[autotune] band +%g/−%g → +%g/−%g (%s) / автоподбор "
                    "полосы", old[0], old[1], d.upper, d.lower, res)
        try:
            await asyncio.to_thread(autotune.save, cfg.ticker_file, d.upper,
                                    d.lower, now)
        except Exception:
            log.exception("[autotune] pair file not updated — the new band "
                          "holds until the bot restarts")
        try:
            journal.append_row(
                journal.path_in(cfg.trades_csv, "autotune.csv"),
                AUTOTUNE_HEADER,
                [f"{now:.0f}", self.session_id, cfg.symbol, cfg.hedge_venue,
                 f"{old[0]:g}", f"{old[1]:g}", f"{d.upper:g}", f"{d.lower:g}",
                 f"{d.hours:.1f}", f"{slip:.2f}", res, d.reason])
        except Exception:
            log.exception("autotune.csv write failed")
        await self._tg_send(tgmsg.autotune_text(self, old, d))

    async def _tg_send(self, text: str) -> None:
        if self.telegram is None:
            return
        nick = getattr(self.cfg, "nickname", "")
        if nick:
            text = f"[{nick}] {text}"
        try:
            await asyncio.wait_for(self.telegram.send(text), 12.0)
        except Exception as e:
            log.warning("telegram: %r", e)

    def _tg_paths(self):
        base = getattr(self.cfg, "trades_csv", None) or "logs/trades.csv"
        return (journal.path_in(base, TG_SNAPSHOT),
                journal.path_in(base, TG_CONTROL))

    def _tg_write_snapshot(self, path: str) -> None:
        import json
        import os
        try:
            status = tgmsg.status_text(self)
        except Exception:
            log.exception("telegram status text failed")
            status = "Не удалось собрать состояние — подробности в логе бота."
        try:
            pnl = tgmsg.pnl_text(self)
        except Exception:
            log.exception("telegram pnl text failed")
            pnl = "Не удалось собрать результат — подробности в логе бота."
        data = {"ts": time.time(), "pid": os.getpid(), "status": status,
                "pnl": pnl, "paused": "manual" in self.pauses.active()}
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)

    def _tg_take_control(self, path: str) -> Optional[str]:
        """/pause or /resume handed over by the listener (a small file it
        writes); stale requests (older than 2 minutes) are dropped."""
        import json
        import os
        try:
            with open(path, encoding="utf-8") as fh:
                d = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception:
            d = {}
        try:
            os.remove(path)
        except OSError:
            pass
        if time.time() - float(d.get("ts") or 0) > 120:
            return None
        return self._tg_control(str(d.get("cmd") or ""))

    async def _tg_bridge_loop(self) -> None:
        snap, ctl = self._tg_paths()
        last = 0.0
        while not self.stop.is_set():
            now = time.time()
            if now - last >= TG_SNAPSHOT_SEC:
                last = now
                try:
                    self._tg_write_snapshot(snap)
                except Exception:
                    log.exception("telegram snapshot write failed")
            try:
                reply = self._tg_take_control(ctl)
            except Exception:
                log.exception("telegram control failed")
                reply = None
            if reply:
                await self._tg_send(reply)
            try:
                await asyncio.wait_for(self.stop.wait(), 2.0)
            except asyncio.TimeoutError:
                pass

    async def _tg_commands_loop(self) -> None:
        """Answer /status, /pnl (and help) from the owner's chat. Read-only:
        no command changes anything."""
        tg = self.telegram
        try:
            await tg.skip_backlog()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("telegram: %r", e)
        while not self.stop.is_set():
            try:
                updates = await tg.get_updates(25)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("telegram poll failed: %r", e)
                await asyncio.sleep(5.0)
                continue
            for u in updates:
                cmd = tg.command_of(u)
                if cmd is None:
                    continue          # другие чаты и не текст — игнорируем
                try:
                    reply = self._tg_control(cmd) or tgmsg.reply_for(self, cmd)
                except Exception:
                    log.exception("telegram reply failed")
                    reply = "Не удалось собрать ответ — подробности в логе бота."
                await self._tg_send(reply)

    async def _read_request_budgets(self) -> None:
        done = set()
        for v in self.venues.values():
            fetch = getattr(v, "fetch_request_budget", None)
            if fetch is None or not v.ready_to_trade() or v.key in done:
                continue
            try:
                b = await asyncio.wait_for(fetch(), RISK_READ_TIMEOUT_SEC)
            except Exception as e:
                log.debug("[%s] request budget read failed: %r", v.name, e)
                continue
            if b is None:
                continue
            for k in self._budget_keys(v):   # one address = one budget
                self.req_budget[k] = b
                done.add(k)
            limited = self.req_limited(v)
            if limited != self._req_state_logged.get(v.key, False):
                self._req_state_logged[v.key] = limited
                if limited:
                    log.warning(
                        "[%s] Hyperliquid request budget exhausted: used %d "
                        "of %d (+%d reserved), headroom %d — sending at most "
                        "1 order per %.0fs until traded volume catches up / "
                        "лимит запросов исчерпан: не чаще 1 ордера в %.0f с",
                        v.name, b["used"], b["cap"], b["surplus"],
                        b["headroom"], REQ_SLOW_GAP_SEC, REQ_SLOW_GAP_SEC)
                else:
                    log.warning("[%s] Hyperliquid request budget OK again "
                                "(headroom %d) / лимит запросов в норме",
                                v.name, b["headroom"])

    async def _request_budget_loop(self) -> None:
        """Read Hyperliquid's action budget every REQ_BUDGET_POLL_SEC and
        right after each trade (info requests do not spend the budget)."""
        while not self.stop.is_set():
            self._req_evt.clear()
            await self._read_request_budgets()
            try:
                await asyncio.wait_for(self._req_evt.wait(),
                                       timeout=REQ_BUDGET_POLL_SEC)
                await asyncio.sleep(1.0)   # let the fill register first
            except asyncio.TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except asyncio.TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    def account_delta(self) -> Optional[float]:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> Optional[float]:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    def premium_stats(self, window_sec: float = None):
        """(min, median, max, minutes covered) of the sampled premium over
        the window, or None before the first sample."""
        window = PREM_WINDOW_SEC if window_sec is None else window_sec
        cutoff = time.time() - window
        vals = [b for t, b in self._prem_hist if t >= cutoff]
        if not vals:
            return None
        first = next(t for t, _ in self._prem_hist if t >= cutoff)
        vals.sort()
        n = len(vals)
        med = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
        return vals[0], med, vals[-1], (time.time() - first) / 60.0

    async def _premium_sampler(self) -> None:
        last_feed = 0.0
        while not self.stop.is_set():
            if time.time() - last_feed >= autonomy.SAMPLE_SEC:
                last_feed = time.time()
                try:
                    self._feed_centre(last_feed)
                except Exception:
                    log.exception("[autonomy] centre update failed")
                try:
                    self._feed_band(last_feed)
                except Exception:
                    log.exception("[autoband] update failed")
            try:
                p = self.premium_bps()
            except Exception:
                p = None
            now = time.time()
            if p is not None:
                self._prem_hist.append((now, p))
            while self._prem_hist and self._prem_hist[0][0] < now - PREM_WINDOW_SEC:
                self._prem_hist.popleft()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (f" REQ {self.req_budget[v.key]['headroom']:+d}"
                   if self.req_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            self._note_unhedged()
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            rl = self.risk_loss()
            risk = (f" | loss ${rl:.4f}/${self.risk_guard.limit_usd:.4f}"
                    if rl is not None else "")
            log.info("[status] %s | prem %s bps (band %+.2f..%+.2f) | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s%s",
                     books, prem_s, self.mid_bps() - cfg.lower_bps,
                     self.mid_bps() + cfg.upper_bps, pos, net, self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, risk, rec,
                     f" *** HALTED: {self.halt_reason} ***" if self.halted
                     else "")

    def _log_csv(self, direction, buy, sell, plan: ArbPlan, ok: bool, bfill,
                 sfill, bstatus, sstatus, fill_edge, inv_bps,
                 bpx=None, spx=None, timing=None, action="",
                 mode=None) -> None:
        f = journal.fmt
        t = timing or {}
        bsp, ssp = t.get("spreads") or (None, None)
        q = plan.qty or 0.0
        try:
            journal.append_row(self.cfg.trades_csv, journal.TRADES_HEADER, [
                f"{time.time():.3f}",
                direction, buy.name, sell.name, f"{plan.qty:.8g}",
                plan.buy_limit, plan.sell_limit,
                f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                f"{plan.marginal_premium_bps:.3f}",
                f"{self.mid_bps():.3f}",
                f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}",
                f(bpx, 8), f(spx, 8),
                f(plan.buy_notional / q, 8) if q else "",
                f(plan.sell_notional / q, 8) if q else "",
                f(plan.buy_fee * 1e4, 3), f(plan.sell_fee * 1e4, 3),
                f(t.get("buy_ms"), 1), f(t.get("sell_ms"), 1),
                f(t.get("gap_ms"), 1), f(bsp, 3), f(ssp, 3),
                self.session_id, self.cfg.symbol, self.hedge_symbol(),
                self.cfg.hedge_venue, action,
                mode or getattr(self.cfg, "exec_mode", "simultaneous")])
        except Exception:
            log.exception("csv write failed")
