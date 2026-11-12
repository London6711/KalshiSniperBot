#!/usr/bin/env python3
"""
Real-Time Whale WebSocket Fill Streamer & Instant Kalshi Orderbook Trigger
==========================================================================
Streams live fills across all 100 wallets in `top_trader_registry` (50 Hyperliquid +
50 Polymarket) in <200ms and triggers immediate Kalshi orderbook sniping checks.

Architecture:
1. Hyperliquid VIP UserFills Stream: Subscribes directly to `userFills` for the Top 10
   highest-weighted wallets (respecting Hyperliquid's 10-unique-user per-IP limit),
   filtering out `isSnapshot: true` historical dumps.
2. Hyperliquid Public `WsTrade` Interceptor: Subscribes to `trades` across all target coins
   and inspects the `users: [buyer, seller]` tuple in O(1) time to catch fills from
   wallets #11 through #50 (plus bonus weighting when the whale is the aggressive Taker).
3. Polymarket RTDS Live Trade Tape (`wss://ws-live-data.polymarket.com`): Streams live
   `activity` (`trades` and `orders_matched`) and filters against all 50 Polymarket wallets.
4. Instant Kalshi Orderbook Callback: Pushes qualifying fills (>= $2,500 USD) into an
   async priority queue that immediately invokes Kalshi orderbook evaluation on the
   corresponding 15m, 1h, and Daily series.
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("whale_ws.log")]
)

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")

# WebSocket Endpoints
HL_WS_URL = os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")
POLY_RTDS_WS_URL = os.getenv("POLY_RTDS_WS_URL", "wss://ws-live-data.polymarket.com")

# Tracked Hyperliquid Perp Symbols mapped to Kalshi Series
HL_TRACKED_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE", "NEAR", "ZEC"]

ASSET_TO_KALSHI_SERIES: Dict[str, List[str]] = {
    "BTC": ["KXBTC15M", "KXBTCH", "KXBTCD"],
    "ETH": ["KXETH15M", "KXETHH", "KXETHD"],
    "SOL": ["KXSOL15M", "KXSOLH", "KXSOLD"],
    "XRP": ["KXXRP15M", "KXXRPH", "KXXRPD"],
    "DOGE": ["KXDOGE15M", "KXDOGEH", "KXDOGED"],
    "BNB": ["KXBNB15M", "KXBNBH", "KXBNBD"],
    "HYPE": ["KXHYPE15M", "KXHYPEH", "KXHYPED"],
    "NEAR": ["KXNEAR15M", "KXNEARH"],
    "ZEC": ["KXZEC15M", "KXZEC1H"],
    "GOLD": ["KXGOLD15M", "KXGOLDH"],
    "WTI": ["KXWTI15M", "KXWTIH"],
    "INX": ["KXINX15M", "KXINXH", "KXINXD"],
    "NDQ": ["KXNDQ15M", "KXNDQH", "KXNDQD"]
}

# Minimum USD notional fill to trigger an immediate out-of-cycle Kalshi orderbook check
MIN_FILL_NOTIONAL_USD = 2500.0
# Debounce window (seconds) per Kalshi series to avoid hammering the book on multi-slice fills
SERIES_TRIGGER_COOLDOWN_SECS = 2.0


# ---------------------------------------------------------
# 1. WAL-Mode SQLite Helpers & In-Memory O(1) Registry Cache
# ---------------------------------------------------------
def get_db_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


class WhaleRegistryCache:
    """
    Maintains an in-memory O(1) lookup table of all 100 wallets in `top_trader_registry`.
    Automatically partitions the Top 10 Hyperliquid wallets for direct `userFills` WS
    and all 50 Hyperliquid + 50 Polymarket wallets for tape interception.
    """
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self.hl_wallets: Dict[str, Dict[str, Any]] = {}
        self.poly_wallets: Dict[str, Dict[str, Any]] = {}
        self.hl_top_10_vip: List[str] = []
        self.last_loaded: float = 0.0

    def reload_from_sqlite(self):
        if not os.path.exists(self.db_path):
            return
        conn = get_db_connection(self.db_path)
        c = conn.cursor()
        try:
            c.execute("""
                SELECT wallet_address, venue, pnl_30d, win_rate, sharpe_ratio, weight_multiplier
                FROM top_trader_registry
                ORDER BY weight_multiplier DESC, pnl_30d DESC
            """)
            rows = c.fetchall()
        except Exception as e:
            logging.debug(f"Registry reload notice: {e}")
            rows = []
        finally:
            conn.close()

        hl_map: Dict[str, Dict[str, Any]] = {}
        poly_map: Dict[str, Dict[str, Any]] = {}

        for addr, venue, pnl, wr, sharpe, weight in rows:
            norm_addr = str(addr).lower().strip()
            meta = {
                "address": norm_addr,
                "venue": venue,
                "pnl_30d": float(pnl or 0.0),
                "win_rate": float(wr or 0.68),
                "sharpe": float(sharpe or 2.0),
                "weight": float(weight or 1.15)
            }
            if str(venue).lower() == "hyperliquid":
                hl_map[norm_addr] = meta
            elif str(venue).lower() == "polymarket":
                poly_map[norm_addr] = meta

        self.hl_wallets = hl_map
        self.poly_wallets = poly_map
        # Respect Hyperliquid's 10-unique-user IP cap for direct userFills subscriptions
        self.hl_top_10_vip = list(hl_map.keys())[:10]
        self.last_loaded = time.time()
        logging.info(
            f"[WhaleCache] Loaded {len(self.hl_wallets)} Hyperliquid wallets "
            f"(Top {len(self.hl_top_10_vip)} VIP direct) + {len(self.poly_wallets)} Polymarket wallets."
        )


def log_live_whale_fill(wallet_addr: str, label: str, source: str, asset: str,
                        action: str, size_usd: float, confidence: float, db_path: str = DB_FILE):
    """Persists a sub-second WebSocket fill directly into `wallet_alpha_feed`."""
    conn = get_db_connection(db_path)
    c = conn.cursor()
    c.execute("""
        INSERT INTO wallet_alpha_feed
        (id, wallet_address, label, source, asset, action, size_usd, confidence_score, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(uuid.uuid4()),
        wallet_addr.lower(),
        label,
        source,
        asset.upper(),
        action.upper(),
        round(size_usd, 2),
        round(min(0.99, max(0.50, confidence)), 3),
        int(time.time())
    ))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# 2. Instant Kalshi Orderbook Trigger Dispatcher
# ---------------------------------------------------------
class InstantKalshiTriggerDispatcher:
    """
    Consumes real-time whale fill events from an asyncio.Queue, applies a 2-second per-series
    cooldown (so a 20-slice whale sweep only triggers one immediate orderbook check), and fires
    either the linked V2 engine's `evaluate_and_trade_series` callback or a standalone
    Kalshi orderbook inspection.
    """
    def __init__(self,
                 kalshi_client: Any = None,
                 http_session: Optional[aiohttp.ClientSession] = None,
                 series_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
                 get_equity_fn: Optional[Callable[[], float]] = None):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self.kalshi_client = kalshi_client
        self.http_session = http_session
        self.series_callback = series_callback
        self.get_equity_fn = get_equity_fn
        self.last_triggered_ts: Dict[str, float] = {}
        self.seen_fill_ids: Set[str] = set()

    def enqueue_whale_fill(self, fill_uid: str, wallet_addr: str, venue: str,
                           asset: str, action: str, size_usd: float,
                           confidence: float, is_taker: bool, db_path: str = DB_FILE):
        """Deduplicates fills across overlapping channels and pushes to the instant trigger queue."""
        if fill_uid in self.seen_fill_ids:
            return
        self.seen_fill_ids.add(fill_uid)
        if len(self.seen_fill_ids) > 10000:
            # Prevent unbounded set growth
            self.seen_fill_ids.clear()

        if size_usd < MIN_FILL_NOTIONAL_USD:
            return

        # Taker sweeps receive a +6% confidence boost because they cross the spread urgently
        final_conf = min(0.99, confidence + (0.06 if is_taker else 0.0))
        label = f"{venue}_{'TakerSweep' if is_taker else 'MakerFill'}"

        log_live_whale_fill(wallet_addr, label, venue, asset, action, size_usd, final_conf, db_path)

        try:
            self.queue.put_nowait({
                "wallet": wallet_addr,
                "venue": venue,
                "asset": asset,
                "action": action,
                "size_usd": size_usd,
                "confidence": final_conf,
                "is_taker": is_taker,
                "detected_ms": int(time.time() * 1000)
            })
        except asyncio.QueueFull:
            pass

    async def run_dispatcher_loop(self):
        """Continuously drains the priority queue and triggers instant Kalshi book checks."""
        while True:
            event = await self.queue.get()
            try:
                asset = event["asset"]
                target_series_list = ASSET_TO_KALSHI_SERIES.get(asset, [])
                now = time.time()

                for series in target_series_list:
                    last_ts = self.last_triggered_ts.get(series, 0.0)
                    if (now - last_ts) < SERIES_TRIGGER_COOLDOWN_SECS:
                        continue
                    self.last_triggered_ts[series] = now

                    latency_ms = int(time.time() * 1000) - event["detected_ms"]
                    logging.info(
                        f"⚡ [WHALE FILL -> KALSHI TRIGGER] {event['venue']} {event['wallet'][:10]}... "
                        f"{event['action']} ${event['size_usd']:,.0f} {asset} (Conf: {event['confidence']:.2f}, "
                        f"Queue Latency: {latency_ms}ms) -> Auditing {series} NOW"
                    )

                    # If wired into kalshi_sniper_engine_consolidated_v2.py, invoke its trade evaluator directly
                    if self.series_callback and self.kalshi_client and self.http_session:
                        equity = self.get_equity_fn() if self.get_equity_fn else 25.0
                        category = "CRYPTO_15M" if "15M" in series else "CRYPTO_1H"
                        asyncio.create_task(
                            self.series_callback(self.kalshi_client, self.http_session, category, series, equity)
                        )
                    elif self.http_session:
                        # Standalone mode: perform immediate public Kalshi orderbook top-of-book check
                        asyncio.create_task(self._standalone_kalshi_book_probe(series, event))
            except Exception as e:
                logging.error(f"[TriggerDispatcher] Error dispatching Kalshi check: {e}")
            finally:
                self.queue.task_done()

    async def _standalone_kalshi_book_probe(self, series: str, event: Dict[str, Any]):
        """Queries live Kalshi public market/orderbook when running standalone."""
        url = "https://external-api.kalshi.com/trade-api/v2/markets"
        try:
            async with self.http_session.get(url, params={"series_ticker": series, "status": "open"}, timeout=4) as r:
                if r.status != 200:
                    return
                data = await r.json()
                markets = data.get("markets", [])
                if not markets:
                    return
                markets.sort(key=lambda m: m.get("close_time", ""))
                top_mkt = markets[0]
                ticker = top_mkt.get("ticker")
                yes_bid = top_mkt.get("yes_bid", 0)
                no_bid = top_mkt.get("no_bid", 0)
                logging.info(
                    f"   ↳ [Kalshi Book Probe] {ticker} | Best YES Bid: {yes_bid}¢ | Best NO Bid: {no_bid}¢ "
                    f"| Whale Signal: {event['action']} {event['asset']}"
                )
        except Exception:
            pass


# ---------------------------------------------------------
# 3. Hyperliquid WebSocket Streamer (Top 10 VIP + 50-Wallet Tape)
# ---------------------------------------------------------
def parse_hl_message_and_dispatch(raw_msg: str, cache: WhaleRegistryCache,
                                  dispatcher: InstantKalshiTriggerDispatcher, db_path: str = DB_FILE) -> int:
    """
    Parses a raw Hyperliquid WebSocket JSON frame (`userFills` or `trades`) and routes
    any matching wallet fill to `dispatcher`. Returns number of whale fills matched.
    """
    try:
        msg = json.loads(raw_msg)
    except Exception:
        return 0

    channel = msg.get("channel")
    data = msg.get("data")
    if not data:
        return 0

    matched_count = 0

    # Path A: Direct `userFills` stream for Top 10 VIP Wallets
    if channel == "userFills" and isinstance(data, dict):
        # CRITICAL: Ignore initial historical snapshot dump on subscribe
        if data.get("isSnapshot") is True:
            return 0

        user_addr = str(data.get("user", "")).lower()
        wallet_meta = cache.hl_wallets.get(user_addr)
        if not wallet_meta:
            return 0

        fills = data.get("fills", [])
        for f in fills:
            coin = str(f.get("coin", "")).upper()
            if coin not in ASSET_TO_KALSHI_SERIES:
                continue
            px = float(f.get("px", 0.0) or 0.0)
            sz = float(f.get("sz", 0.0) or 0.0)
            notional = px * sz
            side_code = str(f.get("side", "B")).upper()  # "B" = Bid/Buy, "A" = Ask/Sell
            action = "BUY" if side_code == "B" else "SELL"
            is_taker = bool(f.get("crossed", False))
            tid = str(f.get("tid") or f.get("hash") or uuid.uuid4())

            conf = min(0.98, 0.65 * wallet_meta["weight"])
            dispatcher.enqueue_whale_fill(
                f"hl_uf_{user_addr}_{tid}", user_addr, "Hyperliquid",
                coin, action, notional, conf, is_taker, db_path
            )
            matched_count += 1

    # Path B: Public `trades` stream (`WsTrade[]`) covering all 50 Hyperliquid Wallets via `users: [buyer, seller]`
    elif channel == "trades" and isinstance(data, list):
        for trade in data:
            coin = str(trade.get("coin", "")).upper()
            if coin not in ASSET_TO_KALSHI_SERIES:
                continue
            users = trade.get("users")
            if not isinstance(users, list) or len(users) < 2:
                continue

            buyer_addr = str(users[0]).lower()
            seller_addr = str(users[1]).lower()
            px = float(trade.get("px", 0.0) or 0.0)
            sz = float(trade.get("sz", 0.0) or 0.0)
            notional = px * sz
            taker_side = str(trade.get("side", "B")).upper()  # "B" means buyer was taker; "A" means seller was taker
            tid = str(trade.get("tid") or trade.get("hash") or time.time_ns())

            if buyer_addr in cache.hl_wallets:
                w_meta = cache.hl_wallets[buyer_addr]
                is_taker = (taker_side == "B")
                conf = min(0.98, 0.65 * w_meta["weight"])
                dispatcher.enqueue_whale_fill(
                    f"hl_tr_{buyer_addr}_{tid}", buyer_addr, "Hyperliquid",
                    coin, "BUY", notional, conf, is_taker, db_path
                )
                matched_count += 1

            if seller_addr in cache.hl_wallets:
                w_meta = cache.hl_wallets[seller_addr]
                is_taker = (taker_side == "A")
                conf = min(0.98, 0.65 * w_meta["weight"])
                dispatcher.enqueue_whale_fill(
                    f"hl_tr_{seller_addr}_{tid}", seller_addr, "Hyperliquid",
                    coin, "SELL", notional, conf, is_taker, db_path
                )
                matched_count += 1

    return matched_count


async def stream_hyperliquid_whale_fills(cache: WhaleRegistryCache,
                                         dispatcher: InstantKalshiTriggerDispatcher,
                                         db_path: str = DB_FILE):
    """
    Maintains a persistent, auto-reconnecting WebSocket connection to `wss://api.hyperliquid.xyz/ws`
    with a 25-second ping heartbeat.
    """
    backoff = 1.0
    while True:
        try:
            cache.reload_from_sqlite()
            async with websockets.connect(HL_WS_URL, ping_interval=20, ping_timeout=10, max_size=2**22) as ws:
                logging.info(f"[Hyperliquid WS] Connected to {HL_WS_URL}")
                backoff = 1.0

                # 1. Subscribe to public `trades` stream for all tracked coins (catches all 50 HL wallets)
                for coin in HL_TRACKED_COINS:
                    sub_trades = {
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin}
                    }
                    await ws.send(json.dumps(sub_trades))

                # 2. Subscribe to `userFills` for the Top 10 VIP wallets (max 10 unique users allowed per IP)
                for vip_addr in cache.hl_top_10_vip:
                    sub_uf = {
                        "method": "subscribe",
                        "subscription": {"type": "userFills", "user": vip_addr, "aggregateByTime": True}
                    }
                    await ws.send(json.dumps(sub_uf))

                logging.info(
                    f"[Hyperliquid WS] Subscribed to {len(HL_TRACKED_COINS)} coin tapes + "
                    f"{len(cache.hl_top_10_vip)} VIP userFills streams."
                )

                last_ping = time.time()
                while True:
                    # Send explicit application-level ping every 25s per Hyperliquid spec
                    if time.time() - last_ping > 25.0:
                        await ws.send(json.dumps({"method": "ping"}))
                        last_ping = time.time()

                    # Periodically refresh the in-memory wallet cache every 10 minutes
                    if time.time() - cache.last_loaded > 600.0:
                        cache.reload_from_sqlite()

                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=35.0)
                    parse_hl_message_and_dispatch(raw_msg, cache, dispatcher, db_path)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"[Hyperliquid WS] Disconnected ({e}). Reconnecting in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.8)


# ---------------------------------------------------------
# 4. Polymarket RTDS Live Activity Streamer (All 50 Poly Wallets)
# ---------------------------------------------------------
def map_polymarket_title_to_asset(title_or_slug: str) -> Optional[str]:
    text = title_or_slug.upper()
    mapping = [
        ("BITCOIN", "BTC"), ("BTC", "BTC"),
        ("ETHEREUM", "ETH"), ("ETH", "ETH"),
        ("SOLANA", "SOL"), ("SOL", "SOL"),
        ("XRP", "XRP"), ("DOGE", "DOGE"),
        ("BNB", "BNB"), ("HYPERLIQUID", "HYPE"),
        ("GOLD", "GOLD"), ("CRUDE", "WTI"), ("WTI", "WTI"),
        ("S&P", "INX"), ("SPX", "INX"), ("NASDAQ", "NDQ")
    ]
    for kw, sym in mapping:
        if kw in text:
            return sym
    return None


def parse_polymarket_message_and_dispatch(raw_msg: str, cache: WhaleRegistryCache,
                                          dispatcher: InstantKalshiTriggerDispatcher, db_path: str = DB_FILE) -> int:
    """
    Parses Polymarket RTDS (`wss://ws-live-data.polymarket.com`) `activity` frames
    and matches `proxyWallet` / `user` / `maker` / `taker` against the 50 Polymarket top traders.
    """
    if not raw_msg or raw_msg in ("PING", "PONG"):
        return 0
    try:
        msg = json.l