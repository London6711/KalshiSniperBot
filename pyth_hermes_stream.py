#!/usr/bin/env python3
"""
Pyth Network Hermes Sub-Second Oracle Streamer & Real-Time 1m ATR Engine
========================================================================
Streams 400ms institutional oracle ticks from Pyth Network (`hermes.pyth.network`)
across Commodities, Equities/Indices, Forex, and Crypto to compute:
1. True 14-Period Wilder 1-Minute ATR on the underlying spot asset.
2. Real-time Distance-to-Target (Spot vs. 15m/1h Window Open or Fixed Strike).
3. Deterministic Terminal 60-Second TWAP Settlement Projections.
4. Institutional Confidence-Band Shock Detection (`conf / price` ratio).

Supports both:
- Primary WebSocket (`wss://hermes.pyth.network/ws`)
- Automatic Fallback to Hermes v2 SSE (`https://hermes.pyth.network/v2/updates/price/stream`)
"""

import asyncio
import collections
import json
import logging
import math
import os
import sqlite3
import sys
import time
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

import aiohttp
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("pyth_oracle.log")]
)

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")

# Pyth Network Hermes Endpoints
PYTH_HERMES_WS_URL = os.getenv("PYTH_HERMES_WS_URL", "wss://hermes.pyth.network/ws")
PYTH_HERMES_SSE_URL = os.getenv("PYTH_HERMES_SSE_URL", "https://hermes.pyth.network/v2/updates/price/stream")
PYTH_FEEDS_CATALOG_URL = "https://hermes.pyth.network/v2/price_feeds"
PYTH_LATEST_REST_URL = "https://hermes.pyth.network/v2/updates/price/latest"

# Canonical Verified Hex Feed IDs + Dynamic Symbol Matchers for Commodities, Indices, FX, and Crypto
# Note: Leading '0x' is normalized automatically for both WS and REST/SSE calls.
CANONICAL_PYTH_FEEDS: Dict[str, Dict[str, Any]] = {
    # --- Commodities & Precious Metals ---
    "GOLD": {
        "id": "765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2",
        "search_terms": ["XAU/USD", "Metal.XAU/USD"],
        "kalshi_series": ["KXGOLD15M", "KXGOLDH", "KXGOLDW", "KXGOLDM"]
    },
    "SILVER": {
        "id": "f2fb02c32b055c805e7238d628e5e9dadef274376114eb1f012337cabe93871e",
        "search_terms": ["XAG/USD", "Metal.XAG/USD"],
        "kalshi_series": ["KXSILVER15M", "KXSILVERH"]
    },
    "PLATINUM": {
        "id": "398e4bbc7cbf89d6b48c21e080be9d7a65c3f8b1469c183c8d45d8b89f8e8c12",
        "search_terms": ["XPT/USD", "Metal.XPT/USD"],
        "kalshi_series": ["KXPLATINUM15M"]
    },
    "PALLADIUM": {
        "id": "28a3d8d59a4a33b6e989d6155d6e6f92a75c40e5c8f3d280702210b3a0c21b3e",
        "search_terms": ["XPD/USD", "Metal.XPD/USD"],
        "kalshi_series": ["KXPALLADIUM15M"]
    },
    "WTI": {
        "id": "925ca92ff005ae943c158e3563f59698ce7e75c5a8c8dd43303a0a154887b3e6",
        "search_terms": ["WTI/USD", "USOILSPOT", "Commodities.USOILSPOT"],
        "kalshi_series": ["KXWTI15M", "KXWTIH", "KXWTIW", "KXWTIM"]
    },
    "BRENT": {
        "id": "7b86772a5d7f63a8c760f8f7e6e1d35f2c4e3d2160a1e9203a4b929d8d13b4a1",
        "search_terms": ["UKOILSPOT", "BCO/USD", "BRENT"],
        "kalshi_series": ["KXBRENT15M", "KXBRENTH"]
    },
    "NATGAS": {
        "id": "b0948a5e5313200c632b51bb5ca32f6de0d36e9950a942d19751e833f70dabfd",
        "search_terms": ["NATGAS/USD", "NG/USD", "XNG/USD"],
        "kalshi_series": ["KXNATGAS15M", "KXNATGASH"]
    },
    "COPPER": {
        "id": "8bc9619b0c53478921495c25f0d984f536e56e57b933b419b1e39524002f36d4",
        "search_terms": ["XCU/USD", "HG/USD", "COPPER"],
        "kalshi_series": ["KXCOPPER15M", "KXCOPPERH"]
    },
    # --- Forex Pairs ---
    "EURUSD": {
        "id": "a995d00bb36a63cef7fd2c287dc105fc8f3d93779f062f09551b0af3e81ec30b",
        "search_terms": ["FX.EUR/USD", "EUR/USD"],
        "kalshi_series": ["KXEURUSD15M", "KXEURUSDH"]
    },
    "GBPUSD": {
        "id": "84c2dde9633d93d1bcad84e7dc41c9d56578b7ec52fabedc1f335d673df0a7c1",
        "search_terms": ["FX.GBP/USD", "GBP/USD"],
        "kalshi_series": ["KXGBPUSD15M", "KXGBPUSDH"]
    },
    "USDJPY": {
        "id": "ef2c98c804ba503c6a707e38be4dfbb16683775f195b091252bf24693042fd52",
        "search_terms": ["FX.USD/JPY", "USD/JPY"],
        "kalshi_series": ["KXUSDJPY15M", "KXUSDJPYH"]
    },
    "AUDUSD": {
        "id": "67a6f93030420c1c9e3fe37c1ab6b77966af82f995944a9fefce357a22854a80",
        "search_terms": ["FX.AUD/USD", "AUD/USD"],
        "kalshi_series": ["KXAUDUSD15M"]
    },
    # --- Equities & Indices ---
    "INX": {
        "id": "2817b78438c769357182c04346fddaad1178c82f4048828fe0997c3c64624e14",
        "search_terms": ["SPY/USD", "SPX", "US500"],
        "kalshi_series": ["KXINX15M", "KXINXH", "KXINXD", "KXINXW", "KXINXM"]
    },
    "NDQ": {
        "id": "9695e2b96ea7b3859da9ed25b7a46a920a776e2f019a7942b21f23a45218082b",
        "search_terms": ["QQQ/USD", "NDX", "NAS100"],
        "kalshi_series": ["KXNDQ15M", "KXNDQH", "KXNDQD"]
    },
    # --- Core Crypto Benchmarks ---
    "BTC": {
        "id": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
        "search_terms": ["Crypto.BTC/USD", "BTC/USD"],
        "kalshi_series": ["KXBTC15M", "KXBTCH", "KXBTCD", "KXBTCW", "KXBTCM"]
    },
    "ETH": {
        "id": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
        "search_terms": ["Crypto.ETH/USD", "ETH/USD"],
        "kalshi_series": ["KXETH15M", "KXETHH", "KXETHD", "KXETHW", "KXETHM"]
    },
    "SOL": {
        "id": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
        "search_terms": ["Crypto.SOL/USD", "SOL/USD"],
        "kalshi_series": ["KXSOL15M", "KXSOLH", "KXSOLD"]
    },
    "XRP": {
        "id": "ec5d399846a9209f3fe5881d70aae9268c94339ff9817e8d18ff19fa05eea1c8",
        "search_terms": ["Crypto.XRP/USD", "XRP/USD"],
        "kalshi_series": ["KXXRP15M", "KXXRPH", "KXXRPD"]
    },
    "DOGE": {
        "id": "dcef50dd0a4cd2dcc17e45df1676dcb336a11a61c69df7a0299b0150c672d25c",
        "search_terms": ["Crypto.DOGE/USD", "DOGE/USD"],
        "kalshi_series": ["KXDOGE15M", "KXDOGEH", "KXDOGED"]
    },
    "BNB": {
        "id": "2f95862b045670cd22bee3114c39763a4a08beeb663b145d283c31d7d1101c4f",
        "search_terms": ["Crypto.BNB/USD", "BNB/USD"],
        "kalshi_series": ["KXBNB15M", "KXBNBH", "KXBNBD"]
    },
    "HYPE": {
        "id": "4279e31cc369bbcc2faf022b382b080e32a8e689ff20fbc530d2a603eb6cd98b",
        "search_terms": ["Crypto.HYPE/USD", "HYPE/USD"],
        "kalshi_series": ["KXHYPE15M", "KXHYPEH", "KXHYPED"]
    },
    "NEAR": {
        "id": "c415de8d2eba7db216527dff4b60e8f3a5311c740dadb233e13e12547e226750",
        "search_terms": ["Crypto.NEAR/USD", "NEAR/USD"],
        "kalshi_series": ["KXNEAR15M", "KXNEARH"]
    },
    "ZEC": {
        "id": "be9b59d178f0d6a97ab4c343bff2aa69ca1e3c1b04a8c28c882de9a5b762a21f",
        "search_terms": ["Crypto.ZEC/USD", "ZEC/USD"],
        "kalshi_series": ["KXZEC15M", "KXZEC1H"]
    }
}


# ---------------------------------------------------------
# 1. Per-Asset Rolling 1m OHLC, Wilder ATR & TWAP State
# ---------------------------------------------------------
class PythAssetVolatilityTracker:
    """
    Processes sub-second Pyth ticks (`price * 10^expo`) for a single asset:
    - Builds 1-minute OHLC bars in memory.
    - Computes 14-period Wilder ATR (blended with live forming-bar True Range).
    - Tracks exact 15-minute and 1-hour boundary opening baseline prices.
    - Computes deterministic 60-second settlement TWAP trajectories during minute 14:00-15:00.
    """
    def __init__(self, asset_symbol: str, atr_period: int = 14):
        self.asset_symbol = asset_symbol
        self.atr_period = atr_period
        self.completed_bars: Deque[Dict[str, float]] = collections.deque(maxlen=90)
        self.sec_ticks_60s: Deque[Tuple[float, float]] = collections.deque(maxlen=150)

        self.current_minute_bucket: Optional[int] = None
        self.cur_open: float = 0.0
        self.cur_high: float = 0.0
        self.cur_low: float = 0.0
        self.cur_close: float = 0.0
        self.cur_conf: float = 0.0

        self.window_15m_open: Dict[int, float] = {}
        self.window_1h_open: Dict[int, float] = {}

        self.wilder_atr_1m: float = 0.0
        self.last_publish_time: float = 0.0
        self.tick_count: int = 0

    def on_pyth_tick(self, raw_price: int, raw_conf: int, expo: int, publish_time: float):
        """Ingests a raw integer Pyth price update and updates 1m bars & Wilder ATR."""
        scale = math.pow(10.0, int(expo))
        price = float(raw_price) * scale
        conf = float(raw_conf) * scale
        if price <= 0:
            return

        # Ignore out-of-order stale ticks
        if publish_time < self.last_publish_time:
            return

        self.last_publish_time = publish_time
        self.cur_conf = conf
        self.tick_count += 1
        self.sec_ticks_60s.append((publish_time, price))

        # Record 15-minute (900s) and 1-hour (3600s) candle opening baseline prices
        bucket_15m = int(publish_time // 900)
        bucket_1h = int(publish_time // 3600)
        if bucket_15m not in self.window_15m_open:
            self.window_15m_open[bucket_15m] = price
            # Prune old 15m buckets
            if len(self.window_15m_open) > 8:
                oldest = min(self.window_15m_open.keys())
                del self.window_15m_open[oldest]

        if bucket_1h not in self.window_1h_open:
            self.window_1h_open[bucket_1h] = price
            if len(self.window_1h_open) > 6:
                oldest_h = min(self.window_1h_open.keys())
                del self.window_1h_open[oldest_h]

        min_bucket = int(publish_time // 60)
        if self.current_minute_bucket is None:
            self.current_minute_bucket = min_bucket
            self.cur_open = self.cur_high = self.cur_low = self.cur_close = price
        elif min_bucket > self.current_minute_bucket:
            # Finalize completed 1-minute bar
            self.completed_bars.append({
                "minute": float(self.current_minute_bucket),
                "open": self.cur_open,
                "high": self.cur_high,
                "low": self.cur_low,
                "close": self.cur_close
            })
            self._recompute_wilder_atr()
            # Roll into new minute bar
            self.current_minute_bucket = min_bucket
            self.cur_open = self.cur_high = self.cur_low = self.cur_close = price
        else:
            self.cur_high = max(self.cur_high, price)
            self.cur_low = min(self.cur_low, price)
            self.cur_close = price

    def _recompute_wilder_atr(self):
        bars = list(self.completed_bars)
        if not bars:
            return
        trs: List[float] = []
        prev_close: Optional[float] = None
        for b in bars:
            if prev_close is None:
                tr = b["high"] - b["low"]
            else:
                tr = max(
                    b["high"] - b["low"],
                    abs(b["high"] - prev_close),
                    abs(b["low"] - prev_close)
                )
            trs.append(max(1e-9, tr))
            prev_close = b["close"]

        # Apply Wilder's smoothing over up to `atr_period` bars
        n = min(len(trs), self.atr_period)
        if len(trs) <= self.atr_period:
            self.wilder_atr_1m = sum(trs) / float(n)
        else:
            seed = sum(trs[:self.atr_period]) / float(self.atr_period)
            atr_val = seed
            for tr in trs[self.atr_period:]:
                atr_val = ((atr_val * (self.atr_period - 1)) + tr) / float(self.atr_period)
            self.wilder_atr_1m = atr_val

    def get_effective_atr_1m(self) -> float:
        """
        Returns the 14-period 1m Wilder ATR blended with the current forming minute's True Range
        so sudden sub-second breakouts during cold-start or news spikes are immediately reflected.
        """
        prev_c = self.completed_bars[-1]["close"] if self.completed_bars else self.cur_open
        forming_tr = max(
            self.cur_high - self.cur_low,
            abs(self.cur_high - prev_c),
            abs(self.cur_low - prev_c)
        )
        if self.wilder_atr_1m <= 0:
            # Cold-start fallback: max of forming TR or 8 bps of spot
            return max(forming_tr, self.cur_close * 0.0008)
        return max(self.wilder_atr_1m, forming_tr * 0.40)

    def get_window_metrics(self, timeframe: str = "15m", target_strike: Optional[float] = None) -> Dict[str, float]:
        """
        Computes real-time Spot, Baseline/Strike, signed Distance, 1m ATR, Normalized Sigma,
        60-second rolling TWAP, and Confidence Shock Ratio (`conf_bps`).
        """
        atr = self.get_effective_atr_1m()
        if target_strike is not None and target_strike > 0:
            baseline = target_strike
        elif timeframe == "15m":
            b15 = int(self.last_publish_time // 900)
            baseline = self.window_15m_open.get(b15, self.cur_open)
        else:
            b1h = int(self.last_publish_time // 3600)
            baseline = self.window_1h_open.get(b1h, self.cur_open)

        distance = self.cur_close - baseline
        sigma = abs(distance) / atr if atr > 0 else 0.0

        # Compute rolling 60-second TWAP
        cutoff_60s = self.last_publish_time - 60.0
        recent_pxs = [px for ts, px in self.sec_ticks_60s if ts >= cutoff_60s]
        twap_60s = sum(recent_pxs) / len(recent_pxs) if recent_pxs else self.cur_close

        conf_bps = (self.cur_conf / self.cur_close) * 10000.0 if self.cur_close > 0 else 0.0

        return {
            "spot": self.cur_close,
            "baseline": baseline,
            "distance": distance,
            "atr_1m": atr,
            "sigma": round(sigma, 3),
            "twap_60s": twap_60s,
            "conf_bps": round(conf_bps, 2),
            "completed_bars": float(len(self.completed_bars)),
            "last_update_ts": self.last_publish_time
        }


# ---------------------------------------------------------
# 2. Central Pyth Oracle Registry & Dynamic Feed Resolver
# ---------------------------------------------------------
class PythOracleManager:
    """
    Manages all 23+ Commodity, Forex, Index, and Crypto volatility trackers,
    resolves canonical hex IDs from Hermes `/v2/price_feeds`, persists snapshots
    to SQLite (`pyth_oracle_snapshots`), and triggers instant Kalshi series checks
    when any asset breaks out past `>= 2.0x ATR` during an active trading window.
    """
    def __init__(self,
                 db_path: str = DB_FILE,
                 breakout_callback: Optional[Callable[[str, str, Dict[str, float]], Coroutine[Any, Any, None]]] = None):
        self.db_path = db_path
        self.breakout_callback = breakout_callback
        self.trackers: Dict[str, PythAssetVolatilityTracker] = {}
        self.feed_id_to_asset: Dict[str, str] = {}
        self.series_to_asset: Dict[str, str] = {}
        self.last_breakout_trigger_ts: Dict[str, float] = {}
        self.last_sqlite_flush_ts: float = 0.0

        self._init_catalog_mappings()
        self._init_sqlite_table()

    def _init_catalog_mappings(self):
        for asset_sym, meta in CANONICAL_PYTH_FEEDS.items():
            clean_id = meta["id"].lower().replace("0x", "")
            self.trackers[asset_sym] = PythAssetVolatilityTracker(asset_sym)
            self.feed_id_to_asset[clean_id] = asset_sym
            for series in meta["kalshi_series"]:
                self.series_to_asset[series] = asset_sym

    def _init_sqlite_table(self):
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS pyth_oracle_snapshots (
                asset TEXT PRIMARY KEY,
                spot_price REAL,
                baseline_15m REAL,
                distance_15m REAL,
                atr_1m REAL,
                sigma_15m REAL,
                twap_60s REAL,
                conf_bps REAL,
                updated_at INTEGER
            )
        """)
        conn.commit()
        conn.close()

    async def verify_and_discover_feed_ids(self, session: aiohttp.ClientSession):
        """
        Queries `https://hermes.pyth.network/v2/price_feeds` at startup to verify and
        dynamically update any commodity/FX/index feed IDs against Pyth's live catalog.
        """
        try:
            async with session.get(PYTH_FEEDS_CATALOG_URL, timeout=12) as resp:
                if resp.status != 200:
                    return
                catalog = await resp.json()
                if not isinstance(catalog, list):
                    return

                symbol_lookup: Dict[str, str] = {}
                for item in catalog:
                    fid = str(item.get("id", "")).lower().replace("0x", "")
                    attrs = item.get("attributes", {}) or {}
                    sym = str(attrs.get("symbol", "")).upper()
                    base = str(attrs.get("base", "")).upper()
                    quote = str(attrs.get("quote_currency", "")).upper()
                    if fid and sym:
                        symbol_lookup[sym] = fid
                        if base and quote:
                            symbol_lookup[f"{base}/{quote}"] = fid

                updated_count = 0
                for asset_sym, meta in CANONICAL_PYTH_FEEDS.items():
                    for term in meta["search_terms"]:
                        t_up = term.upper()
                        if t_up in symbol_lookup:
                            live_fid = symbol_lookup[t_up]
                            old_fid = meta["id"].lower().replace("0x", "")
                            if live_fid != old_fid:
                                self.feed_id_to_asset.pop(old_fid, None)
                                meta["id"] = live_fid
                                self.feed_id_to_asset[live_fid] = asset_sym
                                updated_count += 1
                            break

                logging.info(
                    f"[Pyth Catalog] Verified {len(self.trackers)} Feeds across Commodities, FX, Indices & Crypto "
                    f"({updated_count} dynamically refreshed)."
                )
        except Exception as e:
            logging.debug(f"[Pyth Catalog] Using static verified feed IDs ({e}).")

    def process_parsed_price_item(self, item: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, float]]]:
        """
        Parses a single Pyth price object (from either WebSocket `price_feed` or SSE `parsed` array)
        and updates the corresponding `PythAssetVolatilityTracker`.
        """
        fid = str(item.get("id", "")).lower().replace("0x", "")
        asset_sym = self.feed_id_to_asset.get(fid)
        if not asset_sym:
            return None

        price_obj = item.get("price")
        if not isinstance(price_obj, dict):
            return None

        raw_price = int(price_obj.get("price", 0))
        raw_conf = int(price_obj.get("conf", 0))
        expo = int(price_obj.get("expo", 0))
        pub_time = float(price_obj.get("publish_time", time.time()))

        tracker = self.trackers[asset_sym]
        tracker.on_pyth_tick(raw_price, raw_conf, expo, pub_time)
        metrics = tracker.get_window_metrics("15m")

        # Periodically flush all live oracle metrics to SQLite every 3 seconds
        now = time.time()
        if (now - self.last_sqlite_flush_ts) >= 3.0:
            self.last_sqlite_flush_ts = now
            self.flush_snapshots_to_sqlite()

        return asset_sym, metrics

    def flush_snapshots_to_sqlite(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()
        now_ts = int(time.time())
        rows = []
        for sym, trk in self.trackers