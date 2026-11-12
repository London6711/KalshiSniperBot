#!/usr/bin/env python3
"""
Polymarket-to-Kalshi Cross-Exchange Arbitrage Scanner & Execution Module
========================================================================
Continuously compares live Polymarket CLOB orderbooks against Kalshi brackets
across Crypto, Commodities, and Equity Indices to snipe net spreads > 4.0 cents.

Supports two execution modes:
1. SYNTHETIC_BOX_ARB: Buy Kalshi YES + Buy Polymarket NO (or vice versa) when
   combined ask/entry cost <= 95c-96c (net spread > 4.0c after fee curve).
2. LEAD_LAG_VALUE_SNIPE: Polymarket CLOB mid-price leads Kalshi orderbook by > 4.0c
   with >= $500 book depth -> fires immediate Kalshi order before Kalshi MMs reprice.
"""

import asyncio
import datetime
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("cross_arb.log")]
)

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")

# Public Endpoints
KALSHI_API_BASE = os.getenv("KALSHI_API_BASE", "https://external-api.kalshi.com/trade-api/v2")
POLY_GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
POLY_CLOB_BOOK_URL = "https://clob.polymarket.com/book"

# Minimum net spread (in cents) required after Kalshi fee-curve deduction
MIN_NET_ARB_SPREAD_CENTS = 4.0
# Minimum Polymarket top-of-book liquidity (USD) required to trust the reference price
MIN_POLY_BOOK_DEPTH_USD = 500.0

# Asset Keyword to Kalshi Series Mapping
ARB_ASSET_SERIES_MAP: Dict[str, List[str]] = {
    "BTC": ["KXBTCH", "KXBTCD", "KXBTC15M", "KXBTCW"],
    "ETH": ["KXETHH", "KXETHD", "KXETH15M", "KXETHW"],
    "SOL": ["KXSOLH", "KXSOLD", "KXSOL15M"],
    "XRP": ["KXXRPH", "KXXRPD", "KXXRP15M"],
    "DOGE": ["KXDOGEH", "KXDOGED", "KXDOGE15M"],
    "BNB": ["KXBNBH", "KXBNBD", "KXBNB15M"],
    "GOLD": ["KXGOLDH", "KXGOLD15M", "KXGOLDW"],
    "WTI": ["KXWTIH", "KXWTI15M", "KXWTIW"],
    "INX": ["KXINXH", "KXINXD", "KXINX15M"],
    "NDQ": ["KXNDQH", "KXNDQD", "KXNDQ15M"]
}


# ---------------------------------------------------------
# 1. WAL-Enabled SQLite Schema & Telemetry Logger
# ---------------------------------------------------------
def get_db_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_arb_tables(db_path: str = DB_FILE):
    conn = get_db_connection(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS cross_arb_opportunities (
            arb_id TEXT PRIMARY KEY,
            timestamp INTEGER,
            asset TEXT,
            kalshi_ticker TEXT,
            poly_market_slug TEXT,
            arb_type TEXT,           -- 'SYNTHETIC_BOX' or 'LEAD_LAG_SNIPE'
            kalshi_side TEXT,        -- 'yes' or 'no'
            kalshi_price_cents INTEGER,
            poly_yes_cents REAL,
            poly_no_cents REAL,
            gross_spread_cents REAL,
            fee_cents REAL,
            net_spread_cents REAL,
            executed INTEGER
        )
    """)
    conn.commit()
    conn.close()


def log_arb_opportunity(record: Dict[str, Any], db_path: str = DB_FILE):
    conn = get_db_connection(db_path)
    c = conn.cursor()
    c.execute("""
        INSERT INTO cross_arb_opportunities (
            arb_id, timestamp, asset, kalshi_ticker, poly_market_slug,
            arb_type, kalshi_side, kalshi_price_cents, poly_yes_cents,
            poly_no_cents, gross_spread_cents, fee_cents, net_spread_cents, executed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record["arb_id"],
        int(time.time()),
        record["asset"],
        record["kalshi_ticker"],
        record["poly_slug"],
        record["arb_type"],
        record["kalshi_side"],
        int(record["kalshi_price_cents"]),
        round(float(record["poly_yes_cents"]), 2),
        round(float(record["poly_no_cents"]), 2),
        round(float(record["gross_spread_cents"]), 2),
        round(float(record["fee_cents"]), 2),
        round(float(record["net_spread_cents"]), 2),
        int(record["executed"])
    ))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# 2. Fee-Curve & Arbitrage Spread Mathematics
# ---------------------------------------------------------
def compute_kalshi_fee_cents_per_contract(price_cents: int, post_only: bool = True) -> float:
    """
    Computes exact Kalshi per-contract fee in cents using the P * (1 - P) curve.
    - Maker (post_only=True): ~1.75% * P * (1 - P) (or 0c on zero-maker-fee series).
    - Taker (post_only=False): 7.0% * P * (1 - P).
    """
    p = max(0.01, min(0.99, price_cents / 100.0))
    coeff = 0.0175 if post_only else 0.07
    return round(coeff * p * (1.0 - p) * 100.0, 2)


def evaluate_cross_exchange_spread(kalshi_yes_ask: int,
                                   kalshi_no_ask: int,
                                   poly_yes_ask_cents: float,
                                   poly_no_ask_cents: float,
                                   poly_yes_mid_cents: float,
                                   poly_no_mid_cents: float,
                                   post_only: bool = True) -> Optional[Dict[str, Any]]:
    """
    Evaluates a matched (Kalshi, Polymarket) contract pair for > 4.0c net arbitrage:
    1. Checks Synthetic Box Arb (Kalshi YES + Poly NO < 100c, or Kalshi NO + Poly YES < 100c).
    2. Checks Lead-Lag Fair-Value Snipe (Polymarket mid exceeds Kalshi entry price by > 4.0c + fees).
    Returns the highest-EV opportunity dict if net_spread_cents > MIN_NET_ARB_SPREAD_CENTS (4.0c).
    """
    candidates: List[Dict[str, Any]] = []

    # --- Check 1A: Synthetic Box (Buy Kalshi YES + Buy Polymarket NO) ---
    if 1 <= kalshi_yes_ask <= 95 and 1.0 <= poly_no_ask_cents <= 95.0:
        fee_c = compute_kalshi_fee_cents_per_contract(kalshi_yes_ask, post_only)
        gross_box_1 = 100.0 - (float(kalshi_yes_ask) + poly_no_ask_cents)
        net_box_1 = gross_box_1 - fee_c
        if net_box_1 > MIN_NET_ARB_SPREAD_CENTS:
            candidates.append({
                "arb_type": "SYNTHETIC_BOX",
                "kalshi_side": "yes",
                "kalshi_price_cents": kalshi_yes_ask,
                "poly_yes_cents": poly_yes_ask_cents,
                "poly_no_cents": poly_no_ask_cents,
                "gross_spread_cents": gross_box_1,
                "fee_cents": fee_c,
                "net_spread_cents": net_box_1
            })

    # --- Check 1B: Synthetic Box (Buy Kalshi NO + Buy Polymarket YES) ---
    if 1 <= kalshi_no_ask <= 95 and 1.0 <= poly_yes_ask_cents <= 95.0:
        fee_c = compute_kalshi_fee_cents_per_contract(kalshi_no_ask, post_only)
        gross_box_2 = 100.0 - (float(kalshi_no_ask) + poly_yes_ask_cents)
        net_box_2 = gross_box_2 - fee_c
        if net_box_2 > MIN_NET_ARB_SPREAD_CENTS:
            candidates.append({
                "arb_type": "SYNTHETIC_BOX",
                "kalshi_side": "no",
                "kalshi_price_cents": kalshi_no_ask,
                "poly_yes_cents": poly_yes_ask_cents,
                "poly_no_cents": poly_no_ask_cents,
                "gross_spread_cents": gross_box_2,
                "fee_cents": fee_c,
                "net_spread_cents": net_box_2
            })

    # --- Check 2A: Lead-Lag Fair-Value Snipe on Kalshi YES ---
    if 15 <= kalshi_yes_ask <= 88 and poly_yes_mid_cents > float(kalshi_yes_ask):
        fee_c = compute_kalshi_fee_cents_per_contract(kalshi_yes_ask, post_only)
        gross_fv_yes = poly_yes_mid_cents - float(kalshi_yes_ask)
        net_fv_yes = gross_fv_yes - fee_c
        if net_fv_yes > MIN_NET_ARB_SPREAD_CENTS:
            candidates.append({
                "arb_type": "LEAD_LAG_SNIPE",
                "kalshi_side": "yes",
                "kalshi_price_cents": kalshi_yes_ask,
                "poly_yes_cents": poly_yes_mid_cents,
                "poly_no_cents": poly_no_mid_cents,
                "gross_spread_cents": gross_fv_yes,
                "fee_cents": fee_c,
                "net_spread_cents": net_fv_yes
            })

    # --- Check 2B: Lead-Lag Fair-Value Snipe on Kalshi NO ---
    if 15 <= kalshi_no_ask <= 88 and poly_no_mid_cents > float(kalshi_no_ask):
        fee_c = compute_kalshi_fee_cents_per_contract(kalshi_no_ask, post_only)
        gross_fv_no = poly_no_mid_cents - float(kalshi_no_ask)
        net_fv_no = gross_fv_no - fee_c
        if net_fv_no > MIN_NET_ARB_SPREAD_CENTS:
            candidates.append({
                "arb_type": "LEAD_LAG_SNIPE",
                "kalshi_side": "no",
                "kalshi_price_cents": kalshi_no_ask,
                "poly_yes_cents": poly_yes_mid_cents,
                "poly_no_cents": poly_no_mid_cents,
                "gross_spread_cents": gross_fv_no,
                "fee_cents": fee_c,
                "net_spread_cents": net_fv_no
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["net_spread_cents"], reverse=True)
    return candidates[0]


# ---------------------------------------------------------
# 3. Strike Price & Asset Normalization Parser
# ---------------------------------------------------------
STRIKE_REGEX = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)")


def extract_asset_and_strike(text: str) -> Tuple[Optional[str], Optional[float]]:
    upper = text.upper()
    asset_map = [
        ("BITCOIN", "BTC"), ("BTC", "BTC"),
        ("ETHEREUM", "ETH"), ("ETH", "ETH"),
        ("SOLANA", "SOL"), ("SOL", "SOL"),
        ("XRP", "XRP"), ("DOGECOIN", "DOGE"), ("DOGE", "DOGE"),
        ("BNB", "BNB"), ("GOLD", "GOLD"), ("CRUDE", "WTI"), ("WTI", "WTI"),
        ("S&P", "INX"), ("SPX", "INX"), ("NASDAQ", "NDQ")
    ]
    detected_asset = None
    for kw, sym in asset_map:
        if kw in upper:
            detected_asset = sym
            break

    if not detected_asset:
        return None, None

    matches = STRIKE_REGEX.findall(text)
    strike_val: Optional[float] = None
    for raw_num in matches:
        clean_num = float(raw_num.replace(",", ""))
        if detected_asset == "BTC" and clean_num >= 20000:
            strike_val = clean_num
            break
        elif detected_asset in ("ETH", "GOLD", "INX", "NDQ") and clean_num >= 1000 and clean_num not in (2025, 2026, 2027):
            strike_val = clean_num
            break
        elif detected_asset in ("SOL", "BNB", "WTI") and 30 <= clean_num <= 1500:
            strike_val = clean_num
            break
        elif detected_asset in ("XRP", "DOGE") and 0.05 <= clean_num <= 25.0:
            strike_val = clean_num
            break

    return detected_asset, strike_val


def is_basis_safe_from_pyth(asset: str, strike: Optional[float], db_path: str = DB_FILE) -> bool:
    if strike is None or not os.path.exists(db_path):
        return True
    conn = get_db_connection(db_path)
    c = conn.cursor()
    try:
        c.execute("SELECT spot_price, atr_1m FROM pyth_oracle_snapshots WHERE asset = ?", (asset,))
        row = c.fetchone()
    except Exception:
        row = None
    finally:
        conn.close()

    if not row:
        return True
    spot, atr_1m = float(row[0] or 0.0), float(row[1] or 0.0)
    if spot <= 0 or atr_1m <= 0:
        return True

    return abs(spot - strike) >= (0.35 * atr_1m)


# ---------------------------------------------------------
# 4. Live Polymarket & Kalshi Book Ingestion + Matcher
# ---------------------------------------------------------
async def fetch_active_polymarket_brackets(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    params = {
        "active": "true",
        "closed": "false",
        "limit": 100,
        "order": "volume24hr",
        "ascending": "false"
    }
    normalized_markets: List[Dict[str, Any]] = []
    try:
        async with session.get(POLY_GAMMA_MARKETS_URL, params=params, timeout=10) as resp:
            if resp.status != 200:
                return []
            raw_list = await resp.json()
            if not isinstance(raw_list, list):
                return []

            for m in raw_list:
                question = str(m.get("question") or m.get("title") or "")
                slug = str(m.get("slug") or m.get("id") or "")
                liq = float(m.get("liquidityNum", m.get("liquidity", 0.0)) or 0.0)
                if liq < MIN_POLY_BOOK_DEPTH_USD:
                    continue

                asset, strike = extract_asset_and_strike(question)
                if not asset:
                    continue

                raw_prices = m.get("outcomePrices")
                if isinstance(raw_prices, str):
                    try:
                        raw_prices = json.loads(raw_prices)
                    except Exception:
                        raw_prices = None

                if not isinstance(raw_prices, list) or len(raw_prices) < 2:
                    continue

                yes_prob = float(raw_prices[0])
                no_prob = float(raw_prices[1])
                yes_cents = yes_prob * 100.0
                no_cents = no_prob * 100.0

                best_ask_yes = min(99.0, float(m.get("bestAsk", yes_prob + 0.005) or (yes_prob + 0.005)) * 100.0)
                best_ask_no = min(99.0, max(1.0, 100.0 - (float(m.get("bestBid", yes_prob - 0.005) or (yes_prob - 
