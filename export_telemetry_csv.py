#!/usr/bin/env python3
"""
Automated Weekly & Year-End Tax Telemetry Exporter (`export_telemetry_csv.py`)
==============================================================================
Reads `trade_telemetry_v2.sqlite` in Read-Only WAL mode (`?mode=ro`) and exports
spreadsheet-ready CSV reports into `~/kalshi_workspace/exports/`:
1. `kalshi_trades_<WEEK>.csv`          -> Itemized trade ledger with costs, fees, PnL & yield.
2. `cross_exchange_arbs_<WEEK>.csv`    -> Itemized Polymarket <-> Kalshi (>4.0c) arbitrage log.
3. `weekly_kpi_summary_<WEEK>.csv`     -> Category & Tier win-rate, profit-factor & ROI rollup.
4. `year_end_tax_summary_<YEAR>.csv`   -> Dedicated tax summary grouped by short-term gains,
                                          fees paid, and Treasury interest income.

Usage:
  python3 export_telemetry_csv.py --verify               # Run offline verification suite
  python3 export_telemetry_csv.py --days 7               # Export weekly spreadsheets
  python3 export_telemetry_csv.py --tax-year 2026        # Export dedicated year-end tax summary
  python3 export_telemetry_csv.py --all --telegram       # Export all history & push to Telegram
"""

import argparse
import csv
import datetime
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("csv_exporter.log")]
)

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")
EXPORT_DIR = os.getenv(
    "KALSHI_EXPORT_DIR",
    os.path.join(os.path.dirname(os.path.abspath(DB_FILE)), "exports")
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def get_ro_wal_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True, timeout=10.0)
    except Exception:
        return sqlite3.connect(db_path, timeout=10.0)


def estimate_kalshi_maker_fee_usd(entry_price_cents: int, count: int) -> float:
    p = max(0.01, min(0.99, float(entry_price_cents) / 100.0))
    return round(0.0175 * float(count) * p * (1.0 - p), 4)


def format_iso_week(unix_ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(int(unix_ts), tz=datetime.timezone.utc)
    iso_yr, iso_wk, _ = dt.isocalendar()
    return f"{iso_yr}-W{iso_wk:02d}"


def format_utc_timestamp(unix_ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(int(unix_ts), tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------
# 1. Weekly Spreadsheets Generator
# ---------------------------------------------------------
def export_weekly_spreadsheets(db_path: str = DB_FILE,
                               export_dir: str = EXPORT_DIR,
                               lookback_days: Optional[int] = 7) -> Dict[str, Any]:
    os.makedirs(export_dir, exist_ok=True)
    now_ts = int(time.time())
    cutoff_ts = 0 if (lookback_days is None or lookback_days <= 0) else (now_ts - (lookback_days * 86400))

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    iso_yr, iso_wk, _ = now_dt.isocalendar()
    tag = f"{iso_yr}_W{iso_wk:02d}_{now_dt.strftime('%Y%m%d')}"

    trades_csv_path = os.path.join(export_dir, f"kalshi_trades_{tag}.csv")
    arbs_csv_path = os.path.join(export_dir, f"cross_exchange_arbs_{tag}.csv")
    summary_csv_path = os.path.join(export_dir, f"weekly_kpi_summary_{tag}.csv")

    conn = get_ro_wal_connection(db_path)
    c = conn.cursor()

    try:
        c.execute("""
            SELECT trade_id, market_ticker, series, category, timeframe, timestamp,
                   side, tier, count, entry_price, seconds_remaining, distance_to_target,
                   atr_1m, normalized_distance, wallet_confluence_score,
                   resolved_price, pnl_cents, interest_earned_cents, status
            FROM trade_logs
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
        """, (cutoff_ts,))
        trade_rows = c.fetchall()
    except Exception:
        trade_rows = []

    try:
        c.execute("""
            SELECT arb_id, timestamp, asset, kalshi_ticker, poly_market_slug,
                   arb_type, kalshi_side, kalshi_price_cents, poly_yes_cents,
                   poly_no_cents, gross_spread_cents, fee_cents, net_spread_cents, executed
            FROM cross_arb_opportunities
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
        """, (cutoff_ts,))
        arb_rows = c.fetchall()
    except Exception:
        arb_rows = []

    conn.close()

    trade_headers = [
        "iso_week", "utc_timestamp", "trade_id", "status", "category", "series",
        "market_ticker", "timeframe", "tier", "side", "contracts", "entry_price_cents",
        "exit_or_resolved_cents", "cost_basis_usd", "gross_proceeds_usd",
        "est_maker_fee_usd", "realized_pnl_usd", "accrued_yield_usd",
        "net_total_return_usd", "roi_pct", "seconds_remaining_at_entry",
        "distance_to_target", "atr_1m", "normalized_sigma", "wallet_confluence_score"
    ]

    kpi_buckets: Dict[Tuple[str, str, str], Dict[str, float]] = {}
    total_realized_pnl_usd = 0.0
    total_yield_usd = 0.0
    settled_count = 0
    win_count = 0

    with open(trades_csv_path, mode="w", newline="", encoding="utf-8") as f_tr:
        writer = csv.writer(f_tr)
        writer.writerow(trade_headers)

        for row in trade_rows:
            (
                tid, ticker, series, cat, tf, ts, side, tier, cnt, entry_px,
                secs_left, dist, atr, norm_dist, conf, res_px, pnl_c, yield_c, status
            ) = row

            cnt_i = int(cnt or 1)
            entry_i = int(entry_px or 0)
            ts_i = int(ts or now_ts)
            iso_w = format_iso_week(ts_i)
            utc_str = format_utc_timestamp(ts_i)

            cost_basis_usd = round((entry_i / 100.0) * cnt_i, 4)
            fee_usd = estimate_kalshi_maker_fee_usd(entry_i, cnt_i)
            yield_usd = round(float(yield_c or 0.0) / 100.0, 4)

            if str(status).upper() == "SETTLED" and res_px is not None:
                res_i = int(res_px)
                gross_proceeds_usd = round((res_i / 100.0) * cnt_i, 4)
                pnl_usd = round(float(pnl_c or 0) / 100.0, 4)
                net_total_usd = round(pnl_usd + yield_usd - fee_usd, 4)
                roi_pct = round((net_total_usd / cost_basis_usd) * 100.0, 2) if cost_basis_usd > 0 else 0.0

                settled_count += 1
                if pnl_usd > 0:
                    win_count += 1
                total_realized_pnl_usd += pnl_usd
                total_yield_usd += yield_usd

                b_key = (iso_w, str(cat or "OTHER"), str(tier or "TIER_1"))
                b = kpi_buckets.setdefault(b_key, {
                    "trades": 0.0, "wins": 0.0, "losses": 0.0,
                    "outlay_usd": 0.0, "gross_win_usd": 0.0, "gross_loss_usd": 0.0,
                    "fees_usd": 0.0, "yield_usd": 0.0, "net_pnl_usd": 0.0
                })
                b["trades"] += 1
                if pnl_usd > 0:
                    b["wins"] += 1
                    b["gross_win_usd"] += pnl_usd
                else:
                    b["losses"] += 1
                    b["gross_loss_usd"] += abs(pnl_usd)
                b["outlay_usd"] += cost_basis_usd
                b["fees_usd"] += fee_usd
                b["yield_usd"] += yield_usd
                b["net_pnl_usd"] += net_total_usd
            else:
                res_i = ""
                gross_proceeds_usd = ""
                pnl_usd = ""
                net_total_usd = ""
                roi_pct = ""

            writer.writerow([
                iso_w, utc_str, tid, status, cat, series, ticker, tf, tier,
                str(side).upper(), cnt_i, entry_i, res_i,
                f"{cost_basis_usd:.2f}",
                f"{gross_proceeds_usd:.2f}" if gross_proceeds_usd != "" else "",
                f"{fee_usd:.4f}",
                f"{pnl_usd:.2f}" if pnl_usd != "" else "",
                f"{yield_usd:.4f}",
                f"{net_total_usd:.2f}" if net_total_usd != "" else "",
                f"{roi_pct:.2f}%" if roi_pct != "" else "",
                secs_left,
                round(float(dist or 0.0), 4),
                round(float(atr or 0.0), 4),
                round(float(norm_dist or 0.0), 3),
                round(float(conf or 0.0), 3)
            ])

    arb_headers = [
        "iso_week", "utc_timestamp", "arb_id", "asset", "arb_type",
        "kalshi_ticker", "poly_market_slug", "kalshi_side",
        "kalshi_price_cents", "poly_yes_cents", "poly_no_cents",
        "gross_spread_cents", "kalshi_fee_cents", "net_spread_cents",
        "executed_status"
    ]

    executed_arbs = 0
    with open(arbs_csv_path, mode="w", newline="", encoding="utf-8") as f_arb:
        writer = csv.writer(f_arb)
        writer.writerow(arb_headers)
        for row in arb_rows:
            (
                aid, ts, asset, k_ticker, p_slug, a_type, k_side,
                k_px, py_c, pn_c, gross_c, fee_c, net_c, exec_flag
            ) = row
            ts_i = int(ts or now_ts)
            is_exec = int(exec_flag or 0) == 1
            if is_exec:
                executed_arbs += 1
            writer.writerow([
                format_iso_week(ts_i),
                format_utc_timestamp(ts_i),
                aid, asset, a_type, k_ticker, p_slug,
                str(k_side).upper(), int(k_px or 0),
                f"{float(py_c or 0.0):.2f}", f"{float(pn_c or 0.0):.2f}",
                f"{float(gross_c or 0.0):.2f}", f"{float(fee_c or 0.0):.2f}",
                f"{float(net_c or 0.0):.2f}",
                "FILLED" if is_exec else "LOGGED_ONLY"
            ])

    summary_headers = [
        "iso_week", "category", "tier", "settled_trades", "wins", "losses",
        "win_rate_pct", "profit_factor", "total_outlay_usd", "gross_profit_usd",
        "gross_loss_usd", "est_fees_usd", "accrued_yield_usd",
        "net_total_pnl_usd", "net_roi_pct", "circuit_breaker_status"
    ]

    with open(summary_csv_path, mode="w", newline="", encoding="utf-8") as f_sum:
        writer = csv.writer(f_sum)
        writer.writerow(summary_headers)
        for (iso_w, cat, tier), m in sorted(kpi_buckets.items()):
            t_cnt = int(m["trades"])
            w_cnt = int(m["wins"])
            l_cnt = int(m["losses"])
            wr_pct = (w_cnt / t_cnt * 100.0) if t_cnt > 0 else 0.0
            pf = (m["gross_win_usd"] / m["gross_loss_usd"]) if m["gross_loss_usd"] > 0 else (99.9 if m["gross_win_usd"] > 0 else 0.0)
            roi_pct = (m["net_pnl_usd"] / m["outlay_usd"] * 100.0) if m["outlay_usd"] > 0 else 0.0
            cb_state = "HEALTHY (>=62%)" if wr_pct >= 62.0 else "WARNING (<62%)"

            writer.writerow([
                iso_w, cat, tier, t_cnt, w_cnt, l_cnt,
                f"{wr_pct:.2f}%", f"{pf:.2f}", f"{m['outlay_usd']:.2f}",
                f"{m['gross_win_usd']:.2f}", f"{m['gross_loss_usd']:.2f}",
                f"{m['fees_usd']:.4f}", f"{m['yield_usd']:.4f}",
                f"{m['net_pnl_usd']:.2f}", f"{roi_pct:.2f}%", cb_state
            ])

    overall_wr = round((win_count / settled_count * 100.0), 2) if settled_count > 0 else 0.0
    return {
        "trades_csv": trades_csv_path,
        "arbs_csv": arbs_csv_path,
        "summary_csv": summary_csv_path,
        "total_trades_exported": len(trade_rows),
        "settled_trades": settled_count,
        "overall_win_rate_pct": overall_wr,
        "realized_pnl_usd": round(total_realized_pnl_usd, 2),
        "accrued_yield_usd": round(total_yield_usd, 4),
        "total_arbs_exported": len(arb_rows),
        "executed_arbs": executed_arbs
    }


# ---------------------------------------------------------
# 2. Dedicated Year-End Tax Summary CSV Generator
# ---------------------------------------------------------
def export_year_end_tax_summary(db_path: str = DB_FILE,
                                export_dir: str = EXPORT_DIR,
                                tax_year: int = 2026) -> str:
    """
    Generates a dedicated Year-End Tax Summary CSV (`year_end_tax_summary_<YEAR>.csv`)
    grouped by short-term event contract capital gains/losses, fees paid, and
    Treasury interest / collateral yield income.
    """
    os.makedirs(export_dir, exist_ok=True)
    tax_csv_path = os.path.join(export_dir, f"year_end_tax_summary_{tax_year}.csv")

    # Timestamp boundaries for the target tax year in UTC
    start_dt = datetime.datetime(tax_year, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime(tax_year, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    conn = get_ro_wal_connection(db_path)
    c = conn.cursor()
    try:
        c.execute("""
            SELECT category, entry_price, count, pnl_cents, interest_earned_cents, timestamp
            FROM trade_logs
            WHERE status = 'SETTLED' AND timestamp >= ? AND timestamp <= ?
        """, (start_ts, end_ts))
        rows = c.fetchall()
    except Exception:
        rows = []
    conn.close()

    # Rollup structure grouped by category
    tax_buckets: Dict[str, Dict[str, float]] = {}

    for cat, entry_px, cnt, pnl_c, yield_c, _ in rows:
        category_key = str(cat or "OTHER_EVENT_CONTRACTS")
        b = tax_buckets.setdefault(category_key, {
            "settled_count": 0.0,
            "short_term_gains_usd": 0.0,
            "short_term_losses_usd": 0.0,
            "gross_realized_pnl_usd": 0.0,
            "total_fees_paid_usd": 0.0,
            "treasury_interest_usd": 0.0
        })

        b["settled_count"] += 1.0
        pnl_usd = float(pnl_c or 0) / 100.0
        b["gross_realized_pnl_usd"] += pnl_usd
        if pnl_usd > 0:
            b["short_term_gains_usd"] += pnl_usd
        else:
            b["short_term_losses_usd"] += abs(pnl_usd)

        fee_usd = estimate_kalshi_maker_fee_usd(int(entry_px or 0), int(cnt or 1))
        b["total_fees_paid_usd"] += fee_usd

        y_usd = float(yield_c or 0.0) / 100.0
        b["treasury_interest_usd"] += y_usd

    tax_headers = [
        "tax_year", "asset_category", "settled_contract_count",
        "short_term_capital_gains_usd", "short_term_capital_losses_usd",
        "gross_realized_capital_pnl_usd", "total_transaction_fees_paid_usd",
        "treasury_interest_income_usd", "net_taxable_amount_usd"
    ]

    with open(tax_csv_path, mode="w", newline="", encoding="utf-8") as f_tax:
        writer = csv.writer(f_tax)
        writer.writerow(tax_headers)

        for cat, m in sorted(tax_buckets.items()):
            net_taxable = (m["gross_realized_pnl_usd"] - m["total_fees_paid_usd"]) + m["treasury_interest_usd"]
            writer.writerow([
                tax_year,
                cat,
                int(m["settled_count"]),
                f"{m['short_term_gains_usd']:.2f}",
                f"{m['short_term_losses_usd']:.2f}",
                f"{m['gross_realized_pnl_usd']:.2f}",
                f"{m['total_fees_paid_usd']:.4f}",
                f"{m['treasury_interest_usd']:.4f}",
                f"{net_taxable:.2f}"
            ])

    logging.info(f"[Tax Summary Export] Generated year-end tax summary for {tax_year} at {tax_csv_path}")
    return tax_csv_path


# ---------------------------------------------------------
# 3. Optional Telegram Push
# ---------------------------------------------------------
async def push_csv_reports_to_telegram(meta: Dict[str, Any], tax_csv_path: Optional[str] = None) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files_to_send = [
        (meta["summary_csv"], f"📊 *Weekly KPI Summary CSV*\n• Settled: `{meta['settled_trades']}` | WR: `{meta['overall_win_rate_pct']}%` | Net PnL: `${meta['realized_pnl_usd']:+.2f}`"),
        (meta["trades_csv"], f"📑 *Itemized Trade Ledger CSV* (`{meta['total_trades_exported']}` rows)"),
        (meta["arbs_csv"], f"💎 *Cross-Exchange Arbitrage CSV* (`{meta['total_arbs_exported']}` spreads >4¢)")
    ]
    if tax_csv_path and os.path.exists(tax_csv_path):
        files_to_send.append((tax_csv_path, "🏛️ *Year-End Tax Summary CSV* (Short-Term Capital Gains, Fees & Treasury Interest)"))

    sent_ok = True
    async with aiohttp.ClientSession() as session:
        for fpath, caption in files_to_send:
            if not os.path.exists(fpath):
                continue
            try:
                with open(fpath, "rb") as f:
                    form = aiohttp.FormData()
                    form.add_field("chat_id", str(TELEGRAM_CHAT_ID))
                    form.add_field("caption", caption)
                    form.add_field("parse_mode", "Markdown")
                    form.add_field("document", f, filename=os.path.basename(fpath), content_type="text/csv")
                    async with session.post(url, data=form, timeout=25) as resp:
                        if resp.status != 200:
                            sent_ok = False
            except Exception:
                sent_ok = False
    return sent_ok


# ---------------------------------------------------------
# 4. Verification Suite (--verify)
# ---------------------------------------------------------
def run_verification_suite():
    print("=" * 84)
    print("RUNNING YEAR-END TAX & TELEMETRY CSV EXPORTER VERIFICATION SUITE")
    print("=" * 84)

    tmp_dir = tempfile.mkdtemp(prefix="kalshi_tax_csv_verify_")
    test_db = os.path.join(tmp_dir, "trade_telemetry_v2.sqlite")
    test_out = os.path.join(tmp_dir, "exports")

    try:
        conn = sqlite3.connect(test_db)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE trade_logs (
                trade_id TEXT PRIMARY KEY, market_ticker TEXT, series TEXT, category TEXT,
                timeframe TEXT, timestamp INTEGER, side TEXT, tier TEXT, count INTEGER,
                entry_price INTEGER, seconds_remaining INTEGER, distance_to_target REAL,
                atr_1m REAL, normalized_distance REAL, wallet_confluence_score REAL,
                resolved_price INTEGER, pnl_cents INTEGER, interest_earned_cents REAL, status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE cross_arb_opportunities (
                arb_id TEXT PRIMARY KEY, timestamp INTEGER, asset TEXT, kalshi_ticker TEXT,
                poly_market_slug TEXT, arb_type TEXT, kalshi_side TEXT, kalshi_price_cents INTEGER,
                poly_yes_cents REAL, poly_no_cents REAL, gross_spread_cents REAL,
                fee_cents REAL, net_spread_cents REAL, executed INTEGER
            )
        """)

        now = int(time.time())
        conn.execute("""
            INSERT INTO trade_logs VALUES
            ('tr_1', 'KXBTC15M-26NOV11-T89500', 'KXBTC15M', 'CRYPTO_15M', '15m', ?, 'yes', 'TIER_1', 4, 75, 210, 48.0, 18.0, 2.67, 0.88, 92, 68, 0.12, 'SETTLED'),
            ('tr_2', 'KXGOLD15M-26NOV11-T2680', 'KXGOLD15M', 'COMMODITIES_15M_1H', '15m', ?, 'no', 'TIER_1', 2, 78, 185, 5.4, 2.1, 2.57, 0.82, 100, 44, 0.08, 'SETTLED')
        """, (now - 3600, now - 7200))
        conn.commit()
        conn.close()

        meta = export_weekly_spreadsheets(db_path=test_db, export_dir=test_out, lookback_days=7)
        tax_path = export_year_end_tax_summary(db_path=test_db, export_dir=test_out, tax_year=2026)

        assert os.path.exists(tax_path) and os.path.getsize(tax_path) > 30
        print(f" [PASS] Generated Year-