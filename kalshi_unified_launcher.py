#!/usr/bin/env python3
"""
Unified Master Supervisor, Paper-Mode Launcher & Full Telegram Command Controller
=================================================================================
Includes Remote Telegram Commands:
  /status     -> Real-time Equity, Win Rate, Open Positions & Top Pyth Sigma Breakouts
  /arbs       -> Latest 5 Polymarket <-> Kalshi (>4.0c) arbitrage spreads
  /top10      -> Top 10 Ranked Hyperliquid & Polymarket weekly whale leaders
  /backup     -> Online SQLite WAL hot-backup + instant .sqlite.gz upload to Telegram
  /force_sync -> On-demand Top 50 Hyperliquid + Top 50 Polymarket leaderboard scrape
  /optimize   -> On-demand 24h feedback optimizer audit & hot-reload of live_config_v2.py
  /freeze     -> Instant Emergency Stop & Kalshi resting order purge
  /unfreeze   -> Resume Autonomous Execution
"""

import asyncio
import datetime
import logging
import os
import sqlite3
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

import aiohttp

import kalshi_sniper_engine_consolidated_v2 as v2_engine
from leaderboard_scraper import sync_top_50_leaderboards
from realtime_whale_ws import start_realtime_whale_streams
from pyth_hermes_stream import PythOracleManager, start_pyth_oracle_stream
from cross_exchange_arb import ensure_arb_tables, start_cross_exchange_arb_loop
from kalshi_terminal_dashboard import run_live_dashboard_async, generate_dashboard_layout
from sqlite_backup_manager import create_hot_sqlite_backup, upload_backup_to_telegram_async

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def get_wal_conn(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def get_engine_state_val(key: str, default: str = "", db_path: str = DB_FILE) -> str:
    if not os.path.exists(db_path):
        return default
    conn = get_wal_conn(db_path)
    c = conn.cursor()
    try:
        c.execute("SELECT value FROM engine_state WHERE key = ?", (key,))
        row = c.fetchone()
        return str(row[0]) if row else default
    except Exception:
        return default
    finally:
        conn.close()


def set_engine_state_val(key: str, value: str, db_path: str = DB_FILE):
    conn = get_wal_conn(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS engine_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER
        )
    """)
    c.execute("""
        INSERT INTO engine_state (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    """, (key, str(value), int(time.time())))
    conn.commit()
    conn.close()


def is_engine_frozen(db_path: str = DB_FILE) -> bool:
    return get_engine_state_val("engine_status", "ACTIVE", db_path).upper() == "FROZEN"


async def send_telegram_notification(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        async with session.post(url, json=payload, timeout=6) as resp:
            await resp.read()
    except Exception as e:
        logging.debug(f"Telegram alert skip: {e}")


class TelegramCommandController:
    def __init__(self,
                 kalshi_client: Any,
                 session: aiohttp.ClientSession,
                 db_path: str = DB_FILE,
                 bot_token: str = TELEGRAM_BOT_TOKEN,
                 authorized_chat_id: str = TELEGRAM_CHAT_ID):
        self.kalshi_client = kalshi_client
        self.session = session
        self.db_path = db_path
        self.bot_token = bot_token
        self.authorized_chat_id = str(authorized_chat_id).strip()
        self.last_update_id: int = 0

    def build_status_report(self) -> str:
        conn = get_wal_conn(self.db_path)
        c = conn.cursor()

        mode = get_engine_state_val("execution_mode", "PAPER SHADOW", self.db_path)
        status = get_engine_state_val("engine_status", "ACTIVE", self.db_path)
        equity = float(get_engine_state_val("current_equity", "25.00", self.db_path))

        c.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END),
                   COALESCE(SUM(pnl_cents), 0),
                   COALESCE(SUM(interest_earned_cents), 0.0)
            FROM trade_logs WHERE status = 'SETTLED'
        """)
        row = c.fetchone() or (0, 0, 0, 0.0)
        settled_cnt = int(row[0] or 0)
        wins = int(row[1] or 0)
        wr = (wins / settled_cnt * 100.0) if settled_cnt > 0 else 0.0
        net_pnl_dollars = float(row[2] or 0) / 100.0
        yield_cents = float(row[3] or 0.0)

        c.execute("SELECT COUNT(*) FROM trade_logs WHERE status = 'OPEN'")
        open_cnt = int((c.fetchone() or [0])[0])

        pyth_lines: List[str] = []
        try:
            c.execute("""
                SELECT asset, spot_price, sigma_15m, atr_1m
                FROM pyth_oracle_snapshots
                ORDER BY sigma_15m DESC LIMIT 3
            """)
            for asset, spot, sig, atr in c.fetchall():
                pyth_lines.append(f"  • `{asset}`: ${float(spot):,.2f} (`{float(sig):.2f}σ` | ATR: {float(atr):.3f})")
        except Exception:
            pass

        conn.close()

        state_icon = "🟢" if status == "ACTIVE" else "🛑"
        pyth_block = "\n".join(pyth_lines) if pyth_lines else "  • Awaiting stream ticks..."

        return (
            f"{state_icon} *KALSHI V2 ENGINE STATUS*\n"
            f"• *Mode:* `{mode}`\n"
            f"• *State:* `{status}`\n"
            f"• *Current Equity:* `${equity:,.2f}`\n"
            f"• *Net Realized PnL:* `{net_pnl_dollars:+.2f} USD`\n"
            f"• *Realized Win Rate:* `{wr:.1f}%` ({wins}W / {settled_cnt - wins}L)\n"
            f"• *Open Positions:* `{open_cnt}` | *Yield:* `+{yield_cents:.2f}¢`\n"
            f"• *ATR Gate:* `{getattr(v2_engine.live_config, 'MIN_ATR_MULTIPLIER', 2.0)}x`\n"
            f"📊 *Top Pyth Sigma Breakouts:*\n{pyth_block}"
        )

    def build_arbs_report(self) -> str:
        ensure_arb_tables(self.db_path)
        conn = get_wal_conn(self.db_path)
        c = conn.cursor()
        c.execute("""
            SELECT asset, kalshi_ticker, arb_type, kalshi_side,
                   kalshi_price_cents, net_spread_cents, executed, timestamp
            FROM cross_arb_opportunities
            ORDER BY timestamp DESC LIMIT 5
        """)
        rows = c.fetchall()
        conn.close()

        if not rows:
            return "💎 *CROSS-EXCHANGE ARBS (>4.0¢)*\nNo spreads > 4.0¢ recorded yet in current session."

        lines = ["💎 *LATEST POLY <-> KALSHI ARBS (>4.0¢)*"]
        now_ts = int(time.time())
        for asset, k_ticker, a_type, side, px, net_c, exec_flag, ts in rows:
            age_m = max(0, (now_ts - int(ts or now_ts)) // 60)
            tag = "✅ FILLED" if int(exec_flag or 0) == 1 else "👀 LOGGED"
            short_type = "BOX" if "BOX" in str(a_type) else "SNIPE"
            lines.append(
                f"• `{k_ticker}` ({short_type})\n"
                f"  Buy `{str(side).upper()}` @ `{px}¢` | Net: `+{float(net_c):.2f}¢` | {tag} ({age_m}m ago)"
            )
        return "\n".join(lines)

    def build_top10_report(self) -> str:
        conn = get_wal_conn(self.db_path)
        c = conn.cursor()
        c.execute("""
            SELECT venue, wallet_address, pnl_30d, win_rate, weight_multiplier
            FROM top_trader_registry
            ORDER BY weight_multiplier DESC, pnl_30d DESC
            LIMIT 10
        """)
        rows = c.fetchall()
        conn.close()

        if not rows:
            return "🐋 *TOP 10 WHALE REGISTRY*\nRegistry is empty. Send `/force_sync` to populate."

        lines = ["🐋 *TOP 10 TRACKED WHALE LEADERS*"]
        for idx, (venue, addr, pnl, wr, weight) in enumerate(rows, start=1):
            v_tag = "HL" if "hyper" in str(venue).lower() else "PM"
            lines.append(
                f"{idx}. `[{v_tag}]` `{str(addr)[:10]}..` | "
                f"7d: `+${float(pnl):,.0f}` | WR: `{float(wr)*100:.0f}%` | `{float(weight):.2f}x`"
            )
        return "\n".join(lines)

    async def execute_command(self, raw_text: str) -> str:
        cmd = raw_text.strip().split()[0].lower().split("@")[0]

        if cmd in ("/status", "/balance"):
            return self.build_status_report()

        elif cmd == "/arbs":
            return self.build_arbs_report()

        elif cmd == "/top10":
            return self.build_top10_report()

        elif cmd == "/backup":
            meta = await asyncio.to_thread(create_hot_sqlite_backup, self.db_path)
            uploaded = False
            if self.session and self.bot_token and self.authorized_chat_id:
                uploaded = await upload_backup_to_telegram_async(self.session, meta)
            rc = meta["row_counts"]
            return (
                f"💾 *HOT SQLITE BACKUP COMPLETED*\n"
                f"• *Archive:* `{meta['filename']}` (`{meta['size_kb']} KB`)\n"
                f"• *Duration:* `{meta['elapsed_ms']}ms` (Zero WAL lock)\n"
                f"• *Rows Saved:* Trades=`{rc.get('trade_logs', 0)}`, Whales=`{rc.get('top_trader_registry', 0)}`, "
                f"Signals=`{rc.get('wallet_alpha_feed', 0)}`, Arbs=`{rc.get('cross_arb_opportunities', 0)}`\n"
                f"• *Off-Disk Cloud Copy:* `{'Uploaded Above ⬆️' if uploaded else 'Saved Locally Only'}`"
            )

        elif cmd == "/force_sync":
            await send_telegram_notification(
                self.session, "🔄 *Force-Syncing Top 50 Hyperliquid + Top 50 Polymarket Leaderboards...*"
            )
            summary = await sync_top_50_leaderboards(self.db_path)
            return (
                f"✅ *LEADERBOARD FORCE-SYNC COMPLETE*\n"
                f"• *Hyperliquid Top Wallets:* `{summary.get('hyperliquid_count', 0)}`\n"
                f"• *Polymarket Top Wallets:* `{summary.get('polymarket_count', 0)}`\n"
                f"• *Total Registered:* `{summary.get('total_registered', 0)}`\n"
                f"• *New Active Whale Signals:* `{summary.get('new_alpha_signals', 0)}`"
            )

        elif cmd == "/optimize":
            old_atr = getattr(v2_engine.live_config, "MIN_ATR_MULTIPLIER", 2.0)
            old_conf = getattr(v2_engine.live_config, "MIN_WALLET_CONFLUENCE", 0.65)
            v2_engine.run_feedback_optimizer()
            v2_engine.reload_dynamic_config()
            new_atr = getattr(v2_engine.live_config, "MIN_ATR_MULTIPLIER", 2.0)
            new_conf = getattr(v2_engine.live_config, "MIN_WALLET_CONFLUENCE", 0.65)
            cutoff_15m = getattr(v2_engine.live_config, "CUTOFF_SECONDS_15M", 150)
            return (
                f"🧠 *FEEDBACK OPTIMIZER RECALIBRATED*\n"
                f"• *ATR Breakout Gate:* `{old_atr:.2f}x` ➔ `{new_atr:.2f}x`\n"
                f"• *Min Wallet Confluence:* `{old_conf:.2f}` ➔ `{new_conf:.2f}`\n"
                f"• *15m Entry Cutoff:* `{cutoff_15m}s` before expiry\n"
                f"• *Config Status:* `live_config_v2.py` hot-reloaded."
            )

        elif cmd == "/freeze":
            set_engine_state_val("engine_status", "FROZEN", self.db_path)
            if self.kalshi_client and self.session:
                await self.kalshi_client.request(self.session, "DELETE", "/portfolio/orders")
            logging.warning("[Telegram Control] Remote /freeze executed by operator.")
            return (
                "🛑 *ENGINE FROZEN REMOTELY*\n"
                "• `engine_status` set to `FROZEN`.\n"
                "• All resting Kalshi limit orders cancelled.\n"
                "• Send `/unfreeze` when ready to resume execution."
            )

        elif cmd == "/unfreeze":
            set_engine_state_val("engine_status", "ACTIVE", self.db_path)
            v2_engine.reload_dynamic_config()
            logging.info("[Telegram Control] Remote /unfreeze executed by operator.")
            return (
                "🟢 *ENGINE UNFROZEN & ACTIVE*\n"
                "• `engine_status` restored to `ACTIVE`.\n"
                "• Dynamic config (`live_config_v2.py`) reloaded.\n"
                "• Resuming 62-series scanning, Whale WS triggers & Cross-Exchange Arbs."
            )

        elif cmd in ("/help", "/start"):
            return (
                "⚡ *KALSHI V2 REMOTE COMMAND MENU*\n"
                "• `/status` — Equity, Win Rate, Open Trades & Pyth Sigma\n"
                "• `/arbs` — Latest 5 Polymarket <-> Kalshi (>4¢) spreads\n"
                "• `/top10` — Top 10 Ranked Hyperliquid & Polymarket Whales\n"
                "• `/backup` — Hot SQLite WAL Backup + Off-Disk Cloud Upload\n"
                "• `/force_sync` — Refresh Top-100 Whale Leaderboards Now\n"
                "• `/optimize` — Run 24h Feedback Optimizer & Reload Config\n"
                "• `/freeze` — Instant Emergency Stop & Order Purge\n"
                "• `/unfreeze` — Resume Autonomous Execution"
            )

        return f"❓ Unrecognized command `{cmd}`. Send `/help` for available controls."

    async def run_polling_loop(self):
        if not self.bot_token or not self.authorized_chat_id:
            logging.info("[Telegram Control] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID unset; listener idle.")
            return

        logging.info(f"[Telegram Control] Two-Way Command Listener Active for Chat ID {self.authorized_chat_id}.")
        base_url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"

        while True:
            try:
                params = {"offset": self.last_update_id + 1, "timeout": 20, "allowed_updates": ["message"]}
                async with self.session.get(base_url, params=params, timeout=28) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await resp.json()

                for upd in data.get("result", []):
                    upd_id = int(upd.get("update_id", 0))
                    if upd_id > self.last_update_id:
                        self.last_update_id = upd_id

                    msg = upd.get("message") or {}
                    chat_id = str((msg.get("chat") or {}).get("id", "")).strip()
                    text = str(msg.get("text", "")).strip()

                    if chat_id != self.authorized_chat_id:
                        logging.warning(f"[Telegram Security] Blocked unauthorized command from chat_id={chat_id}")
                        continue

                    if text.startswith("/"):
                        reply_md = await self.execute_command(text)
                        await send_telegram_notification(self.session, reply_md)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.debug(f"[Telegram Control] Poll reconnect notice: {e}")
                await asyncio.sleep(3.0)


class PaperAwareKalshiClient(v2_engine.AsyncKalshiClient):
    def __init__(self, key_id: str, key_path: str, paper_mode: bool = True, db_path: str = DB_FILE):
        super().__init__(key_id, key_path)
        self.paper_mode = paper_mode
        self.db_path = db_path
        self._init_paper_state()

    def _init_paper_state(self):
        mode_label = "PAPER SHADOW" if self.paper_mode else "LIVE PRODUCTION"
        set_engine_state_val("execution_mode", mode_label, self.db_path)
        if not get_engine_state_val("engine_status", "", self.db_path):
            set_engine_state_val("engine_status", "ACTIVE", self.db_path)
        if not get_engine_state_val("current_equity", "", self.db_path):
            set_engine_state_val("current_equity", "25.00", self.db_path)

    def get_paper_equity(self) -> float:
        return float(get_engine_state_val("current_equity", "25.00", self.db_path))

    def adjust_paper_equity(self, delta_dollars: float) -> float:
        cur = self.get_paper_equity()
        new_eq = max(0.0, round(cur + delta_dollars, 4))
        set_engine_state_val("current_equity", f"{new_eq:.4f}", self.db_path)
        return new_eq

    async def request(self, session: aiohttp.ClientSession, method: str, path: str,
                      params: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        if path == "/portfolio/orders" and method.upper() == "POST":
            action = str((json_data or {}).get("action", "buy")).lower()
            if action == "buy" and is_engine_frozen(self.db_path):
                logging.info("[Freeze Gate] Blocked new BUY order while engine_status == FROZEN.")
                return {}

        if self.paper_mode and path == "/portfolio/balance":
            eq = self.get_paper_equity()
            return {"balance": int(round(eq * 100))}

        if self.paper_mode and path == "/portfolio/orders" and method.upper() == "POST":
            payload = json_data or {}
            action = str(payload.get("action", "buy")).lower()
            side = str(payload.get("side", "yes")).lower()
            count = int(payload.get("count", 1))
            px_cents = int(payload.get("yes_price") or payload.get("no_price") or 75)
            ticker = str(payload.get("ticker", "UNKNOWN"))
            sim_order_id = f"paper_{uuid.uuid4().hex[:12]}"

            if action == "buy":
                outlay = (px_cents / 100.0) * count
                new_eq = self.adjust_paper_equity(-outlay)
                await send_telegram_notification(
                    session,
                    f"🟢 *PAPER ORDER FILLED*\n• *Ticker:* `{ticker}`\n• *Side:* {count}x {side.upper()} @ {px_cents}¢\n"
                    f"• *Outlay:* -${outlay:.2f} | *Cash:* ${new_eq:.2f}"
                )
            elif action == "sell":
                proceeds = (px_cents / 100.0) * count
                new_eq = self.adjust_paper_equity(proceeds)
                await send_telegram_notification(
                    session,
                    f"🎯 *PAPER SCALP/STOP EXIT*\n• *Ticker:* `{ticker}`\n• *Exit:* {count}x {side.upper()} @ {px_cents}¢\n"
                    f"• *Proceeds:* +${proceeds:.2f} | *Equity:* ${new_eq:.2f}"
                )

            return {"order": {"order_id": sim_order_id, "status": "resting"}}

        if self.paper_mode and path == "/portfolio/orders" and method.upper() == "DELETE":
            return {"orders_cancelled": "all_paper_orders"}

        if method.upper() == "GET" and not self.private_key:
            url = f"{v2_engine.KALSHI_API_BASE}{path}"
            async with self.semaphore:
                try:
                    async with session.get(url, params=params, timeout=8) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        return {}
                except Exception:
                    return {}

        res = await super().request(session, method, path, params=params, json_data=json_data)
        if not self.paper_mode and path == "/portfolio/balance":
            live_eq = max(res.get("balance", 0) / 100.0, 0.0)
            set_engine_state_val("current_equity", f"{live_eq:.4f}", self.db_path)
        return res


async def pyth_enhanced_evaluate_series(client: PaperAwareKalshiClient,
                                        session: aiohttp.ClientSession,
                                        pyth_manager: PythOracleManager,
                                        category: str,
                                        series: str,
                                        equity: float):
    i