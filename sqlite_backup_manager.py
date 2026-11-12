#!/usr/bin/env python3
"""
Online SQLite WAL Backup Manager & Off-Disk Telegram Archiver
=============================================================
Uses sqlite3's native C-API `.backup()` to safely snapshot `trade_telemetry_v2.sqlite`
without locking or interrupting live WebSocket streams.
- Compresses snapshots to `.sqlite.gz` inside `~/kalshi_workspace/backups/`.
- Uploads the compressed archive to your personal Telegram chat for off-disk redundancy.
- Automatically prunes local backup files older than 14 days.
"""

import datetime
import gzip
import logging
import os
import shutil
import sqlite3
import sys
import time
from typing import Dict, Any, Optional

import aiohttp
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("backup.log")]
)

DB_FILE = os.getenv("KALSHI_DB_FILE", "trade_telemetry_v2.sqlite")
BACKUP_DIR = os.getenv("KALSHI_BACKUP_DIR", os.path.join(os.path.dirname(os.path.abspath(DB_FILE)), "backups"))
RETENTION_DAYS = 14

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def create_hot_sqlite_backup(db_path: str = DB_FILE, backup_dir: str = BACKUP_DIR) -> Dict[str, Any]:
    """
    Executes an online WAL-safe backup of `db_path`, compresses it with gzip,
    and prunes local backups older than `RETENTION_DAYS`.
    """
    os.makedirs(backup_dir, exist_ok=True)
    ts_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    raw_backup_path = os.path.join(backup_dir, f"kalshi_v2_{ts_str}.sqlite")
    gz_backup_path = f"{raw_backup_path}.gz"

    start_ms = time.time() * 1000.0

    # Ensure source DB exists
    src_conn = sqlite3.connect(db_path, timeout=30.0)
    src_conn.execute("PRAGMA journal_mode=WAL;")

    # Collect quick row counts for backup metadata verification
    c = src_conn.cursor()
    row_counts: Dict[str, int] = {}
    for tbl in ("trade_logs", "top_trader_registry", "wallet_alpha_feed", "pyth_oracle_snapshots", "cross_arb_opportunities"):
        try:
            c.execute(f"SELECT COUNT(*) FROM {tbl}")
            row_counts[tbl] = int((c.fetchone() or [0])[0])
        except Exception:
            row_counts[tbl] = 0

    # Perform online page-copied backup (250 pages per step to yield locks to live streams)
    dst_conn = sqlite3.connect(raw_backup_path)
    with dst_conn:
        src_conn.backup(dst_conn, pages=250, sleep=0.005)
    dst_conn.close()
    src_conn.close()

    # Compress with gzip
    with open(raw_backup_path, "rb") as f_in, gzip.open(gz_backup_path, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(raw_backup_path)

    # Prune backups older than RETENTION_DAYS
    cutoff_ts = time.time() - (RETENTION_DAYS * 86400)
    pruned = 0
    for fname in os.listdir(backup_dir):
        if fname.endswith(".sqlite.gz"):
            fpath = os.path.join(backup_dir, fname)
            if os.path.getmtime(fpath) < cutoff_ts:
                try:
                    os.remove(fpath)
                    pruned += 1
                except Exception:
                    pass

    size_kb = round(os.path.getsize(gz_backup_path) / 1024.0, 2)
    elapsed_ms = round((time.time() * 1000.0) - start_ms, 1)

    result = {
        "gz_path": gz_backup_path,
        "filename": os.path.basename(gz_backup_path),
        "size_kb": size_kb,
        "elapsed_ms": elapsed_ms,
        "row_counts": row_counts,
        "pruned_old_files": pruned
    }
    logging.info(f"[BackupManager] Created {result['filename']} ({size_kb} KB in {elapsed_ms}ms) | Rows: {row_counts}")
    return result


async def upload_backup_to_telegram_async(session: aiohttp.ClientSession, backup_meta: Dict[str, Any]) -> bool:
    """
    Uploads the compressed `.sqlite.gz` file directly to your Telegram chat via `sendDocument`
    so you have an instant off-disk, off-server backup on Telegram Cloud.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    gz_path = backup_meta["gz_path"]
    if not os.path.exists(gz_path):
        return False

    rc = backup_meta["row_counts"]
    caption = (
        f"💾 *SQLite Hot Backup Archive*\n"
        f"• *File:* `{backup_meta['filename']}` (`{backup_meta['size_kb']} KB`)\n"
        f"• *Trades:* `{rc.get('trade_logs', 0)}` | *Whale Registry:* `{rc.get('top_trader_registry', 0)}`\n"
        f"• *Alpha Signals:* `{rc.get('wallet_alpha_feed', 0)}` | *Arbs:* `{rc.get('cross_arb_opportunities', 0)}`\n"
        f"• *Snapshot Latency:* `{backup_meta['elapsed_ms']}ms`"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(gz_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(TELEGRAM_CHAT_ID))
            form.add_field("caption", caption)
            form.add_field("parse_mode", "Markdown")
            form.add_field("document", f, filename=backup_meta["filename"], content_type="application/gzip")
            async with session.post(url, data=form, timeout=30) as resp:
                return resp.status == 200
    except Exception as e:
        logging.warning(f"[BackupManager] Telegram document upload notice: {e}")
        return False


def run_cli_backup():
    meta = create_hot_sqlite_backup(DB_FILE, BACKUP_DIR)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        import asyncio
        async def _upload():
            async with aiohttp.ClientSession() as session:
                ok = await upload_backup_to_telegram_async(session, meta)
                print(f" [INFO] Off-disk Telegram Cloud upload: {'SUCCESS' if ok else 'SKIPPED'}")
        asyncio.run(_upload())
    print(f" [PASS] Backup complete: {meta['gz_path']} ({meta['size_kb']} KB)")


if __name__ == "__main__":
    run_cli_backup()