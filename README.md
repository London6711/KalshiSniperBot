# KalshiSniperBot
Automated trading on Kalshi 
Kalshi Quant Meta-Agent & Unified Autonomous V2 Stack
<p align="center">
<b>A production-grade, asynchronous quantitative trading and event-contract sniper engine for Kalshi.</b>

Features sub-second Pyth Hermes oracles, whale copy-trading, cross-exchange arbitrage, and a secure two-way Telegram command controller.
</p>  
🚀 Overview
The Kalshi Quant Meta-Agent is an autonomous, high-frequency quantitative execution framework built for prediction markets and event contracts. It merges sub-second institutional oracles, real-time whale fill interception, cross-exchange arbitrage, and an automated self-improving feedback loop into a unified, zero-friction daemon.  
🏛️ System Architecture
The unified stack is partitioned into 6 integrated subsystems communicating via a shared, high-performance SQLite WAL-mode telemetry database (trade_telemetry_v2.sqlite):  
1. V2 Sniper Engine (kalshi_sniper_engine_consolidated_v2.py): Multi-tier execution engine enforcing strict pricing ceilings (84\text{¢} Tier-1 caps), ATR-driven volatility hurdles, and automated 45\text{¢} stop-loss controls.
2. Leaderboard Scraper (leaderboard_scraper.py): Keyless ingestion and ranking of top-performing Hyperliquid and Polymarket whale accounts, filtering out delta-neutral noise and wash trading.  
3. Whale WebSocket Streamer (kalshi_whale_ws_listener.py): Multiplexed real-time WebSocket connection to Kalshi (wss://[external-api-ws.kalshi.com/trade-api/ws/v2](https://external-api-ws.kalshi.com/trade-api/ws/v2)) and external order books, intercepting whale fills in <200\text{ms} to trigger instant order book audits.  
4. Pyth Hermes Oracle (pyth_hermes_stream.py): Sub-second price feeds across Crypto, Commodities, FX, and Indices, computing real-time 1-minute Wilder ATR and sigma breakouts (\ge 2.0\sigma).  
5. Cross-Exchange Arb Scanner (cross_exchange_arb.py): Identifies Synthetic Box and Lead-Lag Value Spreads (>4.0\text{¢} net of Kalshi fee curves) between Polymarket and Kalshi.  
6. Telegram Command Controller (kalshi_unified_launcher.py): Two-way mobile control interface supporting remote status checks, emergency freezes, configuration hot-reloads, and on-demand CSV exports.  
