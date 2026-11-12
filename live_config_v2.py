#!/usr/bin/env python3
"""
Live Calibrated Parameters (live_config_v2.py)
==============================================
Automatically generated and hot-reloaded by the autonomous agent orchestrator
and feedback optimizer loops. Do not manually edit while the engine is live.
"""

# Volatility Breakout Hurdle (Multiplier over 1-minute Wilder ATR)
MIN_ATR_MULTIPLIER = 2.0

# Execution timing cutoffs before contract settlement / window expiry (seconds)
CUTOFF_SECONDS_15M = 150
CUTOFF_SECONDS_1H = 900

# Maximum pricing threshold for Tier 1 core scalp entry (in cents)
TIER1_MAX_PRICE = 84

# Minimum smart-money / top-trader confluence score required for execution (0.0 to 1.0)
MIN_WALLET_CONFLUENCE = 0.65