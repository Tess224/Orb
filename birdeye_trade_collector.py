"""
Birdeye Trade Collector for Solana Tokens

This module replaces the Helius-based polling collector with Birdeye's
superior trade data API. Birdeye provides pre-parsed trade information
that works across ALL DEXs (Raydium, Pump.fun, Orca, etc.) without the
complexity of parsing raw Solana transactions.

KEY ADVANTAGES OVER HELIUS:
1. Pre-parsed trade data (no raw transaction parsing needed)
2. Works across all DEXs without special handling
3. Returns actual SOL amounts (Helius was returning 0.0000)
4. Cleaner API designed specifically for DEX trade queries
5. No need to find pool addresses - works with token addresses directly

API COSTS:
- Birdeye charges per API call
- We poll every 2 minutes (120 seconds) by default
- Cost: ~720 API calls per token per day
- Much more reliable than trying to parse Helius raw transactions
"""

import asyncio
import aiohttp
import time
import logging
from typing import Dict, List, Set, Optional, Callable
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)


class BirdeyeTradeCollector:
    """
    Efficiently collects trades by polling Birdeye's /defi/txs/token endpoint.
    
    This collector monitors multiple tokens simultaneously, checking for new
    trades at regular intervals. It maintains state to avoid duplicate
    processing and provides a callback system to notify your metrics manager
    when new trades arrive.
    """
    
    def __init__(self, birdeye_api_key: str, poll_interval_seconds: int = 120):
        """
        Initialize the Birdeye trade collector.
        
        Args:
            birdeye_api_key: Your Birdeye API key from birdeye.so
            poll_interval_seconds: How often to check for new trades (default: 120 = 2 minutes)
        """
        self.api_key = birdeye_api_key
        
        # Birdeye's trade history endpoint
        self.api_url = "https://public-api.birdeye.so/defi/txs/token"
        
        self.poll_interval = poll_interval_seconds
        
        # Track which tokens we're monitoring
        # Structure: {token_address: {'symbol': str, 'added_at': timestamp, 'last_poll': timestamp}}
        self.monit