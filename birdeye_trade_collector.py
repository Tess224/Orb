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
        self.monitored_tokens: Dict[str, Dict] = {}
        
        # Keep track of transaction signatures we've already processed
        # Structure: {token_address: Set of signature strings}
        self.processed_signatures: Dict[str, Set[str]] = {}
        
        # Callbacks to notify when we parse trades
        self.trade_callbacks: List[Callable] = []
        
        # For making HTTP requests
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Control the polling loop
        self.is_running = False
        
        # Statistics for monitoring
        self.stats = {
            'polls_completed': 0,
            'transactions_fetched': 0,
            'trades_parsed': 0,
            'api_calls_made': 0,
            'last_poll_time': None
        }
        
        logger.info(f"✅ Birdeye trade collector initialized (poll interval: {poll_interval_seconds}s)")

    def add_trade_callback(self, callback: Callable):
        """Register a function to call when we parse a trade."""
        self.trade_callbacks.append(callback)
        logger.info(f"📝 Registered trade callback: {callback.__name__}")

    async def start(self):
        """Start the polling system."""
        self.http_session = aiohttp.ClientSession()
        self.is_running = True
        logger.info("🚀 Starting Birdeye polling system...")
        
        # Start the polling loop
        await self.polling_loop()

    async def stop(self):
        """Stop the polling system."""
        self.is_running = False
        if self.http_session:
            await self.http_session.close()
        logger.info("🛑 Birdeye polling system stopped")

    def add_token(self, token_address: str, token_symbol: str = "UNKNOWN"):
        """
        Start monitoring a token for trades.
        
        Args:
            token_address: The token's mint address
            token_symbol: Optional symbol for logging
        """
        self.monitored_tokens[token_address] = {
            'symbol': token_symbol,
            'added_at': time.time(),
            'last_poll': 0
        }
        
        # Initialize the processed signatures set for this token
        self.processed_signatures[token_address] = set()
        
        logger.info(f"📡 Now monitoring token {token_address[:8]}... (Symbol: {token_symbol})")

    async def fetch_recent_trades(self, token_address: str, limit: int = 100) -> List[Dict]:
        """
        Fetch recent trades for a token using Birdeye's API.
        
        This is the key method. Birdeye's /defi/txs/token endpoint gives us
        pre-parsed trade data with actual SOL amounts, DEX info, and timestamps.
        
        Args:
            token_address: The token to fetch trades for
            limit: Maximum number of trades to fetch (default: 100)
            
        Returns:
            List of trade objects from Birdeye
        """
        try:
            # Build the request parameters
            params = {
                'address': token_address,
                'tx_type': 'swap',  # Only get swap transactions
                'sort_type': 'desc',  # Most recent first
                'offset': 0,
                'limit': limit
            }
            
            headers = {
                'X-API-KEY': self.api_key,
                'chain': 'solana',
                'accept': 'application/json'
            }
            
            # Make the request
            async with self.http_session.get(
                self.api_url, 
                params=params, 
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Track API usage
                    self.stats['api_calls_made'] += 1
                    
                    # Birdeye returns: {'success': true, 'data': {'items': [...]}}
                    if data.get('success') and data.get('data'):
                        items = data['data'].get('items', [])
                        return items
                    else:
                        logger.warning(f"⚠️ Birdeye returned success=false for {token_address[:8]}")
                        return []
                        
                elif response.status == 429:
                    # Rate limited
                    logger.warning(f"⚠️ Rate limited by Birdeye API")
                    return []
                else:
                    logger.warning(f"HTTP {response.status} when fetching trades for {token_address[:8]}")
                    return []
                    
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Timeout fetching trades for {token_address[:8]}")
            return []
        except Exception as e:
            logger.error(f"❌ Error fetching trades for {token_address[:8]}: {e}")
            return []

    def parse_birdeye_trade(self, trade_data: Dict, token_address: str) -> Optional[Dict]:
        """
        Parse a trade from Birdeye's API response into our standard format.
        
        Birdeye gives us clean, pre-parsed data:
        - txHash: Transaction signature
        - blockUnixTime: Timestamp
        - side: 'buy' or 'sell'
        - source: DEX name (Raydium, Pump.fun, etc.)
        - amount: Token amount
        - quote: Quote token info (usually SOL)
        - volumeUSD: Trade size in USD
        
        Args:
            trade_data: Trade object from Birdeye API
            token_address: The token this trade is for
            
        Returns:
            Parsed trade dict or None if invalid
        """
        try:
            # Extract basic info
            signature = trade_data.get('txHash', '')
            timestamp = trade_data.get('blockUnixTime', 0)
            side = trade_data.get('side', '').lower()  # 'buy' or 'sell'
            
            # Validate required fields
            if not signature or not timestamp or side not in ['buy', 'sell']:
                return None
            
            # Extract amounts
            token_amount = float(trade_data.get('amount', 0))
            
            # Birdeye provides the quote token info
            quote_data = trade_data.get('quote', {})
            sol_amount = float(quote_data.get('amount', 0))
            
            # Get USD value (Birdeye calculates this for us)
            size_usd = float(trade_data.get('volumeUSD', 0))
            
            # Calculate price
            price = sol_amount / token_amount if token_amount > 0 else 0
            
            # Get DEX source
            dex_source = trade_data.get('source', 'Unknown')
            
            # Validate amounts
            if token_amount == 0 or size_usd == 0:
                return None
            
            # Build the trade object in our standard format
            trade = {
                'token_address': token_address,
                'timestamp': timestamp,
                'direction': side,
                'token_amount': token_amount,
                'sol_amount': sol_amount,
                'price': price,
                'size_usd': size_usd,
                'transaction_signature': signature,
                'dex_source': dex_source  # Extra info from Birdeye
            }
            
            self.stats['trades_parsed'] += 1
            
            return trade
            
        except Exception as e:
            logger.warning(f"⚠️ Error parsing Birdeye trade: {e}")
            return None

    async def poll_token(self, token_address: str):
        """
        Poll a single token for new trades.
        
        This is called every poll_interval for each monitored token.
        It fetches recent trades, filters to new ones we haven't seen,
        parses them, and notifies callbacks.
        """
        try:
            token_info = self.monitored_tokens.get(token_address, {})
            token_symbol = token_info.get('symbol', 'UNKNOWN')
            
            logger.info("=" * 70)
            logger.info(f"🔄 POLLING CYCLE for token {token_address[:8]}... (Symbol: {token_symbol})")
            logger.info("=" * 70)
            
            # Fetch recent trades from Birdeye
            trades = await self.fetch_recent_trades(token_address, limit=100)
            
            if not trades:
                logger.info(f"  ℹ️ No trades returned for {token_address[:8]}")
                logger.info("=" * 70)
                return
            
            self.stats['transactions_fetched'] += len(trades)
            logger.info(f"  📥 Fetched {len(trades)} trades from Birdeye")
            
            # Filter to only new signatures we haven't processed
            new_trades = []
            for trade in trades:
                sig = trade.get('txHash', '')
                if sig and sig not in self.processed_signatures[token_address]:
                    new_trades.append(trade)
                    self.processed_signatures[token_address].add(sig)
            
            if not new_trades:
                logger.info(f"  ℹ️ All trades already processed (0 new out of {len(trades)} total)")
                logger.info("=" * 70)
                return
            
            logger.info(f"  🆕 Found {len(new_trades)} NEW trades to process")
            logger.info(f"  📊 Processing each trade...")
            
            # Parse each new trade and notify callbacks
            trades_found = 0
            for idx, trade_data in enumerate(new_trades):
                logger.info(f"  ──────── Trade {idx + 1}/{len(new_trades)} ────────")
                
                trade = self.parse_birdeye_trade(trade_data, token_address)
                
                if trade:
                    trades_found += 1
                    
                    # Log the trade
                    logger.info(f"  💱 Trade #{trades_found}: {trade['direction'].upper()} "
                               f"${trade['size_usd']:.2f} on {trade.get('dex_source', 'Unknown')}")
                    
                    # Notify all registered callbacks
                    for callback in self.trade_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(trade)
                            else:
                                callback(trade)
                        except Exception as e:
                            logger.error(f"  ❌ Error in trade callback: {e}")
            
            logger.info(f"  ✅ Polling complete: {trades_found} trades found from {len(new_trades)} new transactions")
            logger.info("=" * 70)
            
            # Keep the processed signatures set from growing unbounded
            # Only keep the last 1000 signatures per token
            if len(self.processed_signatures[token_address]) > 1000:
                # Convert to list, keep newest 500
                sigs_list = list(self.processed_signatures[token_address])
                self.processed_signatures[token_address] = set(sigs_list[-500:])
                logger.info(f"  🧹 Cleaned up old signatures (kept 500 most recent)")
            
            # Update last poll time
            self.monitored_tokens[token_address]['last_poll'] = time.time()
            
        except Exception as e:
            logger.error(f"❌ Error polling token {token_address[:8]}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def polling_loop(self):
        """
        Main polling loop that checks all monitored tokens periodically.
        
        This runs forever (until is_running = False) and polls each token
        at the specified interval.
        """
        logger.info(f"🔄 Polling loop started (checking every {self.poll_interval}s)")
        
        while self.is_running:
            try:
                poll_start = time.time()
                
                # Poll each monitored token
                for token_address in list(self.monitored_tokens.keys()):
                    await self.poll_token(token_address)
                    
                    # Small delay between tokens to be nice to the API
                    await asyncio.sleep(0.5)
                
                self.stats['polls_completed'] += 1
                self.stats['last_poll_time'] = time.time()
                
                # Log statistics every 10 polls
                if self.stats['polls_completed'] % 10 == 0:
                    logger.info(f"📈 Polling stats: {self.stats['polls_completed']} polls, "
                               f"{self.stats['trades_parsed']} trades parsed, "
                               f"{self.stats['api_calls_made']} API calls made")
                
                # Calculate how long to sleep until next poll
                elapsed = time.time() - poll_start
                sleep_time = max(0, self.poll_interval - elapsed)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
            except Exception as e:
                logger.error(f"❌ Error in polling loop: {e}")
                await asyncio.sleep(5)
        
        logger.info("🛑 Polling loop ended")

    def get_stats(self) -> Dict:
        """Return current statistics about polling activity."""
        return self.stats.copy()
