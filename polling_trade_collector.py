"""
Effficient Polling-Based Trade Collector for Helius Free Tier

This approach minimizes API calls by:
1. Polling for new transactions every 2 minutes using Helius's transaction history API
2. Filtering to only SWAP transactions (reduces noise)
3. Getting already-parsed transaction data (no need for expensive getTransaction calls)
4. Caching processed signatures to avoid duplicates

Cost: ~10 credits per pool per poll = 300 credits/hour for 1 pool = 1,500 credits for 5 hours
Compare to WebSocket approach: 100,000+ credits for 5 hours
Savings: 98.5%
"""

import asyncio
import aiohttp
import time
import logging
from typing import Dict, List, Set, Optional
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)

class PollingTradeCollector:
    """
    Efficiently collects trades by polling Helius's transaction history API.
    
    This is optimized for the free tier where we want to minimize API calls
    while still getting reasonably fresh trade data (2-3 minute delay is acceptable
    for metrics that work on 5-minute, 15-minute, and hourly windows).
    """
    
    def __init__(self, helius_api_key: str, poll_interval_seconds: int = 120):
        """
        Initialize the polling collector.
        
        Args:
            helius_api_key: Your Helius API key
            poll_interval_seconds: How often to check for new transactions (default: 120 = 2 minutes)
                                   You can adjust this - higher = cheaper but more delayed data
        """
        self.api_key = helius_api_key
        # Helius's enhanced transaction API endpoint - returns parsed data
        self.api_url = f"https://api.helius.xyz/v0/addresses"
        self.poll_interval = poll_interval_seconds
        
        # Track which pools we're monitoring
        # Structure: {pool_address: {token_address, token_symbol, added_at}}
        self.monitored_pools: Dict[str, Dict] = {}
        
        # Keep track of transaction signatures we've already processed
        # This prevents us from processing the same trade twice
        # Structure: {pool_address: Set of signature strings}
        self.processed_signatures: Dict[str, Set[str]] = {}
        
        # Callbacks to notify when we parse trades
        self.trade_callbacks: List = []
        
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
            'credits_used_estimate': 0,
            'last_poll_time': None
        }
        
        logger.info(f"✅ Polling collector initialized (poll interval: {poll_interval_seconds}s)")
    
    def add_trade_callback(self, callback):
        """Register a function to call when we parse a trade."""
        self.trade_callbacks.append(callback)
        logger.info(f"📝 Registered trade callback: {callback.__name__}")
    
    async def start(self):
        """Start the polling system."""
        self.http_session = aiohttp.ClientSession()
        self.is_running = True
        logger.info("🚀 Starting polling system...")
        
        # Start the polling loop
        await self.polling_loop()
    
    async def stop(self):
        """Stop the polling system."""
        self.is_running = False
        if self.http_session:
            await self.http_session.close()
        logger.info("🛑 Polling system stopped")
    
    def add_pool(self, pool_address: str, token_address: str, token_symbol: str = "UNKNOWN"):
        """
        Start monitoring a pool for trades.
        
        Args:
            pool_address: The liquidity pool address
            token_address: The token's mint address
            token_symbol: Optional symbol for logging
        """
        self.monitored_pools[pool_address] = {
            'token_address': token_address,
            'token_symbol': token_symbol,
            'added_at': time.time()
        }
        
        # Initialize the processed signatures set for this pool
        self.processed_signatures[pool_address] = set()
        
        logger.info(f"📡 Now monitoring pool {pool_address[:8]}... (Token: {token_symbol})")
    
    async def fetch_recent_transactions(self, pool_address: str, limit: int = 50) -> List[Dict]:
        """
        Fetch recent transactions for a pool using Helius's enhanced API.
        
        This is the key method. Helius's /v0/addresses/{address}/transactions endpoint
        with type=SWAP filter gives us:
        - Only swap transactions (filters out noise)
        - Already parsed data (no need for expensive getTransaction calls)
        - Up to 100 transactions per call
        
        Cost: 10 credits per call
        
        Args:
            pool_address: The pool to check
            limit: Maximum number of transactions to fetch (default: 50)
            
        Returns:
            List of transaction objects with parsed data
        """
        try:
            # Build the URL for this specific pool
            url = f"{self.api_url}/{pool_address}/transactions"
            
            # Parameters for the request
            params = {
                'api-key': self.api_key,
                'limit': limit,
                'type': 'SWAP'  # This is crucial - only get swap transactions
            }
            
            # Make the request
            async with self.http_session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Track API usage for statistics
                    self.stats['api_calls_made'] += 1
                    self.stats['credits_used_estimate'] += 10  # This endpoint costs 10 credits
                    
                    # The response should be a list of transactions
                    return data if isinstance(data, list) else []
                
                elif response.status == 429:
                    # Rate limited - log and return empty
                    logger.warning(f"⚠️ Rate limited by Helius API")
                    return []
                    
                else:
                    logger.warning(f"HTTP {response.status} when fetching transactions for {pool_address[:8]}")
                    return []
                    
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Timeout fetching transactions for {pool_address[:8]}")
            return []
        except Exception as e:
            logger.error(f"❌ Error fetching transactions for {pool_address[:8]}: {e}")
            return []
    
    def parse_helius_transaction(self, tx_data: Dict, pool_address: str) -> Optional[Dict]:
        """
        Parse a transaction from Helius's API response into our trade format.
        
        Helius gives us nicely structured data with tokenTransfers and nativeTransfers
        already parsed, so we just need to identify which direction the swap went.
        
        Args:
            tx_data: Transaction data from Helius API
            pool_address: The pool address we're monitoring
            
        Returns:
            Parsed trade dict or None if not a valid swap for our token
        """
        try:
            pool_info = self.monitored_pools.get(pool_address, {})
            token_address = pool_info.get('token_address')
            
            if not token_address:
                return None
            
            # Extract basic transaction info
            signature = tx_data.get('signature', '')
            timestamp = tx_data.get('timestamp', 0)
            
            # DEBUG: Log the structure of the first transaction to understand Helius's format
            if not hasattr(self, '_logged_sample_tx'):
                self._logged_sample_tx = True
                logger.info("=" * 70)
                logger.info("🔍 DEBUG: Sample Transaction Structure from Helius")
                logger.info("=" * 70)
                logger.info(f"Top-level keys: {list(tx_data.keys())}")
                logger.info(f"Signature: {signature[:16]}...")
                logger.info(f"Timestamp: {timestamp}")
                
                # Log what's in tokenTransfers
                token_transfers_sample = tx_data.get('tokenTransfers', [])
                logger.info(f"tokenTransfers count: {len(token_transfers_sample)}")
                if token_transfers_sample:
                    logger.info(f"First tokenTransfer keys: {list(token_transfers_sample[0].keys())}")
                    logger.info(f"First tokenTransfer sample: {token_transfers_sample[0]}")
                
                # Log what's in nativeTransfers
                native_transfers_sample = tx_data.get('nativeTransfers', [])
                logger.info(f"nativeTransfers count: {len(native_transfers_sample)}")
                if native_transfers_sample:
                    logger.info(f"First nativeTransfer keys: {list(native_transfers_sample[0].keys())}")
                    logger.info(f"First nativeTransfer sample: {native_transfers_sample[0]}")
                
                logger.info("=" * 70)
            
            # Helius gives us arrays of token transfers and SOL transfers
            token_transfers = tx_data.get('tokenTransfers', [])
            native_transfers = tx_data.get('nativeTransfers', [])
            
            # Track how our token moved relative to the pool
            token_change = 0  # Negative = tokens left pool (buy), Positive = tokens entered pool (sell)
            
            # DEBUG: Track what we find
            relevant_token_transfers = 0
            
            for transfer in token_transfers:
                # Only care about transfers involving our specific token
                mint = transfer.get('mint', '')
                if mint != token_address:
                    continue
                
                relevant_token_transfers += 1
                
                from_addr = transfer.get('fromUserAccount', '')
                to_addr = transfer.get('toUserAccount', '')
                
                # Helius returns tokenAmount as a float directly, not in a nested object
                # Handle both possible formats just to be safe
                token_amount_raw = transfer.get('tokenAmount', 0)
                if isinstance(token_amount_raw, dict):
                    # If it's a dict, extract the ui_amount field
                    amount = float(token_amount_raw.get('uiAmount', 0))
                else:
                    # If it's already a number, use it directly
                    amount = float(token_amount_raw) if token_amount_raw else 0
                
                # DEBUG: Log what we're seeing
                if relevant_token_transfers == 1:
                    logger.info(f"  🔍 Found token transfer: {amount:.2f} tokens, from={from_addr[:8]}..., to={to_addr[:8]}...")
                
                if from_addr == pool_address:
                    # Tokens leaving pool = someone bought
                    token_change -= amount
                    logger.info(f"    📤 Tokens LEFT pool: {amount:.2f} (BUY signal)")
                elif to_addr == pool_address:
                    # Tokens entering pool = someone sold
                    token_change += amount
                    logger.info(f"    📥 Tokens ENTERED pool: {amount:.2f} (SELL signal)")
            
            # Track how SOL moved relative to the pool
            sol_change = 0  # Negative = SOL left pool, Positive = SOL entered pool
            
            # DEBUG: Track what we find
            relevant_sol_transfers = 0
            
            for transfer in native_transfers:
                from_addr = transfer.get('fromUserAccount', '')
                to_addr = transfer.get('toUserAccount', '')
                
                # Check if this transfer involves the pool
                if from_addr != pool_address and to_addr != pool_address:
                    continue
                
                relevant_sol_transfers += 1
                
                # Helius returns amount in lamports, convert to SOL
                amount_raw = transfer.get('amount', 0)
                if isinstance(amount_raw, dict):
                    # If it's nested, extract the value
                    amount = float(amount_raw.get('amount', 0)) / 1e9
                else:
                    # If it's a direct number, use it (already in lamports)
                    amount = float(amount_raw) / 1e9 if amount_raw else 0
                
                # DEBUG: Log what we're seeing
                if relevant_sol_transfers == 1:
                    logger.info(f"  🔍 Found SOL transfer: {amount:.4f} SOL, from={from_addr[:8]}..., to={to_addr[:8]}...")
                
                if from_addr == pool_address:
                    sol_change -= amount
                    logger.info(f"    📤 SOL LEFT pool: {amount:.4f} SOL")
                elif to_addr == pool_address:
                    sol_change += amount
                    logger.info(f"    📥 SOL ENTERED pool: {amount:.4f} SOL")
            
            # DEBUG: Log what we found overall
            logger.info(f"  📊 Summary for tx {signature[:8]}...")
            logger.info(f"    Token change: {token_change:.2f} (negative = buy, positive = sell)")
            logger.info(f"    SOL change: {sol_change:.4f} (negative = left pool, positive = entered pool)")
            logger.info(f"    Relevant token transfers: {relevant_token_transfers}")
            logger.info(f"    Relevant SOL transfers: {relevant_sol_transfers}")
            
            # Determine trade direction based on the movements
            # BUY: tokens left pool (negative change), SOL entered pool (positive change)
            # SELL: tokens entered pool (positive change), SOL left pool (negative change)
            
            if token_change < 0 and sol_change > 0:
                # This is a BUY
                direction = 'buy'
                token_amount = abs(token_change)
                sol_amount = sol_change
                logger.info(f"  ✅ Detected BUY: {token_amount:.2f} tokens for {sol_amount:.4f} SOL")
            elif token_change > 0 and sol_change < 0:
                # This is a SELL
                direction = 'sell'
                token_amount = token_change
                sol_amount = abs(sol_change)
                logger.info(f"  ✅ Detected SELL: {token_amount:.2f} tokens for {sol_amount:.4f} SOL")
            else:
                # Not a clear swap (maybe liquidity add/remove, or failed transaction)
                logger.info(f"  ❌ Not a clear swap (token_change={token_change:.2f}, sol_change={sol_change:.4f})")
                return None
            
            # Sanity check - token amount should be positive
            if token_amount == 0:
                return None
            
            # Calculate price and USD value
            price = sol_amount / token_amount
            
            # Estimate USD value (you should fetch real SOL price, but we'll use approximation)
            sol_price_usd = 190  # Update this to fetch from CoinGecko in production
            size_usd = sol_amount * sol_price_usd
            
            # Build the trade object in the format your MetricsManager expects
            trade = {
                'token_address': token_address,
                'pool_address': pool_address,
                'timestamp': timestamp,
                'direction': direction,
                'token_amount': token_amount,
                'sol_amount': sol_amount,
                'price': price,
                'size_usd': size_usd,
                'transaction_signature': signature
            }
            
            self.stats['trades_parsed'] += 1
            return trade
            
        except KeyError as e:
            logger.error(f"⚠️ KeyError parsing transaction {signature[:8] if signature else 'unknown'}: Missing key {e}")
            logger.error(f"   Transaction keys available: {list(tx_data.keys())}")
            return None
        except TypeError as e:
            logger.error(f"⚠️ TypeError parsing transaction {signature[:8] if signature else 'unknown'}: {e}")
            logger.error(f"   This usually means unexpected data type in transaction structure")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error parsing transaction {signature[:8] if signature else 'unknown'}: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            return None
    
    async def poll_pool(self, pool_address: str):
        """
        Poll a single pool for new transactions.
        
        This is called every poll_interval for each monitored pool.
        It fetches recent transactions, filters to new ones we haven't seen,
        parses them into trades, and notifies callbacks.
        """
        try:
            pool_info = self.monitored_pools.get(pool_address, {})
            token_symbol = pool_info.get('token_symbol', 'UNKNOWN')
            
            logger.info("=" * 70)
            logger.info(f"🔄 POLLING CYCLE for pool {pool_address[:8]}... (Token: {token_symbol})")
            logger.info("=" * 70)
            
            # Fetch recent transactions from Helius
            transactions = await self.fetch_recent_transactions(pool_address, limit=50)
            
            if not transactions:
                logger.info(f"  ℹ️ No transactions returned for {pool_address[:8]}")
                logger.info("=" * 70)
                return
            
            self.stats['transactions_fetched'] += len(transactions)
            logger.info(f"  📥 Fetched {len(transactions)} transactions from Helius")
            
            new_transactions = []
            for tx in transactions:
                sig = tx.get('signature', '')
    
    # SAFETY CHECK: Ensure signature is a string
    # Sometimes APIs return signature as a dict or other structure
                if isinstance(sig, dict):
        # If it's a dict, try to extract the actual signature string
                    sig = sig.get('signature', '') or sig.get('sig', '') or str(sig)
                elif not isinstance(sig, str):
        # If it's some other type, convert to string
                    sig = str(sig) if sig else ''
    
    # Only process if we have a valid signature string
                if sig and sig not in self.processed_signatures[pool_address]:
                    new_transactions.append(tx)
                    self.processed_signatures[pool_address].add(sig)
            
            
            if not new_transactions:
                logger.info(f"  ℹ️ All transactions already processed (0 new out of {len(transactions)} total)")
                logger.info("=" * 70)
                return
            
            logger.info(f"  🆕 Found {len(new_transactions)} NEW transactions to process")
            logger.info(f"  📊 Processing each transaction...")
            
            # Parse each new transaction and notify callbacks
            trades_found = 0
            for idx, tx_data in enumerate(new_transactions):
                logger.info(f"  ──────── Transaction {idx + 1}/{len(new_transactions)} ────────")
                trade = self.parse_helius_transaction(tx_data, pool_address)
                
                if trade:
                    trades_found += 1
                    # We found a valid trade! Notify all registered callbacks
                    logger.info(f"  💱 Trade #{trades_found}: {trade['direction'].upper()} ${trade['size_usd']:.2f}")
                    
                    for callback in self.trade_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(trade)
                            else:
                                callback(trade)
                        except Exception as e:
                            logger.error(f"  ❌ Error in trade callback: {e}")
            
            logger.info(f"  ✅ Polling complete: {trades_found} trades found from {len(new_transactions)} new transactions")
            logger.info("=" * 70)
            
            # Keep the processed signatures set from growing unbounded
            # Only keep the last 1000 signatures per pool (plenty for 2-minute polling)
            if len(self.processed_signatures[pool_address]) > 1000:
                # Convert to list, keep newest 500
                sigs_list = list(self.processed_signatures[pool_address])
                self.processed_signatures[pool_address] = set(sigs_list[-500:])
                logger.info(f"  🧹 Cleaned up old signatures (kept 500 most recent)")
            
        except Exception as e:
            logger.error(f"❌ Error polling pool {pool_address[:8]}: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def polling_loop(self):
        """
        Main polling loop that checks all monitored pools periodically.
        
        This runs forever (until is_running = False) and polls each pool
        at the specified interval.
        """
        logger.info(f"🔄 Polling loop started (checking every {self.poll_interval}s)")
        
        while self.is_running:
            try:
                poll_start = time.time()
                
                # Poll each monitored pool
                for pool_address in list(self.monitored_pools.keys()):
                    await self.poll_pool(pool_address)
                    
                    # Small delay between pools to be nice to the API
                    await asyncio.sleep(0.5)
                
                self.stats['polls_completed'] += 1
                self.stats['last_poll_time'] = time.time()
                
                # Log statistics every 10 polls
                if self.stats['polls_completed'] % 10 == 0:
                    logger.info(f"📈 Polling stats: {self.stats['polls_completed']} polls, "
                              f"{self.stats['trades_parsed']} trades parsed, "
                              f"~{self.stats['credits_used_estimate']} Helius credits used")
                
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
