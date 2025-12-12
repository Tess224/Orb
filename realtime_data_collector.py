"""
Real-Time Transaction Streaming using Helius WebSocket

MAJOR CHANGE FROM ORIGINAL:
- Now uses logsSubsscribe instead of accountSubscribe
- Fetches full transaction data when we receive notifications
- This gives us the actual trade details we need for parsing
"""

import asyncio
import websockets
import json
import logging
import aiohttp
from typing import Dict, List, Callable, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class HeliusWebSocketClient:
    """
    Manages WebSocket connection to Helius for real-time transaction monitoring.
    
    KEY ARCHITECTURAL CHANGE:
    Instead of subscribing to account state updates (which only show us the
    new balance after a trade), we now subscribe to transaction logs (which
    show us the actual transactions as they happen). This gives us access to
    the token balance changes, swap details, and signatures we need.
    """

    def __init__(self, api_key: str):
        """
        Initialize the WebSocket client.
        
        Args:
            api_key: Your Helius API key from environment variables
        """
        # WebSocket connection URL
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={api_key}"
        
        # REST API URL for fetching full transaction data
        # When we get a transaction signature from logs, we'll use this to fetch details
        self.http_url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        
        self.websocket = None
        
        # Track which pool addresses we're monitoring
        self.subscribed_pools: Dict[str, Dict] = {}
        
        # Callbacks that get notified when we successfully parse a trade
        self.trade_callbacks: List[Callable] = []
        
        # For making HTTP requests to fetch transaction details
        self.http_session = None
        
        self.is_running = False
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'transactions_fetched': 0,
            'trades_parsed': 0,
            'errors': 0,
            'last_message_time': None
        }

        logger.info(f"✅ Helius WebSocket client initialized with transaction streaming")


    def add_trade_callback(self, callback: Callable):
        """Register a function to be called when we successfully parse a trade."""
        self.trade_callbacks.append(callback)
        logger.info(f"📝 Registered trade callback: {callback.__name__}")


    async def connect(self):
        """Establish WebSocket connection to Helius."""
        try:
            logger.info(f"🔌 Connecting to Helius WebSocket...")
            
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
        
            
            # Also create an HTTP session for fetching transaction details
            self.http_session = aiohttp.ClientSession()
            
            logger.info(f"✅ WebSocket connected successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to connect to Helius WebSocket: {e}")
            return False


    async def subscribe_to_pool(self, pool_address: str, token_address: str, token_symbol: str = "UNKNOWN"):
        """
        Subscribe to transaction logs for a specific pool.
        
        CRITICAL CHANGE: We're now using logsSubscribe instead of accountSubscribe.
        
        Why this matters:
        - accountSubscribe only tells us "the account changed" but not how or why
        - logsSubscribe tells us "a transaction happened that mentioned this account"
        - With logsSubscribe, we get transaction signatures that we can fetch for full details
        
        Args:
            pool_address: The Solana address of the liquidity pool
            token_address: The token's mint address (for our records)
            token_symbol: Optional symbol for logging
        """
        if not self.websocket:
            logger.error("❌ Cannot subscribe - WebSocket not connected")
            return False

        try:
            # Build the subscription message using logsSubscribe
            # This tells Helius: "notify me whenever a transaction mentions this address"
            subscription_message = {
                "jsonrpc": "2.0",
                "id": pool_address,
                "method": "logsSubscribe",  # Changed from accountSubscribe
                "params": [
                    {
                        "mentions": [pool_address]  # Watch for any transaction mentioning this pool
                    },
                    {
                        "commitment": "confirmed"  # Wait for confirmation
                    }
                ]
            }

            await self.websocket.send(json.dumps(subscription_message))

            # Store info about this subscription
            self.subscribed_pools[pool_address] = {
                'token_address': token_address,
                'token_symbol': token_symbol,
                'subscribed_at': datetime.now().timestamp()
            }

            logger.info(f"📡 Subscribed to transaction logs for pool {pool_address[:8]}... (Token: {token_symbol})")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to subscribe to pool {pool_address[:8]}: {e}")
            return False


    async def fetch_transaction_details(self, signature: str, retry_count: int = 0) -> Optional[Dict]:
        """
        Fetch full transaction details with rate limit handling.
        """
        max_retries = 3
        base_delay = 1  # Start with 1 second delay
    
        try:
            request_body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                        "commitment": "confirmed"
                    }
                ]
            }

            async with self.http_session.post(
                self.http_url,
                json=request_body,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                if response.status == 429:
                    # Rate limited
                    if retry_count < max_retries:
                        # Exponential backoff: 1s, 2s, 4s
                        delay = base_delay * (2 ** retry_count)
                        logger.warning(f"Rate limited, retrying in {delay}s (attempt {retry_count + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        return await self.fetch_transaction_details(signature, retry_count + 1)
                    else:
                        logger.warning(f"Rate limited and max retries reached for {signature[:8]}")
                        return None
                    
                elif response.status == 200:
                    data = await response.json()
                    if 'result' in data and data['result']:
                        self.stats['transactions_fetched'] += 1
                        return data['result']
                    return None
                else:
                    logger.warning(f"HTTP {response.status} when fetching transaction {signature[:8]}")
                    return None

        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching transaction {signature[:8]}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching transaction {signature[:8]}: {e}")
            return None


    def parse_raydium_swap(self, transaction_data: Dict, pool_address: str) -> Optional[Dict]:
        """
        Parse a transaction to extract swap trade information.
        
        This method now receives FULL transaction data (not just account state),
        so it can properly identify swaps and extract all the details.
        """
        try:
            logger.debug(f"🔍 Parsing transaction for pool {pool_address[:8]}...")
            
            # Get token info for this pool
            pool_info = self.subscribed_pools.get(pool_address, {})
            token_address = pool_info.get('token_address')

            if not token_address:
                return None

            # Extract metadata and transaction details
            meta = transaction_data.get('meta', {})
            transaction = transaction_data.get('transaction', {})
            
            # Get timestamp
            block_time = transaction_data.get('blockTime', 0)
            
            # Get transaction signature
            message = transaction.get('message', {})
            signatures = message.get('signatures', [])
            signature = signatures[0] if signatures else ''

            # Extract balance changes
            pre_token_balances = meta.get('preTokenBalances', [])
            post_token_balances = meta.get('postTokenBalances', [])
            pre_balances = meta.get('preBalances', [])
            post_balances = meta.get('postBalances', [])

            # Calculate token amount change for our specific token
            token_change = 0
            for i, post_bal in enumerate(post_token_balances):
                if post_bal.get('mint') == token_address:
                    pre_amount = 0
                    if i < len(pre_token_balances):
                        pre_amount = float(pre_token_balances[i].get('uiTokenAmount', {}).get('uiAmount', 0) or 0)
                    
                    post_amount = float(post_bal.get('uiTokenAmount', {}).get('uiAmount', 0) or 0)
                    token_change = post_amount - pre_amount
                    break

            # Calculate SOL change (typically the first account is the pool)
            sol_change = 0
            if len(pre_balances) > 0 and len(post_balances) > 0:
                sol_change = (post_balances[0] - pre_balances[0]) / 1e9  # Convert lamports to SOL

            # Determine trade direction and calculate amounts
            # BUY: tokens leave pool (negative change), SOL enters pool (positive change)
            # SELL: tokens enter pool (positive change), SOL leaves pool (negative change)
            
            if token_change < 0 and sol_change > 0:
                # This is a BUY
                direction = 'buy'
                token_amount = abs(token_change)
                sol_amount = sol_change
                
            elif token_change > 0 and sol_change < 0:
                # This is a SELL
                direction = 'sell'
                token_amount = token_change
                sol_amount = abs(sol_change)
                
            else:
                # Not a swap (maybe liquidity add/remove or failed transaction)
                logger.debug(f"   ❌ Not a swap - token_change: {token_change:.2f}, sol_change: {sol_change:.4f}")
                return None

            # Calculate price and USD value
            if token_amount == 0:
                return None
                
            price = sol_amount / token_amount
            
            # Estimate USD value (using approximate SOL price)
            # In production, you'd want to fetch current SOL price
            sol_price_usd = 190  # Approximate - you should fetch this dynamically
            size_usd = sol_amount * sol_price_usd

            # Build the parsed trade object
            trade = {
                'token_address': token_address,
                'pool_address': pool_address,
                'timestamp': block_time,
                'direction': direction,
                'token_amount': token_amount,
                'sol_amount': sol_amount,
                'price': price,
                'size_usd': size_usd,
                'transaction_signature': signature
            }

            self.stats['trades_parsed'] += 1
            logger.info(f"💱 Trade parsed: {direction.upper()} ${size_usd:.2f} @ {pool_address[:8]}")
            
            return trade

        except Exception as e:
            logger.warning(f"⚠️ Error parsing transaction: {e}")
            self.stats['errors'] += 1
            return None


    async def handle_message(self, message: str):
        """
        Process incoming WebSocket messages from Helius.
        
        NEW FLOW:
        1. Receive log notification with transaction signature
        2. Fetch full transaction data using that signature
        3. Parse the transaction to extract trade information
        4. Notify all registered callbacks
        """
        try:
            data = json.loads(message)
            self.stats['messages_received'] += 1
            self.stats['last_message_time'] = datetime.now()

            logger.info(f"📨 WebSocket message received")
            logger.info(f"   Message structure: {list(data.keys())}")

            # Check if this is a log notification
            if 'params' in data:
                logger.info(f"   ✓ Has params - this is a notification")
                
                result = data['params'].get('result', {})
                logger.info(f"   Result structure: {list(result.keys()) if result else 'None'}")
                
                # Extract the logs from the notification
                value = result.get('value', {})
                
                if not value:
                    logger.debug("   No value in result, skipping")
                    return
                
                # The value contains the transaction signature and logs
                signature = value.get('signature')
                
                if not signature:
                    logger.debug("   No signature in notification, skipping")
                    return
                
                logger.info(f"   📝 Transaction signature: {signature[:16]}...")
                
                # Now fetch the full transaction details using this signature
                logger.info(f"   🔄 Fetching full transaction data...")
                transaction_data = await self.fetch_transaction_details(signature)
                
                if not transaction_data:
                    logger.debug(f"   Could not fetch transaction data for {signature[:8]}")
                    return
                
                logger.info(f"   ✓ Transaction data fetched successfully")
                
                # Try to parse this as a swap for each of our subscribed pools
                for pool_addr, pool_info in self.subscribed_pools.items():
                    trade = self.parse_raydium_swap(transaction_data, pool_addr)
                    
                    if trade:
                        # We successfully parsed a trade! Notify all callbacks
                        logger.info(f"   ✅ Trade detected for pool {pool_addr[:8]}")
                        
                        for callback in self.trade_callbacks:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(trade)
                                else:
                                    callback(trade)
                            except Exception as e:
                                logger.error(f"❌ Error in trade callback {callback.__name__}: {e}")
                        
                        break  # Found the trade, no need to check other pools

        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse WebSocket message: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"❌ Error handling WebSocket message: {e}")
            self.stats['errors'] += 1


    async def listen(self):
        """
        Main listening loop that receives and processes WebSocket messages.
        """
        self.is_running = True

        while self.is_running:
            try:
                if not self.websocket:
                    logger.warning("⚠️ WebSocket disconnected, attempting to reconnect...")
                    connected = await self.connect()

                    if connected and self.subscribed_pools:
                        logger.info("🔄 Resubscribing to pools after reconnection...")
                        for pool_addr, pool_info in self.subscribed_pools.items():
                            await self.subscribe_to_pool(
                                pool_addr,
                                pool_info['token_address'],
                                pool_info.get('token_symbol', 'UNKNOWN')
                            )

                    await asyncio.sleep(5)
                    continue

                # Wait for and receive next message
                message = await self.websocket.recv()

                # Process the message
                await self.handle_message(message)

            except websockets.exceptions.ConnectionClosed:
                logger.warning("⚠️ WebSocket connection closed")
                self.websocket = None
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ Error in listen loop: {e}")
                await asyncio.sleep(1)


    async def close(self):
        """Clean shutdown of WebSocket connection and HTTP session."""
        self.is_running = False
        
        if self.websocket:
            await self.websocket.close()
            logger.info("🔌 WebSocket connection closed")
        
        if self.http_session:
            await self.http_session.close()
            logger.info("🔌 HTTP session closed")


    def get_stats(self) -> Dict:
        """Return current statistics about activity."""
        return self.stats.copy()
