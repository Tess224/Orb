"""
Real-Time Data Collection System using Helius WebSocket
This module establishes a persistent connection to Helius and streams
all trades for tokens we're tracking in real-time.
"""

import asyncio
import websockets
import json
import logging
from typing import Dict, List, Callable, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class HeliusWebSocketClient:
    """
    Manages WebSocket connection to Helius for real-time transaction monitoring.
    
    This class handles:
    - Establishing and maintaining WebSocket connection
    - Subscribing to specific token pool addresses
    - Parsing incoming transaction data
    - Routing parsed trades to callback functions
    - Automatic reconnection if connection drops
    """
    
    def __init__(self, api_key: str):
        """
        Initialize the WebSocket client.
        
        Args:
            api_key: Your Helius API key from environment variables
        """
        # Build the WebSocket URL - Helius uses wss:// for secure WebSocket
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={api_key}"
        
        # This will hold our active WebSocket connection
        self.websocket = None
        
        # Track which pool addresses we're subscribed to
        # Dictionary maps pool address to token info for quick lookup
        self.subscribed_pools: Dict[str, Dict] = {}
        
        # Callback functions that get called when we receive trade data
        # Multiple parts of your system might want to know about trades
        self.trade_callbacks: List[Callable] = []
        
        # Flag to control the main loop
        self.is_running = False
        
        # Statistics for monitoring
        self.stats = {
            'messages_received': 0,
            'trades_parsed': 0,
            'errors': 0,
            'last_message_time': None
        }
        
        logger.info(f"✅ Helius WebSocket client initialized")
    
    
    def add_trade_callback(self, callback: Callable):
        """
        Register a function to be called whenever we receive trade data.
        
        Your callback function should accept a dictionary with trade info:
        {
            'token_address': str,
            'pool_address': str,
            'timestamp': int,
            'price': float,
            'amount': float,
            'direction': 'buy' or 'sell',
            'size_usd': float,
            'transaction_signature': str
        }
        
        Args:
            callback: Function that will be called with trade data
        """
        self.trade_callbacks.append(callback)
        logger.info(f"📝 Registered trade callback: {callback.__name__}")
    
    
    async def connect(self):
        """
        Establish WebSocket connection to Helius.
        
        This creates the persistent connection that will stay open
        and stream data to us continuously.
        """
        try:
            logger.info(f"🔌 Connecting to Helius WebSocket...")
            
            # Connect to Helius - this might take a few seconds
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=20,  # Send ping every 20 seconds to keep connection alive
                ping_timeout=10,   # If no pong response in 10 seconds, reconnect
                close_timeout=10
            )
            
            logger.info(f"✅ WebSocket connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Helius WebSocket: {e}")
            return False
    
    
    async def subscribe_to_pool(self, pool_address: str, token_address: str, token_symbol: str = "UNKNOWN"):
        """
        Subscribe to transaction updates for a specific liquidity pool.
        
        When you subscribe to a pool, Helius will send us every transaction
        that affects that pool - buys, sells, liquidity adds/removes, etc.
        
        Args:
            pool_address: The Solana address of the Raydium/Orca pool
            token_address: The token's mint address (for our records)
            token_symbol: Optional symbol like "BONK" for logging
        """
        if not self.websocket:
            logger.error("❌ Cannot subscribe - WebSocket not connected")
            return False
        
        try:
            # Build subscription message in Helius's required format
            subscription_message = {
                "jsonrpc": "2.0",
                "id": pool_address,  # Use pool address as ID for tracking
                "method": "accountSubscribe",  # This is Helius's method name
                "params": [
                    pool_address,  # The account we want to watch
                    {
                        "encoding": "jsonParsed",  # We want parsed JSON, not raw bytes
                        "commitment": "confirmed"   # Wait for confirmation (not just processed)
                    }
                ]
            }
            
            # Send the subscription request
            await self.websocket.send(json.dumps(subscription_message))
            
            # Store info about this subscription
            self.subscribed_pools[pool_address] = {
                'token_address': token_address,
                'token_symbol': token_symbol,
                'subscribed_at': datetime.now().timestamp()
            }
            
            logger.info(f"📡 Subscribed to pool {pool_address[:8]}... (Token: {token_symbol})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to subscribe to pool {pool_address[:8]}: {e}")
            return False
    
    
    def parse_raydium_swap(self, transaction_data: Dict, pool_address: str) -> Optional[Dict]:
        """
        Parse a Raydium swap transaction to extract trade information.
        
        This is the complex part where we dig through Solana transaction
        structure to figure out: was this a buy or sell? How much? At what price?
        
        Raydium transactions have a specific structure. We look for:
        - Token balance changes (preTokenBalances vs postTokenBalances)
        - SOL balance changes (preBalances vs postBalances)
        - Which direction the swap went
        
        Args:
            transaction_data: Raw transaction data from Helius
            pool_address: The pool address this transaction affected
            
        Returns:
            Parsed trade dict or None if not a valid swap
        """
        try:
            # NEW: Log the raw transaction data so we can see what Helius is sending
            logger.info(f"🔍 Parsing transaction for pool {pool_address[:8]}...")
            logger.info(f"   Transaction keys: {list(transaction_data.keys())}")
        
        # Get token info for this pool
            pool_info = self.subscribed_pools.get(pool_address, {})
            # Get token info for this pool
            pool_info = self.subscribed_pools.get(pool_address, {})
            token_address = pool_info.get('token_address')
            
            if not token_address:
                return None
            
            # Extract the transaction and metadata
            # Helius sends this in their specific format
            meta = transaction_data.get('meta', {})
            transaction = transaction_data.get('transaction', {})

            # NEW: Log what we found in the metadata
            logger.info(f"   Meta keys: {list(meta.keys()) if meta else 'None'}")
            logger.info(f"   Has preTokenBalances: {bool(meta.get('preTokenBalances'))}")
            logger.info(f"   Has postTokenBalances: {bool(meta.get('postTokenBalances'))}")
            logger.info(f"   Has preBalances: {bool(meta.get('preBalances'))}")
            logger.info(f"   Has postBalances: {bool(meta.get('postBalances'))}")

            # Get timestamp from block time
            block_time = transaction_data.get('blockTime', 0)
            
            # Get the transaction signature (like a transaction ID)
            signature = transaction.get('signatures', [''])[0]
            
            # Extract balance changes
            pre_token_balances = meta.get('preTokenBalances', [])
            post_token_balances = meta.get('postTokenBalances', [])
            pre_balances = meta.get('preBalances', [])
            post_balances = meta.get('postBalances', [])
            
            # We need to find the balance changes for our token
            # This tells us how many tokens and how much SOL were exchanged
            
            token_change = 0
            sol_change = 0
            
            # Calculate token amount change
            # We're looking for changes in the pool's token balance
            for i, post_bal in enumerate(post_token_balances):
                if post_bal.get('mint') == token_address:
                    # Found our token, calculate the change
                    pre_amount = 0
                    if i < len(pre_token_balances):
                        pre_amount = float(pre_token_balances[i].get('uiTokenAmount', {}).get('uiAmount', 0))
                    
                    post_amount = float(post_bal.get('uiTokenAmount', {}).get('uiAmount', 0))
                    token_change = post_amount - pre_amount
                    break
            
            # Calculate SOL change for the pool
            # Usually the pool is the first account in the transaction
            if len(pre_balances) > 0 and len(post_balances) > 0:
                sol_change = (post_balances[0] - pre_balances[0]) / 1e9  # Convert lamports to SOL
            
            # Determine trade direction and calculate amounts
            # BUY: tokens go out of pool (negative change), SOL comes in (positive change)
            # SELL: tokens come into pool (positive change), SOL goes out (negative change)
            
            if token_change < 0 and sol_change > 0:
                # This is a BUY (someone bought tokens)
                direction = 'buy'
                token_amount = abs(token_change)
                sol_amount = sol_change
                
            elif token_change > 0 and sol_change < 0:
                # This is a SELL (someone sold tokens)
                direction = 'sell'
                token_amount = token_change
                sol_amount = abs(sol_change)
                
            else:
                # Not a swap, maybe liquidity add/remove or something else
                return None
            
            # Calculate price and USD value
            # Price = SOL per token
            price = sol_amount / token_amount if token_amount > 0 else 0
            
            # For USD value, we'd need current SOL price
            # For now, we'll estimate or you can fetch from your existing get_sol_price_usd()
            sol_price_usd = 150  # Placeholder - you should fetch this
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
            return trade

        except Exception as e:
            logger.warning(f"⚠️ Error parsing swap transaction: {e}")
            self.stats['errors'] += 1
            logger.info(f"   ❌ Not a valid swap - token_change: {token_change}, sol_change: {sol_change}")

            return None
    
    
    async def handle_message(self, message: str):
        """
        Process incoming WebSocket messages from Helius.
        
        Helius sends us messages in JSON format. We parse them,
        extract trade information, and notify all registered callbacks.
        
        Args:
            message: Raw JSON string from WebSocket
        """
        try:
            data = json.loads(message)
            self.stats['messages_received'] += 1
            self.stats['last_message_time'] = datetime.now()
            
            # Check if this is a subscription notification (has 'params')
            if 'params' in data:
                result = data['params'].get('result', {})
                context = result.get('context', {})
                value = result.get('value', {})
                
                # Extract account address this update is for
                # This should match one of our subscribed pools
                account = data['params'].get('subscription')
                
                # The actual transaction data is in value
                if 'account' in value:
                    account_data = value['account']
                    
                    # Check if this account is one of our subscribed pools
                    for pool_addr, pool_info in self.subscribed_pools.items():
                        # Try to parse this as a swap transaction
                        trade = self.parse_raydium_swap(value, pool_addr)
                        
                        if trade:
                            # We got a valid trade! Notify all callbacks
                            logger.info(
                                f"💱 Trade detected: {trade['direction'].upper()} "
                                f"${trade['size_usd']:.2f} @ {pool_addr[:8]}..."
                            )
                            
                            # Call all registered callback functions
                            for callback in self.trade_callbacks:
                                try:
                                    # Run the callback
                                    # If it's async, await it; if sync, just call it
                                    if asyncio.iscoroutinefunction(callback):
                                        await callback(trade)
                                    else:
                                        callback(trade)
                                except Exception as e:
                                    logger.error(f"❌ Error in trade callback {callback.__name__}: {e}")
                            
                            break
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse WebSocket message: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"❌ Error handling WebSocket message: {e}")
            self.stats['errors'] += 1
    
    
    async def listen(self):
        """
        Main listening loop that receives and processes WebSocket messages.
        
        This runs continuously, receiving messages from Helius and
        processing them. If the connection drops, it will attempt to reconnect.
        """
        self.is_running = True
        
        while self.is_running:
            try:
                if not self.websocket:
                    # Connection lost, try to reconnect
                    logger.warning("⚠️ WebSocket disconnected, attempting to reconnect...")
                    connected = await self.connect()
                    
                    if connected and self.subscribed_pools:
                        # Resubscribe to all our pools
                        logger.info("🔄 Resubscribing to pools after reconnection...")
                        for pool_addr, pool_info in self.subscribed_pools.items():
                            await self.subscribe_to_pool(
                                pool_addr,
                                pool_info['token_address'],
                                pool_info.get('token_symbol', 'UNKNOWN')
                            )
                    
                    await asyncio.sleep(5)  # Wait before retrying
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
        """Clean shutdown of WebSocket connection."""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("🔌 WebSocket connection closed")
    
    
    def get_stats(self) -> Dict:
        """Return current statistics about WebSocket activity."""
        return self.stats.copy()
