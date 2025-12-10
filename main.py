"""
Solana Token Pump/Dump Detection System - Backend API
This server analyzes token slippage patterns to detect pre-pump and pre-dump signals
by probing Jupiter's quote API and analyzing liquidity structure asymmetries.
"""

import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
import asyncio 
import threading

from solana.rpc.api import Client
from solders.pubkey import Pubkey
from solders.signature import Signature
from realtime_data_collector import HeliusWebSocketClient
from realtime_metrics import MetricsManager, MetricsSnapshot
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CORS(app)

SOL_MINT = 'So11111111111111111111111111111111111111112'

PROBE_SIZES_USD = [100, 250, 500, 1000, 2500, 5000]

CACHE_DURATION_SECONDS = 120

MAX_HISTORICAL_MEASUREMENTS = 10

BIRDEYE_API_KEY = os.environ.get('BIRDEYE_API_KEY')

HELIUS_RPC = os.environ.get('HELIUS_RPC_URL')
CHAINLINK_RPC = os.environ.get('CHAINLINK_RPC_URL')

analysis_cache: Dict[str, Dict] = {}
historical_slippage: Dict[str, List[Dict]] = {}
wallet_analysis_cache: Dict[str, Dict] = {}
# NEW CODE STARTS HERE - Add these lines
# Rate limiting storage - tracks how many analyses each access code has used
# Structure: {'ACCESS-CODE': {'count': 5, 'reset_time': 1234567890}}
rate_limit_storage: Dict[str, Dict] = {}
# NEW GLOBALS FOR REAL-TIME SYSTEM
websocket_client: Optional[HeliusWebSocketClient] = None
metrics_manager: Optional[MetricsManager] = None
websocket_thread: Optional[threading.Thread] = None
token_to_pool_map: Dict[str, str] = {}
websocket_loop = None  # Will hold the WebSocket's event loop
state_analyzer = None

# Rate limiting configuration
DAILY_ANALYSIS_LIMIT = 10  # Default limit per access code per day
RATE_LIMIT_WINDOW_HOURS = 24  # Reset every 24 hours

# Access code limits (you can customize these for different users)
ACCESS_CODE_LIMITS = {
    'ADMIN-2025': 999,  # Your admin code gets unlimited analyses
    'ALPHA-TEST-1': 10,
    'ALPHA-TEST-2': 10,
    'BETA-TEST-1': 5,
}

# ============================================================================
# RATE LIMITING SYSTEM
# ============================================================================

def check_rate_limit(access_code: str) -> Dict:
    """
    Check if an access code has exceeded its daily analysis limit.
    
    This function looks up how many analyses this access code has performed
    today. If they have hit their limit, it returns allowed as False.
    Otherwise, it returns allowed as True so they can proceed.
    
    Args:
        access_code: The access code from the user's request
        
    Returns:
        Dictionary with allowed (bool), remaining (int), resets_at (timestamp), limit (int)
    """
    # Get the limit for this specific access code
    # If this code is not in our ACCESS_CODE_LIMITS dictionary, use the default limit
    limit = ACCESS_CODE_LIMITS.get(access_code, DAILY_ANALYSIS_LIMIT)
    
    # Get current time as a Unix timestamp
    current_time = time.time()
    
    # Check if we have any record for this access code yet
    if access_code not in rate_limit_storage:
        # First time seeing this code today, so initialize it with zero usage
        reset_time = current_time + (RATE_LIMIT_WINDOW_HOURS * 3600)  # 3600 seconds in an hour
        rate_limit_storage[access_code] = {
            'count': 0,
            'reset_time': reset_time
        }
    
    # Get the stored data for this access code
    usage_data = rate_limit_storage[access_code]
    
    # Check if the rate limit window has expired (meaning it's a new day)
    if current_time >= usage_data['reset_time']:
        # Time window expired, so reset the counter to zero
        logger.info(f"⏰ Rate limit reset for access code {access_code}")
        reset_time = current_time + (RATE_LIMIT_WINDOW_HOURS * 3600)
        rate_limit_storage[access_code] = {
            'count': 0,
            'reset_time': reset_time
        }
        usage_data = rate_limit_storage[access_code]
    
    # Check if they have exceeded their limit
    current_count = usage_data['count']
    remaining = limit - current_count
    
    if current_count >= limit:
        logger.warning(f"⛔ Rate limit exceeded for {access_code}: {current_count}/{limit}")
        return {
            'allowed': False,
            'remaining': 0,
            'resets_at': int(usage_data['reset_time']),
            'limit': limit
        }
    
    logger.info(f"✅ Rate limit OK for {access_code}: {current_count}/{limit} used")
    return {
        'allowed': True,
        'remaining': remaining,
        'resets_at': int(usage_data['reset_time']),
        'limit': limit
    }


def increment_usage(access_code: str) -> None:
    """
    Increment the usage counter for an access code after a successful analysis.
    
    This should be called AFTER you have performed the analysis successfully.
    It increases their usage count by one.
    
    Args:
        access_code: The access code that just performed an analysis
    """
    if access_code in rate_limit_storage:
        rate_limit_storage[access_code]['count'] += 1
        new_count = rate_limit_storage[access_code]['count']
        limit = ACCESS_CODE_LIMITS.get(access_code, DAILY_ANALYSIS_LIMIT)
        logger.info(f"📊 Usage incremented for {access_code}: {new_count}/{limit}")

# ============================================================================
# PHASE 2: WALLET ANALYSIS SYSTEM - BIRDEYE HELPERS
# ============================================================================

def fetch_birdeye_win_rate(wallet_address: str) -> Optional[float]:
    """
    Fetch a wallet's trading win rate from BirdEye's PnL API.
    
    This function calls BirdEye's profit and loss summary endpoint to get
    statistics about how many of the wallet's trades were profitable versus
    unprofitable. A high win rate indicates a skilled trader, while a low
    win rate suggests gambling behavior.
    
    Args:
        wallet_address: The Solana wallet address to analyze
        
    Returns:
        Win rate as a percentage (0-100), or None if data unavailable
    """
    try:
        # BirdEye's PnL summary endpoint provides win/loss statistics
        url = f"https://public-api.birdeye.so/wallet/v2/pnl/summary"
        
        # The wallet address goes in the query parameters
        params = {
            'address': wallet_address
        }
        
        # BirdEye requires API key authentication in the headers
        headers = {
            'X-API-KEY': BIRDEYE_API_KEY
        }
        
        logger.info(f"📊 Fetching win rate from BirdEye for {wallet_address[:8]}...")
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        # If BirdEye doesn't have data on this wallet, that's okay
        # It just means they haven't tracked this wallet's trades
        if response.status_code == 404:
            logger.info(f"ℹ️ BirdEye has no PnL data for {wallet_address[:8]}")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        # BirdEye returns success=false if something went wrong
        if not data.get('success', False):
            logger.warning(f"⚠️ BirdEye PnL API returned success=false for {wallet_address[:8]}")
            return None
        
        # The actual data is nested in a 'data' field
        pnl_data = data.get('data', {})
        
        if not pnl_data:
            logger.info(f"ℹ️ BirdEye returned empty PnL data for {wallet_address[:8]}")
            return None
        
        # Extract the total number of trades and winning trades
        total_trades = pnl_data.get('total', 0)
        winning_trades = pnl_data.get('win', 0)
        
        # If the wallet hasn't made any trades according to BirdEye, return None
        if total_trades == 0:
            logger.info(f"ℹ️ Wallet {wallet_address[:8]} has zero trades in BirdEye")
            return None
        
        # Calculate win rate as a percentage
        # We clamp it between 0 and 100 to handle any data anomalies
        win_rate = (winning_trades / total_trades) * 100
        win_rate = max(0.0, min(100.0, win_rate))
        
        logger.info(f"✅ Win rate for {wallet_address[:8]}: {win_rate:.1f}% ({winning_trades}/{total_trades} trades)")
        
        return win_rate
        
    except requests.exceptions.Timeout:
        logger.error(f"⏱️ Timeout fetching BirdEye win rate for {wallet_address[:8]}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ BirdEye PnL API error for {wallet_address[:8]}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching win rate for {wallet_address[:8]}: {e}")
        return None


def fetch_birdeye_trades_count(wallet_address: str) -> Optional[int]:
    """
    Fetch the number of trades a wallet has made in the last 30 days from BirdEye.
    
    This function queries BirdEye's transaction history endpoint to count how many
    swap transactions (token trades) the wallet has executed recently. A high trade
    count might indicate an active trader or a bot, while a low trade count suggests
    a more patient, long-term holder.
    
    Args:
        wallet_address: The Solana wallet address to analyze
        
    Returns:
        Number of qualifying trades (value >= $50), or None if data unavailable
    """
    try:
        # BirdEye's transaction history endpoint
        url = "https://public-api.birdeye.so/trader/txs/seek_by_time"
        
        # Calculate the time range: last 30 days
        # BirdEye expects Unix timestamps (seconds since epoch)
        now = int(time.time())
        thirty_days_ago = now - (30 * 24 * 60 * 60)
        
        # Build the query parameters
        params = {
            'address': wallet_address,
            'from_time': thirty_days_ago,
            'to_time': now,
            'tx_type': 'swap',  # We only want swap transactions, not transfers or other tx types
            'sort_type': 'desc',  # Most recent first
            'limit': 1000  # Maximum number of transactions to retrieve
        }
        
        headers = {
            'X-API-KEY': BIRDEYE_API_KEY
        }
        
        logger.info(f"📊 Fetching trade count from BirdEye for {wallet_address[:8]}...")
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        if response.status_code == 404:
            logger.info(f"ℹ️ BirdEye has no transaction data for {wallet_address[:8]}")
            return None
        
        response.raise_for_status()
        data = response.json()
        
        if not data.get('success', False):
            logger.warning(f"⚠️ BirdEye txs API returned success=false for {wallet_address[:8]}")
            return None
        
        # Transaction data is nested: data.items is an array of transactions
        items = data.get('data', {}).get('items', [])
        
        if not items:
            logger.info(f"ℹ️ No transactions found for {wallet_address[:8]} in last 30 days")
            return 0
        
        # Filter transactions to only count significant trades
        # We ignore dust trades under $50 because they don't indicate real trading behavior
        # They might be test transactions, airdrops, or spam
        significant_trades = [
            tx for tx in items 
            if float(tx.get('value', 0)) >= 50.0
        ]
        
        trade_count = len(significant_trades)
        
        logger.info(f"✅ Trade count for {wallet_address[:8]}: {trade_count} trades (filtered from {len(items)} total)")
        
        return trade_count
        
    except requests.exceptions.Timeout:
        logger.error(f"⏱️ Timeout fetching BirdEye trades for {wallet_address[:8]}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ BirdEye txs API error for {wallet_address[:8]}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching trades for {wallet_address[:8]}: {e}")
        return None

# ============================================================================
# PHASE 2: WALLET ANALYSIS SYSTEM - BLOCKCHAIN TRANSACTION PARSING
# ============================================================================


def get_wallet_transaction_signatures(wallet_address: str, limit: int = 100) -> List[Dict]:
    """
    Fetch transaction signatures for a wallet from the Solana blockchain.
    
    This function queries the blockchain to get a list of all transactions
    this wallet has been involved in. We get signatures first (which are
    like transaction IDs) and then fetch the full transaction details later.
    This two-step process is more efficient than fetching everything at once.
    
    Args:
        wallet_address: The Solana wallet address to query
        limit: Maximum number of signatures to fetch (default 100)
        
    Returns:
        List of signature objects with metadata like block time
    """
    try:
        # Try Chainlink RPC first, fall back to Helius if it fails
        rpc_url = CHAINLINK_RPC or HELIUS_RPC
        
        if not rpc_url:
            logger.error("❌ No RPC URL configured for blockchain queries")
            return []
        
        # Create a connection to the Solana blockchain
        client = Client(rpc_url)
        
        # Convert the wallet address string to a Pubkey object
        # Solana's Python library requires addresses as Pubkey objects
        wallet_pubkey = Pubkey.from_string(wallet_address)
        
        logger.info(f"🔗 Fetching transaction signatures for {wallet_address[:8]}... (limit: {limit})")
        
        # Query the blockchain for transaction signatures
        # This returns a list of signatures with metadata like block time
        response = client.get_signatures_for_address(wallet_pubkey, limit=limit)
        
        if hasattr(response, 'value') and response.value:
            signatures = response.value
            logger.info(f"✅ Retrieved {len(signatures)} transaction signatures for {wallet_address[:8]}")
            
            # Convert the response objects to simple dictionaries for easier handling
            sig_list = []
            for sig in signatures:
                sig_dict = {
                    'signature': str(sig.signature),
                    'blockTime': sig.block_time,
                    'slot': sig.slot
                }
                sig_list.append(sig_dict)
            
            return sig_list
        else:
            logger.warning(f"⚠️ No transactions found for {wallet_address[:8]}")
            return []
            
    except Exception as e:
        logger.error(f"❌ Error fetching transaction signatures for {wallet_address[:8]}: {e}")
        return []


def fetch_parsed_transactions_batch(signatures: List[str], rpc_url: str) -> List[Optional[Dict]]:
    """
    Fetch full transaction data for a batch of signatures from the blockchain.
    
    This function takes a list of transaction signatures (IDs) and retrieves
    the complete transaction data for each one. We do this in batches rather
    than one at a time for efficiency. Each transaction contains detailed
    information about what happened, including balance changes for all accounts.
    
    Args:
        signatures: List of transaction signature strings
        rpc_url: The RPC endpoint URL to query
        
    Returns:
        List of parsed transaction objects (some may be None if fetch failed)
    """
    try:
        client = Client(rpc_url)
        transactions = []
        
        logger.info(f"📥 Fetching {len(signatures)} transactions in batch...")
        
        # Fetch each transaction individually
        # Note: Some RPC providers support batch requests, but for simplicity
        # and reliability, we fetch one at a time here
        for idx, sig in enumerate(signatures):
            try:
                # Get the transaction with all details parsed into readable format
                sig_object = Signature.from_string(sig)
                tx_response = client.get_transaction(
                    sig_object,

                    encoding="jsonParsed",
                    max_supported_transaction_version=0
                )
                
                if hasattr(tx_response, 'value') and tx_response.value:
                    transactions.append(tx_response.value)
                else:
                    transactions.append(None)
                    
                # Log progress every 10 transactions so we can see it's working
                if (idx + 1) % 10 == 0:
                    logger.info(f"  ... fetched {idx + 1}/{len(signatures)} transactions")
                    
            except Exception as e:
                logger.warning(f"  ⚠️ Failed to fetch transaction {sig[:8]}: {e}")
                transactions.append(None)
                
            # Small delay to avoid overwhelming the RPC endpoint
            time.sleep(0.1)
        
        success_count = sum(1 for tx in transactions if tx is not None)
        logger.info(f"✅ Successfully fetched {success_count}/{len(signatures)} transactions")
        
        return transactions
        
    except Exception as e:
        logger.error(f"❌ Error in batch transaction fetch: {e}")
        return [None] * len(signatures)


def parse_wallet_trades_from_transactions(
    wallet_address: str,
    transactions: List[Dict],
    signatures_data: List[Dict]
) -> Dict[str, Dict]:
    """
    Parse through transactions to identify token trades and build trading history.
    
    This is the complex heart of the transaction analysis system. We examine
    each transaction to identify when the wallet bought or sold tokens by
    looking at balance changes. A transaction where SOL decreased and tokens
    increased is a buy. A transaction where tokens decreased and SOL increased
    is a sell. We track all trades by token to build a complete picture.
    
    Args:
        wallet_address: The wallet we're analyzing
        transactions: List of parsed transaction objects
        signatures_data: List of signature metadata (for timestamps)
        
    Returns:
        Dictionary mapping token addresses to their trade data:
        {
            'TokenMintAddress': {
                'buys': [list of buy transactions with amounts and timestamps],
                'sells': [list of sell transactions with amounts and timestamps]
            }
        }
    """
    # This will store all trades organized by token
    trades_by_token = {}
    
    logger.info(f"📊 Parsing {len(transactions)} transactions for trading activity...")
    
    for idx, tx in enumerate(transactions):
        # Skip transactions that failed to fetch
        if not tx or not hasattr(tx, 'transaction'):
            continue

        # TEMPORARY DIAGNOSTIC CODE - ADD THIS
        if idx == 0:  # Only log the first transaction to avoid spam
            logger.info(f"🔍 DEBUG: Transaction object type: {type(tx)}")
            logger.info(f"🔍 DEBUG: Transaction attributes: {dir(tx)}")
            if hasattr(tx, 'transaction'):
                logger.info(f"🔍 DEBUG: Has 'transaction' attribute")
            if hasattr(tx, 'meta'):
                logger.info(f"🔍 DEBUG: Has 'meta' attribute")
        # END TEMPORARY DIAGNOSTIC CODE
        
        try:
            # Get the transaction's metadata including timestamp
            sig_data = signatures_data[idx] if idx < len(signatures_data) else {}
            block_time = sig_data.get('blockTime', 0)

            # Extract the actual transaction and its metadata
            # In solders library, transaction contains both the tx data and metadata
            transaction_data = getattr(tx, 'transaction', None)
            if not transaction_data:
                continue
                
            # The metadata is inside the transaction object, not at the top level
            meta = getattr(transaction_data, 'meta', None)
            if not meta:
                continue
            
            # Token balance changes are in preTokenBalances and postTokenBalances
            pre_token_balances = meta.pre_token_balances or []
            post_token_balances = meta.post_token_balances or []
            
            # SOL balance changes are in preBalances and postBalances
            pre_balances = meta.pre_balances or []
            post_balances = meta.post_balances or []
            
            # Find the account index for our wallet in the transaction
            # Transactions involve multiple accounts, we need to find ours
            transaction = getattr(tx, 'transaction', None)
            if not transaction:
                continue
            account_keys = transaction_data.message.account_keys
            wallet_index = None
            
            for i, key in enumerate(account_keys):
                if str(key) == wallet_address:
                    wallet_index = i
                    break
            
            if wallet_index is None:
                continue  # Wallet not found in this transaction
            
            # Calculate SOL balance change for this wallet
            sol_pre = pre_balances[wallet_index] if wallet_index < len(pre_balances) else 0
            sol_post = post_balances[wallet_index] if wallet_index < len(post_balances) else 0
            sol_change = (sol_post - sol_pre) / 1e9  # Convert lamports to SOL
            
            # Now look for token balance changes
            for post_balance in post_token_balances:
                # Only look at tokens owned by our wallet
                if str(post_balance.owner) != wallet_address:
                    continue
                
                token_mint = str(post_balance.mint)
                
                # Find the corresponding pre-balance for this token
                pre_balance = None
                for pb in pre_token_balances:
                    if str(pb.mint) == token_mint and str(pb.owner) == wallet_address:
                        pre_balance = pb
                        break
                
                # Calculate token amount change
                pre_amount = float(pre_balance.ui_token_amount.ui_amount) if pre_balance else 0.0
                post_amount = float(post_balance.ui_token_amount.ui_amount)
                token_change = post_amount - pre_amount
                
                # Skip if the change is negligible (dust)
                if abs(token_change) < 0.000001:
                    continue
                
                # Initialize tracking for this token if we haven't seen it before
                if token_mint not in trades_by_token:
                    trades_by_token[token_mint] = {
                        'buys': [],
                        'sells': []
                    }
                
                # Classify the transaction as a buy or sell based on balance changes
                # BUY: token balance increased, SOL balance decreased, and SOL change is significant
                if token_change > 0 and sol_change < 0 and abs(sol_change) >= 0.05:
                    trades_by_token[token_mint]['buys'].append({
                        'tokenAmount': token_change,
                        'solAmount': abs(sol_change),
                        'timestamp': block_time,
                        'value': abs(sol_change)  # Value in SOL
                    })
                    
                # SELL: token balance decreased, SOL balance increased, and SOL change is significant
                elif token_change < 0 and sol_change > 0 and sol_change >= 0.05:
                    trades_by_token[token_mint]['sells'].append({
                        'tokenAmount': abs(token_change),
                        'solAmount': sol_change,
                        'timestamp': block_time,
                        'value': sol_change  # Value in SOL
                    })
        
        except Exception as e:
            # If parsing this transaction fails, log it but continue with others
            logger.warning(f"  ⚠️ Error parsing transaction {idx}: {e}")
            continue
    
    # Log summary of what we found
    total_tokens = len(trades_by_token)
    total_buys = sum(len(data['buys']) for data in trades_by_token.values())
    total_sells = sum(len(data['sells']) for data in trades_by_token.values())
    
    logger.info(f"✅ Parsed trading history: {total_tokens} unique tokens, {total_buys} buys, {total_sells} sells")
    
    return trades_by_token

# ============================================================================
# PHASE 2: WALLET ANALYSIS SYSTEM - MAIN ANALYSIS FUNCTION
# ============================================================================

# Cache for wallet analysis results (24-hour TTL)
wallet_analysis_cache: Dict[str, Dict] = {}
WALLET_ANALYSIS_CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds


def calculate_wallet_iq(
    wallet_address: str,
    holding_percent: float = 0.0,
    current_token_address: Optional[str] = None
) -> Dict:
    """
    Calculate comprehensive intelligence metrics for a Solana wallet.
    
    This is the main wallet analysis function that orchestrates all the helper
    functions we've built. It gathers data from multiple sources, applies
    proprietary scoring algorithms, and returns a complete intelligence
    assessment of the wallet's trading behavior and sophistication.
    
    The analysis includes:
    - Overall IQ score (0-100) indicating trading sophistication
    - Win rate percentage showing profitable trade ratio
    - Trade count and activity level assessment  
    - Trading pattern classification (Aggressive Winner, Calculated Trader, etc.)
    - Hold score indicating patience and long-term strategy
    - First buy time for the current token being analyzed
    
    Args:
        wallet_address: The Solana wallet address to analyze
        holding_percent: What percentage of a token this wallet holds (0-100)
        current_token_address: Optional token address to track first buy time
        
    Returns:
        Dictionary containing complete wallet intelligence metrics
    """
    try:
        # Check if we have recent cached analysis for this wallet
        cache_key = f"wallet_analysis:{wallet_address}"
        if cache_key in wallet_analysis_cache:
            cached = wallet_analysis_cache[cache_key]
            cache_age = time.time() - cached['timestamp']
            
            if cache_age < WALLET_ANALYSIS_CACHE_TTL:
                logger.info(f"💾 Using cached analysis for {wallet_address[:8]}... (age: {cache_age/3600:.1f}h)")
                return cached['data']
        
        logger.info(f"🧠 Starting comprehensive analysis for wallet {wallet_address[:8]}...")
        
        # ====================================================================
        # STEP 1: FETCH WIN RATE FROM BIRDEYE
        # ====================================================================
        
        win_rate = fetch_birdeye_win_rate(wallet_address)
        
        # If BirdEye doesn't have win rate data, we'll calculate it ourselves
        # from blockchain data later in the function
        if win_rate is None:
            logger.info(f"ℹ️ No BirdEye win rate data, will calculate from blockchain")
            calculate_win_rate_from_chain = True
        else:
            calculate_win_rate_from_chain = False
        
        # ====================================================================
        # STEP 2: FETCH TRADE COUNT FROM BIRDEYE
        # ====================================================================
        
        trades_count = fetch_birdeye_trades_count(wallet_address)
        
        # If BirdEye doesn't have trade count, we'll count from blockchain data
        if trades_count is None:
            logger.info(f"ℹ️ No BirdEye trade count, will count from blockchain")
            trades_count = 0
            use_chain_trade_count = True
        else:
            use_chain_trade_count = False
        
        # ====================================================================
        # STEP 3: FETCH AND PARSE BLOCKCHAIN TRANSACTIONS
        # ====================================================================
        
        logger.info(f"📡 Fetching blockchain transaction history...")
        
        # Get transaction signatures from the blockchain
        signatures_data = get_wallet_transaction_signatures(wallet_address, limit=100)
        
        if not signatures_data:
            logger.warning(f"⚠️ No transaction history found for {wallet_address[:8]}")
            # Return minimal analysis for wallets with no history
            return {
                'iq': 50,
                'winRate': '0.0',
                'trades': 0,
                'tradesScore': 0,
                'portfolio': 0,
                'pattern': 'Unknown',
                'holdScore': 0,
                'firstBuyTime': None
            }
        
        # Extract just the signature strings for fetching full transactions
        signature_strings = [sig['signature'] for sig in signatures_data]
        
        # Fetch the full transaction data in batch
        rpc_url = CHAINLINK_RPC or HELIUS_RPC
        transactions = fetch_parsed_transactions_batch(signature_strings, rpc_url)
        
        # Parse the transactions to build trading history
        trades_by_token = parse_wallet_trades_from_transactions(
            wallet_address,
            transactions,
            signatures_data
        )
        
        # ====================================================================
        # STEP 4: CALCULATE WIN RATE FROM BLOCKCHAIN IF NEEDED
        # ====================================================================
        
        if calculate_win_rate_from_chain and len(trades_by_token) > 0:
            logger.info(f"📊 Calculating win rate from blockchain data...")
            
            # Look at the last 30 days of closed trades
            thirty_days_ago = time.time() - (30 * 24 * 60 * 60)
            
            closed_trades = []
            for token_mint, trade_data in trades_by_token.items():
                # A closed trade has both buys and sells
                if len(trade_data['buys']) > 0 and len(trade_data['sells']) > 0:
                    # Check if any trades are within the last 30 days
                    all_trades = trade_data['buys'] + trade_data['sells']
                    recent_trades = [t for t in all_trades if t.get('timestamp', 0) >= thirty_days_ago]
                    
                    if recent_trades:
                        closed_trades.append((token_mint, trade_data))
            
            # Calculate P&L for each closed position
            winning_trades = 0
            for token_mint, trade_data in closed_trades[:10]:  # Analyze up to 10 recent positions
                try:
                    recent_buys = [b for b in trade_data['buys'] if b.get('timestamp', 0) >= thirty_days_ago]
                    recent_sells = [s for s in trade_data['sells'] if s.get('timestamp', 0) >= thirty_days_ago]
                    
                    if not recent_buys or not recent_sells:
                        continue
                    
                    total_buy_value = sum(b.get('value', 0) for b in recent_buys)
                    total_sell_value = sum(s.get('value', 0) for s in recent_sells)
                    
                    if total_buy_value > 0:
                        pnl_percent = ((total_sell_value - total_buy_value) / total_buy_value) * 100
                        if pnl_percent > 0:
                            winning_trades += 1
                            
                except Exception as e:
                    logger.warning(f"  ⚠️ Error calculating P&L for token: {e}")
                    continue
            
            if len(closed_trades) > 0:
                calculated_win_rate = (winning_trades / len(closed_trades)) * 100
                win_rate = max(0.0, min(100.0, calculated_win_rate))
                logger.info(f"  ✓ Calculated win rate: {win_rate:.1f}%")
            else:
                win_rate = 0.0
        
        # Default to 0 if we still don't have a win rate
        if win_rate is None:
            win_rate = 0.0
        
        # ====================================================================
        # STEP 5: CALCULATE TRADE COUNT FROM BLOCKCHAIN IF NEEDED
        # ====================================================================
        
        if use_chain_trade_count:
            # Count all buys and sells across all tokens
            total_trades = sum(
                len(trade_data['buys']) + len(trade_data['sells'])
                for trade_data in trades_by_token.values()
            )
            trades_count = total_trades
            logger.info(f"  ✓ Counted {trades_count} trades from blockchain")
        
        # ====================================================================
        # STEP 6: CALCULATE HOLD SCORE
        # ====================================================================
        
        logger.info(f"⏱️ Calculating hold time patterns...")
        
        # Calculate average hold time across all tokens with closed positions
        total_hold_time = 0
        hold_time_samples = 0
        
        for token_mint, trade_data in trades_by_token.items():
            if len(trade_data['buys']) > 0 and len(trade_data['sells']) > 0:
                # Sort by timestamp
                sorted_buys = sorted(trade_data['buys'], key=lambda x: x.get('timestamp', 0))
                sorted_sells = sorted(trade_data['sells'], key=lambda x: x.get('timestamp', 0))
                
                # Calculate time between first buy and first sell
                first_buy_time = sorted_buys[0].get('timestamp', 0)
                first_sell_time = sorted_sells[0].get('timestamp', 0)
                
                if first_buy_time > 0 and first_sell_time > first_buy_time:
                    hold_hours = (first_sell_time - first_buy_time) / 3600
                    total_hold_time += hold_hours
                    hold_time_samples += 1
        
        # Calculate average hold time in hours
        avg_hold_hours = total_hold_time / hold_time_samples if hold_time_samples > 0 else 0
        
        # Convert hold time to hold score (0-50 points)
        if avg_hold_hours < 8:
            base_hold_score = 0  # Day trader / very short term
        elif avg_hold_hours <= 24:
            base_hold_score = 10  # Holds for a day
        elif avg_hold_hours <= 72:
            base_hold_score = 20  # Holds for a few days
        elif avg_hold_hours <= 150:
            base_hold_score = 30  # Holds for about a week
        else:
            base_hold_score = 50  # Long-term holder (week+)
        
        logger.info(f"  ✓ Average hold time: {avg_hold_hours:.1f} hours → score: {base_hold_score}")
        
        # ====================================================================
        # STEP 7: CALCULATE TRADE FREQUENCY SCORE
        # ====================================================================
        
        # Score based on trading frequency (fewer trades = more patient = higher score)
        if trades_count <= 2:
            trades_score = 100
        elif trades_count <= 6:
            trades_score = 85
        elif trades_count <= 15:
            trades_score = 60
        elif trades_count <= 30:
            trades_score = 35
        elif trades_count <= 100:
            trades_score = 10
        else:
            trades_score = 0
        
        logger.info(f"  ✓ Trade count: {trades_count} → score: {trades_score}")
        
        # ====================================================================
        # STEP 8: CALCULATE HOLDING POSITION SCORE
        # ====================================================================
        
        # Normalize holding percentage to 0-100 score
        # Holding a significant portion (up to 30%) of a token is positive
        normalized_holdings = min(100.0, max(0.0, (holding_percent / 30.0) * 100))
        
        # ====================================================================
        # STEP 9: CALCULATE FINAL IQ SCORE
        # ====================================================================
        
        # Weighted formula:
        # - 70% weight on hold behavior (patience and long-term thinking)
        # - 10% weight on win rate (profitability)
        # - 10% weight on trade frequency (avoiding overtrading)
        # - 10% weight on position size (conviction in holdings)
        
        final_iq = (
            (base_hold_score / 50.0 * 100) * 0.70 +  # Normalize hold score to 0-100, then apply 70% weight
            win_rate * 0.10 +
            trades_score * 0.10 +
            normalized_holdings * 0.10
        )
        
        final_iq = int(max(0, min(100, final_iq)))  # Clamp to 0-100 range
        
        # ====================================================================
        # STEP 10: CLASSIFY TRADING PATTERN
        # ====================================================================
        
        if win_rate > 70:
            pattern = 'Aggressive Winner'
        elif win_rate > 50:
            pattern = 'Calculated Trader'
        else:
            pattern = 'Degen Gambler'
        
        # ====================================================================
        # STEP 11: FIND FIRST BUY TIME FOR CURRENT TOKEN
        # ====================================================================
        
        first_buy_time = None
        if current_token_address and current_token_address in trades_by_token:
            token_trades = trades_by_token[current_token_address]
            if len(token_trades['buys']) > 0:
                sorted_buys = sorted(token_trades['buys'], key=lambda x: x.get('timestamp', 0))
                first_buy_time = sorted_buys[0].get('timestamp')
        
        # ====================================================================
        # STEP 12: BUILD FINAL RESULT
        # ====================================================================
        
        result = {
            'iq': final_iq,
            'winRate': f"{win_rate:.1f}",
            'trades': trades_count,
            'tradesScore': trades_score,
            'portfolio': 0,  # Could be enhanced with portfolio value calculation
            'pattern': pattern,
            'holdScore': base_hold_score,
            'firstBuyTime': first_buy_time
        }
        
        # Cache the result for 24 hours
        wallet_analysis_cache[cache_key] = {
            'data': result,
            'timestamp': time.time()
        }
        
        logger.info(f"✅ Analysis complete for {wallet_address[:8]}: IQ={final_iq}, WinRate={win_rate:.1f}%, Pattern={pattern}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error analyzing wallet {wallet_address[:8]}: {e}")
        # Return safe defaults on error
        return {
            'iq': 50,
            'winRate': '0.0',
            'trades': 0,
            'tradesScore': 0,
            'portfolio': 0,
            'pattern': 'Unknown',
            'holdScore': 0,
            'firstBuyTime': None
    }


def get_sol_price_usd() -> float:
    """
    Fetches the current price of SOL in USD from CoinGecko.
    
    We need this to convert our dollar probe amounts into SOL amounts
    that Jupiter's API can understand. Returns a sensible fallback
    if the API call fails so our system doesn't break completely.
    
    Returns:
        float: Current SOL price in USD
    """
    try:
        response = requests.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        price = data['solana']['usd']
        logger.info(f"Current SOL price: ${price}")
        return price
    except Exception as e:
        logger.error(f"Error fetching SOL price: {e}")
        logger.warning("Using fallback SOL price of $150")
        return 150.0
    
def get_token_liquidity_simple(token_address: str) -> Dict:
    """Fetches liquidity AND Volume from Birdeye with simple fallback."""
    logger.info(f"Fetching liquidity and volume data for {token_address[:8]}...")

    try:
        url = "https://public-api.birdeye.so/defi/token_overview"
        params = {'address': token_address}
        headers = {'X-API-KEY': BIRDEYE_API_KEY}

        response = requests.get(url, params=params, headers=headers, timeout=8)

        if response.status_code == 200:
            data = response.json()

            if data.get('success'):
                token_data = data.get('data', {})
                liquidity = token_data.get('liquidity')
                market_cap = token_data.get('mc')
                volume_24h = token_data.get('v24hUSD')  # <--- NEW: Grab Volume

                if liquidity and liquidity > 0:
                    logger.info(f"✓ Birdeye liquidity: ${liquidity:,.0f}")
                    if volume_24h:
                        logger.info(f"✓ Birdeye 24h Vol: ${volume_24h:,.0f}")

                    return {
                        'liquidity_usd': float(liquidity),
                        'market_cap_usd': float(market_cap) if market_cap else None,
                        'volume_24h_usd': float(volume_24h) if volume_24h else 0.0, # <--- Return Volume
                        'source': 'birdeye'
                    }

    except Exception as e:
        logger.warning(f"Birdeye API error: {e}")

    # Fallback (Volume is 0 for estimated data)
    market_cap = get_token_market_cap(token_address)
    if market_cap:
        estimated_liquidity = estimate_liquidity_from_market_cap(market_cap)
        return {
            'liquidity_usd': estimated_liquidity,
            'market_cap_usd': market_cap,
            'volume_24h_usd': 0.0,
            'source': 'estimated'
        }

    return {
        'liquidity_usd': 25000,
        'market_cap_usd': None,
        'volume_24h_usd': 0.0,
        'source': 'default'
    }


def get_token_market_cap(token_address: str) -> Optional[float]:
    """Fetches market cap from Birdeye."""
    try:
        if not BIRDEYE_API_KEY:
            return None
            
        url = "https://public-api.birdeye.so/defi/token_overview"
        params = {'address': token_address}
        headers = {'X-API-KEY': BIRDEYE_API_KEY}
        
        response = requests.get(url, params=params, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                mc = data.get('data', {}).get('mc')
                if mc:
                    return float(mc)
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching market cap: {e}")
        return None


def estimate_liquidity_from_market_cap(market_cap: float) -> float:
    """Estimates liquidity from market cap using empirical data."""
    mapping = [
        (30000, 90000, 20000, 29000),
        (90001, 150000, 30000, 39000),
        (150001, 300000, 30000, 59000),
        (300001, 500000, 60000, 75000),
        (500001, 1000000, 76000, 150000),
        (1000001, 5000000, 151000, 550000),
        (5000001, 10000000, 300000, 950000),
        (10000001, float('inf'), 950000, 3000000)
    ]
    
    for mc_min, mc_max, liq_min, liq_max in mapping:
        if mc_min <= market_cap <= mc_max:
            return (liq_min + liq_max) / 2
    
    if market_cap < 30000:
        return 15000
    else:
        return 2000000


def classify_liquidity_tier(liquidity_usd: float) -> str:
    """Classifies liquidity into tiers."""
    if liquidity_usd < 40000:
        return 'MICRO'
    elif liquidity_usd < 150000:
        return 'SMALL'
    elif liquidity_usd < 550000:
        return 'MEDIUM'
    else:
        return 'LARGE'


def get_probe_sizes_for_liquidity(liquidity_usd: float, sol_price_usd: float) -> List[Dict]:
    """Determines probe sizes based on your liquidity tier framework."""
    
    logger.info(f"Classifying liquidity ${liquidity_usd:,.0f}...")
    
    if liquidity_usd < 30000:
        tier = 'MICRO'
        adaptive_sizes = [50, 100, 250, 500]
        stress_size = 750
        logger.info(f"  → MICRO tier")
        
    elif liquidity_usd < 150000:
        tier = 'SMALL'
        adaptive_sizes = [100, 250, 500, 1000]
        stress_size = 2000
        logger.info(f"  → SMALL tier")
        
    elif liquidity_usd < 1000000:
        tier = 'MEDIUM'
        adaptive_sizes = [1000, 2500, 5000, 10000]
        stress_size = 20000
        logger.info(f"  → MEDIUM tier")
        
    else:
        tier = 'LARGE'
        adaptive_sizes = [2000, 5000, 10000, 20000]
        stress_size = 40000
        logger.info(f"  → LARGE tier")
    
    probe_configs = []
    
    for usd_amount in adaptive_sizes:
        sol_amount = usd_amount / sol_price_usd
        lamports = int(sol_amount * 1_000_000_000)
        
        probe_configs.append({
            'lamports': lamports,
            'usd_amount': usd_amount,
            'probe_type': 'adaptive',
            'percentage_of_pool': (usd_amount / liquidity_usd) * 100
        })
    
    stress_sol = stress_size / sol_price_usd
    stress_lamports = int(stress_sol * 1_000_000_000)
    
    probe_configs.append({
        'lamports': stress_lamports,
        'usd_amount': stress_size,
        'probe_type': 'stress',
        'percentage_of_pool': (stress_size / liquidity_usd) * 100
    })
    
    logger.info(f"  → Probes: {[f'${p['usd_amount']}' for p in probe_configs]}")
    
    return probe_configs


def probe_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    direction: str
) -> Optional[Dict]:
    """
    Makes a single probe request to Jupiter's Ultra API.
    Now uses the new Ultra API with authentication.
    """
    try:

# Get API key from environment variable
        api_key = os.environ.get('JUPITER_API_KEY')
        print(f"DEBUG: API key loaded: {api_key[:10]}..." if api_key else "DEBUG: NO API KEY FOUND")  # ADD THIS LINE
        if not api_key:
            logger.error("JUPITER_API_KEY not found in environment variables")
            return None
        
        # Build the Jupiter Ultra API URL
        url = (
            f"https://api.jup.ag/ultra/v1/order?"
            f"inputMint={input_mint}&"
            f"outputMint={output_mint}&"
            f"amount={amount_lamports}&"
            f"slippageBps=50"
        )
        
        # Add this debug line
        print(f"DEBUG: Calling Jupiter Ultra API with URL: {url}")
        
        # Make the request with API key in headers
        headers = {
            'x-api-key': api_key,
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        # Add debug output
        print(f"DEBUG: Jupiter response status: {response.status_code}")
        print(f"DEBUG: Jupiter response body: {response.text[:500]}")
        
        response.raise_for_status()
        data = response.json()
        
        # Check if Jupiter returned an error
        if 'error' in data or not data:
            logger.error(f"Jupiter API error for {direction}: {data.get('error', 'Unknown')}")
            return None
        
        # Extract the input and output amounts from Jupiter's response
        in_amount = float(data['inAmount'])
        out_amount = float(data['outAmount'])
        
        # Calculate the execution price (rate of exchange)
        execution_price = out_amount / in_amount
        
        # Get the price impact that Jupiter calculated
        price_impact_pct = abs(float(data.get('priceImpactPct', 0)))
        
        return {
            'success': True,
            'execution_price': execution_price,
            'price_impact_pct': price_impact_pct,
            'in_amount': in_amount,
            'out_amount': out_amount,
            'direction': direction
        }
        
    except requests.exceptions.Timeout:
        logger.error(f"Jupiter API timeout for {direction} probe")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Jupiter API request failed for {direction}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in {direction} probe: {e}")
        return None


def probe_slippage_curve(token_address: str) -> Dict:
    """Probes Jupiter with adaptive sizing based on actual pool liquidity."""
    logger.info(f"=" * 70)
    logger.info(f"Starting adaptive probe for {token_address[:8]}...")
    logger.info(f"=" * 70)

    sol_price_usd = get_sol_price_usd()

    # Get liquidity
    liquidity_data = get_token_liquidity_simple(token_address)
    
    liquidity_usd = liquidity_data['liquidity_usd']
    market_cap_usd = liquidity_data.get('market_cap_usd')
    source = liquidity_data['source']
    
    logger.info(f"Liquidity: ${liquidity_usd:,.0f} (source: {source})")
    if market_cap_usd:
        logger.info(f"Market cap: ${market_cap_usd:,.0f}")
    
    tier = classify_liquidity_tier(liquidity_usd)
    
    # Get probe sizes
    probe_configs = get_probe_sizes_for_liquidity(liquidity_usd, sol_price_usd)

    # Baseline
    logger.info("Establishing baseline...")
    baseline_probe = probe_jupiter_quote(SOL_MINT, token_address, 1_000_000, 'baseline')

    if not baseline_probe or not baseline_probe['success']:
        raise Exception("Could not establish baseline")

    baseline_price = baseline_probe['execution_price']
    logger.info(f"✓ Baseline: {baseline_price:.10f}")

    # Execute probes
    logger.info(f"Executing {len(probe_configs)} probes...")
    logger.info("-" * 70)
    
    paired_probes = []
    stress_test_result = None

    for idx, config in enumerate(probe_configs):
        lamports = config['lamports']
        probe_usd = config['usd_amount']
        probe_type = config['probe_type']
        pool_pct = config['percentage_of_pool']
        
        indicator = "🔥 STRESS" if probe_type == 'stress' else "📊 PROBE"
        logger.info(f"{indicator} {idx + 1}: ${probe_usd:.0f} ({pool_pct:.1f}%)")

        # BUY
        buy_probe = probe_jupiter_quote(SOL_MINT, token_address, lamports, 'buy')
        if not buy_probe or not buy_probe['success']:
            logger.warning(f"  ✗ BUY failed")
            time.sleep(0.3)
            continue

        buy_slippage = abs(((buy_probe['execution_price'] - baseline_price) / baseline_price) * 100)
        logger.info(f"  ✓ BUY: {buy_slippage:.2f}%")
        time.sleep(0.3)

        # SELL
        token_amount = int((lamports / 1_000_000_000) * baseline_price * 1_000_000_000)
        sell_probe = probe_jupiter_quote(token_address, SOL_MINT, token_amount, 'sell')
        
        if not sell_probe or not sell_probe['success']:
            logger.warning(f"  ✗ SELL failed")
            time.sleep(0.3)
            continue

        expected_price = 1 / baseline_price
        sell_slippage = abs(((sell_probe['execution_price'] - expected_price) / expected_price) * 100)
        logger.info(f"  ✓ SELL: {sell_slippage:.2f}%")
        
        ratio = sell_slippage / (buy_slippage + 0.001)
        logger.info(f"  → Ratio: {ratio:.2f}x")

        result = {
            'size_usd': probe_usd,
            'probe_type': probe_type,
            'percentage_of_pool': pool_pct,
            'buy': {
                'execution_price': buy_probe['execution_price'],
                'slippage_pct': buy_slippage,
                'price_impact_pct': buy_probe['price_impact_pct']
            },
            'sell': {
                'execution_price': sell_probe['execution_price'],
                'slippage_pct': sell_slippage,
                'price_impact_pct': sell_probe['price_impact_pct']
            }
        }

        if probe_type == 'stress':
            stress_test_result = result
        
        paired_probes.append(result)
        logger.info("-" * 70)
        time.sleep(0.3)

    if len(paired_probes) < 3:
        raise Exception(f"Insufficient data: {len(paired_probes)} probes")

    logger.info(f"✅ Complete: {len(paired_probes)} measurements")
    logger.info(f"=" * 70)

    return {
        'baseline_price': baseline_price,
        'paired_probes': paired_probes,
        'stress_test': stress_test_result,
        'tier': tier,
        'liquidity_usd': liquidity_usd,
        'market_cap_usd': market_cap_usd,
        'liquidity_source': source,
        'timestamp': int(time.time())
    }


def analyze_slippage_patterns(slippage_data: Dict, token_address: str) -> Dict:
    """
    MERGED LOGIC:
    1. Detects Complex Manipulation (Fortress, Cliff, Honeypot)
    2. Detects 'Silent Rugs' (Liquidity Decay over time)
    """
    logger.info("Analyzing slippage patterns for manipulation signals...")

    paired_probes = slippage_data['paired_probes']

    if not paired_probes:
        return {
            'state': 'ERROR', 
            'confidence': 0, 
            'patterns': [], 
            'scores': {'pre_pump_score': 0, 'pre_dump_score': 0},
            'asymmetry': {}
        }

    analysis = {
        'asymmetry': {},
        'patterns': [],
        'scores': {
            'pre_pump_score': 0,
            'pre_dump_score': 0
        },
        'confidence': 50,
        'timestamp': slippage_data['timestamp']
    }

    # --- 1. CALCULATE ASYMMETRY ---
    asymmetry_ratios = []
    for probe in paired_probes:
        buy_slip = probe['buy']['slippage_pct']
        sell_slip = probe['sell']['slippage_pct']
        ratio = sell_slip / (buy_slip + 0.001)

        asymmetry_ratios.append({
            'size_usd': probe['size_usd'],
            'ratio': ratio,
            'buy_slippage': buy_slip,
            'sell_slippage': sell_slip
        })

    avg_asymmetry = sum(item['ratio'] for item in asymmetry_ratios) / len(asymmetry_ratios)

    analysis['asymmetry'] = {
        'average_ratio': avg_asymmetry,
        'ratios_by_size': asymmetry_ratios
    }

    # Get Deepest Probe (Stress Test)
    deep_probe = paired_probes[-1]
    deep_buy_slip = deep_probe['buy']['slippage_pct']
    deep_sell_slip = deep_probe['sell']['slippage_pct']
    deep_ratio = deep_sell_slip / (deep_buy_slip + 0.001)

    # --- 2. PATTERN DETECTION (SNAPSHOT) ---

    # Supply Shock (Bullish)
    if deep_ratio < 0.5 and deep_sell_slip < 1.0:
        analysis['patterns'].append({
            'type': 'SUPPLY_SHOCK',
            'severity': 'HIGH',
            'description': f'Thin asks (buy: {deep_buy_slip:.1f}%) + Strong bids. Ratio: {deep_ratio:.2f}'
        })
        analysis['scores']['pre_pump_score'] += 60
        analysis['confidence'] += 20

    # Accumulation (Bullish)
    elif deep_buy_slip < 1.0 and deep_sell_slip < 1.0 and avg_asymmetry < 1.2:
        analysis['patterns'].append({
            'type': 'WHALE_ACCUMULATION',
            'severity': 'MEDIUM',
            'description': 'Deep liquidity on both sides. Smart money zone.'
        })
        analysis['scores']['pre_pump_score'] += 20

    # Liquidity Trap (Bearish)
    if deep_ratio > 2.0:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_TRAP',
            'severity': 'CRITICAL',
            'description': f'Sell Wall! Ratio: {deep_ratio:.2f}'
        })
        analysis['scores']['pre_dump_score'] += 60
        analysis['confidence'] += 25

    # --- 3. HONEYPOT / TOXIC CHECKS ---
    tier = slippage_data.get('tier', 'SMALL')
    honeypot_thresholds = {'MICRO': 40.0, 'SMALL': 25.0, 'MEDIUM': 15.0, 'LARGE': 10.0}
    threshold = honeypot_thresholds.get(tier, 25.0)

    stress_test = slippage_data.get('stress_test')
    if stress_test:
        stress_sell = stress_test['sell']['slippage_pct']
        if stress_sell > threshold:
            analysis['patterns'].append({
                'type': 'HONEYPOT_DETECTED',
                'severity': 'CRITICAL',
                'description': f'Stress test sell slippage {stress_sell:.1f}% > {threshold}%'
            })
            analysis['scores']['pre_dump_score'] += 100
            analysis['is_honeypot'] = True

    if deep_sell_slip > threshold * 1.5:
        analysis['patterns'].append({'type': 'TOXIC_LIQUIDITY', 'severity': 'CRITICAL', 'description': 'Toxic slippage levels.'})
        analysis['scores']['pre_dump_score'] += 100
        analysis['is_honeypot'] = True

    # --- 4. TEMPORAL ANALYSIS (HISTORY) ---
    if token_address in historical_slippage and len(historical_slippage[token_address]) > 0:
        history = historical_slippage[token_address]
        # Look back 2-3 cycles (~5 mins)
        lookback_index = min(len(history), 3)
        prev_analysis = history[-lookback_index]

        # A. CHECK FOR COMPLEX MANIPULATION (The Original Logic)
        if 'asymmetry' in prev_analysis:
            prev_asymmetry = prev_analysis['asymmetry'].get('average_ratio', 0)
            asymmetry_change = avg_asymmetry - prev_asymmetry

            if asymmetry_change > 0.3:
                analysis['patterns'].append({'type': 'ACTIVE_FORTRESS_BUILDING', 'severity': 'CRITICAL', 'description': 'Asymmetry rising rapidly.'})
                analysis['scores']['pre_pump_score'] += 25
                analysis['manipulation_in_progress'] = True
            elif asymmetry_change < -0.3:
                analysis['patterns'].append({'type': 'ACTIVE_CLIFF_CARVING', 'severity': 'CRITICAL', 'description': 'Liquidity being pulled.'})
                analysis['scores']['pre_dump_score'] += 35
                analysis['manipulation_in_progress'] = True

        # B. CHECK FOR SILENT RUG / LIQUIDITY DECAY (The New Logic)
        # We need to extract the previous DEEP SELL SLIPPAGE
        prev_deep_sell = 0
        if 'asymmetry' in prev_analysis and 'ratios_by_size' in prev_analysis['asymmetry']:
            prev_ratios = prev_analysis['asymmetry']['ratios_by_size']
            if prev_ratios:
                # Assuming the last item is the deepest probe
                prev_deep_sell = prev_ratios[-1]['sell_slippage']

        if prev_deep_sell > 0:
            slip_increase_pct = ((deep_sell_slip - prev_deep_sell) / prev_deep_sell) * 100
            
            # If sell slippage got 20% worse in 5 minutes -> DANGER
            if slip_increase_pct > 20.0 and deep_sell_slip > 2.0:
                analysis['patterns'].append({
                    'type': 'LIQUIDITY_DECAY',
                    'severity': 'CRITICAL',
                    'description': f'SILENT RUG: Support crumbling! Slippage up {slip_increase_pct:.0f}%'
                })
                analysis['scores']['pre_dump_score'] += 75
                analysis['confidence'] += 30
                logger.info(f"🚨 LIQUIDITY DECAY DETECTED: +{slip_increase_pct:.1f}%")

    # --- 5. SAVE HISTORY ---
    if token_address not in historical_slippage:
        historical_slippage[token_address] = []

    # We save the FULL analysis object so we can check it next time
    historical_slippage[token_address].append(analysis)

    if len(historical_slippage[token_address]) > MAX_HISTORICAL_MEASUREMENTS:
        historical_slippage[token_address].pop(0)

    # Cap confidence
    analysis['confidence'] = min(analysis['confidence'], 95)

    return analysis


def classify_market_state(analysis: Dict) -> Dict:
    """
    Takes the raw analysis scores and classifies the market into a final state.
    
    Simplified classification logic that prioritizes dump signals and uses
    lower thresholds for actionable pump signals.
    
    Args:
        analysis: The complete analysis from analyze_slippage_patterns()
    
    Returns:
        Dict with final classification, confidence, and recommended action
    """
    pump_score = analysis['scores']['pre_pump_score']
    dump_score = analysis['scores']['pre_dump_score']
    confidence = analysis['confidence']
    patterns = analysis['patterns']
    is_honeypot = analysis.get('is_honeypot', False)
    
    result = {
        'state': 'UNCERTAIN',
        'severity': 'LOW',
        'timeframe': None,
        'confidence': confidence,
        'action': '❓ WAIT - Monitor for clearer signals',
        'signals': [],
        'scores': analysis['scores']
    }
    
    # Add pattern descriptions to signals
    for pattern in patterns:
        result['signals'].append(f"{pattern['type']}: {pattern['description']}")
    
    urgent = analysis.get('manipulation_in_progress', False)
    
    # PRIORITY 1: Dump signals (toxic liquidity, honeypots, rug pulls)
    # These take absolute priority to protect users
    # PRIORITY 1.1: Honeypot (absolute priority)
    if is_honeypot:
        result['state'] = 'PRE_DUMP_HONEYPOT'
        result['severity'] = 'CRITICAL'
        result['timeframe'] = 'IMMEDIATE'
        result['action'] = '🛑 HONEYPOT - CANNOT SELL'
        logger.info("🎯 HONEYPOT DETECTED")
        return result
    
    # PRIORITY 1.2: Other dump signals
    if dump_score >= 80:
        result['state'] = 'PRE_DUMP'
        result['severity'] = 'CRITICAL'
        result['timeframe'] = '2-15 minutes' if urgent else '5-30 minutes'
        result['action'] = '🛑 DO NOT BUY / EXIT IMMEDIATELY'
        logger.info("🎯 Final classification: CRITICAL PRE-DUMP")
    
    elif dump_score >= 50:
        result['state'] = 'PRE_DUMP'
        result['severity'] = 'HIGH'
        result['timeframe'] = '10-30 minutes'
        result['action'] = '⚠️ SELL SIGNAL - Exit or avoid'
        logger.info("🎯 Final classification: HIGH PRE-DUMP")
    
    # PRIORITY 2: Pump signals (only if dump score is low)
    elif pump_score >= 60 and dump_score < 30:
        result['state'] = 'PRE_PUMP'
        result['severity'] = 'HIGH'
        result['timeframe'] = '5-30 minutes' if urgent else '10-45 minutes'
        result['action'] = '🚀 BUY SIGNAL - Supply shock detected'
        logger.info("🎯 Final classification: HIGH PRE-PUMP")
    
    elif pump_score >= 40 and dump_score < 30:
        result['state'] = 'PRE_PUMP'
        result['severity'] = 'MEDIUM'
        result['timeframe'] = '20-60 minutes'
        result['action'] = '👀 WATCH - Potential accumulation'
        logger.info("🎯 Final classification: MEDIUM PRE-PUMP")
    
    # PRIORITY 3: Stable/balanced liquidity
    elif abs(pump_score - dump_score) < 20 and pump_score < 40 and dump_score < 40:
        result['state'] = 'HOLDING'
        result['severity'] = 'STABLE'
        result['timeframe'] = 'Current'
        result['action'] = '⏸️ HOLD - Balanced liquidity'
        logger.info("🎯 Final classification: HOLDING STATE")
    
    # PRIORITY 4: Mixed/unclear signals
    else:
        result['state'] = 'UNCERTAIN'
        result['severity'] = 'MIXED'
        result['timeframe'] = None
        result['action'] = '❓ WAIT - Mixed signals, need more data'
        result['confidence'] = max(30, confidence - 20)
        logger.info("🎯 Final classification: UNCERTAIN (mixed signals)")
    
    return result


def analyze_velocity(liquidity_usd: float, volume_24h_usd: float) -> Dict:
    """
    Analyzes the 'Velocity' of the token to detect Zombie vs Active tokens.
    Velocity Ratio = 24h Volume / Liquidity
    """
    if not liquidity_usd or liquidity_usd == 0:
        return {'ratio': 0, 'status': 'UNKNOWN', 'description': 'No liquidity data'}
    
    # Calculate turnover
    ratio = volume_24h_usd / liquidity_usd
    ratio_percent = ratio * 100

    result = {
        'ratio': ratio,
        'ratio_percent': ratio_percent,
        'volume_usd': volume_24h_usd,
        'liquidity_usd': liquidity_usd
    }

    # === VELOCITY RULES (Optimized for $200k-$1M MC) ===
    
    # 1. ZOMBIE (< 1.5%): Dead. Do not buy.
    if ratio < 0.015: 
        result['status'] = 'ZOMBIE'
        result['health_score'] = 0
        result['description'] = f"💀 ZOMBIE: Vol is {ratio_percent:.2f}% of Liq. Dead."
        
    # 2. LOW ACTIVITY (1.5% - 15%): Sleeping. Safe but slow.
    elif ratio < 0.15:
        result['status'] = 'LOW_ACTIVITY'
        result['health_score'] = 50
        result['description'] = f"💤 SLEEPING: Vol is {ratio_percent:.2f}% of Liq."

    # 3. HEALTHY (15% - 200%): The Sweet Spot for Entry.
    elif ratio <= 2.0:
        result['status'] = 'HEALTHY'
        result['health_score'] = 100
        result['description'] = f"✅ HEALTHY: Vol is {ratio_percent:.0f}% of Liq. Good Activity."

    # 4. FRENZY (> 200%): Dangerous volatility.
    else:
        result['status'] = 'FRENZY'
        result['health_score'] = 70 
        result['description'] = f"🔥 FRENZY: Vol is {ratio_percent:.0f}% of Liq. Extreme Risk."

    logger.info(f"Velocity Check: {result['description']}")
    return result


# ============================================================================
# WEBSOCKET BACKGROUND THREAD SYSTEM
# ============================================================================

def run_websocket_loop(api_key: str):
    """
    This function runs in a separate background thread.
    It handles all the async WebSocket operations.
    
    Think of this as a separate worker that runs alongside your Flask server,
    constantly listening for trades.
    """
    global websocket_client, metrics_manager, websocket_loop

    try:
        logger.info("🚀 Starting WebSocket background thread...")

        # Create a new event loop for this thread
        # (Each thread needs its own event loop for async operations)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        websocket_loop = loop

        # Create the WebSocket client
        websocket_client = HeliusWebSocketClient(api_key)

        # Create the metrics manager
        metrics_manager = MetricsManager()

        # Connect the two: when WebSocket gets trade data, send it to metrics manager
        websocket_client.add_trade_callback(metrics_manager.handle_trade)

        # Define an async function to connect and start listening
        async def connect_and_listen():
            connected = await websocket_client.connect()
            if connected:
                logger.info("✅ WebSocket connected, now listening for trades...")
                # This will run forever until an error occurs
                await websocket_client.listen()
            else:
                logger.error("❌ Failed to connect WebSocket")

        # CRITICAL FIX: Keep the loop running forever
        # Don't let it close after the first connection attempt
        try:
            loop.run_until_complete(connect_and_listen())
        except KeyboardInterrupt:
            logger.info("🛑 WebSocket thread stopping due to keyboard interrupt")
        except Exception as e:
            logger.error(f"❌ Error in WebSocket connection: {e}")
        
        # Keep the loop alive to handle scheduled coroutines
        # This is crucial - the loop needs to stay open for subscribe_to_pool calls
        logger.info("🔄 WebSocket listener ended, but keeping event loop alive...")
        
        # Run the loop forever to handle any future coroutines scheduled from Flask
        loop.run_forever()

    except Exception as e:
        logger.error(f"❌ Critical error in WebSocket thread: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Only close when the entire thread is shutting down
        logger.info("🔌 Closing WebSocket event loop")
        if websocket_loop:
            websocket_loop.close()


def start_websocket_background():
    """
    Start the WebSocket system in a background thread.
    
    This gets called when your Flask app starts up.
    """
    global websocket_thread
    
    # Get Helius API key from environment
    helius_api_key = os.environ.get('HELIUS_API_KEY')
    
    if not helius_api_key:
        logger.warning("⚠️ HELIUS_API_KEY not found - real-time monitoring will not work")
        logger.warning("⚠️ Set it in your environment variables to enable real-time features")
        return
    
    logger.info("🔧 Starting WebSocket background thread...")
    
    # Create and start the thread
    websocket_thread = threading.Thread(
        target=run_websocket_loop,
        args=(helius_api_key,),
        daemon=True  # Thread will close when main program exits
    )
    websocket_thread.start()
    
    logger.info("✅ WebSocket thread started successfully")
    
    # Give it a moment to initialize
    time.sleep(2)


def subscribe_to_pool_from_sync(pool_address: str, token_address: str):
    """
    Subscribe to a pool from synchronous Flask code.
    
    This bridges the gap between sync Flask and async WebSocket code.
    """
    global websocket_client, websocket_loop
    
    if not websocket_client or not websocket_loop:
        logger.warning("⚠️ Cannot subscribe - WebSocket not initialized")
        return False
    
    try:
        # Create the async subscription coroutine
        async def do_subscribe():
            await websocket_client.subscribe_to_pool(
                pool_address,
                token_address,
                token_address[:8]  # Use shortened address as symbol
            )
        
        # Schedule it to run in the WebSocket's event loop
        future = asyncio.run_coroutine_threadsafe(do_subscribe(), websocket_loop)
        
        # Wait up to 5 seconds for it to complete
        future.result(timeout=5)
        
        logger.info(f"✅ Subscribed WebSocket to pool {pool_address[:8]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error subscribing to pool: {e}")
        return False
        

def get_raydium_pool_address(token_address: str) -> Optional[str]:
    """
    Find the pool address for a token using Birdeye, with detailed debugging.
    """
    try:
        if not BIRDEYE_API_KEY:
            logger.warning("⚠️ BIRDEYE_API_KEY not set, can't find pool address")
            return None
        
        # Use Birdeye's market list endpoint
        url = f"https://public-api.birdeye.so/defi/v2/markets"
        params = {'address': token_address}
        headers = {'X-API-KEY': BIRDEYE_API_KEY}
        
        logger.info(f"🔍 Looking for pool for token {token_address[:8]}...")
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        # Debug: Log the response status
        logger.info(f"  Birdeye response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Debug: Log the raw response structure
            logger.info(f"  Response success: {data.get('success')}")
            logger.info(f"  Response has data: {bool(data.get('data'))}")
            
            if data.get('success') and data.get('data'):
                markets = data['data'].get('items', [])
                
                logger.info(f"  Found {len(markets)} markets total")
                
                if not markets:
                    logger.warning(f"⚠️ No markets found for {token_address[:8]}")
                    return None
                
                # Debug: Log ALL markets we found
                for i, m in enumerate(markets):
                    logger.info(f"    Market {i+1}:")
                    logger.info(f"      - Source: {m.get('source')}")
                    logger.info(f"      - Address: {m.get('address', 'N/A')[:16]}...")
                    logger.info(f"      - Liquidity: ${m.get('liquidity', 0):,.0f}")
                    logger.info(f"      - Type: {m.get('type', 'N/A')}")
                
                # Try to find ANY pool - be very flexible
                # Just take the one with the most liquidity
                if markets:
                    # Sort by liquidity
                    markets.sort(key=lambda x: x.get('liquidity', 0), reverse=True)
                    
                    # Take the pool with highest liquidity
                    best_pool = markets[0]
                    pool_addr = best_pool.get('address')
                    
                    if pool_addr:
                        source = best_pool.get('source', 'Unknown')
                        liq = best_pool.get('liquidity', 0)
                        logger.info(f"✅ Selected highest liquidity pool:")
                        logger.info(f"   Address: {pool_addr[:16]}...")
                        logger.info(f"   Source: {source}")
                        logger.info(f"   Liquidity: ${liq:,.0f}")
                        return pool_addr
                    else:
                        logger.warning(f"⚠️ Market found but no address field")
        
        else:
            logger.error(f"❌ Birdeye API error: Status {response.status_code}")
            logger.error(f"   Response: {response.text[:200]}")
        
        logger.warning(f"⚠️ Could not find any pool for {token_address[:8]}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error finding pool: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        return None
        


def start_tracking_token_realtime(token_address: str, pool_address: str, liquidity_usd: float):
    """
    Start tracking a token in real-time.
    
    This tells the WebSocket to watch this token's pool and tells the
    metrics manager to start calculating metrics for it.
    """
    global websocket_client, metrics_manager
    
    if not websocket_client or not metrics_manager:
        logger.warning("⚠️ Real-time system not initialized - can't start tracking")
        return False
    
    try:
        # Add to metrics manager
        metrics_manager.add_token(token_address, liquidity_usd)

        # Store the mapping
        token_to_pool_map[token_address] = pool_address

        # Actually subscribe the WebSocket to this pool
        subscription_success = subscribe_to_pool_from_sync(pool_address, token_address)
        
        if not subscription_success:
            logger.warning("⚠️ WebSocket subscription failed - real-time data may not work")

        logger.info(f"✅ Started real-time tracking for {token_address[:8]}...")
        logger.info(f"   Pool: {pool_address[:8]}...")
        logger.info(f"   Liquidity: ${liquidity_usd:,.0f}")

        return True

    except Exception as e:
        logger.error(f"❌ Error starting tracking: {e}")
        return False

@app.route('/')
def home():
    """
    Simple health check endpoint to verify the server is running.
    When you visit the root URL of your Replit, you'll see this message.
    """
    return jsonify({
        'status': 'online',
        'service': 'Solana Token Analysis API',
        'version': '1.0.0',
        'endpoints': {
            '/analyze': 'POST with token_address to get pump/dump analysis',
            '/health': 'GET to check server health'
        }
    })


@app.route('/health')
def health():
    """
    Detailed health check that shows system status and cache statistics.
    Useful for monitoring and debugging.
    """
    return jsonify({
        'status': 'healthy',
        'timestamp': int(time.time()),
        'cache_size': len(analysis_cache),
        'tokens_tracked': len(historical_slippage),
        'uptime': 'running'
    })


@app.route('/analyze', methods=['POST'])
def analyze_token():
    """
    Main analysis endpoint.
    Orchestrates: Data Fetch -> Velocity Check -> Slippage Probe -> Pattern Analysis -> Classification
    """
    try:
        # --- 1. INPUT VALIDATION ---
        # DEBUG: Log raw request data
        print("=" * 50)
        print("DEBUG: Request received")
        print(f"Content-Type: {request.headers.get('Content-Type')}")
        
        data = request.get_json()
        print(f"Parsed JSON: {data}")

        if not data or 'token_address' not in data:
            logger.warning("Request missing token_address field")
            return jsonify({
                'error': 'Missing required field: token_address',
                'status': 'error'
            }), 400

        token_address = data['token_address']
        # NEW CODE STARTS HERE
        # Extract the access code from the request
        # If no access code is provided, we use 'anonymous' as a default
        access_code = data.get('access_code', 'anonymous')
        # NEW CODE ENDS HERE

        if not token_address or len(token_address) < 32:
            logger.warning(f"Invalid token address format: {token_address}")
            return jsonify({
                'error': 'Invalid token address format',
                'status': 'error'
            }), 400

        # NEW CODE STARTS HERE - Update the log message to include access code
        logger.info(f"📥 Analysis request received for token: {token_address[:8]}... (access_code: {access_code})")

        # Check rate limit BEFORE doing any expensive operations
        rate_check = check_rate_limit(access_code)
        
        if not rate_check['allowed']:
            logger.warning(f"⛔ Rate limit exceeded for access code: {access_code}")
            return jsonify({
                'error': 'Daily analysis limit exceeded',
                'limit': rate_check['limit'],
                'resets_at': rate_check['resets_at'],
                'message': f"You have used all {rate_check['limit']} daily analyses. Limit resets at timestamp {rate_check['resets_at']}",
                'status': 'rate_limited'
            }), 429  # HTTP 429 means "Too Many Requests"
        # NEW CODE ENDS HERE

        # --- 2. CACHE CHECK ---
        if token_address in analysis_cache:
            cached = analysis_cache[token_address]
            cache_age = time.time() - cached['timestamp']

            if cache_age < CACHE_DURATION_SECONDS:
                logger.info(f"💾 Returning cached result (age: {cache_age:.0f}s)")
                cached_result = cached['result'].copy()
                cached_result['cached'] = True
                cached_result['cache_age_seconds'] = int(cache_age)
                return jsonify(cached_result), 200
            else:
                logger.info(f"🔄 Cache expired (age: {cache_age:.0f}s), fetching fresh data")

        # --- 3. FETCH DATA (LIQUIDITY & VOLUME) ---
        logger.info("Step 1/3: Fetching Data...")
        liq_data = get_token_liquidity_simple(token_address)
        
        # --- 4. CHECK VELOCITY (ZOMBIE DETECTOR) ---
        velocity_analysis = analyze_velocity(liq_data['liquidity_usd'], liq_data['volume_24h_usd'])
        
        # --- 5. PROBE SLIPPAGE (STRUCTURE CHECK) ---
        logger.info("Step 2/3: Probing Slippage...")
        slippage_data = probe_slippage_curve(token_address)
        
        # --- 6. ANALYZE PATTERNS (HISTORICAL DECAY + STRUCTURE) ---
        logger.info("Step 3/3: Analyzing History & Patterns...")
        analysis = analyze_slippage_patterns(slippage_data, token_address)
        
        # --- 7. CLASSIFY MARKET STATE ---
        result = classify_market_state(analysis)

        # --- 8. APPLY VELOCITY OVERRIDES ---
        # If it's a Zombie, we must warn the user even if the slippage looks good
        if velocity_analysis['status'] == 'ZOMBIE':
            result['action'] = "⚠️ CAUTION: ZOMBIE TOKEN (No Volume)"
            result['severity'] = 'LOW_VOL'
            result['signals'].insert(0, "⛔ Low Velocity: Token is dead/inactive")
            # Reduce confidence score significantly
            result['confidence'] = max(0, result['confidence'] - 50)
        
        elif velocity_analysis['status'] == 'FRENZY':
             result['signals'].insert(0, f"🔥 High Velocity ({velocity_analysis['ratio_percent']:.0f}%): Expect Volatility")

        # Add Velocity Data to Final JSON response
        result['velocity'] = velocity_analysis

        # Prepare the Slippage Data section
        result['slippage_data'] = {
            'baseline_price': slippage_data['baseline_price'],
            'buy_slippage': [{'size_usd': p['size_usd'], **p['buy']} for p in slippage_data['paired_probes']],
            'sell_slippage': [{'size_usd': p['size_usd'], **p['sell']} for p in slippage_data['paired_probes']],
            'asymmetry': analysis['asymmetry']
        }

        result['token_address'] = token_address
        result['timestamp'] = int(time.time())
        result['cached'] = False

        # Save to Cache
        analysis_cache[token_address] = {
            'result': result,
            'timestamp': time.time()
        }

        logger.info(f"✅ Analysis complete and cached for {token_address[:8]}...")
      # NEW CODE STARTS HERE
        # Increment usage counter for this access code since the analysis was successful
        increment_usage(access_code)
        # NEW CODE ENDS HERE
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"❌ Error during analysis: {str(e)}")
        return jsonify({
            'error': str(e),
            'status': 'error',
            'timestamp': int(time.time())
        }), 500



@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """
    Endpoint to manually clear the analysis cache.
    Useful for testing or if you want to force fresh analysis.
    """
    global analysis_cache, historical_slippage
    
    cache_size = len(analysis_cache)
    history_size = len(historical_slippage)
    
    analysis_cache.clear()
    historical_slippage.clear()
    
    logger.info(f"Cache cleared: {cache_size} cached analyses, {history_size} historical records")
    
    return jsonify({
        'status': 'success',
        'message': 'Cache cleared successfully',
        'cleared': {
            'analyses': cache_size,
            'historical_records': history_size
        }
    }), 200


# ============================================================================
# WALLET ANALYSIS ENDPOINT
# ============================================================================

@app.route('/api/wallet/analyze', methods=['POST'])
def analyze_wallet():
    """
    Endpoint to analyze a Solana wallet's trading intelligence.
    
    This endpoint provides comprehensive analysis of a wallet's trading behavior,
    calculating an IQ score, win rate, trading patterns, and other metrics that
    indicate whether the wallet belongs to smart money or a degen gambler.
    
    Request body should be JSON:
    {
        "wallet_address": "SolanaWalletAddressHere...",
        "holding_percent": 0.0,  // Optional: what % of a token they hold
        "current_token_address": "TokenAddressHere..."  // Optional: for tracking first buy
    }
    
    Returns:
    {
        "success": true,
        "data": {
            "iq": 75,
            "winRate": "65.5",
            "trades": 15,
            "tradesScore": 60,
            "portfolio": 0,
            "pattern": "Calculated Trader",
            "holdScore": 30,
            "firstBuyTime": 1234567890
        },
        "cached": true/false,
        "timestamp": 1234567890
    }
    """
    try:
        # Parse the request body
        data = request.get_json()
        
        if not data or 'wallet_address' not in data:
            logger.warning("⚠️ Wallet analysis request missing 'wallet_address' field")
            return jsonify({
                'success': False,
                'error': 'Missing required field: wallet_address'
            }), 400
        
        wallet_address = data['wallet_address'].strip()
        # NEW CODE STARTS HERE
        # Extract the access code from the request
        access_code = data.get('access_code', 'anonymous')
        # NEW CODE ENDS HERE
        # Validate wallet address format
        # Solana addresses are 32-44 characters of base58 characters
        if not wallet_address or len(wallet_address) < 32:
            logger.warning(f"⚠️ Invalid wallet address format: {wallet_address}")
            return jsonify({
                'success': False,
                'error': 'Invalid wallet address format'
            }), 400

        # NEW CODE STARTS HERE
        # Check rate limit before performing expensive blockchain queries
        rate_check = check_rate_limit(access_code)
        
        if not rate_check['allowed']:
            logger.warning(f"⛔ Rate limit exceeded for access code: {access_code}")
            return jsonify({
                'success': False,
                'error': 'Daily analysis limit exceeded',
                'limit': rate_check['limit'],
                'resets_at': rate_check['resets_at'],
                'message': f"You have used all {rate_check['limit']} daily analyses."
            }), 429
        # NEW CODE ENDS HERE
        # Extract optional parameters
        holding_percent = float(data.get('holding_percent', 0.0))
        current_token_address = data.get('current_token_address', None)
        
        logger.info(f"📥 Wallet analysis request: {wallet_address[:8]}...")
        
        # Check if we're returning cached data
        cache_key = f"wallet_analysis:{wallet_address}"
        is_cached = cache_key in wallet_analysis_cache
        
        if is_cached:
            cache_age = time.time() - wallet_analysis_cache[cache_key]['timestamp']
            logger.info(f"  💾 Will return cached data (age: {cache_age/3600:.1f}h)")
        
        # Perform the analysis (will use cache if available)
        analysis_result = calculate_wallet_iq(
            wallet_address,
            holding_percent,
            current_token_address
        )
        
        # Build the response
        response = {
            'success': True,
            'data': analysis_result,
            'cached': is_cached,
            'timestamp': int(time.time())
        }
        
        logger.info(f"✅ Wallet analysis complete: {wallet_address[:8]} → IQ={analysis_result['iq']}")
        # NEW CODE STARTS HERE
        # Increment usage counter for successful wallet analysis
        increment_usage(access_code)
        # NEW CODE ENDS HERE
        return jsonify(response), 200
        
    except ValueError as e:
        logger.error(f"❌ Invalid input for wallet analysis: {e}")
        return jsonify({
            'success': False,
            'error': f'Invalid input: {str(e)}',
            'timestamp': int(time.time())
        }), 400
        
    except Exception as e:
        logger.error(f"❌ Error in wallet analysis endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Internal server error during wallet analysis',
            'timestamp': int(time.time())
        }), 500


# ============================================================================
# NEW ENDPOINTS FOR REAL-TIME SYSTEM
# ============================================================================

@app.route('/tracking/status', methods=['GET'])
def tracking_status():
    """
    Check if real-time tracking is working and see which tokens are tracked.
    
    Use this to verify your WebSocket is connected and working.
    """
    try:
        status = {
            'websocket_active': websocket_client is not None,
            'metrics_manager_active': metrics_manager is not None,
            'tracked_tokens': [],
            'websocket_stats': None,
            'timestamp': int(time.time())
        }
        
        if metrics_manager:
            tracked = metrics_manager.get_all_tracked_tokens()
            status['tracked_tokens'] = [
                {
                    'token_address': addr,
                    'pool_address': token_to_pool_map.get(addr, 'Unknown')
                }
                for addr in tracked
            ]
            status['tracked_count'] = len(tracked)
        
        if websocket_client:
            status['websocket_stats'] = websocket_client.get_stats()
        
        return jsonify(status), 200
        
    except Exception as e:
        logger.error(f"❌ Error getting tracking status: {e}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500


@app.route('/tracking/start', methods=['POST'])
def start_tracking():
    """
    Start tracking a token in real-time.
    
    Request body should be:
    {
        "token_address": "TokenAddressHere",
        "access_code": "YourAccessCode"
    }
    
    We'll automatically find the pool address for you.
    """
    try:
        data = request.get_json()
        
        if not data or 'token_address' not in data:
            return jsonify({
                'error': 'Missing required field: token_address',
                'status': 'error'
            }), 400
        
        token_address = data['token_address']
        access_code = data.get('access_code', 'anonymous')
        
        logger.info(f"📥 Request to start tracking {token_address[:8]}...")
        
        # Check rate limit
        rate_check = check_rate_limit(access_code)
        if not rate_check['allowed']:
            return jsonify({
                'error': 'Daily analysis limit exceeded',
                'limit': rate_check['limit'],
                'resets_at': rate_check['resets_at'],
                'status': 'rate_limited'
            }), 429
        
        # Check if WebSocket is active
        if not websocket_client or not metrics_manager:
            return jsonify({
                'error': 'Real-time tracking system not initialized',
                'message': 'Make sure HELIUS_API_KEY is set in environment variables',
                'status': 'error'
            }), 503
        
        # Get liquidity data
        logger.info(f"  Fetching liquidity data...")
        liq_data = get_token_liquidity_simple(token_address)
        
        # Find the pool address
        logger.info(f"  Finding Raydium pool...")
        pool_address = get_raydium_pool_address(token_address)
        
        if not pool_address:
            return jsonify({
                'error': 'Could not find Raydium pool for this token',
                'message': 'Token might not have a Raydium pool or Birdeye data unavailable',
                'status': 'error'
            }), 404
        
        # Start tracking
        success = start_tracking_token_realtime(
            token_address,
            pool_address,
            liq_data['liquidity_usd']
        )
        
        if success:
            increment_usage(access_code)
            return jsonify({
                'status': 'success',
                'message': f'Started tracking {token_address[:8]}...',
                'token_address': token_address,
                'pool_address': pool_address,
                'liquidity_usd': liq_data['liquidity_usd'],
                'timestamp': int(time.time())
            }), 200
        else:
            return jsonify({
                'error': 'Failed to start tracking',
                'status': 'error'
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Error in start_tracking: {e}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500


@app.route('/metrics/realtime/<token_address>', methods=['GET'])
def get_realtime_metrics(token_address: str):
    """
    Get the current real-time metrics for a token that's being tracked.
    
    Use this to see live volume, price changes, buy/sell ratios, etc.
    """
    try:
        if not metrics_manager:
            return jsonify({
                'error': 'Real-time tracking not initialized',
                'status': 'error'
            }), 503
        
        metrics = metrics_manager.get_metrics(token_address)
        
        if not metrics:
            return jsonify({
                'error': f'Token {token_address[:8]}... is not being tracked',
                'message': 'Use /tracking/start to begin tracking this token',
                'status': 'not_found',
                'tracked_tokens': metrics_manager.get_all_tracked_tokens()
            }), 404
        
        # Convert metrics to a nice JSON format
        result = {
            'token_address': token_address,
            'timestamp': metrics.timestamp,
            'phase': metrics.phase,
            'volume': {
                '1_minute': metrics.volume_1m,
                '5_minutes': metrics.volume_5m,
                '15_minutes': metrics.volume_15m,
                '1_hour': metrics.volume_1h,
                '24_hours': metrics.volume_24h
            },
            'buy_volume': {
                '1_minute': metrics.buy_volume_1m,
                '5_minutes': metrics.buy_volume_5m,
                '1_hour': metrics.buy_volume_1h
            },
            'sell_volume': {
                '1_minute': metrics.sell_volume_1m,
                '5_minutes': metrics.sell_volume_5m,
                '1_hour': metrics.sell_volume_1h
            },
            'trade_counts': {
                '1_minute': metrics.trade_count_1m,
                '5_minutes': metrics.trade_count_5m,
                '1_hour': metrics.trade_count_1h
            },
            'price': {
                'current': metrics.current_price,
                'change_1m_percent': metrics.price_change_1m,
                'change_5m_percent': metrics.price_change_5m,
                'change_1h_percent': metrics.price_change_1h
            },
            'key_metrics': {
                'buy_sell_ratio_1h': metrics.bsr_1h,
                'volume_liquidity_ratio': metrics.vlr_1h,
                'pressure_intensity_index': metrics.pii,
                'volume_trend_score': metrics.vts,
                'volume_exhaustion_index': metrics.vei,
                'conviction_multiplier': metrics.conviction_multiplier,
                'conviction_weighted_pressure': metrics.conviction_weighted_pressure,
                'size_entropy': metrics.size_entropy,
                'large_trade_pct': metrics.large_trade_pct
            },
            'predictions': {
            'next_phase_probabilities': metrics.next_phase_probabilities,
            'transition_confidence': metrics.transition_confidence,
            'transition_observations': metrics.transition_observations
            },
            'liquidity_usd': metrics.liquidity_usd,
            'total_trades_processed': metrics.total_trades_processed
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"❌ Error getting real-time metrics: {e}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500
        
@app.route('/analysis/build-transitions', methods=['POST'])
def build_transition_matrix():
    """
    Trigger analysis of historical data to build transition probabilities.
    
    Run this after collecting data for a while (ideally at least a day).
    This processes all your historical snapshots and builds a transition matrix
    that predicts what's likely to happen next given the current state.
    """
    try:
        data = request.get_json() or {}
        access_code = data.get('access_code', '')
        
        # Only allow with proper access code
        if access_code != 'ADMIN-2025':
            return jsonify({
                'error': 'Admin access required',
                'status': 'unauthorized'
            }), 403
        
        global state_analyzer
        
        from state_transition_analyzer import StateTransitionAnalyzer
        
        # Create fresh analyzer for building
        analyzer = StateTransitionAnalyzer()
        
        # Build new matrix from all historical data
        logger.info("🔬 Starting transition matrix analysis...")
        analyzer.build_transition_matrix(min_observations=10)
        
        if len(analyzer.transition_matrix) == 0:
            return jsonify({
                'status': 'insufficient_data',
                'message': 'Not enough historical data yet to build reliable probabilities',
                'recommendation': 'Keep tracking tokens for longer to accumulate more data'
            }), 200
        
        # Save the matrix to disk
        analyzer.save_matrix()
        
        # CRITICAL: Update the global analyzer so real-time predictions use the new matrix
        state_analyzer = analyzer
        
        # Return statistics about what was built
        total_observations = sum(analyzer.observation_counts.values())
        avg_observations = total_observations / len(analyzer.transition_matrix) if analyzer.transition_matrix else 0
        
        return jsonify({
            'status': 'success',
            'message': 'Transition matrix built and loaded successfully',
            'statistics': {
                'num_states': len(analyzer.transition_matrix),
                'total_observations': total_observations,
                'avg_observations_per_state': round(avg_observations, 1)
            },
            'timestamp': int(time.time())
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Error building transition matrix: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500


@app.route('/historical/status', methods=['GET'])
def historical_data_status():
    """
    Check what historical data has been collected.
    
    This endpoint shows you which tokens have data files and how many
    snapshots are in each file. Use this to verify data collection is working.
    """
    try:
        from historical_data_collector import HistoricalDataCollector
        from pathlib import Path
        import json
        
        collector = HistoricalDataCollector()
        data_dir = Path(collector.data_directory)
        
        if not data_dir.exists():
            return jsonify({
                'status': 'no_data',
                'message': 'Historical data directory does not exist yet',
                'directory': collector.data_directory
            }), 200
        
        # Get all JSON files
        token_files = list(data_dir.glob("*.json"))
        
        file_info = []
        total_snapshots = 0
        
        for file_path in token_files:
            try:
                with open(file_path, 'r') as f:
                    snapshots = json.load(f)
                
                snapshot_count = len(snapshots)
                total_snapshots += snapshot_count
                
                # Get first and last snapshot times for this token
                if snapshots:
                    first_time = snapshots[0].get('timestamp', 0)
                    last_time = snapshots[-1].get('timestamp', 0)
                    duration_hours = (last_time - first_time) / 3600
                else:
                    first_time = 0
                    last_time = 0
                    duration_hours = 0
                
                file_info.append({
                    'token_address': file_path.stem,  # Filename without .json
                    'snapshot_count': snapshot_count,
                    'duration_hours': round(duration_hours, 2),
                    'first_snapshot': first_time,
                    'last_snapshot': last_time
                })
                
            except Exception as e:
                logger.error(f"Error reading {file_path.name}: {e}")
        
        # Sort by snapshot count (most data first)
        file_info.sort(key=lambda x: x['snapshot_count'], reverse=True)
        
        return jsonify({
            'status': 'success',
            'directory': collector.data_directory,
            'total_tokens': len(file_info),
            'total_snapshots': total_snapshots,
            'tokens': file_info,
            'timestamp': int(time.time())
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Error checking historical data: {e}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500
        
# ============================================================================
# INITIALIZATION - This runs when Gunicorn imports the file
# ============================================================================

def initialize_state_analyzer():
    """
    Initialize the state transition analyzer and load the matrix if it exists.
    
    This gets called when the application starts. If we have a saved transition
    matrix from previous analysis, we load it so it's ready to use for real-time
    predictions.
    """
    global state_analyzer
    
    try:
        from state_transition_analyzer import StateTransitionAnalyzer
        
        state_analyzer = StateTransitionAnalyzer()
        
        # Try to load previously calculated matrix
        if state_analyzer.load_matrix():
            logger.info("✅ Loaded existing transition matrix for real-time predictions")
        else:
            logger.info("ℹ️ No existing transition matrix found - predictions will be unavailable until you build one")
            logger.info("   Build a matrix by calling POST /analysis/build-transitions after collecting data")
        
    except Exception as e:
        logger.error(f"❌ Error initializing state analyzer: {e}")
        state_analyzer = None


def initialize_system():
    """
    Initialize the system when the app starts.
    This runs when Gunicorn imports the file, not just when running python main.py
    """
    logger.info("=" * 70)
    logger.info("🚀 Starting Solana Token Analysis Backend Server")
    logger.info("=" * 70)
    logger.info(f"Cache duration: {CACHE_DURATION_SECONDS} seconds")
    logger.info(f"Probe sizes: {PROBE_SIZES_USD}")
    logger.info(f"Historical measurements kept: {MAX_HISTORICAL_MEASUREMENTS}")
    
    # Start the WebSocket background thread
    logger.info("🔧 Initializing real-time tracking system...")
    start_websocket_background()
    # Start the WebSocket background thread
    logger.info("🔧 Initializing real-time tracking system...")
    start_websocket_background()
    
    # NEW CODE: Initialize state transition analyzer
    logger.info("🔧 Initializing state transition analyzer...")
    initialize_state_analyzer()
    
    logger.info("=" * 70)


# IMPORTANT: Call initialize_system() HERE, outside the if __name__ block
# This ensures it runs when Gunicorn imports the file
initialize_system()


# This block only runs when you test locally with "python main.py"
# Gunicorn ignores this block
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
