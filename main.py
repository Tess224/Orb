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

from solana.rpc.api import Client
from solders.pubkey import Pubkey

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

analysis_cache: Dict[str, Dict] = {}
historical_slippage: Dict[str, List[Dict]] = {}

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
                tx_response = client.get_transaction(
                    sig,
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
        
        try:
            # Get the transaction's metadata including timestamp
            sig_data = signatures_data[idx] if idx < len(signatures_data) else {}
            block_time = sig_data.get('blockTime', 0)
            
            # Extract balance information from the transaction
            # pre_balances and post_balances show what changed
            meta = tx.meta
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
            account_keys = tx.transaction.message.account_keys
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
    """
    Probes Jupiter with multiple trade sizes to map the complete slippage curve.
    
    This is the core data collection function. We systematically test what would
    happen if we tried to buy or sell increasing amounts of the token, which
    reveals the underlying liquidity structure and any manipulation patterns.
    
    Args:
        token_address: The Solana token address to analyze
    
    Returns:
        Dict containing buy and sell slippage data at each probe size
    
    Raises:
        Exception: If we can't get enough data to perform analysis
    """
    logger.info(f"Starting slippage curve probe for token {token_address[:8]}...")
    
    sol_price_usd = get_sol_price_usd()
    
    logger.info("Establishing baseline price with micro-trade...")
    baseline_probe = probe_jupiter_quote(
        SOL_MINT,
        token_address,
        1_000_000,
        'baseline'
    )
    
    if not baseline_probe or not baseline_probe['success']:
        raise Exception("Could not establish baseline price - token may not be tradeable on Jupiter")
    
    baseline_price = baseline_probe['execution_price']
    logger.info(f"Baseline price established: {baseline_price:.10f}")
    
    paired_probes = []
    
    for probe_size_usd in PROBE_SIZES_USD:
        sol_amount = probe_size_usd / sol_price_usd
        lamports = int(sol_amount * 1_000_000_000)
        
        logger.info(f"Probing BUY direction with ${probe_size_usd}...")
        
        buy_probe = probe_jupiter_quote(
            SOL_MINT,
            token_address,
            lamports,
            'buy'
        )
        
        if not buy_probe or not buy_probe['success']:
            logger.warning(f"  ✗ BUY ${probe_size_usd} probe failed - skipping this size")
            time.sleep(0.3)
            continue
        
        buy_slippage_pct = abs(
            ((buy_probe['execution_price'] - baseline_price) / baseline_price) * 100
        )
        
        logger.info(f"  ✓ BUY ${probe_size_usd}: {buy_slippage_pct:.2f}% slippage")
        
        time.sleep(0.3)
        
        logger.info(f"Probing SELL direction with ${probe_size_usd}...")
        
        token_amount = int((sol_amount * baseline_price) * 1_000_000_000)
        
        sell_probe = probe_jupiter_quote(
            token_address,
            SOL_MINT,
            token_amount,
            'sell'
        )
        
        if not sell_probe or not sell_probe['success']:
            logger.warning(f"  ✗ SELL ${probe_size_usd} probe failed - skipping this size")
            time.sleep(0.3)
            continue
        
        expected_price = 1 / baseline_price
        sell_slippage_pct = abs(
            ((sell_probe['execution_price'] - expected_price) / expected_price) * 100
        )
        
        logger.info(f"  ✓ SELL ${probe_size_usd}: {sell_slippage_pct:.2f}% slippage")
        
        paired_probes.append({
            'size_usd': probe_size_usd,
            'buy': {
                'execution_price': buy_probe['execution_price'],
                'slippage_pct': buy_slippage_pct,
                'price_impact_pct': buy_probe['price_impact_pct']
            },
            'sell': {
                'execution_price': sell_probe['execution_price'],
                'slippage_pct': sell_slippage_pct,
                'price_impact_pct': sell_probe['price_impact_pct']
            }
        })
        
        time.sleep(0.3)
    
    if len(paired_probes) < 3:
        raise Exception(
            f"Insufficient probe data: only {len(paired_probes)} valid paired measurements. "
            f"Token may have very low liquidity or Jupiter API is experiencing issues."
        )
    
    logger.info(f"Probe complete: {len(paired_probes)} paired measurements collected")
    
    return {
        'baseline_price': baseline_price,
        'paired_probes': paired_probes,
        'timestamp': int(time.time())
    }


def analyze_slippage_patterns(slippage_data: Dict, token_address: str) -> Dict:
    """
    Analyzes slippage curve data to detect manipulation patterns.
    
    This is where the magic happens. We take the raw slippage measurements
    and look for specific patterns that indicate someone is structuring
    the liquidity to prepare for a pump or dump. The analysis calculates
    asymmetry ratios, detects pattern signatures, and generates scores
    that indicate how likely a major price move is.
    
    Args:
        slippage_data: Raw slippage curve from probe_slippage_curve()
        token_address: Token address for historical lookup
    
    Returns:
        Dict containing complete analysis with scores and patterns
    """
    logger.info("Analyzing slippage patterns for manipulation signals...")
    
    paired_probes = slippage_data['paired_probes']
    
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
    
    logger.info(f"Average asymmetry ratio calculated: {avg_asymmetry:.3f}")
    
    if avg_asymmetry > 2.5:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_FORTRESS',
            'severity': 'CRITICAL',
            'description': f'Extreme sell resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_pump_score'] += 40
        analysis['confidence'] += 20
        logger.info("✓ CRITICAL liquidity fortress pattern detected")
        
    elif avg_asymmetry > 2.0:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_FORTRESS',
            'severity': 'HIGH',
            'description': f'Strong sell resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_pump_score'] += 30
        analysis['confidence'] += 15
        logger.info("✓ HIGH liquidity fortress pattern detected")
        
    elif avg_asymmetry > 1.7:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_FORTRESS',
            'severity': 'MEDIUM',
            'description': f'Moderate sell resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_pump_score'] += 20
        analysis['confidence'] += 10
        logger.info("✓ MEDIUM liquidity fortress pattern detected")
    
    if avg_asymmetry < 0.4:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_CLIFF',
            'severity': 'CRITICAL',
            'description': f'Extreme buy resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_dump_score'] += 40
        analysis['confidence'] += 20
        logger.info("✓ CRITICAL liquidity cliff pattern detected")
        
    elif avg_asymmetry < 0.5:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_CLIFF',
            'severity': 'HIGH',
            'description': f'Strong buy resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_dump_score'] += 30
        analysis['confidence'] += 15
        logger.info("✓ HIGH liquidity cliff pattern detected")
        
    elif avg_asymmetry < 0.6:
        analysis['patterns'].append({
            'type': 'LIQUIDITY_CLIFF',
            'severity': 'MEDIUM',
            'description': f'Moderate buy resistance - Asymmetry ratio {avg_asymmetry:.2f}x'
        })
        analysis['scores']['pre_dump_score'] += 20
        analysis['confidence'] += 10
        logger.info("✓ MEDIUM liquidity cliff pattern detected")
    
    if len(paired_probes) >= 4:
        early_buy_slip = paired_probes[1]['buy']['slippage_pct']
        late_buy_slip = paired_probes[-1]['buy']['slippage_pct']
        size_multiplier = paired_probes[-1]['size_usd'] / paired_probes[1]['size_usd']
        buy_slippage_multiplier = late_buy_slip / (early_buy_slip + 0.001)
        
        if buy_slippage_multiplier < size_multiplier * 0.5 and avg_asymmetry > 1.3:
            analysis['patterns'].append({
                'type': 'COMPRESSION_ZONE',
                'severity': 'MEDIUM',
                'description': 'Buy slippage compression - Deep liquidity placed for absorption'
            })
            analysis['scores']['pre_pump_score'] += 20
            logger.info("✓ Compression zone pattern detected")
        
        early_sell_slip = paired_probes[1]['sell']['slippage_pct']
        late_sell_slip = paired_probes[-1]['sell']['slippage_pct']
        sell_slippage_multiplier = late_sell_slip / (early_sell_slip + 0.001)
        
        if sell_slippage_multiplier > size_multiplier * 1.5 and avg_asymmetry < 0.7:
            analysis['patterns'].append({
                'type': 'ACCELERATING_CLIFF',
                'severity': 'HIGH',
                'description': 'Sell slippage accelerating - Support being actively removed'
            })
            analysis['scores']['pre_dump_score'] += 30
            logger.info("✓ Accelerating cliff pattern detected")
    
    if token_address in historical_slippage and len(historical_slippage[token_address]) > 0:
        history = historical_slippage[token_address]
        previous_measurement = history[-1]
        
        if 'asymmetry' in previous_measurement:
            previous_asymmetry = previous_measurement['asymmetry']['average_ratio']
            asymmetry_change = avg_asymmetry - previous_asymmetry
            
            if asymmetry_change > 0.3:
                analysis['patterns'].append({
                    'type': 'ACTIVE_FORTRESS_BUILDING',
                    'severity': 'CRITICAL',
                    'description': f'Asymmetry increased {asymmetry_change:.2f} - Active manipulation detected'
                })
                analysis['scores']['pre_pump_score'] += 25
                analysis['confidence'] += 15
                analysis['manipulation_in_progress'] = True
                logger.info("🚨 ACTIVE fortress building detected via time-series analysis")
            
            elif asymmetry_change < -0.3:
                analysis['patterns'].append({
                    'type': 'ACTIVE_CLIFF_CARVING',
                    'severity': 'CRITICAL',
                    'description': f'Asymmetry decreased {abs(asymmetry_change):.2f} - Liquidity being removed NOW'
                })
                analysis['scores']['pre_dump_score'] += 35
                analysis['confidence'] += 20
                analysis['manipulation_in_progress'] = True
                logger.info("🚨 ACTIVE cliff carving detected via time-series analysis")
    
    if token_address not in historical_slippage:
        historical_slippage[token_address] = []
    
    historical_slippage[token_address].append(analysis)
    
    if len(historical_slippage[token_address]) > MAX_HISTORICAL_MEASUREMENTS:
        historical_slippage[token_address].pop(0)
    
    analysis['confidence'] = min(analysis['confidence'], 95)
    
    logger.info(
        f"Analysis complete - Pre-pump: {analysis['scores']['pre_pump_score']}, "
        f"Pre-dump: {analysis['scores']['pre_dump_score']}, "
        f"Confidence: {analysis['confidence']}%"
    )
    
    return analysis


def classify_market_state(analysis: Dict) -> Dict:
    """
    Takes the raw analysis scores and classifies the market into a final state.
    
    This function makes the ultimate decision: is this token about to pump,
    about to dump, in a holding pattern, or uncertain? It considers the
    scores from slippage analysis and applies thresholds to determine severity
    and expected timeframes.
    
    Args:
        analysis: The complete analysis from analyze_slippage_patterns()
    
    Returns:
        Dict with final classification, confidence, and recommended action
    """
    pump_score = analysis['scores']['pre_pump_score']
    dump_score = analysis['scores']['pre_dump_score']
    confidence = analysis['confidence']
    patterns = analysis['patterns']
    
    result = {
        'state': 'UNCERTAIN',
        'severity': 'UNKNOWN',
        'timeframe': None,
        'confidence': confidence,
        'action': None,
        'signals': [],
        'scores': analysis['scores']
    }
    
    for pattern in patterns:
        result['signals'].append(f"{pattern['type']}: {pattern['description']}")
    
    urgent = analysis.get('manipulation_in_progress', False)
    
    if pump_score >= 80 and pump_score > dump_score:
        result['state'] = 'PRE_PUMP'
        result['severity'] = 'CRITICAL'
        result['timeframe'] = '2-15 minutes' if urgent else '5-20 minutes'
        result['action'] = '🚀 STRONG BUY SIGNAL - Entry opportunity detected'
        logger.info("🎯 Final classification: CRITICAL PRE-PUMP")
        
    elif pump_score >= 60 and pump_score > dump_score:
        result['state'] = 'PRE_PUMP'
        result['severity'] = 'HIGH'
        result['timeframe'] = '10-30 minutes'
        result['action'] = '📈 BUY SIGNAL - Pump likely forming'
        logger.info("🎯 Final classification: HIGH PRE-PUMP")
        
    elif pump_score >= 40 and pump_score > dump_score:
        result['state'] = 'PRE_PUMP'
        result['severity'] = 'MEDIUM'
        result['timeframe'] = '20-60 minutes'
        result['action'] = '👀 WATCH - Accumulation pattern detected'
        logger.info("🎯 Final classification: MEDIUM PRE-PUMP")
        
    elif dump_score >= 80 and dump_score > pump_score:
        result['state'] = 'PRE_DUMP'
        result['severity'] = 'CRITICAL'
        result['timeframe'] = '2-15 minutes' if urgent else '5-20 minutes'
        result['action'] = '⚠️ EXIT IMMEDIATELY - Dump imminent'
        logger.info("🎯 Final classification: CRITICAL PRE-DUMP")
        
    elif dump_score >= 60 and dump_score > pump_score:
        result['state'] = 'PRE_DUMP'
        result['severity'] = 'HIGH'
        result['timeframe'] = '10-30 minutes'
        result['action'] = '⚠️ SELL SIGNAL - Distribution detected'
        logger.info("🎯 Final classification: HIGH PRE-DUMP")
        
    elif dump_score >= 40 and dump_score > pump_score:
        result['state'] = 'PRE_DUMP'
        result['severity'] = 'MEDIUM'
        result['timeframe'] = '20-60 minutes'
        result['action'] = '⚠️ CAUTION - Weakness detected'
        logger.info("🎯 Final classification: MEDIUM PRE-DUMP")
        
    elif abs(pump_score - dump_score) < 20 and pump_score < 40:
        result['state'] = 'HOLDING'
        result['severity'] = 'STABLE'
        result['timeframe'] = 'Current'
        result['action'] = '⏸️ HOLD - Balanced liquidity, no immediate signal'
        logger.info("🎯 Final classification: HOLDING STATE")
        
    else:
        result['state'] = 'UNCERTAIN'
        result['severity'] = 'MIXED'
        result['timeframe'] = None
        result['action'] = '❓ WAIT - Mixed signals, monitor for clarity'
        result['confidence'] = max(30, confidence - 20)
        logger.info("🎯 Final classification: UNCERTAIN (mixed signals)")
    
    return result


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
    Main analysis endpoint that your React app will call.
    
    Accepts a POST request with JSON body containing:
    {
        "token_address": "SolanaTokenAddressHere..."
    }
    
    Returns complete analysis including state classification, confidence,
    detected patterns, and recommended action.
    """
    try:
        # DEBUG: Log raw request data
        print("=" * 50)
        print("DEBUG: Request received")
        print(f"Content-Type: {request.headers.get('Content-Type')}")
        print(f"Raw data: {request.data}")
        print(f"Is JSON: {request.is_json}")
        print("=" * 50)
        
        # Get the token address from the request body
        data = request.get_json()
        
        print(f"Parsed JSON: {data}")
        
        if not data or 'token_address' not in data:
            logger.warning("Request missing token_address field")
            return jsonify({
                'error': 'Missing required field: token_address',
                'status': 'error'
            }), 400
        
        token_address = data['token_address']
        
        if not token_address or len(token_address) < 32:
            logger.warning(f"Invalid token address format: {token_address}")
            return jsonify({
                'error': 'Invalid token address format',
                'status': 'error'
            }), 400
        
        logger.info(f"📥 Analysis request received for token: {token_address[:8]}...")
        
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
        
        logger.info("Step 1/3: Probing slippage curve...")
        slippage_data = probe_slippage_curve(token_address)
        
        logger.info("Step 2/3: Analyzing patterns...")
        analysis = analyze_slippage_patterns(slippage_data, token_address)
        
        logger.info("Step 3/3: Classifying market state...")
        result = classify_market_state(analysis)
        
        buy_slippage = [{'size_usd': p['size_usd'], **p['buy']} for p in slippage_data['paired_probes']]
        sell_slippage = [{'size_usd': p['size_usd'], **p['sell']} for p in slippage_data['paired_probes']]
        
        result['slippage_data'] = {
            'baseline_price': slippage_data['baseline_price'],
            'buy_slippage': buy_slippage,
            'sell_slippage': sell_slippage,
            'asymmetry': analysis['asymmetry']
        }
        
        result['token_address'] = token_address
        result['timestamp'] = int(time.time())
        result['cached'] = False
        
        analysis_cache[token_address] = {
            'result': result,
            'timestamp': time.time()
        }
        
        logger.info(f"✅ Analysis complete and cached for {token_address[:8]}...")
        
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


if __name__ == '__main__':
    logger.info("=" * 70)
    logger.info("🚀 Starting Solana Token Analysis Backend Server")
    logger.info("=" * 70)
    
    logger.info(f"Cache duration: {CACHE_DURATION_SECONDS} seconds")
    logger.info(f"Probe sizes: {PROBE_SIZES_USD}")
    logger.info(f"Historical measurements kept: {MAX_HISTORICAL_MEASUREMENTS}")
    logger.info("=" * 70)
    
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
