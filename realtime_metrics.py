"""
Real-Time Metrics Calculation and Storage
This module maintains running calculations of all metrics as trades arrive.
"""

import time
from typing import Dict, List, Optional, Deque
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """
    Represents a single trade with all relevant information.
    Using a dataclass makes it easy to work with structured trade data.
    """
    timestamp: float
    direction: str  # 'buy' or 'sell'
    token_amount: float
    sol_amount: float
    price: float
    size_usd: float
    transaction_signature: str
    
    def __post_init__(self):
        """Validate trade data after initialization."""
        if self.direction not in ['buy', 'sell']:
            raise ValueError(f"Invalid trade direction: {self.direction}")
        if self.size_usd < 0:
            raise ValueError("Trade size cannot be negative")


@dataclass
class MetricsSnapshot:
    """
    A snapshot of all calculated metrics at a specific point in time.
    This is what gets stored in history for state transition learning.
    """
    timestamp: float
    
    # Volume metrics
    volume_1m: float = 0.0
    volume_5m: float = 0.0
    volume_15m: float = 0.0
    volume_1h: float = 0.0
    volume_24h: float = 0.0
    
    # Buy/sell breakdown
    buy_volume_1m: float = 0.0
    buy_volume_5m: float = 0.0
    buy_volume_1h: float = 0.0
    sell_volume_1m: float = 0.0
    sell_volume_5m: float = 0.0
    sell_volume_1h: float = 0.0
    
    # Trade counts
    trade_count_1m: int = 0
    trade_count_5m: int = 0
    trade_count_1h: int = 0
    
    # Price data
    current_price: float = 0.0
    price_change_1m: float = 0.0
    price_change_5m: float = 0.0
    price_change_1h: float = 0.0
    
    # Derived metrics (calculated from above)
    bsr_1h: float = 1.0  # Buy/Sell Ratio
    vlr_1h: float = 0.0  # Volume/Liquidity Ratio
    pii: float = 0.0     # Pressure Intensity Index
    vts: float = 1.0     # Volume Trend Score
    vei: float = 1.0     # Volume Exhaustion Index
    
    # Phase classification
    phase: str = 'dormant'  # dormant, early, mid, late, exhaustion
    
    # Metadata
    liquidity_usd: float = 0.0
    total_trades_processed: int = 0


class TokenMetricsTracker:
    """
    Tracks real-time metrics for a single token.
    
    This class is the heart of your real-time system. Every time a trade
    comes in, this updates all the metrics, calculates ratios, and stores
    history for state transition analysis.
    """
    
    def __init__(self, token_address: str, liquidity_usd: float, market_cap_usd: Optional[float] = None):
        """
        Initialize metrics tracker for a token.
        
        Args:
            token_address: The token's mint address
            liquidity_usd: Current liquidity pool depth in USD
            market_cap_usd: Optional market cap for additional context
        """
        self.token_address = token_address
        self.liquidity_usd = liquidity_usd
        self.market_cap_usd = market_cap_usd
        
        # Store recent trades in time-windowed deques
        # A deque (double-ended queue) is perfect because we can efficiently
        # add to the end and remove from the front as trades age out
        self.trades_1m: Deque[Trade] = deque(maxlen=1000)   # Last 1 minute
        self.trades_5m: Deque[Trade] = deque(maxlen=5000)   # Last 5 minutes
        self.trades_15m: Deque[Trade] = deque(maxlen=10000) # Last 15 minutes
        self.trades_1h: Deque[Trade] = deque(maxlen=50000)  # Last 1 hour
        self.trades_24h: Deque[Trade] = deque(maxlen=200000) # Last 24 hours
        
        # Historical snapshots taken every minute
        # We'll use these for state transition probability calculation
        self.metric_history: List[MetricsSnapshot] = []
        
        # Track when we last took a snapshot
        self.last_snapshot_time = time.time()
        
        # Track baseline metrics for comparison
        # These get updated hourly to represent "normal" activity
        self.baseline_volume_1h = 0.0
        self.baseline_trade_count_1h = 0.0
        self.baseline_updated_at = 0.0
        
        # Statistics
        self.total_trades = 0
        self.created_at = time.time()
        
        logger.info(f"📊 Initialized metrics tracker for {token_address[:8]}... (Liq: ${liquidity_usd:,.0f})")
    
    
    def _cleanup_old_trades(self):
        """
        Remove trades that have aged beyond their time windows.
        
        This is important for memory management and accuracy. We don't want
        trades from 2 hours ago affecting our 1-minute volume calculations.
        """
        current_time = time.time()
        
        # Clean 1-minute window
        while self.trades_1m and current_time - self.trades_1m[0].timestamp > 60:
            self.trades_1m.popleft()
        
        # Clean 5-minute window
        while self.trades_5m and current_time - self.trades_5m[0].timestamp > 300:
            self.trades_5m.popleft()
        
        # Clean 15-minute window
        while self.trades_15m and current_time - self.trades_15m[0].timestamp > 900:
            self.trades_15m.popleft()
        
        # Clean 1-hour window
        while self.trades_1h and current_time - self.trades_1h[0].timestamp > 3600:
            self.trades_1h.popleft()
        
        # Clean 24-hour window
        while self.trades_24h and current_time - self.trades_24h[0].timestamp > 86400:
            self.trades_24h.popleft()
    
    
    def add_trade(self, trade_data: Dict):
        """
        Process a new trade and update all metrics.
        
        This is called by the WebSocket handler every time a trade comes in.
        It updates all the rolling windows and triggers snapshot creation
        if enough time has passed.
        
        Args:
            trade_data: Dictionary containing trade information from WebSocket
        """
        try:
            # Create Trade object from the incoming data
            trade = Trade(
                timestamp=trade_data['timestamp'],
                direction=trade_data['direction'],
                token_amount=trade_data['token_amount'],
                sol_amount=trade_data['sol_amount'],
                price=trade_data['price'],
                size_usd=trade_data['size_usd'],
                transaction_signature=trade_data['transaction_signature']
            )
            
            # Add to all time windows
            # These deques automatically handle max length, dropping oldest
            self.trades_1m.append(trade)
            self.trades_5m.append(trade)
            self.trades_15m.append(trade)
            self.trades_1h.append(trade)
            self.trades_24h.append(trade)
            
            # Update statistics
            self.total_trades += 1
            
            # Clean up old trades from all windows
            self._cleanup_old_trades()
            
            # Check if we should take a new metrics snapshot
            # We take snapshots every minute for state transition tracking
            current_time = time.time()
            if current_time - self.last_snapshot_time >= 60:
                self._take_snapshot()
                self.last_snapshot_time = current_time
            
            # Log significant trades
            if trade.size_usd >= 1000:
                logger.info(
                    f"💰 Large {trade.direction.upper()}: ${trade.size_usd:,.0f} "
                    f"on {self.token_address[:8]}..."
                )
        
        except Exception as e:
            logger.error(f"❌ Error processing trade for {self.token_address[:8]}: {e}")
    
    
    def _calculate_volume_metrics(self, trades: Deque[Trade]) -> Dict:
        """
        Calculate volume metrics from a collection of trades.
        
        This helper function computes total volume, buy/sell breakdown,
        and trade counts for any time window.
        
        Args:
            trades: Deque of Trade objects
            
        Returns:
            Dictionary with volume breakdown
        """
        total_volume = 0.0
        buy_volume = 0.0
        sell_volume = 0.0
        trade_count = len(trades)
        
        for trade in trades:
            total_volume += trade.size_usd
            if trade.direction == 'buy':
                buy_volume += trade.size_usd
            else:
                sell_volume += trade.size_usd
        
        return {
            'total_volume': total_volume,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'trade_count': trade_count
        }
    
    
    def _take_snapshot(self):
        """
        Create a snapshot of current metrics and store in history.
        
        This is called every minute to build up the historical record
        that we'll use for state transition probability calculations.
        """
        current_time = time.time()
        
        # Calculate metrics for all time windows
        metrics_1m = self._calculate_volume_metrics(self.trades_1m)
        metrics_5m = self._calculate_volume_metrics(self.trades_5m)
        metrics_15m = self._calculate_volume_metrics(self.trades_15m)
        metrics_1h = self._calculate_volume_metrics(self.trades_1h)
        metrics_24h = self._calculate_volume_metrics(self.trades_24h)
        
        # Calculate current price from most recent trade
        current_price = self.trades_1m[-1].price if self.trades_1m else 0.0
        
        # Calculate price changes
        price_change_1m = 0.0
        price_change_5m = 0.0
        price_change_1h = 0.0
        
        if len(self.trades_1m) >= 2:
            price_1m_ago = self.trades_1m[0].price
            if price_1m_ago > 0:
                price_change_1m = ((current_price - price_1m_ago) / price_1m_ago) * 100
        
        if len(self.trades_5m) >= 2:
            price_5m_ago = self.trades_5m[0].price
            if price_5m_ago > 0:
                price_change_5m = ((current_price - price_5m_ago) / price_5m_ago) * 100
        
        if len(self.trades_1h) >= 2:
            price_1h_ago = self.trades_1h[0].price
            if price_1h_ago > 0:
                price_change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100
        
        # Calculate Buy/Sell Ratio
        bsr_1h = 1.0
        if metrics_1h['sell_volume'] > 0:
            bsr_1h = metrics_1h['buy_volume'] / metrics_1h['sell_volume']
        elif metrics_1h['buy_volume'] > 0:
            bsr_1h = 10.0  # Cap at 10 if all buys and no sells
        
        # Calculate Volume/Liquidity Ratio
        vlr_1h = 0.0
        if self.liquidity_usd > 0:
            vlr_1h = metrics_1h['total_volume'] / self.liquidity_usd
        
        # Calculate Volume Trend Score
        # Compare current activity to baseline "normal" activity
        vts = 1.0
        if self.baseline_volume_1h > 0:
            vts = metrics_1h['total_volume'] / self.baseline_volume_1h
        
        # Calculate Volume Exhaustion Index
        # Compare current minute volume to peak volume in last hour
        vei = 1.0
        if metrics_1h['total_volume'] > 0:
            peak_volume = max(
                metrics_1m['total_volume'] * 60,  # Extrapolate 1m to hourly rate
                metrics_5m['total_volume'] * 12,  # Extrapolate 5m to hourly rate
                metrics_1h['total_volume']
            )
            if peak_volume > 0:
                current_rate = metrics_1m['total_volume'] * 60
                vei = current_rate / peak_volume
        
        # Calculate Pressure Intensity Index
        # This combines net pressure with volume trend
        net_pressure_1h = metrics_1h['buy_volume'] - metrics_1h['sell_volume']
        pii = 0.0
        if self.liquidity_usd > 0:
            pii = (net_pressure_1h / self.liquidity_usd) * vts
        
        # Determine phase based on current metrics
        phase = self._classify_phase(vlr_1h, vts, vei, pii, price_change_1h)
        
        # Create the snapshot
        snapshot = MetricsSnapshot(
            timestamp=current_time,
            volume_1m=metrics_1m['total_volume'],
            volume_5m=metrics_5m['total_volume'],
            volume_15m=metrics_15m['total_volume'],
            volume_1h=metrics_1h['total_volume'],
            volume_24h=metrics_24h['total_volume'],
            buy_volume_1m=metrics_1m['buy_volume'],
            buy_volume_5m=metrics_5m['buy_volume'],
            buy_volume_1h=metrics_1h['buy_volume'],
            sell_volume_1m=metrics_1m['sell_volume'],
            sell_volume_5m=metrics_5m['sell_volume'],
            sell_volume_1h=metrics_1h['sell_volume'],
            trade_count_1m=metrics_1m['trade_count'],
            trade_count_5m=metrics_5m['trade_count'],
            trade_count_1h=metrics_1h['trade_count'],
            current_price=current_price,
            price_change_1m=price_change_1m,
            price_change_5m=price_change_5m,
            price_change_1h=price_change_1h,
            bsr_1h=bsr_1h,
            vlr_1h=vlr_1h,
            pii=pii,
            vts=vts,
            vei=vei,
            phase=phase,
            liquidity_usd=self.liquidity_usd,
            total_trades_processed=self.total_trades
        )
        
        # Add to history
        self.metric_history.append(snapshot)
        
        # Keep only last 24 hours of snapshots (1440 minutes)
        if len(self.metric_history) > 1440:
            self.metric_history.pop(0)
        
        # Update baseline if it's been more than an hour
        if current_time - self.baseline_updated_at > 3600:
            self.baseline_volume_1h = metrics_1h['total_volume']
            self.baseline_trade_count_1h = metrics_1h['trade_count']
            self.baseline_updated_at = current_time
        
        logger.debug(
            f"📸 Snapshot taken for {self.token_address[:8]}: "
            f"Phase={phase}, VTS={vts:.2f}, VEI={vei:.2f}, PII={pii:.2f}"
        )
    
    
    def _classify_phase(self, vlr: float, vts: float, vei: float, pii: float, price_change: float) -> str:
        """
        Classify the current market phase based on metrics.
        
        This uses the logic from the original document but applies it
        to real-time metrics. The phase classification drives the
        state transition probability system.
        
        Args:
            vlr: Volume/Liquidity Ratio
            vts: Volume Trend Score
            vei: Volume Exhaustion Index
            pii: Pressure Intensity Index
            price_change: Price change percentage in last hour
            
        Returns:
            Phase name: 'dormant', 'early', 'mid', 'late', or 'exhaustion'
        """
        # DORMANT: Very low activity
        if vlr < 0.2 and vts < 1.2:
            return 'dormant'
        
        # EARLY PHASE: Volume accelerating, high energy, price moving
        if vts > 2.0 and vei > 0.7 and abs(price_change) > 5:
            return 'early'
        
        # MID PHASE: Volume still strong, pressure directional
        if vts > 1.3 and vei > 0.5 and abs(pii) > 0.3:
            return 'mid'
        
        # LATE PHASE: Volume declining but still elevated
        if vts > 1.0 and vei < 0.5 and vei > 0.2:
            return 'late'
        
        # EXHAUSTION: Volume collapsed or extreme divergence
        if vei < 0.3:
            return 'exhaustion'
        
        # Default to dormant if nothing else matches
        return 'dormant'
    
    
    def get_current_metrics(self) -> MetricsSnapshot:
        """
        Get the most recent metrics snapshot.
        
        Returns:
            Latest MetricsSnapshot object
        """
        if self.metric_history:
            return self.metric_history[-1]
        else:
            # If no snapshots yet, create one now
            self._take_snapshot()
            return self.metric_history[-1] if self.metric_history else MetricsSnapshot(timestamp=time.time())
    
    
    def get_historical_snapshots(self, lookback_minutes: int = 60) -> List[MetricsSnapshot]:
        """
        Get historical snapshots for state transition analysis.
        
        Args:
            lookback_minutes: How far back to look
            
        Returns:
            List of MetricsSnapshot objects from the specified time period
        """
        if not self.metric_history:
            return []
        
        cutoff_time = time.time() - (lookback_minutes * 60)
        return [s for s in self.metric_history if s.timestamp >= cutoff_time]


class MetricsManager:
    """
    Manages metrics trackers for all tokens being monitored.
    
    This is the top-level manager that your Flask app will interact with.
    It maintains a tracker for each token and routes trade data to the
    appropriate tracker.
    """
    
    def __init__(self):
        """Initialize the metrics manager."""
        self.trackers: Dict[str, TokenMetricsTracker] = {}
        logger.info("📊 Metrics Manager initialized")
    
    
    def add_token(self, token_address: str, liquidity_usd: float, market_cap_usd: Optional[float] = None):
        """
        Start tracking metrics for a new token.
        
        Args:
            token_address: Token's mint address
            liquidity_usd: Current liquidity pool depth
            market_cap_usd: Optional market cap
        """
        if token_address not in self.trackers:
            self.trackers[token_address] = TokenMetricsTracker(
                token_address,
                liquidity_usd,
                market_cap_usd
            )
            logger.info(f"✅ Now tracking metrics for {token_address[:8]}...")
        else:
            logger.info(f"ℹ️ Already tracking {token_address[:8]}...")
    
    
    def remove_token(self, token_address: str):
        """Stop tracking a token and free up memory."""
        if token_address in self.trackers:
            del self.trackers[token_address]
            logger.info(f"🗑️ Stopped tracking {token_address[:8]}...")
    
    
    def handle_trade(self, trade_data: Dict):
        """
        Route incoming trade data to the appropriate tracker.
        
        This is the callback function that gets registered with the
        WebSocket client.
        
        Args:
            trade_data: Trade information from WebSocket
        """
        token_address = trade_data.get('token_address')
        
        if not token_address:
            logger.warning("⚠️ Trade data missing token_address")
            return
        
        if token_address in self.trackers:
            self.trackers[token_address].add_trade(trade_data)
        else:
            logger.debug(f"ℹ️ Received trade for untracked token {token_address[:8]}...")
    
    
    def get_metrics(self, token_address: str) -> Optional[MetricsSnapshot]:
        """
        Get current metrics for a specific token.
        
        Args:
            token_address: Token to get metrics for
            
        Returns:
            MetricsSnapshot or None if token not tracked
        """
        tracker = self.trackers.get(token_address)
        if tracker:
            return tracker.get_current_metrics()
        return None
    
    
    def get_all_tracked_tokens(self) -> List[str]:
        """Get list of all tokens currently being tracked."""
        return list(self.trackers.keys())