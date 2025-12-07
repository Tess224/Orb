"""
Real-Time Metrics Calculation and Storage
This module maintains running calculations of all metrics as trades arrive.
"""

import time 
from typing import Dict, List, Optional, Deque, Tuple
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a single trade - unchanged from original."""
    timestamp: float
    direction: str  # 'buy' or 'sell'
    token_amount: float
    sol_amount: float
    price: float
    size_usd: float
    transaction_signature: str

    def __post_init__(self):
        if self.direction not in ['buy', 'sell']:
            raise ValueError(f"Invalid trade direction: {self.direction}")
        if self.size_usd < 0:
            raise ValueError("Trade size cannot be negative")


@dataclass
class MetricsSnapshot:
    """
    Snapshot of calculated metrics - now includes token age context.
    
    The age field helps downstream systems understand which timeframes
    are reliable for this token.
    """
    timestamp: float
    token_age_hours: float  # NEW: How many hours we've been tracking this token
    
    # Volume metrics - these windows are now dynamically selected
    volume_1m: float = 0.0
    volume_5m: float = 0.0
    volume_15m: float = 0.0
    volume_1h: float = 0.0
    volume_4h: float = 0.0
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
    
    # Derived metrics
    bsr_1h: float = 1.0
    vlr_1h: float = 0.0
    pii: float = 0.0
    vts: float = 1.0
    vei: float = 1.0
    
    # Phase classification
    phase: str = 'dormant'
    
    # Metadata
    liquidity_usd: float = 0.0
    total_trades_processed: int = 0


class TokenMetricsTracker:
    """
    Tracks real-time metrics for a single token with dynamic timeframe adaptation.
    
    MAJOR ENHANCEMENT:
    The tracker now adjusts which time windows it uses for calculations based on
    how long the token has been monitored. This prevents absurd comparisons like
    "current 5-minute volume is 300% of the 24-hour average" when the token has
    only been tracked for 20 minutes.
    
    Age-Based Window Selection:
    - Under 1 hour: Use ultra-short windows (30s, 2m, 5m)
    - 1-6 hours: Use short windows (1m, 5m, 15m, 30m)
    - 6-24 hours: Use medium windows (5m, 15m, 1h, 4h)
    - Over 24 hours: Use full windows (5m, 15m, 1h, 4h, 24h)
    """

    def __init__(self, token_address: str, liquidity_usd: float, market_cap_usd: Optional[float] = None):
        """
        Initialize metrics tracker for a token.
        
        The tracker records the exact moment it starts monitoring, which becomes
        the reference point for all age calculations.
        """
        self.token_address = token_address
        self.liquidity_usd = liquidity_usd
        self.market_cap_usd = market_cap_usd
        
        # NEW: Record when we started tracking this token
        # This is the anchor point for all age-based decisions
        self.tracking_started_at = time.time()
        
        # Time-windowed trade storage
        # These deques store raw trade data at different granularities
        self.trades_30s: Deque[Trade] = deque(maxlen=500)    # NEW: Ultra-short for very young tokens
        self.trades_1m: Deque[Trade] = deque(maxlen=1000)
        self.trades_2m: Deque[Trade] = deque(maxlen=2000)    # NEW: Short window for young tokens
        self.trades_5m: Deque[Trade] = deque(maxlen=5000)
        self.trades_15m: Deque[Trade] = deque(maxlen=10000)
        self.trades_30m: Deque[Trade] = deque(maxlen=20000)  # NEW: Medium window
        self.trades_1h: Deque[Trade] = deque(maxlen=50000)
        self.trades_4h: Deque[Trade] = deque(maxlen=200000)
        self.trades_24h: Deque[Trade] = deque(maxlen=500000)
        
        # Historical snapshots taken every minute
        self.metric_history: List[MetricsSnapshot] = []
        
        # Baseline tracking with age-appropriate windows
        self.hourly_volumes: Deque[float] = deque(maxlen=168)
        self.baseline_volume_5m = 0.0
        self.baseline_volume_15m = 0.0
        self.baseline_volume_1h = 0.0
        self.baseline_volume_4h = 0.0
        self.baseline_volume_24h = 0.0
        
        self.last_baseline_update = time.time()
        self.last_snapshot_time = time.time()
        
        # Track hourly peak volumes for VEI
        self.hourly_volume_history: Deque[float] = deque(maxlen=24)
        self.last_hourly_record = time.time()
        
        # Statistics
        self.total_trades = 0
        # NEW ADDITIONS START HERE
    # Track VTS history for intelligent capping
    # We keep up to 4320 values (3 days of minute-by-minute snapshots)
        self.vts_history: Deque[float] = deque(maxlen=4320)
    
    # Cache for VTS percentile calculations
    # We don't want to recalculate percentiles every minute because it's slow
    # This cache stores the 99th percentile and when we last calculated it
        self.vts_percentiles_cache = {
            '99th': 15.0,  # Start with a conservative default
            'last_update': 0  # Will be updated when we first calculate
        }
    
    # Track data quality statistics
    # These help us monitor if we're getting bad data from the WebSocket
        self.stats = {
            'rejected_trades': 0,
            'validation_errors': {}
        }
    # NEW ADDITIONS END HERE

        logger.info(f"📊 Initialized dynamic-timeframe tracker for {token_address[:8]}... (Liq: ${liquidity_usd:,.0f})")


    def _validate_trade_data(self, trade_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if incoming trade data is valid and makes sense.
    
        This is our first line of defense against bad data. We check:
        1. All required fields are present
        2. Values are within reasonable ranges
        3. The timestamp makes sense
    
        Think of this like a bouncer at a club - we check IDs before letting anyone in.
    
        Returns:
            A tuple of (is_valid, error_message)
            If valid, error_message will be None
            If invalid, error_message explains what's wrong
        """
    # First check: do we have all the fields we need?
        required_fields = ['timestamp', 'direction', 'size_usd', 'price']
        for field in required_fields:
            if field not in trade_data:
                return False, f"missing_field_{field}"
    
    # Second check: is the trade size reasonable?
    # Negative sizes don't make sense
        if trade_data['size_usd'] < 0:
            return False, "negative_size"
    
    # A trade that's 10x the entire liquidity pool is probably an error
    # Real trades would be limited by available liquidity
        if trade_data['size_usd'] > self.liquidity_usd * 10:
            return False, f"size_exceeds_10x_liquidity (${trade_data['size_usd']:.0f} > ${self.liquidity_usd * 10:.0f})"
    
    # Third check: is the price valid?
        if trade_data['price'] <= 0:
            return False, "invalid_price"
    
    # Fourth check: is the timestamp reasonable?
        current_time = time.time()
        trade_time = trade_data['timestamp']
    
    # Trade can't be from the future (with 60 second tolerance for clock differences)
        if trade_time > current_time + 60:
            return False, "timestamp_in_future"
    
    # Trade can't be from more than 24 hours ago
    # If we're just starting to track, we shouldn't get ancient trades
        if trade_time < current_time - 86400:
            return False, "timestamp_too_old"
    
    # All checks passed
        return True, None

    def _calculate_safe_baseline_floor(self, window_name: str) -> float:
        """
        Calculate a sensible minimum baseline value based on the token's actual behavior.
    
        The problem we're solving: we can't use an arbitrary tiny number like $0.10
        because that might be absurdly small for a token trading thousands of dollars,
        or it might be too large for a truly tiny microcap.
       
        Our solution: look at what the token actually trades and base our floor on that.
      
        Think of it like setting a noise threshold on a microphone. You want it low enough
        to hear real sound, but high enough to filter out electrical hum. The right level
        depends on your specific environment.
    
        Args:
            window_name: Which timeframe we're calculating for ('5m', '15m', '1h', etc.)
        
        Returns:
            A dollar amount to use as the minimum baseline
        """
        age_hours = self.get_token_age_hours()
    
    # For very young tokens, we need to be extra careful
    # We'll look at what we've actually observed and use that to set a floor
        if age_hours < 1.0:
        # Do we have any trading history at all?
            if len(self.trades_30m) > 0:
            # Break our 30 minutes of trades into 5-minute chunks
            # Calculate volume for each chunk
                volumes_5m = []
                trades_list = list(self.trades_30m)
            
            # We want about 6 chunks (30 minutes / 5 minutes)
                chunk_size = max(1, len(trades_list) // 6)
            
                for i in range(0, len(trades_list), chunk_size):
                    chunk = trades_list[i:i + chunk_size]
                    chunk_volume = sum(t.size_usd for t in chunk)
                    volumes_5m.append(chunk_volume)
            
                if volumes_5m:
                # Use the 20th percentile as our floor
                # This means 80% of periods had more volume than this
                # It's conservative but not absurdly low
                    volumes_5m.sort()
                    idx_20th = max(0, len(volumes_5m) // 5)
                    percentile_20 = volumes_5m[idx_20th]
                
                # Also calculate a liquidity-based minimum
                # Even quiet periods should have some volume relative to liquidity
                # We use 0.01% of liquidity as an absolute floor
                    liquidity_based_min = self.liquidity_usd * 0.0001
                
                # Use whichever is larger, but at least $1
                    return max(percentile_20, liquidity_based_min, 1.0)
        
        # If we have no trading history yet, use a liquidity-based estimate
        # This is our best guess before we've seen any real trading
            return max(self.liquidity_usd * 0.0001, 1.0)
    
    # For more mature tokens, we can use the actual historical baselines
    # but we still want to ensure they're not unreasonably small
    
    # Get the appropriate historical baseline for this window
        if window_name == '5m':
            historical = self.baseline_volume_5m
        elif window_name == '15m':
            historical = self.baseline_volume_15m
        elif window_name == '1h':
            historical = self.baseline_volume_1h
        elif window_name == '4h':
            historical = self.baseline_volume_4h
        else:  # '24h'
            historical = self.baseline_volume_24h
    
    # If we have a good historical baseline, use 10% of it as the floor
    # This handles naturally quiet periods without treating them as anomalies
        if historical > 0:
            return historical * 0.10
    
    # If we somehow don't have a historical baseline yet, fall back to liquidity estimate
        return max(self.liquidity_usd * 0.0001, 1.0)

    
    def get_token_age_hours(self) -> float:
        """
        Calculate how many hours we've been tracking this token.
        
        This is the key metric that drives all timeframe selection decisions.
        Returns a float so we can handle fractional hours (e.g., 0.5 hours = 30 minutes).
        """
        age_seconds = time.time() - self.tracking_started_at
        age_hours = age_seconds / 3600.0
        return age_hours


    def get_appropriate_timeframes(self) -> Dict[str, int]:
        """
        Select which time windows to use based on token age.
        
        This is the heart of the dynamic timeframe system. We return a dictionary
        mapping timeframe names to their durations in seconds. The calling code
        uses these windows for all calculations.
        
        The logic here is based on a simple principle: never compare current activity
        to a baseline that spans a longer period than the token's entire tracked lifetime.
        
        Returns:
            Dictionary mapping timeframe labels to durations in seconds
            Example: {'short': 120, 'medium': 300, 'long': 900, 'baseline': 1800}
        """
        age_hours = self.get_token_age_hours()
        
        if age_hours < 1.0:
            # VERY YOUNG TOKEN (under 1 hour old)
            # Use ultra-short windows. We can only compare recent seconds/minutes.
            # Example: A token tracked for 20 minutes can compare 30s to 2m, but not to 1h.
            return {
                'ultra_short': 30,      # 30 seconds for immediate activity
                'short': 120,            # 2 minutes
                'medium': 300,           # 5 minutes
                'baseline_short': 120,   # Compare to 2-minute average
                'baseline_medium': 300   # Compare to 5-minute average
            }
            
        elif age_hours < 6.0:
            # YOUNG TOKEN (1-6 hours old)
            # Use short to medium windows. Hourly comparisons are now meaningful.
            return {
                'short': 60,             # 1 minute
                'medium': 300,           # 5 minutes
                'long': 900,             # 15 minutes
                'baseline_short': 300,   # Compare to 5-minute average
                'baseline_medium': 900,  # Compare to 15-minute average
                'baseline_long': 1800    # Compare to 30-minute average
            }
            
        elif age_hours < 24.0:
            # MATURING TOKEN (6-24 hours old)
            # Use medium to long windows. 4-hour comparisons are meaningful.
            return {
                'short': 300,            # 5 minutes
                'medium': 900,           # 15 minutes
                'long': 3600,            # 1 hour
                'baseline_short': 900,   # Compare to 15-minute average
                'baseline_medium': 3600, # Compare to 1-hour average
                'baseline_long': 14400   # Compare to 4-hour average
            }
            
        else:
            # MATURE TOKEN (over 24 hours)
            # Use full windows. Daily comparisons are meaningful.
            return {
                'short': 300,            # 5 minutes
                'medium': 900,           # 15 minutes
                'long': 3600,            # 1 hour
                'extended': 14400,       # 4 hours
                'baseline_short': 3600,  # Compare to 1-hour average
                'baseline_medium': 14400,# Compare to 4-hour average
                'baseline_long': 86400   # Compare to 24-hour average
            }


    def _cleanup_old_trades(self):
        """
        Remove trades that have aged beyond their time windows.
        
        This now cleans ALL our time windows, including the new ones for young tokens.
        """
        current_time = time.time()
        
        # Clean all windows
        while self.trades_30s and current_time - self.trades_30s[0].timestamp > 30:
            self.trades_30s.popleft()
            
        while self.trades_1m and current_time - self.trades_1m[0].timestamp > 60:
            self.trades_1m.popleft()
            
        while self.trades_2m and current_time - self.trades_2m[0].timestamp > 120:
            self.trades_2m.popleft()
            
        while self.trades_5m and current_time - self.trades_5m[0].timestamp > 300:
            self.trades_5m.popleft()
            
        while self.trades_15m and current_time - self.trades_15m[0].timestamp > 900:
            self.trades_15m.popleft()
            
        while self.trades_30m and current_time - self.trades_30m[0].timestamp > 1800:
            self.trades_30m.popleft()
            
        while self.trades_1h and current_time - self.trades_1h[0].timestamp > 3600:
            self.trades_1h.popleft()
            
        while self.trades_4h and current_time - self.trades_4h[0].timestamp > 14400:
            self.trades_4h.popleft()
            
        while self.trades_24h and current_time - self.trades_24h[0].timestamp > 86400:
            self.trades_24h.popleft()


    def add_trade(self, trade_data: Dict):
        """
        Process a new trade and update all metrics.
        
        Now adds trades to ALL time windows, including the new short ones.
        """
        try:
            trade = Trade(
                timestamp=trade_data['timestamp'],
                direction=trade_data['direction'],
                token_amount=trade_data['token_amount'],
                sol_amount=trade_data['sol_amount'],
                price=trade_data['price'],
                size_usd=trade_data['size_usd'],
                transaction_signature=trade_data['transaction_signature']
            )
            
            # Add to ALL time windows
            self.trades_30s.append(trade)
            self.trades_1m.append(trade)
            self.trades_2m.append(trade)
            self.trades_5m.append(trade)
            self.trades_15m.append(trade)
            self.trades_30m.append(trade)
            self.trades_1h.append(trade)
            self.trades_4h.append(trade)
            self.trades_24h.append(trade)
            
            self.total_trades += 1
            self._cleanup_old_trades()
            
            # Record hourly volumes
            current_time = time.time()
            if current_time - self.last_hourly_record >= 3600:
                metrics_1h = self._calculate_volume_metrics(self.trades_1h)
                self.hourly_volume_history.append(metrics_1h['total_volume'])
                self.last_hourly_record = current_time
            
            # Take snapshot every minute
            if current_time - self.last_snapshot_time >= 60:
                self._take_snapshot()
                self.last_snapshot_time = current_time
            
            if trade.size_usd >= 1000:
                logger.info(
                    f"💰 Large {trade.direction.upper()}: ${trade.size_usd:,.0f} "
                    f"on {self.token_address[:8]}... (age: {self.get_token_age_hours():.1f}h)"
                )
        
        except Exception as e:
            logger.error(f"❌ Error processing trade: {e}")


    def _calculate_volume_metrics(self, trades: Deque[Trade]) -> Dict:
        """Calculate volume metrics - unchanged from original."""
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


    def _get_baseline_volume(self, timeframe_seconds: int) -> float:
        """
        Get the appropriate baseline volume for a given timeframe.
        
        NEW METHOD: This intelligently selects or calculates a baseline that
        makes sense for both the requested timeframe and the token's age.
        
        For very young tokens, we might not have a full hour of history, so
        we scale down from what we do have. For mature tokens, we use the
        established baselines.
        """
        age_hours = self.get_token_age_hours()
        
        # If the token is younger than the requested timeframe, we can't
        # have a meaningful baseline yet. Return a conservative estimate.
        timeframe_hours = timeframe_seconds / 3600.0
        if age_hours < timeframe_hours:
            # Not enough history yet. Use what we have proportionally.
            # If we have 30 minutes of history and need a 1-hour baseline,
            # we'll extrapolate from the 30 minutes we do have.
            if self.trades_30m:
                actual_metrics = self._calculate_volume_metrics(self.trades_30m)
                # Scale up proportionally
                scale_factor = timeframe_hours / (age_hours if age_hours > 0 else 0.5)
                return actual_metrics['total_volume'] * scale_factor
            return 1.0  # Fallback to avoid division by zero
        
        # We have enough history. Return the appropriate baseline.
        if timeframe_seconds <= 300:  # 5 minutes
            return max(self.baseline_volume_5m, 1.0)
        elif timeframe_seconds <= 900:  # 15 minutes
            return max(self.baseline_volume_15m, 1.0)
        elif timeframe_seconds <= 3600:  # 1 hour
            return max(self.baseline_volume_1h, 1.0)
        elif timeframe_seconds <= 14400:  # 4 hours
            return max(self.baseline_volume_4h, 1.0)
        else:  # 24 hours
            return max(self.baseline_volume_24h, 1.0)


    def _update_baselines(self):
        """
        Update baseline "normal" activity levels with age-appropriate windows.
        
        ENHANCED: Now only updates baselines for timeframes where we have
        sufficient history to make them meaningful.
        """
        current_time = time.time()
        
        if current_time - self.last_baseline_update < 3600:
            return
        
        age_hours = self.get_token_age_hours()
        
        # Only calculate baselines for timeframes where we have enough history
        if age_hours >= 0.5:  # At least 30 minutes
            if self.trades_30m:
                metrics = self._calculate_volume_metrics(self.trades_30m)
                self.baseline_volume_5m = metrics['total_volume'] / 6  # 30min / 5min = 6
        
        if age_hours >= 1.0:  # At least 1 hour
            if len(self.hourly_volumes) >= 1:
                self.baseline_volume_1h = sum(self.hourly_volumes) / len(self.hourly_volumes)
            else:
                metrics_1h = self._calculate_volume_metrics(self.trades_1h)
                self.baseline_volume_1h = max(metrics_1h['total_volume'], 1.0)
        
        if age_hours >= 4.0:  # At least 4 hours
            if self.baseline_volume_1h > 0:
                self.baseline_volume_4h = self.baseline_volume_1h * 4
        
        if age_hours >= 24.0:  # At least 24 hours
            if self.baseline_volume_1h > 0:
                self.baseline_volume_24h = self.baseline_volume_1h * 24
        
        # Always derive short timeframe baselines from what we have
        if self.baseline_volume_1h > 0:
            self.baseline_volume_15m = self.baseline_volume_1h / 4
            self.baseline_volume_5m = self.baseline_volume_1h / 12
        
        self.last_baseline_update = current_time
        
        logger.debug(
            f"📊 Updated baselines for {self.token_address[:8]} "
            f"(age: {age_hours:.1f}h, 1h baseline: ${self.baseline_volume_1h:,.0f})"
        )


    def _calculate_volume_trend_score(self, metrics_5m: Dict, metrics_15m: Dict, 
                                 metrics_1h: Dict, metrics_4h: Dict, 
                                 metrics_24h: Dict) -> float:
        """
        Calculate Volume Trend Score using age-appropriate baselines.
       
        CRITICAL FIX: For very young tokens, we can't compare to baselines that
        don't exist yet. Instead, we need to use a different calculation method
        that looks at volume acceleration and absolute volume levels.
        """
        age_hours = self.get_token_age_hours()
    
        # For tokens under 30 minutes old, use a simpler absolute volume approach
        if age_hours < 0.5:
        # Can't do meaningful baseline comparisons yet
        # Instead, look at whether volume is accelerating
            vol_5m = metrics_5m['total_volume']
        
        # Compare 5-minute window to the 15-minute average
            if len(self.trades_15m) > 0:
                vol_15m_avg = metrics_15m['total_volume'] / 3.0  # Average per 5 minutes
                if vol_15m_avg > 0:
                    vts = vol_5m / vol_15m_avg
                else:
                    vts = 1.0
            else:
            # Very first minutes, just return neutral
                vts = 1.0
        
        # Cap at reasonable maximum for young tokens
            vts = min(vts, 5.0)
        
            logger.debug(
                f"🐣 VTS for young token {self.token_address[:8]}: {vts:.2f} "
                f"(age: {age_hours:.2f}h, using simplified calculation)"
            )
        
            return vts
    
    # For tokens 30+ minutes old, we can start using baseline comparisons
        self._update_baselines()
    
    # Get age-appropriate baselines with safety checks
        baseline_5m = max(self._get_baseline_volume(300), 0.1)  # Minimum baseline of $0.10
        baseline_15m = max(self._get_baseline_volume(900), 0.1)
        baseline_1h = max(self._get_baseline_volume(3600), 0.1)
        baseline_4h = max(self._get_baseline_volume(14400), 0.1)
        baseline_24h = max(self._get_baseline_volume(86400), 0.1)
    
    # Calculate ratios with safety caps
        ratio_5m = min(metrics_5m['total_volume'] / baseline_5m, 1000.0)  # Cap at 1000x
        ratio_15m = min(metrics_15m['total_volume'] / baseline_15m, 1000.0)
        ratio_1h = min(metrics_1h['total_volume'] / baseline_1h, 1000.0)
        ratio_4h = min(metrics_4h['total_volume'] / baseline_4h, 1000.0)
        ratio_24h = min(metrics_24h['total_volume'] / baseline_24h, 1000.0)

    # Age-based weighting (same as before)
        if age_hours < 1.0:
            vts = (ratio_5m * 0.7) + (ratio_15m * 0.3)
        elif age_hours < 6.0:
            vts = (ratio_5m * 0.3) + (ratio_15m * 0.4) + (ratio_1h * 0.3)
        elif age_hours < 24.0:
            vts = (ratio_5m * 0.15) + (ratio_15m * 0.20) + (ratio_1h * 0.35) + (ratio_4h * 0.30)
        else:
            vts = (
                (ratio_5m * 0.10) +
                (ratio_15m * 0.15) +
                (ratio_1h * 0.25) +
                (ratio_4h * 0.25) +
                (ratio_24h * 0.25)
            )

        if vts > 3.0:
            logger.info(
                f"🔥 High VTS detected on {self.token_address[:8]}: {vts:.2f} "
                f"(age: {age_hours:.1f}h, baselines: 5m=${baseline_5m:.2f}, 15m=${baseline_15m:.2f})"
            )

        return vts


    def _calculate_volume_exhaustion_index(self, metrics_1h: Dict) -> float:
        """Calculate Volume Exhaustion Index - unchanged from original."""
        current_hour_volume = metrics_1h['total_volume']
        
        if len(self.hourly_volume_history) < 2:
            return 1.0
        
        peak_hour_volume = max(self.hourly_volume_history)
        
        if peak_hour_volume <= 0:
            return 1.0
        
        vei = current_hour_volume / peak_hour_volume
        vei = max(0.0, min(1.0, vei))
        
        return vei


    def _take_snapshot(self):
        """
        Create a snapshot with age-appropriate metrics.
        
        ENHANCED: Now includes token age in the snapshot and uses appropriate
        baselines for all calculations.
        """
        current_time = time.time()
        age_hours = self.get_token_age_hours()
        
        self._update_baselines()
        
        # Calculate metrics for all windows (we always collect all data)
        metrics_1m = self._calculate_volume_metrics(self.trades_1m)
        metrics_5m = self._calculate_volume_metrics(self.trades_5m)
        metrics_15m = self._calculate_volume_metrics(self.trades_15m)
        metrics_1h = self._calculate_volume_metrics(self.trades_1h)
        metrics_4h = self._calculate_volume_metrics(self.trades_4h)
        metrics_24h = self._calculate_volume_metrics(self.trades_24h)
        
        # Current price
        current_price = self.trades_1m[-1].price if self.trades_1m else 0.0
        
        # Price changes
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
        
        # Buy/Sell Ratio
        bsr_1h = 1.0
        if metrics_1h['sell_volume'] > 0:
            bsr_1h = metrics_1h['buy_volume'] / metrics_1h['sell_volume']
        elif metrics_1h['buy_volume'] > 0:
            bsr_1h = 10.0
        
        # Volume/Liquidity Ratio
        vlr_1h = 0.0
        if self.liquidity_usd > 0:
            vlr_1h = metrics_1h['total_volume'] / self.liquidity_usd
        
        # Use age-appropriate VTS calculation
        vts = self._calculate_volume_trend_score(
            metrics_5m, metrics_15m, metrics_1h, metrics_4h, metrics_24h
        )
        
        # VEI
        vei = self._calculate_volume_exhaustion_index(metrics_1h)
        
        # Pressure Intensity Index
        net_pressure_1h = metrics_1h['buy_volume'] - metrics_1h['sell_volume']
        pii = 0.0
        if self.liquidity_usd > 0:
            pii = (net_pressure_1h / self.liquidity_usd) * vts
        
        # Phase classification
        phase = self._classify_phase(vlr_1h, vts, vei, pii, price_change_1h)
        
        # Create snapshot with age information
        snapshot = MetricsSnapshot(
            timestamp=current_time,
            token_age_hours=age_hours,  # NEW: Include age context
            volume_1m=metrics_1m['total_volume'],
            volume_5m=metrics_5m['total_volume'],
            volume_15m=metrics_15m['total_volume'],
            volume_1h=metrics_1h['total_volume'],
            volume_4h=metrics_4h['total_volume'],
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
        
        self.metric_history.append(snapshot)
        
        if len(self.metric_history) > 1440:
            self.metric_history.pop(0)
        
        logger.debug(
            f"📸 Snapshot: {self.token_address[:8]} (age: {age_hours:.1f}h) | "
            f"Phase={phase} VTS={vts:.2f} VEI={vei:.2f} PII={pii:.2f}"
        )


    def _classify_phase(self, vlr: float, vts: float, vei: float, 
                       pii: float, price_change: float) -> str:
        """Phase classification - unchanged from original."""
        if vlr < 0.2 and vts < 1.2:
            return 'dormant'
        
        if vts > 2.0 and vei > 0.7 and abs(price_change) > 5:
            return 'early'
        
        if vts > 1.3 and vei > 0.5 and abs(pii) > 0.3:
            return 'mid'
        
        if vts > 1.0 and vei < 0.5 and vei > 0.2:
            return 'late'
        
        if vei < 0.3:
            return 'exhaustion'
        
        return 'dormant'


    def get_current_metrics(self) -> MetricsSnapshot:
        """Get most recent metrics snapshot - unchanged."""
        if self.metric_history:
            return self.metric_history[-1]
        else:
            self._take_snapshot()
            return self.metric_history[-1] if self.metric_history else MetricsSnapshot(
                timestamp=time.time(),
                token_age_hours=self.get_token_age_hours()
            )


    def get_historical_snapshots(self, lookback_minutes: int = 60) -> List[MetricsSnapshot]:
        """Get historical snapshots - unchanged."""
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
