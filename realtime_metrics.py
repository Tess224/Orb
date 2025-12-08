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
import math

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
    conviction_multiplier: float = 1.0  # NEW: How strong is the volume conviction?
    conviction_weighted_pressure: float = 0.0  # NEW: Pressure adjusted for conviction
    size_entropy: float = 0.0  # NEW: How diverse are trade sizes?
    large_trade_pct: float = 0.0  # NEW: Percentage of volume in large trades
    
    # Phase classification
    phase: str = 'dormant'

    # NEW FIELDS - add these at the end, before the metadata section
    vts_raw: float = 0.0  # The raw VTS before bounding
    vts_is_extreme: bool = False  # Flag if VTS suggests data problems
    vts_explanation: str = "normal"  # Human-readable explanation
    
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


    def _get_baseline_confidence_and_value(self, timeframe_seconds: int) -> Tuple[float, float, str]:
        """
        Get a baseline value AND tell us how confident we should be in it.
    
        The key insight: a baseline calculated from 2 observations is not as reliable
        as one calculated from 100 observations. We need to know the difference.
    
        Think of it like weather forecasting. A forecast based on 50 years of data
        for this date is more reliable than one based on just last year's weather.
    
        Args:
            timeframe_seconds: The window size we want a baseline for (300 for 5min, etc.)
        
        Returns:
            A tuple of (confidence, baseline_value, status_message)
            - confidence: 0.0 to 1.0, where 1.0 means fully reliable
            - baseline_value: the actual baseline to use in calculations
            - status_message: human-readable explanation
        """
        age_hours = self.get_token_age_hours()
        timeframe_hours = timeframe_seconds / 3600.0
    
    # How many complete windows of this timeframe do we need to trust the baseline?
    # These numbers are based on statistical principles - you need multiple observations
    # to establish a reliable average
        min_windows_needed = {
            300: 6,      # 5-minute windows: need 30 minutes (6 complete windows)
            900: 4,      # 15-minute windows: need 1 hour (4 complete windows)
            3600: 4,     # 1-hour windows: need 4 hours (4 complete windows)
            14400: 3,    # 4-hour windows: need 12 hours (3 complete windows)
            86400: 7     # 24-hour windows: need 7 days (7 complete windows)
        }
    
        min_windows = min_windows_needed.get(timeframe_seconds, 4)
    
    # How many windows do we actually have?
        windows_available = age_hours / timeframe_hours
    
    # Calculate confidence based on how many windows we've observed
        if windows_available < 1:
        # We don't even have one complete window yet
            confidence = 0.0
            status = "insufficient_data"
        
        elif windows_available < min_windows:
        # We have some windows, but not enough for full confidence
        # Confidence scales linearly from 0 to 1 as we collect more windows
        # For example, if we need 6 windows and have 3, confidence is 0.5
            confidence = windows_available / min_windows
            status = "building_baseline"
        
        else:
        # We have enough windows - full confidence
            confidence = 1.0
            status = "baseline_reliable"
    
    # Get the actual baseline value using our existing method
        baseline = self._get_baseline_volume(timeframe_seconds)
    
    # Apply our smart floor calculation to prevent tiny denominators
        window_name_map = {
            300: '5m',
            900: '15m', 
            3600: '1h',
            14400: '4h',
            86400: '24h'
        }
        window_name = window_name_map.get(timeframe_seconds, '1h')
        safe_floor = self._calculate_safe_baseline_floor(window_name)
    
    # Use whichever is larger - the calculated baseline or our safety floor
        baseline = max(baseline, safe_floor)
    
    # Log this so we can see what's happening during debugging
        logger.debug(
            f"Baseline for {timeframe_seconds}s window: ${baseline:.2f}, "
            f"confidence: {confidence:.2f} ({status}), "
            f"windows: {windows_available:.1f} of {min_windows} needed"
        )
    
        return confidence, baseline, status


    def _update_vts_percentiles(self):
        """
        Calculate what VTS scores have historically looked like for this token.
    
        We're building a statistical profile that says "for this token, 99% of the time,
        VTS has been below X." This lets us know when we're seeing something truly unusual
        versus just normal variation.
    
        Think of it like tracking your daily step count. After a few months, you know that
        99% of days you walk between 3,000 and 8,000 steps. If you suddenly walk 25,000 steps,
        you know that's genuinely unusual, not just a bit more than average.
    
        We cache this calculation because computing percentiles is slow. We only recalculate
        once per hour, which is plenty frequent for this purpose.
        """
        current_time = time.time()
    
    # Have we updated recently? If so, skip the expensive calculation
        if current_time - self.vts_percentiles_cache['last_update'] < 3600:
            return
    
    # Do we have enough history to calculate meaningful percentiles?
    # We need at least 100 data points for percentiles to be meaningful
        if len(self.vts_history) < 100:
        # Not enough history yet - stick with conservative default
            self.vts_percentiles_cache['99th'] = 15.0
            logger.debug(
                f"VTS percentiles: using default (only {len(self.vts_history)} samples)"
            )
        else:
        # We have enough history - calculate the actual 99th percentile
            sorted_vts = sorted(self.vts_history)
        
        # The 99th percentile means 99% of values are below this point
            idx_99 = int(len(sorted_vts) * 0.99)
            p99 = sorted_vts[idx_99]
        
        # Our dynamic cap is the larger of:
        # - A fixed minimum of 15 (reasonable max for normal activity)
        # - 120% of the 99th percentile (allows some headroom above typical spikes)
            dynamic_cap = max(15.0, p99 * 1.2)
        
            self.vts_percentiles_cache['99th'] = dynamic_cap
        
            logger.debug(
                f"VTS percentiles updated for {self.token_address[:8]}: "
                f"99th percentile = {p99:.2f}, dynamic cap = {dynamic_cap:.2f} "
                f"(based on {len(self.vts_history)} samples)"
            )
    
    # Mark that we've updated
        self.vts_percentiles_cache['last_update'] = current_time


    def _apply_vts_bounds(self, raw_vts: float) -> Tuple[float, bool, str]:
        """
        Take a raw VTS score and apply intelligent limits to prevent absurd values.
    
        This is our final safety net. Even if everything else goes wrong and we calculate
        a VTS of 10,000, this method will catch it and apply reasonable bounds.
    
        We do three things here:
        1. Cap the VTS at our dynamic maximum (based on historical behavior)
        2. Detect if the score is so extreme it suggests a data problem
        3. Record the raw value for our historical analysis
    
        Think of this like a circuit breaker in your home's electrical panel. Normal current
        flows through fine, but if there's a dangerous surge, the breaker trips to prevent
        damage. We're doing the same for VTS scores.
    
        Args:
            raw_vts: The VTS value we calculated from the formulas
          
        Returns:
            A tuple of (bounded_vts, is_extreme, explanation)
            - bounded_vts: The safe value to actually use
            - is_extreme: True if this looks like a data anomaly
            - explanation: Human-readable description of what happened
        """
    # First, make sure our percentile cache is up to date
        self._update_vts_percentiles()
    
    # Get our dynamic cap based on historical behavior
        dynamic_cap = self.vts_percentiles_cache['99th']
    
    # Apply the cap to get our bounded value
    # This is what we'll actually use in downstream calculations
        capped_vts = min(raw_vts, dynamic_cap)
    
    # Is this score so extreme it suggests something is broken?
    # If the raw score is 5x our cap, that's almost certainly a bug, not real activity
    # For example, if the cap is 15, a raw VTS of 75 is suspicious
        is_extreme = (raw_vts / dynamic_cap) >= 5.0
    
    # Build a human-readable explanation of what we did
        if is_extreme:
            explanation = (
                f"EXTREME_ANOMALY: raw VTS of {raw_vts:.1f} is "
                f"{raw_vts/dynamic_cap:.1f}x the dynamic cap of {dynamic_cap:.1f}. "
                f"This likely indicates a data quality issue."
            )
            logger.warning(f"⚠️ {explanation} for {self.token_address[:8]}")
        
        elif raw_vts > dynamic_cap:
            explanation = (
                f"CAPPED: raw VTS of {raw_vts:.1f} exceeded dynamic cap of {dynamic_cap:.1f}"
            )
            logger.info(f"📊 {explanation} for {self.token_address[:8]}")
        
        else:
        # Normal case - no capping needed
            explanation = "normal"
    
    # Add the raw value to our history
    # This is important - we track the raw values, not the capped ones
    # This way our percentile calculations reflect actual behavior
        self.vts_history.append(raw_vts)
    
        return capped_vts, is_extreme, explanation

    def _calculate_early_stage_vts(self, metrics_5m: Dict, metrics_15m: Dict) -> float:
        """
        Calculate VTS for tokens we've only been tracking for a short time.
    
        When we've only been tracking for minutes, we can't do meaningful comparisons
        to historical baselines that don't exist. Instead, we use a simpler approach:
        compare very recent activity to slightly less recent activity.
    
        It's like judging if a party is getting more crowded by comparing how many people
        arrived in the last 5 minutes versus the rate people were arriving earlier.
    
        Args:
            metrics_5m: Volume metrics for the last 5 minutes
            metrics_15m: Volume metrics for the last 15 minutes
        
        Returns:
            A simplified VTS score between 0.5 and 5.0
        """
        vol_5m = metrics_5m['total_volume']
        vol_15m = metrics_15m['total_volume']
    
    # Do we have any data at all?
        if len(self.trades_15m) < 2:
        # Almost no data - return neutral score
            return 1.0
    
    # Calculate the average 5-minute volume from the 15-minute window
    # If volume is steady, this should equal the most recent 5 minutes
    # If volume is accelerating, the recent 5 minutes will be higher
        vol_15m_per_5min = vol_15m / 3.0
    
    # Avoid division by zero
        if vol_15m_per_5min < 0.1:
        # Very low volume overall - use a small baseline
            vol_15m_per_5min = 0.1
    
    # Calculate the ratio
        vts = vol_5m / vol_15m_per_5min
    
    # For early stage, cap at 5.0
    # We don't have enough data to confidently say something is higher than that
        vts = min(vts, 5.0)
    
    # Also apply a floor of 0.5
    # VTS below 0.5 suggests volume is declining, but for very young tokens
    # we don't have enough context to be confident about that
        vts = max(vts, 0.5)
    
        logger.debug(
            f"Early-stage VTS for {self.token_address[:8]}: {vts:.2f} "
            f"(5m: ${vol_5m:.2f}, 15m avg per 5m: ${vol_15m_per_5min:.2f})"
        )
    
        return {
            'vts': vts,
            'vts_raw': vts,
            'is_extreme': False,
            'explanation': 'early_stage_simplified_calculation'
            }

    
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
        
        Now includes comprehensive validation to catch bad data before it corrupts our metrics.
        """
    # VALIDATION FIRST - before we do anything else
        is_valid, error = self._validate_trade_data(trade_data)
    
        if not is_valid:
        # This trade has problems - reject it and log why
            logger.warning(
                f"⚠️ Rejected invalid trade for {self.token_address[:8]}: {error}"
            )
        
        # Track statistics about rejected trades
            self.stats['rejected_trades'] = self.stats.get('rejected_trades', 0) + 1
        
        # Track what types of errors we're seeing
            if error not in self.stats['validation_errors']:
                self.stats['validation_errors'][error] = 0
            self.stats['validation_errors'][error] += 1
        
        # Don't process this trade further
            return
    
    # Validation passed - proceed with your existing logic
        try:
            logger.info(f"✅ TRADE ACCEPTED for {self.token_address[:8]}: ${trade_data['size_usd']:.2f} {trade_data['direction'].upper()}")
    
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
                logger.info(f"📸 TAKING SNAPSHOT for {self.token_address[:8]} (age: {self.get_token_age_hours():.1f}h)")
                self._take_snapshot()
                self.last_snapshot_time = current_time
                logger.info(f"✅ SNAPSHOT COMPLETE")
                
            if trade.size_usd >= 1000:
                logger.info(
                    f"💰 Large {trade.direction.upper()}: ${trade.size_usd:,.0f} "
                    f"on {self.token_address[:8]}... (age: {self.get_token_age_hours():.1f}h)"
                )
        
        except Exception as e:
            logger.error(f"❌ Error processing trade: {e}")


    def _calculate_volume_metrics(self, trades: Deque[Trade]) -> Dict:
        """
        Calculate volume metrics with trade size distribution analysis.
    
        We're now tracking not just total volumes, but how those volumes are
        distributed across different trade sizes. This helps us distinguish between
        organic activity (many diverse trades) and artificial activity (concentrated
        in specific size brackets).
        """
        total_volume = 0.0
        buy_volume = 0.0
        sell_volume = 0.0
        trade_count = len(trades)
    
    # Count trades by direction
        buy_count = 0
        sell_count = 0
    
    # Track trade size distribution
    # We categorize trades into four buckets based on size
        size_buckets = {
            'micro': 0.0,    # Under $100
            'small': 0.0,    # $100 to $1,000
            'medium': 0.0,   # $1,000 to $10,000
            'large': 0.0     # $10,000 and above
        }
    
    # Also track count of trades in each bucket for entropy calculation
        size_bucket_counts = {
            'micro': 0,
            'small': 0,
            'medium': 0,
            'large': 0
        }

        for trade in trades:
            total_volume += trade.size_usd
          
        # Track buy/sell breakdown
            if trade.direction == 'buy':
                buy_volume += trade.size_usd
                buy_count += 1
            else:
                sell_volume += trade.size_usd
                sell_count += 1
            
        # Categorize by size
            if trade.size_usd < 100:
                size_buckets['micro'] += trade.size_usd
                size_bucket_counts['micro'] += 1
            elif trade.size_usd < 1000:
                size_buckets['small'] += trade.size_usd
                size_bucket_counts['small'] += 1
            elif trade.size_usd < 10000:
                size_buckets['medium'] += trade.size_usd
                size_bucket_counts['medium'] += 1
            else:
                size_buckets['large'] += trade.size_usd
                size_bucket_counts['large'] += 1
    
    # Calculate percentage distribution of volume across buckets
        size_distribution = {}
        if total_volume > 0:
            for bucket, volume in size_buckets.items():
                size_distribution[f'{bucket}_pct'] = (volume / total_volume) * 100
        else:
        # No volume - all percentages are zero
            for bucket in size_buckets.keys():
                size_distribution[f'{bucket}_pct'] = 0.0
    
    # Calculate distribution entropy
    # Entropy measures how evenly spread the trades are across size buckets
    # High entropy = trades distributed across many sizes (organic)
    # Low entropy = trades concentrated in one or two sizes (potentially artificial)
        import math
        entropy = 0.0
    
        if trade_count > 0:
            for count in size_bucket_counts.values():
                if count > 0:
                    proportion = count / trade_count
                # Shannon entropy formula
                    entropy -= proportion * math.log2(proportion)
    
    # Normalize entropy to 0-1 scale
    # Maximum entropy for 4 buckets is log2(4) = 2.0
        max_entropy = 2.0
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0.0

        return {
            'total_volume': total_volume,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'trade_count': trade_count,
            'buy_count': buy_count,
            'sell_count': sell_count,
        # Size distribution percentages
            'micro_pct': size_distribution['micro_pct'],
            'small_pct': size_distribution['small_pct'],
            'medium_pct': size_distribution['medium_pct'],
            'large_pct': size_distribution['large_pct'],
        # Entropy metric
            'size_entropy': normalized_entropy
        }

    def _calculate_conviction_multiplier(self, metrics: Dict) -> float:
        """
        Calculate how much to amplify or dampen pressure based on trade conviction.
    
        This is where we distinguish between strong, organic volume and weak,
        potentially artificial volume. We look at two main factors:
    
        1. Volume-Count Alignment: Do volume and trade count tell the same story?
        2. Size Distribution: Are trades diverse or concentrated?
    
        The multiplier ranges from 0.5 (weak conviction) to 2.0 (strong conviction),
        with 1.0 being neutral.
    
        Args:
            metrics: The output from _calculate_volume_metrics containing all our data
        
        Returns:
            A multiplier to apply to pressure calculations
        """
    # Start with neutral multiplier
        conviction = 1.0
    
    # FACTOR ONE: Volume-Count Alignment
    # Calculate imbalance ratios for both volume and trade count
    
        buy_volume = metrics['buy_volume']
        sell_volume = metrics['sell_volume']
        buy_count = metrics['buy_count']
        sell_count = metrics['sell_count']
    
    # Volume imbalance ratio (-1 to +1)
    # Positive means more buying, negative means more selling
        total_volume = buy_volume + sell_volume
        if total_volume > 0:
            volume_imbalance = (buy_volume - sell_volume) / total_volume
        else:
            volume_imbalance = 0.0
    
    # Count imbalance ratio (-1 to +1)
        total_count = buy_count + sell_count
        if total_count > 0:
            count_imbalance = (buy_count - sell_count) / total_count
        else:
            count_imbalance = 0.0
    
    # When volume and count imbalances align strongly in the same direction,
    # that indicates genuine conviction. We multiply them together.
    # If both are strongly positive or both strongly negative, the product
    # will be large and positive. If they disagree, the product will be small.
        alignment_score = abs(volume_imbalance * count_imbalance)
    
    # Scale alignment into a multiplier component (0.5 to 1.5)
    # Strong alignment adds up to +0.5, weak alignment subtracts up to -0.5
        alignment_multiplier = 1.0 + (alignment_score * 0.5)
    
        logger.debug(
            f"Conviction alignment for {self.token_address[:8]}: "
            f"vol_imbalance={volume_imbalance:.2f}, count_imbalance={count_imbalance:.2f}, "
            f"alignment_score={alignment_score:.2f}, multiplier={alignment_multiplier:.2f}"
        )
    
    # FACTOR TWO: Size Distribution Quality
    # Use the entropy metric we calculated
    
        size_entropy = metrics['size_entropy']
    
    # High entropy (close to 1.0) suggests organic, diverse trading
    # Low entropy (close to 0.0) suggests concentrated, potentially artificial trading
    
    # We also want to penalize situations where large trades dominate
    # A healthy market has a mix, but too much concentration in large trades is suspicious
        large_pct = metrics['large_pct']
    
    # Calculate a distribution quality score
        if size_entropy > 0.7:
        # High entropy - trades are well distributed
        # This is good, adds a bonus
            distribution_multiplier = 1.0 + (size_entropy * 0.3)
        elif size_entropy < 0.3:
        # Very low entropy - trades are heavily concentrated
        # This is suspicious, apply a penalty
            distribution_multiplier = 0.7 + (size_entropy * 0.3)
        else:
        # Medium entropy - neutral
            distribution_multiplier = 1.0
    
    # Additional check: if large trades are more than 60% of volume, that's suspicious
        if large_pct > 60:
        # The more concentrated in large trades, the bigger the penalty
            concentration_penalty = 1.0 - ((large_pct - 60) / 100)
            distribution_multiplier *= concentration_penalty
        
            logger.debug(
                f"Large trade concentration detected: {large_pct:.1f}% in large trades, "
                f"applying penalty multiplier of {concentration_penalty:.2f}"
            )
    
        logger.debug(
            f"Distribution quality for {self.token_address[:8]}: "
            f"entropy={size_entropy:.2f}, large_pct={large_pct:.1f}%, "
            f"multiplier={distribution_multiplier:.2f}"
       )
    
    # COMBINE THE FACTORS
    # Multiply alignment and distribution components together
        conviction = alignment_multiplier * distribution_multiplier
    
    # Apply final bounds to prevent extreme values
    # We allow conviction to range from 0.5 (very weak) to 2.0 (very strong)
        conviction = max(0.5, min(2.0, conviction))
    
        if conviction > 1.3 or conviction < 0.7:
            logger.info(
                f"🎯 Notable conviction score for {self.token_address[:8]}: {conviction:.2f} "
                f"(alignment={alignment_multiplier:.2f}, distribution={distribution_multiplier:.2f})"
            )
    
        return conviction
    

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
        Calculate Volume Trend Score with proper baseline handling and confidence weighting.
    
        This is the master method that brings together all our fixes:
        - Smart baseline floors that adapt to token behavior
        - Confidence-based weighting that respects data quality
        - Individual ratio caps to prevent any one window from dominating
        - Final score bounding to catch edge cases
    
        The result is a VTS that stays in interpretable ranges (typically 0.5 to 15.0)
        while still detecting genuine unusual activity.
        """
        age_hours = self.get_token_age_hours()
    
    # SPECIAL CASE: Very young tokens (under 30 minutes)
    # We don't have enough history for proper baseline comparisons yet
    # Use the simplified calculation instead
        if age_hours < 0.5:
                # Use simplified calculation for very young tokens
                return self._calculate_early_stage_vts(metrics_5m, metrics_15m)
    
    # NORMAL CASE: We have enough history to do proper calculations
    
    # Get baselines with confidence scores for each timeframe
    # Each of these returns (confidence, baseline_value, status_message)
        conf_5m, base_5m, status_5m = self._get_baseline_confidence_and_value(300)
        conf_15m, base_15m, status_15m = self._get_baseline_confidence_and_value(900)
        conf_1h, base_1h, status_1h = self._get_baseline_confidence_and_value(3600)
        conf_4h, base_4h, status_4h = self._get_baseline_confidence_and_value(14400)
        conf_24h, base_24h, status_24h = self._get_baseline_confidence_and_value(86400)
    
    # Calculate raw ratios with individual caps
    # We cap each ratio at 100x to prevent any single window from creating absurd values
    # A 100x spike in volume is already extremely unusual - anything beyond that
    # is more likely data corruption than reality
        ratio_5m = min(metrics_5m['total_volume'] / base_5m, 100.0)
        ratio_15m = min(metrics_15m['total_volume'] / base_15m, 100.0)
        ratio_1h = min(metrics_1h['total_volume'] / base_1h, 100.0)
        ratio_4h = min(metrics_4h['total_volume'] / base_4h, 100.0)
        ratio_24h = min(metrics_24h['total_volume'] / base_24h, 100.0)
    
    # Determine base weights based on token age
    # Younger tokens rely more on shorter windows because that's all we can trust
    # Mature tokens use a balanced mix of all timeframes
        if age_hours < 1.0:
        # Under 1 hour: focus heavily on 5m and 15m windows
            base_weights = {'5m': 0.7, '15m': 0.3, '1h': 0.0, '4h': 0.0, '24h': 0.0}
        elif age_hours < 6.0:
        # 1-6 hours: start incorporating 1h window
            base_weights = {'5m': 0.3, '15m': 0.4, '1h': 0.3, '4h': 0.0, '24h': 0.0}
        elif age_hours < 24.0:
   # 6-24 hours: add 4h window but not 24h yet
            base_weights = {'5m': 0.15, '15m': 0.20, '1h': 0.35, '4h': 0.30, '24h': 0.0}
        else:
        # Mature token: use all windows
            base_weights = {'5m': 0.10, '15m': 0.15, '1h': 0.25, '4h': 0.25, '24h': 0.25}
    
    # Now adjust these base weights by confidence scores
    # If a window's baseline is unreliable, we reduce its contribution
    # This prevents low-confidence baselines from distorting the score
    
        confidence_map = {
            '5m': conf_5m,
            '15m': conf_15m,
            '1h': conf_1h,
            '4h': conf_4h,
            '24h': conf_24h
        }
    
    # Multiply each base weight by its confidence
        adjusted_weights = {}
        total_conf_weighted = 0.0
     
        for window, base_weight in base_weights.items():
            conf = confidence_map[window]
        # Confidence-adjusted weight
            adjusted_weights[window] = base_weight * conf
            total_conf_weighted += adjusted_weights[window]
    
    # Normalize weights so they sum to 1.0
    # This is important - we're redistributing the weight from low-confidence
    # windows to high-confidence windows
        if total_conf_weighted > 0:
            for window in adjusted_weights:
                adjusted_weights[window] /= total_conf_weighted
        else:
        # Edge case: all confidences are zero (shouldn't happen, but handle it)
        # Fall back to equal weighting of available windows
            adjusted_weights = base_weights
    
    # Calculate the weighted VTS
        raw_vts = (
            (ratio_5m * adjusted_weights['5m']) +
            (ratio_15m * adjusted_weights['15m']) +
            (ratio_1h * adjusted_weights['1h']) +
            (ratio_4h * adjusted_weights['4h']) +
            (ratio_24h * adjusted_weights['24h'])
        )
    
    # Apply intelligent bounding
        bounded_vts, is_extreme, explanation = self._apply_vts_bounds(raw_vts)
    
    # Log if we see anything interesting
        if bounded_vts > 3.0 or is_extreme:
            logger.info(
                f"🔥 VTS for {self.token_address[:8]}: {bounded_vts:.2f} "
                f"(raw: {raw_vts:.2f}, age: {age_hours:.1f}h) - {explanation}"
            )
            logger.debug(
                f"   Ratios: 5m={ratio_5m:.1f}, 15m={ratio_15m:.1f}, "
                f"1h={ratio_1h:.1f}, 4h={ratio_4h:.1f}, 24h={ratio_24h:.1f}"
            )
            logger.debug(
                f"   Confidences: 5m={conf_5m:.2f}, 15m={conf_15m:.2f}, "
                f"1h={conf_1h:.2f}, 4h={conf_4h:.2f}, 24h={conf_24h:.2f}"
            )
            logger.debug(
                f"   Final weights: 5m={adjusted_weights['5m']:.2f}, "
                f"15m={adjusted_weights['15m']:.2f}, 1h={adjusted_weights['1h']:.2f}, "
                f"4h={adjusted_weights['4h']:.2f}, 24h={adjusted_weights['24h']:.2f}"
            )
    
    # Return a dictionary with all the information about this VTS calculation
        return {
            'vts': bounded_vts,
            'vts_raw': raw_vts,
            'is_extreme': is_extreme,
            'explanation': explanation
        }
                    
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
        
        # Calculate VTS using our comprehensive method
        # This returns a dictionary with vts, vts_raw, is_extreme, and explanation
        vts_result = self._calculate_volume_trend_score(
            metrics_5m, metrics_15m, metrics_1h, metrics_4h, metrics_24h
        )
    
        # Extract the components
        vts = vts_result['vts']
        vts_raw = vts_result['vts_raw']
        vts_is_extreme = vts_result['is_extreme']
        vts_explanation = vts_result['explanation']
            
        # VEI
        vei = self._calculate_volume_exhaustion_index(metrics_1h)
        
        # Pressure Intensity Index with Conviction Weighting
        net_pressure_1h = metrics_1h['buy_volume'] - metrics_1h['sell_volume']

        # Calculate conviction multiplier based on trade quality
        conviction_multiplier = self._calculate_conviction_multiplier(metrics_1h)

        # Calculate standard PII (unchanged)
        pii = 0.0
        if self.liquidity_usd > 0:
            pii = (net_pressure_1h / self.liquidity_usd) * vts

        # Calculate conviction-weighted pressure
        # This is PII adjusted for the quality of the volume
        conviction_weighted_pressure = pii * conviction_multiplier

        # Extract additional metrics for the snapshot
        size_entropy = metrics_1h['size_entropy']
        large_trade_pct = metrics_1h['large_pct']
        
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
            vts_raw=vts_raw,  # NEW
            vts_is_extreme=vts_is_extreme,  # NEW
            vts_explanation=vts_explanation,  # NEW
            vei=vei,
            conviction_multiplier=conviction_multiplier,
            conviction_weighted_pressure=conviction_weighted_pressure,
            size_entropy=size_entropy,
            large_trade_pct=large_trade_pct,
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
    
        Enhanced with detailed error logging to catch any issues from our
        new conviction-weighted pressure calculations.
        """
        try:
            token_address = trade_data.get('token_address')

            if not token_address:
                logger.warning("⚠️ Trade data missing token_address")
                return

            if token_address in self.trackers:
                try:
                    self.trackers[token_address].add_trade(trade_data)
                except Exception as trade_error:
                    logger.error(f"❌ Error processing trade for {token_address[:8]}: {trade_error}")
                    import traceback
                    logger.error(f"   Traceback: {traceback.format_exc()}")
            else:
                logger.debug(f"ℹ️ Received trade for untracked token {token_address[:8]}...")
            
        except Exception as e:
            logger.error(f"❌ Critical error in handle_trade: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
    
    
    
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
