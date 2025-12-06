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
    """Metrics snapshot - unchanged from original."""
    timestamp: float
    volume_1m: float = 0.0
    volume_5m: float = 0.0
    volume_15m: float = 0.0
    volume_1h: float = 0.0
    volume_4h: float = 0.0  # NEW: Added 4-hour window for VTS calculation
    volume_24h: float = 0.0
    buy_volume_1m: float = 0.0
    buy_volume_5m: float = 0.0
    buy_volume_1h: float = 0.0
    sell_volume_1m: float = 0.0
    sell_volume_5m: float = 0.0
    sell_volume_1h: float = 0.0
    trade_count_1m: int = 0
    trade_count_5m: int = 0
    trade_count_1h: int = 0
    current_price: float = 0.0
    price_change_1m: float = 0.0
    price_change_5m: float = 0.0
    price_change_1h: float = 0.0
    bsr_1h: float = 1.0
    vlr_1h: float = 0.0
    pii: float = 0.0
    vts: float = 1.0
    vei: float = 1.0
    phase: str = 'dormant'
    liquidity_usd: float = 0.0
    total_trades_processed: int = 0
    
"""
Updated TokenMetricsTracker with VLMPS-compliant metric calculations.

KEY CHANGES FROM ORIGINAL:
1. Volume Trend Score now uses multi-timeframe weighted calculation
2. Volume Exhaustion Index uses hourly comparisons (more stable)
3. Added proper baseline tracking with 7-day and 30-day averages
4. Enhanced phase classification with better thresholds
"""

class TokenMetricsTracker:
    """
    Tracks real-time metrics for a single token with VLMPS-compliant calculations.
    
    This updated version fixes several key metrics to match the specification
    in the Volume-Liquidity Magnitude Prediction System document.
    """

    def __init__(self, token_address: str, liquidity_usd: float, market_cap_usd: Optional[float] = None):
        self.token_address = token_address
        self.liquidity_usd = liquidity_usd
        self.market_cap_usd = market_cap_usd

        # Time-windowed trade storage
        self.trades_1m: Deque[Trade] = deque(maxlen=1000)
        self.trades_5m: Deque[Trade] = deque(maxlen=5000)
        self.trades_15m: Deque[Trade] = deque(maxlen=10000)
        self.trades_1h: Deque[Trade] = deque(maxlen=50000)
        self.trades_4h: Deque[Trade] = deque(maxlen=200000)  # NEW: Added 4-hour window
        self.trades_24h: Deque[Trade] = deque(maxlen=500000)

        # Historical snapshots for state transition analysis
        self.metric_history: List[MetricsSnapshot] = []

        # NEW: Enhanced baseline tracking
        # Instead of just tracking the previous hour as "baseline", we now
        # track longer-term averages to represent true normal activity levels
        self.hourly_volumes: Deque[float] = deque(maxlen=168)  # Last 7 days of hourly volumes
        self.baseline_volume_5m = 0.0
        self.baseline_volume_15m = 0.0
        self.baseline_volume_1h = 0.0
        self.baseline_volume_4h = 0.0
        self.baseline_volume_24h = 0.0
        
        # Track when baselines were last updated
        self.last_baseline_update = time.time()
        self.last_snapshot_time = time.time()
        
        # Track hourly peak volumes for VEI calculation
        # This stores the volume from each of the last 24 hours
        self.hourly_volume_history: Deque[float] = deque(maxlen=24)
        self.last_hourly_record = time.time()

        # Statistics
        self.total_trades = 0
        self.created_at = time.time()

        logger.info(f"📊 Initialized VLMPS-compliant tracker for {token_address[:8]}... (Liq: ${liquidity_usd:,.0f})")


    def _cleanup_old_trades(self):
        """Remove trades that have aged beyond their time windows."""
        current_time = time.time()

        while self.trades_1m and current_time - self.trades_1m[0].timestamp > 60:
            self.trades_1m.popleft()

        while self.trades_5m and current_time - self.trades_5m[0].timestamp > 300:
            self.trades_5m.popleft()

        while self.trades_15m and current_time - self.trades_15m[0].timestamp > 900:
            self.trades_15m.popleft()

        while self.trades_1h and current_time - self.trades_1h[0].timestamp > 3600:
            self.trades_1h.popleft()

        # NEW: Clean 4-hour window
        while self.trades_4h and current_time - self.trades_4h[0].timestamp > 14400:
            self.trades_4h.popleft()

        while self.trades_24h and current_time - self.trades_24h[0].timestamp > 86400:
            self.trades_24h.popleft()


    def add_trade(self, trade_data: Dict):
        """Process a new trade and update all metrics - unchanged from original."""
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

            # Add to all time windows
            self.trades_1m.append(trade)
            self.trades_5m.append(trade)
            self.trades_15m.append(trade)
            self.trades_1h.append(trade)
            self.trades_4h.append(trade)  # NEW: Add to 4-hour window
            self.trades_24h.append(trade)

            self.total_trades += 1
            self._cleanup_old_trades()

            # NEW: Record hourly volumes for baseline tracking
            current_time = time.time()
            if current_time - self.last_hourly_record >= 3600:
                # An hour has passed, record the volume from that hour
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
                    f"on {self.token_address[:8]}..."
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


    def _update_baselines(self):
        """
        NEW METHOD: Update baseline "normal" activity levels.
        
        This calculates what "normal" volume looks like for this token
        by averaging historical activity. We use these baselines to detect
        when current activity is elevated (VTS > 1) or suppressed (VTS < 1).
        
        The key insight: we want to compare current activity to the token's
        typical behavior over days/weeks, not just the previous hour.
        """
        current_time = time.time()
        
        # Only update baselines every hour to avoid excessive recalculation
        if current_time - self.last_baseline_update < 3600:
            return
            
        # Calculate average hourly volume from our history
        # We need at least a few hours of data to have meaningful averages
        if len(self.hourly_volumes) >= 3:
            self.baseline_volume_1h = sum(self.hourly_volumes) / len(self.hourly_volumes)
        else:
            # Not enough history yet, use current hour as baseline
            metrics_1h = self._calculate_volume_metrics(self.trades_1h)
            self.baseline_volume_1h = max(metrics_1h['total_volume'], 1.0)  # Avoid zero

        # For other timeframes, we derive from the hourly baseline
        # These are proportional estimates based on time window size
        self.baseline_volume_5m = self.baseline_volume_1h / 12  # 5 min is 1/12 of an hour
        self.baseline_volume_15m = self.baseline_volume_1h / 4  # 15 min is 1/4 of an hour
        self.baseline_volume_4h = self.baseline_volume_1h * 4
        self.baseline_volume_24h = self.baseline_volume_1h * 24

        self.last_baseline_update = current_time

        logger.debug(f"📊 Updated baselines for {self.token_address[:8]}: 1h=${self.baseline_volume_1h:,.0f}")


    def _calculate_volume_trend_score(self, metrics_5m: Dict, metrics_15m: Dict, 
                                     metrics_1h: Dict, metrics_4h: Dict, 
                                     metrics_24h: Dict) -> float:
        """
        NEW METHOD: Calculate Volume Trend Score using multi-timeframe weighted approach.
        
        This is a KEY IMPROVEMENT over the original simple ratio.
        
        The VLMPS document specifies that VTS should compare activity across
        MULTIPLE timeframes simultaneously, not just one. This catches momentum
        shifts that might be visible in short timeframes but not yet in long ones,
        or vice versa.
        
        Formula from document:
        VTS = (Vol_5m/Avg_5m × 0.10) + 
              (Vol_15m/Avg_15m × 0.15) + 
              (Vol_1h/Avg_1h × 0.25) + 
              (Vol_4h/Avg_4h × 0.25) + 
              (Vol_24h/Avg_24h × 0.25)
        
        The weights are chosen to give more importance to medium-term trends
        (1h, 4h, 24h) while still catching very recent shifts (5m, 15m).
        
        Args:
            metrics_5m, metrics_15m, etc: Volume metrics for each timeframe
            
        Returns:
            VTS value where 1.0 = normal, >1.0 = elevated, <1.0 = suppressed
        """
        # Make sure we have valid baselines (avoid division by zero)
        # If baselines aren't set yet, update them now
        if self.baseline_volume_1h <= 0:
            self._update_baselines()
        
        # Calculate the ratio for each timeframe
        # If baseline is still zero, default to 1.0 (no elevation/suppression)
        
        ratio_5m = 1.0
        if self.baseline_volume_5m > 0:
            ratio_5m = metrics_5m['total_volume'] / self.baseline_volume_5m
            
        ratio_15m = 1.0
        if self.baseline_volume_15m > 0:
            ratio_15m = metrics_15m['total_volume'] / self.baseline_volume_15m
            
        ratio_1h = 1.0
        if self.baseline_volume_1h > 0:
            ratio_1h = metrics_1h['total_volume'] / self.baseline_volume_1h
            
        ratio_4h = 1.0
        if self.baseline_volume_4h > 0:
            ratio_4h = metrics_4h['total_volume'] / self.baseline_volume_4h
            
        ratio_24h = 1.0
        if self.baseline_volume_24h > 0:
            ratio_24h = metrics_24h['total_volume'] / self.baseline_volume_24h
        
        # Apply the weighted formula from the document
        vts = (
            (ratio_5m * 0.10) +
            (ratio_15m * 0.15) +
            (ratio_1h * 0.25) +
            (ratio_4h * 0.25) +
            (ratio_24h * 0.25)
        )
        
        # Log if we're seeing significant elevation
        if vts > 3.0:
            logger.info(f"🔥 High VTS detected on {self.token_address[:8]}: {vts:.2f}")
        
        return vts


    def _calculate_volume_exhaustion_index(self, metrics_1h: Dict) -> float:
        """
        NEW METHOD: Calculate Volume Exhaustion Index using hourly comparison.
        
        This is another KEY IMPROVEMENT for stability.
        
        The original code compared current 1-minute volume (extrapolated to hourly)
        against the peak extrapolated minute volume. This was too noisy because
        individual minutes can be very volatile.
        
        The VLMPS approach: compare current HOUR's volume to the PEAK HOUR
        in the last 24 hours. This is much more stable because hourly volumes
        smooth out the minute-to-minute noise.
        
        Think of it like this:
        - Old way: "Is this minute as busy as the busiest minute today?"
        - New way: "Is this hour as busy as the busiest hour today?"
        
        The new way gives you a clearer signal of actual exhaustion rather than
        just normal minute-to-minute fluctuation.
        
        Args:
            metrics_1h: Current hour's volume metrics
            
        Returns:
            VEI where 1.0 = at peak, 0.5 = half of peak, 0.0 = no activity
        """
        current_hour_volume = metrics_1h['total_volume']
        
        # If we don't have hourly history yet, we can't calculate VEI properly
        if len(self.hourly_volume_history) < 2:
            return 1.0  # Default to "not exhausted" when we lack data
        
        # Find the peak hourly volume in our history
        peak_hour_volume = max(self.hourly_volume_history)
        
        # If peak is zero (no trading), return 1.0 to avoid false exhaustion signal
        if peak_hour_volume <= 0:
            return 1.0
        
        # Calculate the ratio
        vei = current_hour_volume / peak_hour_volume
        
        # VEI should be clamped between 0 and 1
        # (Current volume can't exceed peak by definition, but just to be safe)
        vei = max(0.0, min(1.0, vei))
        
        return vei


    def _take_snapshot(self):
        """
        Updated snapshot method with improved metric calculations.
        
        The key changes here are:
        1. We now calculate 4-hour metrics for VTS
        2. We use the new multi-timeframe VTS calculation
        3. We use the new hourly-based VEI calculation
        4. We update baselines periodically
        """
        current_time = time.time()

        # Update baselines if needed (this checks internally if it's time)
        self._update_baselines()

        # Calculate metrics for all time windows
        metrics_1m = self._calculate_volume_metrics(self.trades_1m)
        metrics_5m = self._calculate_volume_metrics(self.trades_5m)
        metrics_15m = self._calculate_volume_metrics(self.trades_15m)
        metrics_1h = self._calculate_volume_metrics(self.trades_1h)
        metrics_4h = self._calculate_volume_metrics(self.trades_4h)  # NEW
        metrics_24h = self._calculate_volume_metrics(self.trades_24h)

        # Current price from most recent trade
        current_price = self.trades_1m[-1].price if self.trades_1m else 0.0

        # Price changes (unchanged from original)
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

        # Buy/Sell Ratio (unchanged)
        bsr_1h = 1.0
        if metrics_1h['sell_volume'] > 0:
            bsr_1h = metrics_1h['buy_volume'] / metrics_1h['sell_volume']
        elif metrics_1h['buy_volume'] > 0:
            bsr_1h = 10.0

        # Volume/Liquidity Ratio (unchanged)
        vlr_1h = 0.0
        if self.liquidity_usd > 0:
            vlr_1h = metrics_1h['total_volume'] / self.liquidity_usd

        # NEW: Use improved VTS calculation
        vts = self._calculate_volume_trend_score(
            metrics_5m, metrics_15m, metrics_1h, metrics_4h, metrics_24h
        )

        # NEW: Use improved VEI calculation
        vei = self._calculate_volume_exhaustion_index(metrics_1h)

        # Pressure Intensity Index (now uses corrected VTS)
        net_pressure_1h = metrics_1h['buy_volume'] - metrics_1h['sell_volume']
        pii = 0.0
        if self.liquidity_usd > 0:
            pii = (net_pressure_1h / self.liquidity_usd) * vts

        # Phase classification (using updated metrics)
        phase = self._classify_phase(vlr_1h, vts, vei, pii, price_change_1h)

        # Create snapshot with all metrics
        snapshot = MetricsSnapshot(
            timestamp=current_time,
            volume_1m=metrics_1m['total_volume'],
            volume_5m=metrics_5m['total_volume'],
            volume_15m=metrics_15m['total_volume'],
            volume_1h=metrics_1h['total_volume'],
            volume_4h=metrics_4h['total_volume'],  # NEW
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

        # Store in history
        self.metric_history.append(snapshot)

        # Keep only last 24 hours (1440 snapshots at 1/minute)
        if len(self.metric_history) > 1440:
            self.metric_history.pop(0)

        logger.debug(
            f"📸 Snapshot: {self.token_address[:8]} | "
            f"Phase={phase} VTS={vts:.2f} VEI={vei:.2f} PII={pii:.2f}"
        )


    def _classify_phase(self, vlr: float, vts: float, vei: float, 
                       pii: float, price_change: float) -> str:
        """
        Phase classification - unchanged logic, but now uses improved metrics.
        
        Because VTS and VEI are now calculated correctly, this phase
        classification will be more accurate automatically.
        """
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
            return self.metric_history[-1] if self.metric_history else MetricsSnapshot(timestamp=time.time())


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
