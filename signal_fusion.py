"""
Signal Fusion System

This module combines signals from two independent detection systems:
1. Real-time metrics (volume, VTS, PII, phase detection)
2. Slippage analysis (liquidity structure, asymmetry, honeypot detection)

The key insight: when both systems agree, confidence increases significantly.
When they disagree, we investigate why and adjust accordingly.

Think of it like having two independent witnesses to an event. If both tell
the same story, you can be more confident it's true. If they disagree,
something interesting is happening that deserves attention.
"""

import logging
import time
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class SignalDirection(Enum):
    """The direction a signal is pointing."""
    STRONG_BULLISH = "strong_bullish"
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"
    STRONG_BEARISH = "strong_bearish"
    DANGER = "danger"  # Honeypot, rug, etc.


class SignalUrgency(Enum):
    """How quickly action should be taken."""
    IMMEDIATE = "immediate"      # Act within minutes
    SHORT_TERM = "short_term"    # Act within the hour
    MEDIUM_TERM = "medium_term"  # Monitor over hours
    LOW = "low"                  # No urgency


@dataclass
class MetricsSignal:
    """
    Extracted signal from the real-time metrics system.
    
    This takes the raw MetricsSnapshot and distills it into
    a directional signal with confidence.
    """
    direction: SignalDirection
    confidence: float  # 0.0 to 1.0
    phase: str
    vts: float
    pii: float
    vei: float
    conviction_multiplier: float
    volume_trend: str  # "increasing", "stable", "decreasing"
    pressure_direction: str  # "buying", "neutral", "selling"
    key_factors: List[str] = field(default_factory=list)


@dataclass
class SlippageSignal:
    """
    Extracted signal from the slippage analysis system.
    
    This takes the raw slippage analysis result and distills it
    into a directional signal with confidence.
    """
    direction: SignalDirection
    confidence: float  # 0.0 to 1.0
    state: str  # PRE_PUMP, PRE_DUMP, HOLDING, etc.
    asymmetry_ratio: float
    is_honeypot: bool
    manipulation_detected: bool
    liquidity_health: str  # "healthy", "degrading", "toxic"
    key_factors: List[str] = field(default_factory=list)


@dataclass
class FusedSignal:
    """
    The combined signal from both systems.
    
    This is what your trading logic should use for decisions.
    """
    # Overall assessment
    direction: SignalDirection
    confidence: float  # 0.0 to 1.0
    urgency: SignalUrgency
    
    # Action recommendation
    action: str  # Human-readable recommendation
    action_code: str  # Machine-readable: BUY, SELL, HOLD, AVOID, EXIT
    
    # Agreement analysis
    systems_agree: bool
    agreement_strength: float  # -1.0 (opposite) to 1.0 (perfect agreement)
    disagreement_reason: Optional[str]
    
    # Component signals (for transparency)
    metrics_signal: Optional[MetricsSignal]
    slippage_signal: Optional[SlippageSignal]
    
    # Risk assessment
    risk_level: str  # "low", "medium", "high", "extreme"
    risk_factors: List[str] = field(default_factory=list)
    
    # Metadata
    token_address: str = ""
    timestamp: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'direction': self.direction.value,
            'confidence': round(self.confidence, 3),
            'urgency': self.urgency.value,
            'action': self.action,
            'action_code': self.action_code,
            'systems_agree': self.systems_agree,
            'agreement_strength': round(self.agreement_strength, 3),
            'disagreement_reason': self.disagreement_reason,
            'risk_level': self.risk_level,
            'risk_factors': self.risk_factors,
            'metrics_signal': {
                'direction': self.metrics_signal.direction.value,
                'confidence': round(self.metrics_signal.confidence, 3),
                'phase': self.metrics_signal.phase,
                'vts': round(self.metrics_signal.vts, 2),
                'pii': round(self.metrics_signal.pii, 3),
                'volume_trend': self.metrics_signal.volume_trend,
                'pressure_direction': self.metrics_signal.pressure_direction,
                'key_factors': self.metrics_signal.key_factors
            } if self.metrics_signal else None,
            'slippage_signal': {
                'direction': self.slippage_signal.direction.value,
                'confidence': round(self.slippage_signal.confidence, 3),
                'state': self.slippage_signal.state,
                'asymmetry_ratio': round(self.slippage_signal.asymmetry_ratio, 2),
                'is_honeypot': self.slippage_signal.is_honeypot,
                'manipulation_detected': self.slippage_signal.manipulation_detected,
                'liquidity_health': self.slippage_signal.liquidity_health,
                'key_factors': self.slippage_signal.key_factors
            } if self.slippage_signal else None,
            'token_address': self.token_address,
            'timestamp': self.timestamp
        }


class SignalExtractor:
    """
    Extracts normalized signals from raw analysis outputs.
    
    This class is responsible for taking the complex outputs from your
    two analysis systems and converting them into simple, comparable
    signal objects.
    """
    
    def extract_metrics_signal(self, metrics_snapshot) -> Optional[MetricsSignal]:
        """
        Extract a directional signal from a MetricsSnapshot.
        
        We look at multiple indicators and combine them into a single
        directional assessment with confidence.
        """
        if not metrics_snapshot:
            return None
        
        try:
            # Extract raw values
            phase = metrics_snapshot.phase
            vts = metrics_snapshot.vts
            pii = metrics_snapshot.pii
            vei = metrics_snapshot.vei
            bsr = metrics_snapshot.bsr_1h
            conviction = metrics_snapshot.conviction_multiplier
            
            # Track what factors are driving our signal
            key_factors = []
            
            # Determine volume trend
            # VTS > 1.5 means volume is elevated compared to baseline
            if vts > 2.0:
                volume_trend = "increasing_fast"
                key_factors.append(f"Volume surge (VTS={vts:.1f})")
            elif vts > 1.3:
                volume_trend = "increasing"
                key_factors.append(f"Volume rising (VTS={vts:.1f})")
            elif vts < 0.7:
                volume_trend = "decreasing"
                key_factors.append(f"Volume declining (VTS={vts:.1f})")
            else:
                volume_trend = "stable"
            
            # Determine pressure direction from PII and BSR
            if pii > 0.3 and bsr > 1.5:
                pressure_direction = "strong_buying"
                key_factors.append(f"Strong buy pressure (PII={pii:.2f}, BSR={bsr:.1f})")
            elif pii > 0.1 and bsr > 1.2:
                pressure_direction = "buying"
                key_factors.append(f"Buy pressure (PII={pii:.2f})")
            elif pii < -0.3 and bsr < 0.7:
                pressure_direction = "strong_selling"
                key_factors.append(f"Strong sell pressure (PII={pii:.2f}, BSR={bsr:.1f})")
            elif pii < -0.1 and bsr < 0.9:
                pressure_direction = "selling"
                key_factors.append(f"Sell pressure (PII={pii:.2f})")
            else:
                pressure_direction = "neutral"
            
            # Check conviction quality
            if conviction > 1.3:
                key_factors.append(f"High conviction ({conviction:.2f})")
            elif conviction < 0.7:
                key_factors.append(f"Low conviction ({conviction:.2f}) - possibly artificial")
            
            # Check exhaustion
            if vei < 0.3:
                key_factors.append(f"Volume exhausted (VEI={vei:.2f})")
            
            # Determine overall direction
            direction, confidence = self._calculate_metrics_direction(
                phase, vts, pii, vei, bsr, conviction, volume_trend, pressure_direction
            )
            
            return MetricsSignal(
                direction=direction,
                confidence=confidence,
                phase=phase,
                vts=vts,
                pii=pii,
                vei=vei,
                conviction_multiplier=conviction,
                volume_trend=volume_trend,
                pressure_direction=pressure_direction,
                key_factors=key_factors
            )
            
        except Exception as e:
            logger.error(f"Error extracting metrics signal: {e}")
            return None
    
    def _calculate_metrics_direction(
        self, 
        phase: str, 
        vts: float, 
        pii: float, 
        vei: float,
        bsr: float,
        conviction: float,
        volume_trend: str,
        pressure_direction: str
    ) -> Tuple[SignalDirection, float]:
        """
        Calculate the overall direction and confidence from metrics.
        
        This is where we apply the decision logic to convert raw numbers
        into a directional signal.
        """
        # Start with base confidence
        confidence = 0.5
        
        # Score accumulator: positive = bullish, negative = bearish
        score = 0.0
        
        # Phase contribution
        phase_scores = {
            'early': 0.4,      # Early phase is bullish
            'mid': 0.2,        # Mid phase is slightly bullish
            'late': -0.2,      # Late phase is slightly bearish
            'exhaustion': -0.5, # Exhaustion is bearish
            'dormant': 0.0     # Dormant is neutral
        }
        score += phase_scores.get(phase, 0.0)
        
        # Pressure contribution (PII is already directional)
        # Scale PII contribution - it ranges roughly -1 to 1
        score += pii * 0.5
        
        # Volume trend contribution
        if volume_trend == "increasing_fast":
            # High volume amplifies the pressure direction
            if pressure_direction in ["strong_buying", "buying"]:
                score += 0.3
                confidence += 0.1
            elif pressure_direction in ["strong_selling", "selling"]:
                score -= 0.3
                confidence += 0.1
        elif volume_trend == "decreasing":
            # Decreasing volume reduces conviction
            confidence -= 0.1
        
        # Conviction quality affects confidence
        if conviction > 1.2:
            confidence += 0.1
        elif conviction < 0.8:
            confidence -= 0.15
        
        # Exhaustion is a strong bearish signal
        if vei < 0.3:
            score -= 0.3
            confidence += 0.1  # We're more confident about exhaustion
        
        # BSR extreme values
        if bsr > 2.0:
            score += 0.2
        elif bsr < 0.5:
            score -= 0.2
        
        # Convert score to direction
        if score > 0.5:
            direction = SignalDirection.STRONG_BULLISH
        elif score > 0.2:
            direction = SignalDirection.BULLISH
        elif score < -0.5:
            direction = SignalDirection.STRONG_BEARISH
        elif score < -0.2:
            direction = SignalDirection.BEARISH
        else:
            direction = SignalDirection.NEUTRAL
        
        # Clamp confidence to valid range
        confidence = max(0.2, min(0.95, confidence))
        
        return direction, confidence
    
    def extract_slippage_signal(self, slippage_analysis: Dict) -> Optional[SlippageSignal]:
        """
        Extract a directional signal from slippage analysis results.
        
        The slippage analysis tells us about the structure of liquidity
        and whether it's set up to trap buyers or sellers.
        """
        if not slippage_analysis:
            return None
        
        try:
            # Extract raw values
            state = slippage_analysis.get('state', 'UNCERTAIN')
            scores = slippage_analysis.get('scores', {})
            pump_score = scores.get('pre_pump_score', 0)
            dump_score = scores.get('pre_dump_score', 0)
            
            asymmetry = slippage_analysis.get('slippage_data', {}).get('asymmetry', {})
            avg_asymmetry = asymmetry.get('average_ratio', 1.0)
            
            patterns = slippage_analysis.get('signals', [])
            is_honeypot = slippage_analysis.get('is_honeypot', False) or 'HONEYPOT' in state
            
            # Check for manipulation
            manipulation_detected = any(
                'FORTRESS' in str(p) or 'CLIFF' in str(p) or 'MANIPULATION' in str(p)
                for p in patterns
            )
            
            # Track key factors
            key_factors = []
            
            # Assess liquidity health
            if is_honeypot:
                liquidity_health = "toxic"
                key_factors.append("HONEYPOT DETECTED - Cannot sell")
            elif avg_asymmetry > 2.5:
                liquidity_health = "toxic"
                key_factors.append(f"Severe asymmetry ({avg_asymmetry:.1f}x)")
            elif avg_asymmetry > 1.8:
                liquidity_health = "degrading"
                key_factors.append(f"High asymmetry ({avg_asymmetry:.1f}x)")
            elif avg_asymmetry < 0.6:
                liquidity_health = "favorable"
                key_factors.append(f"Favorable asymmetry ({avg_asymmetry:.1f}x)")
            else:
                liquidity_health = "healthy"
            
            # Add pattern-based factors
            for pattern in patterns[:3]:  # Top 3 patterns
                key_factors.append(pattern)
            
            # Calculate direction and confidence
            direction, confidence = self._calculate_slippage_direction(
                state, pump_score, dump_score, avg_asymmetry, is_honeypot, manipulation_detected
            )
            
            return SlippageSignal(
                direction=direction,
                confidence=confidence,
                state=state,
                asymmetry_ratio=avg_asymmetry,
                is_honeypot=is_honeypot,
                manipulation_detected=manipulation_detected,
                liquidity_health=liquidity_health,
                key_factors=key_factors
            )
            
        except Exception as e:
            logger.error(f"Error extracting slippage signal: {e}")
            return None
    
    def _calculate_slippage_direction(
        self,
        state: str,
        pump_score: float,
        dump_score: float,
        asymmetry: float,
        is_honeypot: bool,
        manipulation_detected: bool
    ) -> Tuple[SignalDirection, float]:
        """
        Calculate direction and confidence from slippage analysis.
        """
        # Honeypot is always DANGER with high confidence
        if is_honeypot:
            return SignalDirection.DANGER, 0.95
        
        # Start with the explicit state from slippage analysis
        if state == 'PRE_DUMP' or state == 'PRE_DUMP_HONEYPOT':
            if dump_score >= 80:
                return SignalDirection.DANGER, 0.85
            else:
                return SignalDirection.STRONG_BEARISH, 0.70
        
        elif state == 'PRE_PUMP':
            confidence = 0.5 + (pump_score / 200)  # Scale pump_score to confidence boost
            if pump_score >= 60:
                return SignalDirection.STRONG_BULLISH, min(0.85, confidence)
            else:
                return SignalDirection.BULLISH, min(0.75, confidence)
        
        elif state == 'HOLDING':
            return SignalDirection.NEUTRAL, 0.60
        
        # For uncertain states, use asymmetry as a guide
        else:
            if asymmetry > 2.0:
                # High asymmetry favoring sellers = bearish
                return SignalDirection.BEARISH, 0.55
            elif asymmetry < 0.5:
                # Low asymmetry favoring buyers = bullish
                return SignalDirection.BULLISH, 0.55
            else:
                return SignalDirection.NEUTRAL, 0.40


class SignalFusion:
    """
    The main fusion engine that combines signals from both systems.
    
    This is where the magic happens. We take two independent signals
    and produce a single, more reliable combined signal.
    """
    
    def __init__(self):
        self.extractor = SignalExtractor()
        
        # Weights for combining signals
        # These can be tuned based on backtesting results
        self.metrics_weight = 0.45
        self.slippage_weight = 0.55  # Slippage slightly higher because it's more direct
        
        logger.info("🔗 Signal Fusion engine initialized")
    
    def fuse_signals(
        self,
        token_address: str,
        metrics_snapshot,  # MetricsSnapshot from your realtime_metrics.py
        slippage_analysis: Dict  # Result from analyze_slippage_patterns + classify_market_state
    ) -> FusedSignal:
        """
        Combine signals from both systems into a single fused signal.
        
        This is the main method you'll call from your Flask endpoints.
        
        Args:
            token_address: The token being analyzed
            metrics_snapshot: Output from metrics_manager.get_metrics()
            slippage_analysis: Output from your /analyze endpoint
            
        Returns:
            FusedSignal with combined assessment
        """
        logger.info(f"🔗 Fusing signals for {token_address[:8]}...")
        
        # Extract normalized signals from both systems
        metrics_signal = self.extractor.extract_metrics_signal(metrics_snapshot)
        slippage_signal = self.extractor.extract_slippage_signal(slippage_analysis)
        
        # Handle cases where one or both signals are missing
        if not metrics_signal and not slippage_signal:
            return self._create_insufficient_data_signal(token_address)
        
        if not metrics_signal:
            return self._create_single_source_signal(token_address, None, slippage_signal, "metrics")
        
        if not slippage_signal:
            return self._create_single_source_signal(token_address, metrics_signal, None, "slippage")
        
        # Both signals available - do full fusion
        return self._fuse_both_signals(token_address, metrics_signal, slippage_signal)
    
    def _fuse_both_signals(
        self,
        token_address: str,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal
    ) -> FusedSignal:
        """
        Perform full fusion when both signals are available.
        
        This is where we get the best results - two independent systems
        providing corroborating or conflicting information.
        """
        # Check for DANGER signals first - these override everything
        if slippage_signal.direction == SignalDirection.DANGER:
            return self._create_danger_signal(token_address, metrics_signal, slippage_signal)
        
        # Calculate agreement between the two systems
        agreement_strength = self._calculate_agreement(metrics_signal, slippage_signal)
        systems_agree = agreement_strength > 0.3
        
        # Determine the fused direction
        fused_direction, fused_confidence = self._calculate_fused_direction(
            metrics_signal, slippage_signal, agreement_strength
        )
        
        # Determine urgency
        urgency = self._calculate_urgency(metrics_signal, slippage_signal, fused_direction)
        
        # Calculate risk
        risk_level, risk_factors = self._assess_risk(metrics_signal, slippage_signal)
        
        # Generate action recommendation
        action, action_code = self._generate_action(
            fused_direction, fused_confidence, risk_level, systems_agree
        )
        
        # Generate disagreement explanation if needed
        disagreement_reason = None
        if not systems_agree:
            disagreement_reason = self._explain_disagreement(metrics_signal, slippage_signal)
        
        return FusedSignal(
            direction=fused_direction,
            confidence=fused_confidence,
            urgency=urgency,
            action=action,
            action_code=action_code,
            systems_agree=systems_agree,
            agreement_strength=agreement_strength,
            disagreement_reason=disagreement_reason,
            metrics_signal=metrics_signal,
            slippage_signal=slippage_signal,
            risk_level=risk_level,
            risk_factors=risk_factors,
            token_address=token_address,
            timestamp=time.time()
        )
    
    def _calculate_agreement(
        self,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal
    ) -> float:
        """
        Calculate how much the two signals agree.
        
        Returns a value from -1.0 (complete disagreement) to 1.0 (perfect agreement).
        
        Agreement means both pointing the same direction with similar confidence.
        Disagreement means pointing opposite directions.
        """
        # Convert directions to numeric scores
        direction_scores = {
            SignalDirection.STRONG_BULLISH: 1.0,
            SignalDirection.BULLISH: 0.5,
            SignalDirection.NEUTRAL: 0.0,
            SignalDirection.BEARISH: -0.5,
            SignalDirection.STRONG_BEARISH: -1.0,
            SignalDirection.DANGER: -1.0
        }
        
        metrics_score = direction_scores.get(metrics_signal.direction, 0.0)
        slippage_score = direction_scores.get(slippage_signal.direction, 0.0)
        
        # If both are pointing the same direction, agreement is positive
        # If opposite directions, agreement is negative
        
        # Method: correlation-like calculation
        # Same direction same magnitude = 1.0
        # Opposite directions = -1.0
        # One neutral = partial agreement toward the non-neutral one
        
        if metrics_score == 0.0 and slippage_score == 0.0:
            # Both neutral = moderate agreement
            return 0.5
        
        if metrics_score == 0.0 or slippage_score == 0.0:
            # One neutral = partial agreement
            return 0.3
        
        # Both have direction - check if same or opposite
        same_sign = (metrics_score > 0 and slippage_score > 0) or (metrics_score < 0 and slippage_score < 0)
        
        if same_sign:
            # Agreement - scale by magnitude similarity
            magnitude_diff = abs(abs(metrics_score) - abs(slippage_score))
            agreement = 1.0 - magnitude_diff
            return agreement
        else:
            # Disagreement - scale by how opposite they are
            total_magnitude = abs(metrics_score) + abs(slippage_score)
            disagreement = -1.0 * (total_magnitude / 2.0)
            return disagreement
    
    def _calculate_fused_direction(
        self,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal,
        agreement_strength: float
    ) -> Tuple[SignalDirection, float]:
        """
        Calculate the combined direction and confidence.
        
        When systems agree: boost confidence
        When systems disagree: reduce confidence, lean toward higher-confidence signal
        """
        # Convert to numeric for weighted combination
        direction_values = {
            SignalDirection.STRONG_BULLISH: 1.0,
            SignalDirection.BULLISH: 0.5,
            SignalDirection.NEUTRAL: 0.0,
            SignalDirection.BEARISH: -0.5,
            SignalDirection.STRONG_BEARISH: -1.0,
            SignalDirection.DANGER: -1.2  # Extra negative for danger
        }
        
        metrics_value = direction_values[metrics_signal.direction]
        slippage_value = direction_values[slippage_signal.direction]
        
        # Weighted combination
        # But also weight by each signal's own confidence
        metrics_weighted = metrics_value * metrics_signal.confidence * self.metrics_weight
        slippage_weighted = slippage_value * slippage_signal.confidence * self.slippage_weight
        
        combined_value = metrics_weighted + slippage_weighted
        
        # Normalize to account for weights
        normalization = (metrics_signal.confidence * self.metrics_weight + 
                        slippage_signal.confidence * self.slippage_weight)
        
        if normalization > 0:
            combined_value = combined_value / normalization
        
        # Convert back to direction
        if combined_value > 0.6:
            direction = SignalDirection.STRONG_BULLISH
        elif combined_value > 0.25:
            direction = SignalDirection.BULLISH
        elif combined_value < -0.6:
            direction = SignalDirection.STRONG_BEARISH
        elif combined_value < -0.25:
            direction = SignalDirection.BEARISH
        else:
            direction = SignalDirection.NEUTRAL
        
        # Calculate fused confidence
        base_confidence = (
            metrics_signal.confidence * self.metrics_weight +
            slippage_signal.confidence * self.slippage_weight
        )
        
        # Agreement boosts confidence, disagreement reduces it
        if agreement_strength > 0.5:
            # Strong agreement - boost confidence
            confidence_boost = 0.15
        elif agreement_strength > 0:
            # Moderate agreement - small boost
            confidence_boost = 0.05
        elif agreement_strength > -0.5:
            # Moderate disagreement - reduce confidence
            confidence_boost = -0.15
        else:
            # Strong disagreement - significantly reduce confidence
            confidence_boost = -0.25
        
        fused_confidence = base_confidence + confidence_boost
        fused_confidence = max(0.15, min(0.95, fused_confidence))
        
        return direction, fused_confidence
    
    def _calculate_urgency(
        self,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal,
        fused_direction: SignalDirection
    ) -> SignalUrgency:
        """
        Determine how urgent action is.
        
        High urgency when:
        - Strong directional signal
        - High VTS (volume surge)
        - Manipulation detected
        - Honeypot or danger signals
        """
        # Danger always immediate
        if fused_direction == SignalDirection.DANGER:
            return SignalUrgency.IMMEDIATE
        
        if slippage_signal.is_honeypot or slippage_signal.manipulation_detected:
            return SignalUrgency.IMMEDIATE
        
        # High VTS = something is happening now
        if metrics_signal.vts > 3.0:
            return SignalUrgency.IMMEDIATE
        elif metrics_signal.vts > 2.0:
            return SignalUrgency.SHORT_TERM
        
        # Strong directional signals
        if fused_direction in [SignalDirection.STRONG_BULLISH, SignalDirection.STRONG_BEARISH]:
            return SignalUrgency.SHORT_TERM
        
        if fused_direction in [SignalDirection.BULLISH, SignalDirection.BEARISH]:
            return SignalUrgency.MEDIUM_TERM
        
        return SignalUrgency.LOW
    
    def _assess_risk(
        self,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal
    ) -> Tuple[str, List[str]]:
        """
        Assess the overall risk level and identify specific risk factors.
        """
        risk_factors = []
        risk_score = 0  # 0-100 scale
        
        # Honeypot = extreme risk
        if slippage_signal.is_honeypot:
            risk_factors.append("🚨 HONEYPOT - Cannot sell tokens")
            risk_score += 50
        
        # Manipulation = high risk
        if slippage_signal.manipulation_detected:
            risk_factors.append("⚠️ Active manipulation detected")
            risk_score += 25
        
        # Toxic liquidity
        if slippage_signal.liquidity_health == "toxic":
            risk_factors.append("☠️ Toxic liquidity structure")
            risk_score += 20
        elif slippage_signal.liquidity_health == "degrading":
            risk_factors.append("📉 Liquidity degrading")
            risk_score += 10
        
        # Low conviction
        if metrics_signal.conviction_multiplier < 0.7:
            risk_factors.append("🤖 Low conviction - possibly artificial volume")
            risk_score += 15
        
        # Exhaustion
        if metrics_signal.vei < 0.3:
            risk_factors.append("😤 Volume exhausted - limited upside potential")
            risk_score += 10
        
        # High asymmetry
        if slippage_signal.asymmetry_ratio > 2.0:
            risk_factors.append(f"⚖️ High slippage asymmetry ({slippage_signal.asymmetry_ratio:.1f}x)")
            risk_score += 15
        
        # Late phase
        if metrics_signal.phase in ['late', 'exhaustion']:
            risk_factors.append(f"📊 {metrics_signal.phase.title()} phase - momentum fading")
            risk_score += 10
        
        # Convert score to level
        if risk_score >= 50:
            risk_level = "extreme"
        elif risk_score >= 30:
            risk_level = "high"
        elif risk_score >= 15:
            risk_level = "medium"
        else:
            risk_level = "low"
        
        return risk_level, risk_factors
    
    def _generate_action(
        self,
        direction: SignalDirection,
        confidence: float,
        risk_level: str,
        systems_agree: bool
    ) -> Tuple[str, str]:
        """
        Generate human-readable action and machine-readable action code.
        """
        # Risk overrides direction
        if risk_level == "extreme":
            return "🛑 DO NOT BUY - Extreme risk detected", "AVOID"
        
        if risk_level == "high" and direction not in [SignalDirection.STRONG_BULLISH]:
            return "⚠️ HIGH RISK - Proceed with extreme caution", "AVOID"
        
        # Direction-based actions
        if direction == SignalDirection.DANGER:
            return "🚨 DANGER - Exit immediately if holding", "EXIT"
        
        if direction == SignalDirection.STRONG_BEARISH:
            if confidence > 0.7:
                return "📉 STRONG SELL SIGNAL - Consider exiting", "SELL"
            else:
                return "📉 Bearish setup - Monitor for exit", "SELL"
        
        if direction == SignalDirection.BEARISH:
            return "⬇️ Bearish pressure - Not recommended for entry", "AVOID"
        
        if direction == SignalDirection.STRONG_BULLISH:
            if systems_agree and confidence > 0.7:
                return "🚀 STRONG BUY SIGNAL - Both systems confirm", "BUY"
            elif confidence > 0.6:
                return "📈 Bullish setup - Consider entry with stop loss", "BUY"
            else:
                return "📈 Potentially bullish - Wait for confirmation", "HOLD"
        
        if direction == SignalDirection.BULLISH:
            if systems_agree:
                return "👀 Accumulation possible - Monitor closely", "HOLD"
            else:
                return "🤔 Mixed signals - Wait for clarity", "HOLD"
        
        # Neutral
        return "⏸️ No clear signal - Continue monitoring", "HOLD"
    
    def _explain_disagreement(
        self,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal
    ) -> str:
        """
        Generate a human-readable explanation of why the systems disagree.
        
        This helps users understand what's happening and make informed decisions.
        """
        metrics_bullish = metrics_signal.direction in [SignalDirection.STRONG_BULLISH, SignalDirection.BULLISH]
        slippage_bullish = slippage_signal.direction in [SignalDirection.STRONG_BULLISH, SignalDirection.BULLISH]
        
        if metrics_bullish and not slippage_bullish:
            # Metrics positive, slippage negative
            return (
                f"Real-time metrics show {metrics_signal.pressure_direction} pressure "
                f"(VTS={metrics_signal.vts:.1f}), but liquidity structure shows "
                f"{slippage_signal.liquidity_health} health with {slippage_signal.asymmetry_ratio:.1f}x asymmetry. "
                f"This could indicate a liquidity trap where buying looks attractive but selling will be difficult."
            )
        
        elif not metrics_bullish and slippage_bullish:
            # Slippage positive, metrics negative
            return (
                f"Liquidity structure looks favorable ({slippage_signal.state}), "
                f"but real-time flow shows {metrics_signal.pressure_direction} pressure "
                f"and {metrics_signal.volume_trend} volume. "
                f"The opportunity may exist but current momentum doesn't support it."
            )
        
        else:
            # Both bearish but different severity, or other cases
            return (
                f"Systems showing different severity: "
                f"Metrics={metrics_signal.direction.value} (conf={metrics_signal.confidence:.0%}), "
                f"Slippage={slippage_signal.direction.value} (conf={slippage_signal.confidence:.0%}). "
                f"Wait for clearer alignment."
            )
    
    def _create_danger_signal(
        self,
        token_address: str,
        metrics_signal: MetricsSignal,
        slippage_signal: SlippageSignal
    ) -> FusedSignal:
        """
        Create a fused signal for DANGER situations (honeypots, rugs, etc.)
        """
        risk_factors = ["🚨 CRITICAL: Danger signal from liquidity analysis"]
        
        if slippage_signal.is_honeypot:
            risk_factors.append("Cannot sell - tokens will be trapped")
        
        risk_factors.extend(slippage_signal.key_factors)
        
        return FusedSignal(
            direction=SignalDirection.DANGER,
            confidence=0.95,
            urgency=SignalUrgency.IMMEDIATE,
            action="🛑 DANGER - DO NOT BUY / EXIT IMMEDIATELY",
            action_code="EXIT",
            systems_agree=True,  # Danger overrides metrics
            agreement_strength=1.0,
            disagreement_reason=None,
            metrics_signal=metrics_signal,
            slippage_signal=slippage_signal,
            risk_level="extreme",
            risk_factors=risk_factors,
            token_address=token_address,
            timestamp=time.time()
        )
    
    def _create_insufficient_data_signal(self, token_address: str) -> FusedSignal:
        """Create a signal when no data is available."""
        return FusedSignal(
            direction=SignalDirection.NEUTRAL,
            confidence=0.0,
            urgency=SignalUrgency.LOW,
            action="❓ Insufficient data - Cannot generate signal",
            action_code="WAIT",
            systems_agree=False,
            agreement_strength=0.0,
            disagreement_reason="Neither metrics nor slippage data available",
            metrics_signal=None,
            slippage_signal=None,
            risk_level="unknown",
            risk_factors=["No data available for analysis"],
            token_address=token_address,
            timestamp=time.time()
        )
    
    def _create_single_source_signal(
        self,
        token_address: str,
        metrics_signal: Optional[MetricsSignal],
        slippage_signal: Optional[SlippageSignal],
        missing_source: str
    ) -> FusedSignal:
        """Create a signal when only one source is available."""
        if metrics_signal:
            # Only metrics available
            return FusedSignal(
                direction=metrics_signal.direction,
                confidence=metrics_signal.confidence * 0.7,  # Reduce confidence
                urgency=SignalUrgency.MEDIUM_TERM,
                action=f"⚠️ Partial signal (no slippage data) - {metrics_signal.direction.value}",
                action_code="HOLD",
                systems_agree=False,
                agreement_strength=0.0,
                disagreement_reason=f"Slippage analysis not available - signal based only on real-time metrics",
                metrics_signal=metrics_signal,
                slippage_signal=None,
                risk_level="medium",
                risk_factors=["Missing slippage analysis - liquidity structure unknown"],
                token_address=token_address,
                timestamp=time.time()
            )
        else:
            # Only slippage available
            return FusedSignal(
                direction=slippage_signal.direction,
                confidence=slippage_signal.confidence * 0.7,  # Reduce confidence
                urgency=SignalUrgency.MEDIUM_TERM if slippage_signal.direction != SignalDirection.DANGER else SignalUrgency.IMMEDIATE,
                action=f"⚠️ Partial signal (no metrics) - {slippage_signal.direction.value}",
                action_code="HOLD" if slippage_signal.direction != SignalDirection.DANGER else "EXIT",
                systems_agree=False,
                agreement_strength=0.0,
                disagreement_reason=f"Real-time metrics not available - signal based only on slippage analysis",
                metrics_signal=None,
                slippage_signal=slippage_signal,
                risk_level="medium" if not slippage_signal.is_honeypot else "extreme",
                risk_factors=["Missing real-time metrics - current momentum unknown"],
                token_address=token_address,
                timestamp=time.time()
            )


# Create a global instance for use throughout the application
signal_fusion = SignalFusion()