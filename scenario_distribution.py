"""
Scenario Distribution System using Monte Carlo Simulation

This system generates probability ranges for token metrics by simulating
thousands of possible futures based on historical patterns. Instead of saying
"volume will be 5000", it says "there's a 50% chance volume will be between
4000-6000, and a 95% chance it will be between 3000-8000".

The core insight: markets are uncertain. Single-point predictions are misleading.
Probability distributions are honest about what we know and don't know.
"""

import logging
import random
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import math

logger = logging.getLogger(__name__)


@dataclass
class ScenarioResult:
    """
    Represents the outcome of a single Monte Carlo simulation.
    
    This is a lightweight structure that captures the key metrics from one
    possible future. We'll generate thousands of these and then aggregate them
    to understand the probability distribution.
    """
    # Projected metrics after the time horizon
    volume_1h: float = 0.0
    bsr_1h: float = 1.0
    vlr_1h: float = 0.0
    vts: float = 1.0
    pii: float = 0.0
    price_change_pct: float = 0.0
    
    # What phase did we end up in?
    final_phase: str = 'dormant'
    
    # Did a phase transition occur during this simulation?
    transition_occurred: bool = False
    transition_to: Optional[str] = None
    
    # Metadata about this scenario
    scenario_id: int = 0
    confidence_weight: float = 1.0  # How much to trust this scenario (0-1)


@dataclass
class ProbabilityBands:
    """
    Statistical summary of many scenarios, showing probability ranges.
    
    This is what makes Monte Carlo useful - instead of one prediction,
    you get a range of predictions with confidence levels attached.
    """
    metric_name: str
    
    # Percentile values (50th percentile = median)
    p10: float = 0.0  # 10th percentile - only 10% of scenarios below this
    p25: float = 0.0  # 25th percentile - first quartile
    p50: float = 0.0  # 50th percentile - median, most likely outcome
    p75: float = 0.0  # 75th percentile - third quartile
    p90: float = 0.0  # 90th percentile - only 10% of scenarios above this
    
    # Confidence bands
    band_50_lower: float = 0.0  # Lower bound of 50% confidence band
    band_50_upper: float = 0.0  # Upper bound of 50% confidence band
    band_90_lower: float = 0.0  # Lower bound of 90% confidence band
    band_90_upper: float = 0.0  # Upper bound of 90% confidence band
    
    # Summary statistics
    mean: float = 0.0
    std_dev: float = 0.0
    
    # How many scenarios contributed to this
    sample_count: int = 0
    
    # Overall confidence in these predictions (0-1)
    # Higher means more historical data supported these scenarios
    confidence_score: float = 0.0


@dataclass
class ScenarioDistribution:
    """
    Complete probabilistic prediction for a token's future state.
    
    This is the main output of the Monte Carlo system - a comprehensive
    view of what might happen, with probabilities attached.
    """
    token_address: str
    current_phase: str
    projection_minutes: int  # How far into the future we projected
    
    # Probability bands for each key metric
    volume_distribution: ProbabilityBands
    bsr_distribution: ProbabilityBands
    vts_distribution: ProbabilityBands
    pii_distribution: ProbabilityBands
    price_change_distribution: ProbabilityBands
    
    # Phase transition probabilities
    # Example: {'mid_growth': 0.35, 'dormant': 0.10, 'early': 0.55}
    phase_probabilities: Dict[str, float] = field(default_factory=dict)
    
    # Overall confidence in this distribution
    overall_confidence: float = 0.0
    
    # How many scenarios were simulated
    num_scenarios: int = 0
    
    # Timestamp when this was generated
    timestamp: float = 0.0


class HistoricalPatternSampler:
    """
    Intelligently samples from historical data to find relevant patterns.
    
    This is crucial for realistic simulations. We can't just randomly sample
    from any historical moment - we need to find moments that were similar to
    the current state. If we're in early phase with high VTS, we should sample
    from other early/high-VTS moments, not from random dormant phases.
    """
    
    def __init__(self, min_similar_samples: int = 5):
        """
        Initialize the pattern sampler.
        
        Args:
            min_similar_samples: Minimum number of similar historical patterns
                                required for confident sampling
        """
        self.min_similar_samples = min_similar_samples
        self.historical_snapshots: List[Dict] = []
        logger.info("🔍 Historical Pattern Sampler initialized")
    
    def load_historical_data(self, snapshots: List[Dict]):
        """
        Load historical snapshot data for sampling.
        
        This should be called with all the MetricsSnapshot data you've collected.
        The more history you have, the better the sampler can find similar patterns.
        
        Args:
            snapshots: List of historical metrics snapshots
        """
        self.historical_snapshots = snapshots
        logger.info(f"📚 Loaded {len(snapshots)} historical snapshots for pattern matching")
    
    def find_similar_patterns(self, current_state: Dict, max_samples: int = 100) -> List[Dict]:
        """
        Find historical moments that were similar to the current state.
        
        This is the heart of intelligent sampling. We use a similarity metric
        that considers phase, VTS, pressure, and other key indicators to find
        moments in history that looked like the current moment.
        
        Args:
            current_state: Dictionary with current metrics
                          Must have keys: phase, vts, pii, bsr, vlr
            max_samples: Maximum number of similar patterns to return
            
        Returns:
            List of similar historical snapshots, sorted by similarity
        """
        if not self.historical_snapshots:
            logger.warning("⚠️ No historical data loaded - cannot find patterns")
            return []
        
        current_phase = current_state.get('phase', 'dormant')
        current_vts = current_state.get('vts', 1.0)
        current_pii = current_state.get('pii', 0.0)
        current_bsr = current_state.get('bsr', 1.0)
        
        # Calculate similarity score for each historical snapshot
        similarities = []
        
        for snapshot in self.historical_snapshots:
            # Extract metrics from snapshot (handle both dict and object formats)
            if isinstance(snapshot, dict):
                hist_phase = snapshot.get('phase', 'dormant')
                hist_vts = snapshot.get('vts', 1.0)
                hist_pii = snapshot.get('pii', 0.0)
                hist_bsr = snapshot.get('bsr', 1.0)
            else:
                # It's a MetricsSnapshot object
                hist_phase = snapshot.phase
                hist_vts = snapshot.vts
                hist_pii = snapshot.pii
                hist_bsr = snapshot.bsr_1h
            
            # Calculate similarity score
            # Phase match is most important (binary: match or don't)
            phase_match = 1.0 if hist_phase == current_phase else 0.3
            
            # Metric similarity uses normalized distance
            # The closer the values, the higher the similarity
            vts_similarity = 1.0 / (1.0 + abs(hist_vts - current_vts) / max(current_vts, 0.1))
            pii_similarity = 1.0 / (1.0 + abs(hist_pii - current_pii) / max(abs(current_pii), 0.1))
            bsr_similarity = 1.0 / (1.0 + abs(hist_bsr - current_bsr) / max(current_bsr, 0.1))
            
            # Weighted combination
            # Phase is 40% of the score, other metrics split the remaining 60%
            total_similarity = (
                phase_match * 0.40 +
                vts_similarity * 0.20 +
                pii_similarity * 0.20 +
                bsr_similarity * 0.20
            )
            
            similarities.append((total_similarity, snapshot))
        
        # Sort by similarity (highest first)
        similarities.sort(key=lambda x: x[0], reverse=True)
        
        # Take top matches
        top_matches = [snapshot for score, snapshot in similarities[:max_samples]]
        
        # Calculate average similarity of matches for confidence scoring
        avg_similarity = np.mean([score for score, _ in similarities[:max_samples]]) if similarities else 0.0
        
        logger.debug(
            f"🎯 Found {len(top_matches)} similar patterns for {current_phase} phase "
            f"(avg similarity: {avg_similarity:.2f})"
        )
        
        return top_matches
    
    def sample_metric_change(self, similar_patterns: List[Dict], metric_name: str) -> float:
        """
        Sample how a metric changed in similar historical situations.
        
        This looks at similar historical moments and samples how the metric
        evolved from there. For example, if VTS was 3.0 in a similar past moment,
        and it increased to 3.5 in the next snapshot, we sample that +0.5 change.
        
        Args:
            similar_patterns: List of similar historical snapshots
            metric_name: Name of the metric to sample (e.g., 'vts', 'volume_1h')
            
        Returns:
            Sampled change value (could be positive or negative)
        """
        if not similar_patterns:
            # No historical data - return neutral change
            return 0.0
        
        # Randomly pick one of the similar patterns
        pattern = random.choice(similar_patterns)
        
        # Extract the current value
        if isinstance(pattern, dict):
            current_value = pattern.get(metric_name, 0.0)
        else:
            current_value = getattr(pattern, metric_name, 0.0)
        
        # In a real implementation, you'd look at the NEXT snapshot to see
        # how this metric changed. For now, we'll use a simplified approach
        # that adds random variation based on historical volatility.
        
        # Calculate historical volatility of this metric
        values = []
        for p in similar_patterns[:20]:  # Use up to 20 patterns
            if isinstance(p, dict):
                values.append(p.get(metric_name, 0.0))
            else:
                values.append(getattr(p, metric_name, 0.0))
        
        if len(values) > 1:
            volatility = np.std(values)
            # Sample from a normal distribution centered at 0 with historical volatility
            change = np.random.normal(0, volatility)
        else:
            change = 0.0
        
        return change


class ScenarioSimulator:
    """
    Simulates individual future scenarios based on historical patterns.
    
    This class takes the current state and projects it forward one step at a time,
    sampling from historical patterns to determine how metrics evolve. Each call
    to simulate() produces one possible future.
    """
    
    def __init__(self, pattern_sampler: HistoricalPatternSampler):
        """
        Initialize the simulator with a pattern sampler.
        
        Args:
            pattern_sampler: HistoricalPatternSampler instance for finding patterns
        """
        self.pattern_sampler = pattern_sampler
        logger.info("🎲 Scenario Simulator initialized")
    
    def simulate_single_scenario(self, 
                                 current_state: Dict,
                                 projection_minutes: int,
                                 scenario_id: int) -> ScenarioResult:
        """
        Run a single Monte Carlo simulation.
        
        This projects the token forward in time by sampling from historical patterns.
        It's like asking "given where we are now, what's ONE possible future based
        on what's happened historically in similar situations?"
        
        Args:
            current_state: Current metrics state (phase, vts, pii, etc.)
            projection_minutes: How many minutes to project forward
            scenario_id: Unique ID for this scenario (for tracking)
            
        Returns:
            ScenarioResult with the projected future state
        """
        # Find similar historical patterns to sample from
        similar_patterns = self.pattern_sampler.find_similar_patterns(current_state)
        
        if len(similar_patterns) < self.pattern_sampler.min_similar_samples:
            # Not enough historical data - return low-confidence scenario
            logger.debug(
                f"⚠️ Only {len(similar_patterns)} similar patterns found "
                f"(need {self.pattern_sampler.min_similar_samples})"
            )
            confidence = 0.3
        else:
            confidence = min(1.0, len(similar_patterns) / 50.0)
        
        # Start with current values
        projected_volume = current_state.get('volume_1h', 0.0)
        projected_vts = current_state.get('vts', 1.0)
        projected_pii = current_state.get('pii', 0.0)
        projected_bsr = current_state.get('bsr', 1.0)
        projected_phase = current_state.get('phase', 'dormant')
        
        # Simulate evolution over time
        # We break the projection into steps (one per minute)
        num_steps = projection_minutes
        
        for step in range(num_steps):
            # Sample changes from historical patterns
            volume_change = self.pattern_sampler.sample_metric_change(
                similar_patterns, 'volume_1h'
            )
            vts_change = self.pattern_sampler.sample_metric_change(
                similar_patterns, 'vts'
            )
            pii_change = self.pattern_sampler.sample_metric_change(
                similar_patterns, 'pii'
            )
            bsr_change = self.pattern_sampler.sample_metric_change(
                similar_patterns, 'bsr_1h'
            )
            
            # Apply changes with some dampening (future is uncertain)
            dampening = 0.8 ** step  # Uncertainty grows with time
            projected_volume += volume_change * dampening
            projected_vts += vts_change * dampening
            projected_pii += pii_change * dampening
            projected_bsr += bsr_change * dampening
            
            # Keep metrics in reasonable bounds
            projected_volume = max(0, projected_volume)
            projected_vts = max(0.1, min(20.0, projected_vts))
            projected_bsr = max(0.1, min(10.0, projected_bsr))
        
        # Calculate derived metrics
        projected_vlr = projected_volume / current_state.get('liquidity_usd', 1000.0)
        
        # Estimate price change based on volume and pressure
        # This is a simplified model - you can make it more sophisticated
        price_change = projected_pii * 10.0  # Rough correlation
        price_change = max(-50.0, min(100.0, price_change))  # Cap at reasonable values
        
        # Determine if phase transition occurred
        # Check if metrics crossed thresholds for phase changes
        transition_occurred = False
        transition_to = None
        
        if projected_vts > 2.5 and projected_phase == 'dormant':
            transition_occurred = True
            transition_to = 'early'
            projected_phase = 'early'
        elif projected_vts < 1.2 and projected_phase == 'early':
            transition_occurred = True
            transition_to = 'dormant'
            projected_phase = 'dormant'
        
        return ScenarioResult(
            volume_1h=projected_volume,
            bsr_1h=projected_bsr,
            vlr_1h=projected_vlr,
            vts=projected_vts,
            pii=projected_pii,
            price_change_pct=price_change,
            final_phase=projected_phase,
            transition_occurred=transition_occurred,
            transition_to=transition_to,
            scenario_id=scenario_id,
            confidence_weight=confidence
        )


class ScenarioDistributionEngine:
    """
    Main engine that runs many simulations and aggregates results.
    
    This is the orchestrator - it runs thousands of scenarios, collects the results,
    and calculates probability bands that tell you the range of possible outcomes.
    """
    
    def __init__(self, num_scenarios: int = 1000):
        """
        Initialize the distribution engine.
        
        Args:
            num_scenarios: How many Monte Carlo simulations to run per prediction
                          More scenarios = more accurate but slower
                          1000 is a good balance for real-time use
        """
        self.num_scenarios = num_scenarios
        self.pattern_sampler = HistoricalPatternSampler()
        self.simulator = ScenarioSimulator(self.pattern_sampler)
        logger.info(f"🎰 Scenario Distribution Engine initialized ({num_scenarios} scenarios per run)")
    
    def load_historical_data(self, snapshots: List[Dict]):
        """
        Load historical data for pattern matching.
        
        Args:
            snapshots: List of historical metrics snapshots
        """
        self.pattern_sampler.load_historical_data(snapshots)
    
    def generate_distribution(self,
                            current_state: Dict,
                            projection_minutes: int = 15) -> ScenarioDistribution:
        """
        Generate a full probability distribution for future states.
        
        This is the main method you'll call. It runs many simulations and returns
        a comprehensive view of what might happen with probabilities attached.
        
        Args:
            current_state: Current metrics (phase, vts, pii, bsr, volume, etc.)
            projection_minutes: How far into the future to project
            
        Returns:
            ScenarioDistribution with probability bands for all metrics
        """
        import time
        start_time = time.time()
        
        token_address = current_state.get('token_address', 'unknown')
        current_phase = current_state.get('phase', 'dormant')
        
        logger.info(
            f"🎲 Generating {self.num_scenarios} scenarios for {token_address[:8]}... "
            f"({projection_minutes}min projection)"
        )
        
        # Run all simulations
        scenarios: List[ScenarioResult] = []
        for i in range(self.num_scenarios):
            scenario = self.simulator.simulate_single_scenario(
                current_state, 
                projection_minutes, 
                scenario_id=i
            )
            scenarios.append(scenario)
        
        # Aggregate results into probability bands
        volume_dist = self._calculate_probability_bands(
            [s.volume_1h for s in scenarios],
            [s.confidence_weight for s in scenarios],
            'volume_1h'
        )
        
        bsr_dist = self._calculate_probability_bands(
            [s.bsr_1h for s in scenarios],
            [s.confidence_weight for s in scenarios],
            'bsr_1h'
        )
        
        vts_dist = self._calculate_probability_bands(
            [s.vts for s in scenarios],
            [s.confidence_weight for s in scenarios],
            'vts'
        )
        
        pii_dist = self._calculate_probability_bands(
            [s.pii for s in scenarios],
            [s.confidence_weight for s in scenarios],
            'pii'
        )
        
        price_dist = self._calculate_probability_bands(
            [s.price_change_pct for s in scenarios],
            [s.confidence_weight for s in scenarios],
            'price_change_pct'
        )
        
        # Calculate phase transition probabilities
        phase_counts = defaultdict(int)
        for scenario in scenarios:
            phase_counts[scenario.final_phase] += 1
        
        phase_probs = {
            phase: count / len(scenarios)
            for phase, count in phase_counts.items()
        }
        
        # Overall confidence is average of scenario confidences
        overall_confidence = np.mean([s.confidence_weight for s in scenarios])
        
        elapsed = time.time() - start_time
        logger.info(
            f"✅ Generated distribution in {elapsed:.2f}s "
            f"(confidence: {overall_confidence:.2f})"
        )
        
        return ScenarioDistribution(
            token_address=token_address,
            current_phase=current_phase,
            projection_minutes=projection_minutes,
            volume_distribution=volume_dist,
            bsr_distribution=bsr_dist,
            vts_distribution=vts_dist,
            pii_distribution=pii_dist,
            price_change_distribution=price_dist,
            phase_probabilities=phase_probs,
            overall_confidence=overall_confidence,
            num_scenarios=self.num_scenarios,
            timestamp=time.time()
        )
    
    def _calculate_probability_bands(self,
                                    values: List[float],
                                    weights: List[float],
                                    metric_name: str) -> ProbabilityBands:
        """
        Calculate probability bands from scenario results.
        
        This takes all the simulated values for a metric and computes percentiles,
        confidence bands, and summary statistics.
        
        Args:
            values: List of metric values from all scenarios
            weights: Confidence weights for each scenario
            metric_name: Name of this metric
            
        Returns:
            ProbabilityBands with statistical summary
        """
        if not values:
            return ProbabilityBands(metric_name=metric_name)
        
        # Convert to numpy arrays for efficient computation
        values_array = np.array(values)
        weights_array = np.array(weights)
        
        # Weighted percentiles
        p10 = self._weighted_percentile(values_array, weights_array, 10)
        p25 = self._weighted_percentile(values_array, weights_array, 25)
        p50 = self._weighted_percentile(values_array, weights_array, 50)
        p75 = self._weighted_percentile(values_array, weights_array, 75)
        p90 = self._weighted_percentile(values_array, weights_array, 90)
        
        # Confidence bands
        # 50% band: from 25th to 75th percentile (middle 50% of outcomes)
        band_50_lower = p25
        band_50_upper = p75
        
        # 90% band: from 5th to 95th percentile (middle 90% of outcomes)
        band_90_lower = self._weighted_percentile(values_array, weights_array, 5)
        band_90_upper = self._weighted_percentile(values_array, weights_array, 95)
        
        # Summary statistics
        mean = np.average(values_array, weights=weights_array)
        # Weighted standard deviation
        variance = np.average((values_array - mean) ** 2, weights=weights_array)
        std_dev = np.sqrt(variance)
        
        # Overall confidence is average weight
        confidence = np.mean(weights_array)
        
        return ProbabilityBands(
            metric_name=metric_name,
            p10=p10,
            p25=p25,
            p50=p50,
            p75=p75,
            p90=p90,
            band_50_lower=band_50_lower,
            band_50_upper=band_50_upper,
            band_90_lower=band_90_lower,
            band_90_upper=band_90_upper,
            mean=mean,
            std_dev=std_dev,
            sample_count=len(values),
            confidence_score=confidence
        )
    
    def _weighted_percentile(self, values: np.ndarray, weights: np.ndarray, percentile: float) -> float:
        """
        Calculate weighted percentile.
        
        This is like a regular percentile, but scenarios with higher confidence
        weights contribute more to the calculation.
        
        Args:
            values: Array of values
            weights: Array of weights (same length as values)
            percentile: Percentile to calculate (0-100)
            
        Returns:
            Weighted percentile value
        """
        # Sort by value
        sorted_indices = np.argsort(values)
        sorted_values = values[sorted_indices]
        sorted_weights = weights[sorted_indices]
        
        # Calculate cumulative weights
        cumsum = np.cumsum(sorted_weights)
        total_weight = cumsum[-1]
        
        # Find the value at the desired percentile
        target = (percentile / 100.0) * total_weight
        idx = np.searchsorted(cumsum, target)
        
        if idx >= len(sorted_values):
            return sorted_values[-1]
        
        return sorted_values[idx]