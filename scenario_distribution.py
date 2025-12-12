"""
Scenario Distribution System using Monte Carlo Simulation - COMPLETE VERSION

This system generates probability ranges for token metrics by simulating
thousands of possible futures based on historical patterns.

FEATURES:
1. Uses actual sequential historical data (before/after pairs)
2. Integrates with State Transition Matrix for realistic phase changes
3. Calculates event probabilities (volume doubling, phase transitions, etc.)
4. Provides human-readable summaries
"""

import logging
import random
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import time

logger = logging.getLogger(__name__)


@dataclass
class ScenarioResult:
    """Represents the outcome of a single Monte Carlo simulation."""
    volume_1h: float = 0.0
    bsr_1h: float = 1.0
    vlr_1h: float = 0.0
    vts: float = 1.0
    pii: float = 0.0
    price_change_pct: float = 0.0
    final_phase: str = 'dormant'
    transition_occurred: bool = False
    transition_to: Optional[str] = None
    volume_doubled: bool = False
    volume_halved: bool = False
    vts_spike: bool = False
    scenario_id: int = 0
    confidence_weight: float = 1.0


@dataclass
class EventProbabilities:
    """Probabilities of specific interesting events occurring."""
    volume_doubles: float = 0.0
    volume_increases_50pct: float = 0.0
    volume_decreases_50pct: float = 0.0
    any_phase_transition: float = 0.0
    transition_to_early: float = 0.0
    transition_to_mid: float = 0.0
    transition_to_late: float = 0.0
    vts_spike: float = 0.0
    buying_pressure_surge: float = 0.0
    price_pump_10pct: float = 0.0
    price_dump_10pct: float = 0.0


@dataclass
class ProbabilityBands:
    """Statistical summary of many scenarios."""
    metric_name: str
    p10: float = 0.0
    p25: float = 0.0
    p50: float = 0.0
    p75: float = 0.0
    p90: float = 0.0
    band_50_lower: float = 0.0
    band_50_upper: float = 0.0
    band_90_lower: float = 0.0
    band_90_upper: float = 0.0
    mean: float = 0.0
    std_dev: float = 0.0
    sample_count: int = 0
    confidence_score: float = 0.0


@dataclass
class ScenarioDistribution:
    """Complete probabilistic prediction for a token's future state."""
    token_address: str
    current_phase: str
    projection_minutes: int
    volume_distribution: ProbabilityBands
    bsr_distribution: ProbabilityBands
    vts_distribution: ProbabilityBands
    pii_distribution: ProbabilityBands
    price_change_distribution: ProbabilityBands
    phase_probabilities: Dict[str, float] = field(default_factory=dict)
    event_probabilities: EventProbabilities = field(default_factory=EventProbabilities)
    summary: str = ""
    overall_confidence: float = 0.0
    num_scenarios: int = 0
    timestamp: float = 0.0


class SequentialPatternTracker:
    """Tracks sequential changes in historical data."""
    
    def __init__(self):
        self.sequential_pairs: List[Tuple[Dict, Dict]] = []
        logger.info("📊 Sequential Pattern Tracker initialized")
    
    def build_sequential_pairs(self, snapshots: List[Dict]):
        """Build database of sequential snapshot pairs."""
        self.sequential_pairs = []
        
        for i in range(len(snapshots) - 1):
            before = snapshots[i]
            after = snapshots[i + 1]
            
            if isinstance(before, dict):
                time_before = before.get('timestamp', 0)
                time_after = after.get('timestamp', 0)
            else:
                time_before = before.timestamp
                time_after = after.timestamp
            
            time_delta_minutes = (time_after - time_before) / 60.0
            
            if 0 < time_delta_minutes <= 5:
                self.sequential_pairs.append((before, after))
        
        logger.info(f"📈 Built {len(self.sequential_pairs)} sequential pairs from {len(snapshots)} snapshots")
    
    def sample_change(self, current_state: Dict, metric_name: str, similar_indices: List[int]) -> float:
        """Sample a realistic metric change from sequential pairs."""
        if not similar_indices or not self.sequential_pairs:
            return 0.0
        
        idx = random.choice(similar_indices)
        before, after = self.sequential_pairs[idx]
        
        if isinstance(before, dict):
            value_before = before.get(metric_name, 0.0)
            value_after = after.get(metric_name, 0.0)
        else:
            value_before = getattr(before, metric_name, 0.0)
            value_after = getattr(after, metric_name, 0.0)
        
        change = value_after - value_before
        return change


class HistoricalPatternSampler:
    """Intelligently samples from historical data to find relevant patterns."""
    
    def __init__(self, min_similar_samples: int = 5):
        self.min_similar_samples = min_similar_samples
        self.historical_snapshots: List[Dict] = []
        self.sequential_tracker = SequentialPatternTracker()
        logger.info("🔍 Historical Pattern Sampler initialized")
    
    def load_historical_data(self, snapshots: List[Dict]):
        """Load historical data and build sequential pairs."""
        self.historical_snapshots = snapshots
        self.sequential_tracker.build_sequential_pairs(snapshots)
        logger.info(f"📚 Loaded {len(snapshots)} historical snapshots for pattern matching")
    
    def find_similar_patterns(self, current_state: Dict, max_samples: int = 100) -> Tuple[List[Dict], List[int]]:
        """Find similar patterns and return both snapshots and sequential pair indices."""
        if not self.historical_snapshots:
            return [], []
        
        current_phase = current_state.get('phase', 'dormant')
        current_vts = current_state.get('vts', 1.0)
        current_pii = current_state.get('pii', 0.0)
        current_bsr = current_state.get('bsr', 1.0)
        
        snapshot_similarities = []
        for snapshot in self.historical_snapshots:
            if isinstance(snapshot, dict):
                hist_phase = snapshot.get('phase', 'dormant')
                hist_vts = snapshot.get('vts', 1.0)
                hist_pii = snapshot.get('pii', 0.0)
                hist_bsr = snapshot.get('bsr', 1.0)
            else:
                hist_phase = snapshot.phase
                hist_vts = snapshot.vts
                hist_pii = snapshot.pii
                hist_bsr = snapshot.bsr_1h
            
            phase_match = 1.0 if hist_phase == current_phase else 0.3
            vts_similarity = 1.0 / (1.0 + abs(hist_vts - current_vts) / max(current_vts, 0.1))
            pii_similarity = 1.0 / (1.0 + abs(hist_pii - current_pii) / max(abs(current_pii), 0.1))
            bsr_similarity = 1.0 / (1.0 + abs(hist_bsr - current_bsr) / max(current_bsr, 0.1))
            
            total_similarity = (
                phase_match * 0.40 +
                vts_similarity * 0.20 +
                pii_similarity * 0.20 +
                bsr_similarity * 0.20
            )
            
            snapshot_similarities.append((total_similarity, snapshot))
        
        sequential_similarities = []
        for idx, (before, after) in enumerate(self.sequential_tracker.sequential_pairs):
            if isinstance(before, dict):
                hist_phase = before.get('phase', 'dormant')
                hist_vts = before.get('vts', 1.0)
                hist_pii = before.get('pii', 0.0)
                hist_bsr = before.get('bsr', 1.0)
            else:
                hist_phase = before.phase
                hist_vts = before.vts
                hist_pii = before.pii
                hist_bsr = before.bsr_1h
            
            phase_match = 1.0 if hist_phase == current_phase else 0.3
            vts_similarity = 1.0 / (1.0 + abs(hist_vts - current_vts) / max(current_vts, 0.1))
            pii_similarity = 1.0 / (1.0 + abs(hist_pii - current_pii) / max(abs(current_pii), 0.1))
            bsr_similarity = 1.0 / (1.0 + abs(hist_bsr - current_bsr) / max(current_bsr, 0.1))
            
            total_similarity = (
                phase_match * 0.40 +
                vts_similarity * 0.20 +
                pii_similarity * 0.20 +
                bsr_similarity * 0.20
            )
            
            sequential_similarities.append((total_similarity, idx))
        
        snapshot_similarities.sort(key=lambda x: x[0], reverse=True)
        sequential_similarities.sort(key=lambda x: x[0], reverse=True)
        
        top_snapshots = [s for _, s in snapshot_similarities[:max_samples]]
        top_indices = [idx for _, idx in sequential_similarities[:max_samples]]
        
        return top_snapshots, top_indices


class ScenarioSimulator:
    """Simulates individual future scenarios based on historical patterns."""
    
    def __init__(self, pattern_sampler: HistoricalPatternSampler, state_analyzer=None):
        self.pattern_sampler = pattern_sampler
        self.state_analyzer = state_analyzer
        logger.info("🎲 Scenario Simulator initialized")
    
    def simulate_single_scenario(self, 
                                 current_state: Dict,
                                 projection_minutes: int,
                                 scenario_id: int) -> ScenarioResult:
        """Run a single Monte Carlo simulation."""
        similar_snapshots, similar_seq_indices = self.pattern_sampler.find_similar_patterns(current_state)
        
        if len(similar_snapshots) < self.pattern_sampler.min_similar_samples:
            confidence = 0.3
        else:
            confidence = min(1.0, len(similar_snapshots) / 50.0)
        
        projected_volume = current_state.get('volume_1h', 0.0)
        projected_vts = current_state.get('vts', 1.0)
        projected_pii = current_state.get('pii', 0.0)
        projected_bsr = current_state.get('bsr', 1.0)
        projected_phase = current_state.get('phase', 'dormant')
        
        initial_volume = projected_volume
        initial_vts = projected_vts
        
        transition_occurred = False
        transition_to = None
        
        for step in range(projection_minutes):
            volume_change = self.pattern_sampler.sequential_tracker.sample_change(
                current_state, 'volume_1h', similar_seq_indices
            )
            vts_change = self.pattern_sampler.sequential_tracker.sample_change(
                current_state, 'vts', similar_seq_indices
            )
            pii_change = self.pattern_sampler.sequential_tracker.sample_change(
                current_state, 'pii', similar_seq_indices
            )
            bsr_change = self.pattern_sampler.sequential_tracker.sample_change(
                current_state, 'bsr_1h', similar_seq_indices
            )
            
            dampening = 0.9 ** step
            projected_volume += volume_change * dampening
            projected_vts += vts_change * dampening
            projected_pii += pii_change * dampening
            projected_bsr += bsr_change * dampening
            
            projected_volume = max(0, projected_volume)
            projected_vts = max(0.1, min(20.0, projected_vts))
            projected_bsr = max(0.1, min(10.0, projected_bsr))
            
            if self.state_analyzer and self.state_analyzer.transition_matrix:
                if projected_phase in self.state_analyzer.transition_matrix:
                    transitions = self.state_analyzer.transition_matrix[projected_phase]
                    
                    if transitions and random.random() < 0.1:
                        phases = list(transitions.keys())
                        probs = [transitions[p]['probability'] for p in phases]
                        
                        if probs and sum(probs) > 0:
                            chosen_phase = random.choices(phases, weights=probs)[0]
                            if chosen_phase != projected_phase:
                                transition_occurred = True
                                transition_to = chosen_phase
                                projected_phase = chosen_phase
        
        projected_vlr = projected_volume / current_state.get('liquidity_usd', 1000.0)
        price_change = projected_pii * 10.0
        price_change = max(-50.0, min(100.0, price_change))
        
        volume_doubled = (projected_volume / initial_volume) >= 2.0 if initial_volume > 0 else False
        volume_halved = (projected_volume / initial_volume) <= 0.5 if initial_volume > 0 else False
        vts_spike = (projected_vts / initial_vts) >= 1.5 if initial_vts > 0 else False
        
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
            volume_doubled=volume_doubled,
            volume_halved=volume_halved,
            vts_spike=vts_spike,
            scenario_id=scenario_id,
            confidence_weight=confidence
        )


class ScenarioDistributionEngine:
    """Main engine that runs many simulations and aggregates results."""
    
    def __init__(self, num_scenarios: int = 1000, state_analyzer=None):
        self.num_scenarios = num_scenarios
        self.pattern_sampler = HistoricalPatternSampler()
        self.simulator = ScenarioSimulator(self.pattern_sampler, state_analyzer)
        self.state_analyzer = state_analyzer
        logger.info(f"🎰 Scenario Distribution Engine initialized ({num_scenarios} scenarios)")
    
    def load_historical_data(self, snapshots: List[Dict]):
        """Load historical data for pattern matching."""
        self.pattern_sampler.load_historical_data(snapshots)
    
    def generate_distribution(self,
                            current_state: Dict,
                            projection_minutes: int = 15) -> ScenarioDistribution:
        """Generate a full probability distribution with event probabilities and summary."""
        start_time = time.time()
        
        token_address = current_state.get('token_address', 'unknown')
        current_phase = current_state.get('phase', 'dormant')
        current_volume = current_state.get('volume_1h', 0.0)
        
        logger.info(
            f"🎲 Generating {self.num_scenarios} scenarios for {token_address[:8]}... "
            f"({projection_minutes}min projection)"
        )
        
        scenarios: List[ScenarioResult] = []
        for i in range(self.num_scenarios):
            scenario = self.simulator.simulate_single_scenario(
                current_state, 
                projection_minutes, 
                scenario_id=i
            )
            scenarios.append(scenario)
        
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
        
        phase_counts = defaultdict(int)
        for scenario in scenarios:
            phase_counts[scenario.final_phase] += 1
        
        phase_probs = {
            phase: count / len(scenarios)
            for phase, count in phase_counts.items()
        }
        
        event_probs = self._calculate_event_probabilities(scenarios, current_volume)
        summary = self._generate_summary(
            current_state, volume_dist, vts_dist, phase_probs, event_probs, projection_minutes
        )
        
        overall_confidence = np.mean([s.confidence_weight for s in scenarios])
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Generated distribution in {elapsed:.2f}s (confidence: {overall_confidence:.2f})")
        
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
            event_probabilities=event_probs,
            summary=summary,
            overall_confidence=overall_confidence,
            num_scenarios=self.num_scenarios,
            timestamp=time.time()
        )
    
    def _calculate_event_probabilities(self, scenarios: List[ScenarioResult], current_volume: float) -> EventProbabilities:
        """Calculate probabilities of specific events from scenario results."""
        total = len(scenarios)
        
        volume_doubled = sum(1 for s in scenarios if s.volume_doubled)
        volume_halved = sum(1 for s in scenarios if s.volume_halved)
        vts_spike = sum(1 for s in scenarios if s.vts_spike)
        any_transition = sum(1 for s in scenarios if s.transition_occurred)
        
        volume_50pct_increase = sum(1 for s in scenarios if s.volume_1h >= current_volume * 1.5)
        volume_50pct_decrease = sum(1 for s in scenarios if s.volume_1h <= current_volume * 0.5)
        
        transition_to_early = sum(1 for s in scenarios if s.transition_to == 'early')
        transition_to_mid = sum(1 for s in scenarios if s.transition_to == 'mid')
        transition_to_late = sum(1 for s in scenarios if s.transition_to == 'late')
        
        buying_pressure = sum(1 for s in scenarios if s.bsr_1h >= 1.5)
        price_pump = sum(1 for s in scenarios if s.price_change_pct >= 10.0)
        price_dump = sum(1 for s in scenarios if s.price_change_pct <= -10.0)
        
        return EventProbabilities(
            volume_doubles=volume_doubled / total,
            volume_increases_50pct=volume_50pct_increase / total,
            volume_decreases_50pct=volume_50pct_decrease / total,
            any_phase_transition=any_transition / total,
            transition_to_early=transition_to_early / total,
            transition_to_mid=transition_to_mid / total,
            transition_to_late=transition_to_late / total,
            vts_spike=vts_spike / total,
            buying_pressure_surge=buying_pressure / total,
            price_pump_10pct=price_pump / total,
            price_dump_10pct=price_dump / total
        )
    
    def _generate_summary(self, current_state: Dict, volume_dist: ProbabilityBands, 
                         vts_dist: ProbabilityBands, phase_probs: Dict[str, float],
                         event_probs: EventProbabilities, projection_minutes: int) -> str:
        """Generate a human-readable summary of the predictions."""
        current_phase = current_state.get('phase', 'dormant')
        current_volume = current_state.get('volume_1h', 0.0)
        
        summary_parts = []
        
        most_likely_phase = max(phase_probs.items(), key=lambda x: x[1])
        if most_likely_phase[0] == current_phase:
            summary_parts.append(f"Token likely to remain in {current_phase} phase ({most_likely_phase[1]*100:.0f}% probability).")
        else:
            summary_parts.append(f"Token may transition to {most_likely_phase[0]} phase ({most_likely_phase[1]*100:.0f}% probability).")
        
        vol_change_pct = ((volume_dist.p50 - current_volume) / current_volume * 100) if current_volume > 0 else 0
        if abs(vol_change_pct) < 10:
            summary_parts.append(f"Volume expected to remain stable around ${volume_dist.p50:,.0f}.")
        elif vol_change_pct > 0:
            summary_parts.append(f"Volume likely to increase {abs(vol_change_pct):.0f}% to ${volume_dist.p50:,.0f}.")
        else:
            summary_parts.append(f"Volume likely to decrease {abs(vol_change_pct):.0f}% to ${volume_dist.p50:,.0f}.")
        
        if event_probs.volume_doubles > 0.2:
            summary_parts.append(f"⚠️ {event_probs.volume_doubles*100:.0f}% chance of volume doubling.")
        
        if event_probs.any_phase_transition > 0.15:
            summary_parts.append(f"📈 {event_probs.any_phase_transition*100:.0f}% chance of phase transition in next {projection_minutes} minutes.")
        
        if event_probs.vts_spike > 0.2:
            summary_parts.append(f"🔥 {event_probs.vts_spike*100:.0f}% chance of momentum spike (VTS surge).")
        
        if volume_dist.std_dev / volume_dist.mean > 0.5:
            summary_parts.append("⚡ High uncertainty - wide range of possible outcomes.")
        else:
            summary_parts.append("✅ Moderate uncertainty - relatively predictable behavior.")
        
        return " ".join(summary_parts)
    
    def _calculate_probability_bands(self, values: List[float], weights: List[float], metric_name: str) -> ProbabilityBands:
        """Calculate probability bands from scenario results."""
        if not values:
            return ProbabilityBands(metric_name=metric_name)
        
        values_array = np.array(values)
        weights_array = np.array(weights)
        
        p10 = self._weighted_percentile(values_array, weights_array, 10)
        p25 = self._weighted_percentile(values_array, weights_array, 25)
        p50 = self._weighted_percentile(values_array, weights_array, 50)
        p75 = self._weighted_percentile(values_array, weights_array, 75)
        p90 = self._weighted_percentile(values_array, weights_array, 90)
        
        band_50_lower = p25
        band_50_upper = p75
        band_90_lower = self._weighted_percentile(values_array, weights_array, 5)
        band_90_upper = self._weighted_percentile(values_array, weights_array, 95)
        
        mean = np.average(values_array, weights=weights_array)
        variance = np.average((values_array - mean) ** 2, weights=weights_array)
        std_dev = np.sqrt(variance)
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
        """Calculate weighted percentile."""
        sorted_indices = np.argsort(values)
        sorted_values = values[sorted_indices]
        sorted_weights = weights[sorted_indices]
        
        cumsum = np.cumsum(sorted_weights)
        total_weight = cumsum[-1]
        
        target = (percentile / 100.0) * total_weight
        idx = np.searchsorted(cumsum, target)
        
        if idx >= len(sorted_values):
            return sorted_values[-1]
        
        return sorted_values[idx]