"""
State Transition Analysis System

This module processes historical snapshot data to calculate transition probablities.
It answers questions like: "Given that a token is currently in early phase with high VTS,
what are the probabilities it will be in each phase one hour from now?"

The approach:
1. Load historical snapshots from multiple tokens
2. For each snapshot, identify its "state" (phase + discretized metrics)
3. Look at what state the token was in N hours later
4. Count frequencies to calculate probabilities
5. Store these probabilities in a lookup table
"""

import json
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path

logger = logging.getLogger(__name__)

class StateTransitionAnalyzer:
    """
    Analyzes phase transitions and builds probability matrices.
    Now includes detailed debug logging and confidence-based building.
    """
    
    def __init__(self, matrix_file: str = "transition_matrix.json", 
                 transitions_file: str = "transition_history.json"):
        """
        Initialize the analyzer with file paths for storing data.
        
        Args:
            matrix_file: Where to save the built transition probability matrix
            transitions_file: Where to save raw transition history for analysis
        """
        self.matrix_file = Path(matrix_file)
        self.transitions_file = Path(transitions_file)
        self.transition_matrix = {}
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        self.confidence_score = 0.0
        self.total_transitions = 0
        self.observation_counts = {}  # NEW: Track how many observations per state
        self.transitions = [] 

         
        logger.info("🔧 Initializing state transition analyzer...")
        self._load_matrix()
        # Load existing transitions from disk into memory
        self.transitions = self._load_transitions()
        logger.info(f"📊 Loaded {len(self.transitions)} historical transitions")

        logger.info("🧠 State Transition Analyzer initialized")


    def _create_state_key(self, phase: str, metrics: Dict) -> str:
        """
        Create a detailed state identifier that captures the current market situation.
    
        Instead of just using phase names like 'early', we create rich descriptors like:
        'early_surging_strong-buy_fresh'
    
        This allows the matrix to learn nuanced patterns like:
        "When you're in early phase with surging volume and strong buy pressure,
         you typically transition to mid phase 60% of the time"
    
        Args:
            phase: Base phase name (dormant, early, mid, late, exhaustion)
            metrics: Dictionary with vts, pii, vei, bsr, conviction_multiplier
    
        Returns:
            Detailed state key string
        """
        vts = metrics.get('vts', 1.0)
        pii = metrics.get('pii', 0.0)
        vei = metrics.get('vei', 1.0)
        bsr = metrics.get('bsr', 1.0)
        conviction = metrics.get('conviction_multiplier', 1.0)
    
    # Volume trend descriptor
        if vts > 3.0:
            volume_desc = "surging"
        elif vts > 2.0:
            volume_desc = "spiking"
        elif vts > 1.3:
            volume_desc = "rising"
        elif vts < 0.5:
            volume_desc = "declining"
        elif vts < 0.8:
            volume_desc = "weakening"
        else:
            volume_desc = "stable"
    
    # Pressure descriptor (based on PII)
        if pii > 0.3:
            pressure_desc = "strong-buy"
        elif pii > 0.1:
            pressure_desc = "buy-pressure"
        elif pii > 0.02:
            pressure_desc = "slight-buy"
        elif pii < -0.3:
            pressure_desc = "strong-sell"
        elif pii < -0.1:
            pressure_desc = "sell-pressure"
        elif pii < -0.02:
            pressure_desc = "slight-sell"
        else:
            pressure_desc = "neutral"
    
    # Energy/Exhaustion descriptor (based on VEI)
        if vei < 0.2:
            energy_desc = "exhausted"
        elif vei < 0.4:
            energy_desc = "tired"
        elif vei > 0.8:
            energy_desc = "fresh"
        elif vei > 0.6:
            energy_desc = "energetic"
        else:
            energy_desc = "moderate"
    
    # Conviction quality descriptor
        if conviction > 1.3:
            conviction_desc = "strong-conviction"
        elif conviction < 0.7:
            conviction_desc = "weak-conviction"
        else:
            conviction_desc = "normal-conviction"
    
    # Combine into a rich state key
    # Format: phase_volume_pressure_energy_conviction
        state_key = f"{phase}_{volume_desc}_{pressure_desc}_{energy_desc}_{conviction_desc}"
    
        return state_key
    def _load_matrix(self):
        """Load existing transition matrix from disk if it exists."""
        if self.matrix_file.exists():
            try:
                with open(self.matrix_file, 'r') as f:
                    data = json.load(f)
                    self.transition_matrix = data.get('matrix', {})
                    self.confidence_score = data.get('confidence', 0.0)
                    self.total_transitions = data.get('total_transitions', 0)
                    self.observation_counts = data.get('observation_counts', {})  # NEW
                    logger.info(f"✅ Loaded transition matrix with {len(self.transition_matrix)} states")
                    logger.info(f"   Confidence: {self.confidence_score:.1%}, Total transitions: {self.total_transitions}")
            except Exception as e:
                logger.error(f"❌ Error loading transition matrix: {e}")
                self.transition_matrix = {}
        else:
            logger.warning(f"⚠️ Matrix file {self.matrix_file} doesn't exist")
            logger.info("ℹ️ No existing transition matrix found - predictions will be unavailable until you build one")
            logger.info("   Build a matrix by calling POST /analysis/build-transitions after collecting data")
    
    def _save_matrix(self):
        """Save the transition matrix to disk with metadata."""
        try:
            data = {
                'matrix': self.transition_matrix,
                'confidence': self.confidence_score,
                'total_transitions': self.total_transitions,
                'observation_counts': self.observation_counts, 
                'last_updated': datetime.utcnow().isoformat(),
                'states': list(self.transition_matrix.keys())
            }
            with open(self.matrix_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"💾 Saved transition matrix to {self.matrix_file}")
        except Exception as e:
            logger.error(f"❌ Error saving transition matrix: {e}")

    def save_matrix(self):
        """
        Public method to save the transition matrix.
        
        This is called by external code (like the build-transitions endpoint)
        when it needs to save the matrix after building it. It's a simple
        wrapper around the internal _save_matrix() method.
        """
        self._save_matrix()
        logger.info("💾 Transition matrix saved via public method")
    
    def log_transition(self, token_address: str, from_phase: str, to_phase: str, 
                   metrics: Dict, duration_seconds: float):
        """
        Log a state transition with rich context.
    
        Now creates detailed state keys that capture the full market situation,
        not just simple phase names.
        """
    # Create detailed state keys using metrics
        from_state_key = self._create_state_key(from_phase, metrics)
        to_state_key = self._create_state_key(to_phase, metrics)
    
        transition_data = {
            'token_address': token_address,
            'timestamp': datetime.now().isoformat(),
            'from_phase': from_phase,  # Keep simple phase for reference
            'to_phase': to_phase,      # Keep simple phase for reference
            'from_state': from_state_key,  # NEW: Detailed state
            'to_state': to_state_key,      # NEW: Detailed state
            'duration_seconds': duration_seconds,
            'metrics': metrics
        }
    
        self.transitions.append(transition_data)
    
    # Also update observation counts for the detailed state
        if from_state_key not in self.observation_counts:
            self.observation_counts[from_state_key] = 0
        self.observation_counts[from_state_key] += 1
    
    # Save to file periodically
        if len(self.transitions) % 10 == 0:  # Save every 10 transitions
            self._save_transitions(self.transitions)
    
        logger.info(
            f"📝 Logged transition: {from_state_key} → {to_state_key} "
            f"(duration: {duration_seconds:.0f}s)"
        )
    
    def _load_transitions(self) -> List[dict]:
        """Load all historical transitions from disk."""
        if self.transitions_file.exists():
            try:
                with open(self.transitions_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"❌ Error loading transitions: {e}")
                return []
        return []
    
    def _save_transitions(self, transitions: List[dict]):
        """Save transitions to disk."""
        try:
            with open(self.transitions_file, 'w') as f:
                json.dump(transitions, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Error saving transitions: {e}")
    
    def build_transition_matrix(self, min_observations: int = 5):
        """
        Build transition probability matrix from logged transitions.
    
        Now uses detailed state keys instead of just phase names.
    
        Args:
            min_observations: Minimum number of observations required for a state
                             to be included in the matrix (default: 5)
        """
        if len(self.transitions) < min_observations:
            logger.warning(f"Not enough transitions to build matrix (have {len(self.transitions)}, need at least {min_observations})")
            self.confidence_score = 0.0
            return
    
    # Count transitions between detailed states
        transition_counts = {}
        state_totals = {}
    
        for trans in self.transitions:
            from_state = trans.get('from_state', trans.get('from_phase', 'unknown'))
            to_state = trans.get('to_state', trans.get('to_phase', 'unknown'))
        
        # Initialize nested dict if needed
            if from_state not in transition_counts:
                transition_counts[from_state] = {}
                state_totals[from_state] = 0
        
        # Count this transition
            if to_state not in transition_counts[from_state]:
                transition_counts[from_state][to_state] = 0
        
            transition_counts[from_state][to_state] += 1
            state_totals[from_state] += 1
    
    # Filter out states with too few observations
        logger.info(f"📊 Filtering states with fewer than {min_observations} observations...")
        filtered_counts = {}
        filtered_totals = {}
        excluded_states = 0
    
        for from_state, to_states in transition_counts.items():
            if state_totals[from_state] >= min_observations:
                filtered_counts[from_state] = to_states
                filtered_totals[from_state] = state_totals[from_state]
            else:
                excluded_states += 1
    
        logger.info(f"   ✅ Kept {len(filtered_counts)} states with sufficient data")
        logger.info(f"   ⏭️  Excluded {excluded_states} states with insufficient data")
    
        transition_counts = filtered_counts
        state_totals = filtered_totals
    
        if not transition_counts:
            logger.warning(f"❌ No states have {min_observations}+ observations. Lower min_observations or collect more data.")
            self.confidence_score = 0.0
            return
    
    # Convert counts to probabilities
        self.transition_matrix = {}
        self.observation_counts = state_totals.copy()
    
        for from_state, to_states in transition_counts.items():
            self.transition_matrix[from_state] = {}
            total = state_totals[from_state]
        
            for to_state, count in to_states.items():
                probability = count / total if total > 0 else 0.0
            
                self.transition_matrix[from_state][to_state] = {
                    'probability': probability,
                    'count': count,
                    'total_observations': total
                }
    
    # Calculate confidence score
        total_obs = sum(state_totals.values())
        num_states = len(self.transition_matrix)
        avg_obs_per_state = total_obs / num_states if num_states > 0 else 0
    
        # Confidence based on average observations per state
        if avg_obs_per_state >= 20:
            self.confidence_score = 0.9
        elif avg_obs_per_state >= 10:
            self.confidence_score = 0.7
        elif avg_obs_per_state >= 5:
            self.confidence_score = 0.5
        else:
            self.confidence_score = 0.3
    
        self.total_transitions = len(self.transitions)
    
        logger.info(f"✅ Built transition matrix:")
        logger.info(f"   States: {num_states}")
        logger.info(f"   Total transitions: {self.total_transitions}")
        logger.info(f"   Avg observations per state: {avg_obs_per_state:.1f}")
        logger.info(f"   Confidence: {self.confidence_score:.1%}")
    
    # Save the matrix
        self._save_matrix()
        
    
    def _calculate_confidence(self, num_transitions: int) -> float:
        """
        Calculate confidence score based on number of transitions.
        
        Confidence tiers:
        - < 5 transitions: Too little data (don't build)
        - 5-14 transitions: LOW confidence (0.3-0.5)
        - 15-29 transitions: MEDIUM confidence (0.5-0.7)
        - 30+ transitions: HIGH confidence (0.7-0.9)
        """
        if num_transitions < 5:
            return 0.0
        elif num_transitions < 15:
            # LOW: scale from 0.3 to 0.5
            return 0.3 + (num_transitions - 5) / 10 * 0.2
        elif num_transitions < 30:
            # MEDIUM: scale from 0.5 to 0.7
            return 0.5 + (num_transitions - 15) / 15 * 0.2
        else:
            # HIGH: scale from 0.7 to 0.9, capped at 0.9
            return min(0.9, 0.7 + (num_transitions - 30) / 70 * 0.2)
    
    def _get_confidence_label(self, confidence: float) -> str:
        """Convert confidence score to human-readable label."""
        if confidence < 0.3:
            return "INSUFFICIENT"
        elif confidence < 0.5:
            return "LOW"
        elif confidence < 0.7:
            return "MEDIUM"
        else:
            return "HIGH"
    
    def predict_next_phase(self, current_phase: str, metrics: dict) -> dict:
        """
        Predict the next phase and its probability.
        
        Args:
            current_phase: Current phase the token is in
            metrics: Current metrics snapshot
            
        Returns:
            Dictionary with predictions and confidence
        """
        if not self.transition_matrix or current_phase not in self.transition_matrix:
            return {
                'success': False,
                'reason': 'no_data',
                'message': f'No historical data for phase: {current_phase}'
            }
        
        # Get transition probabilities from current phase
        transitions = self.transition_matrix[current_phase]
        
        # Sort by probability
        sorted_transitions = sorted(
            transitions.items(),
            key=lambda x: x[1]['probability'],
            reverse=True
        )
        
        # Get most likely next phase
        if sorted_transitions:
            most_likely_phase, data = sorted_transitions[0]
            
            return {
                'success': True,
                'current_phase': current_phase,
                'predictions': [
                    {
                        'phase': phase,
                        'probability': data['probability'],
                        'sample_size': data['sample_size']
                    }
                    for phase, data in sorted_transitions
                ],
                'most_likely': {
                    'phase': most_likely_phase,
                    'probability': data['probability'],
                    'sample_size': data['sample_size']
                },
                'model_confidence': self.confidence_score,
                'confidence_label': self._get_confidence_label(self.confidence_score)
            }
        
        return {
            'success': False,
            'reason': 'no_transitions',
            'message': f'No transitions found from phase: {current_phase}'
        }

    def get_transition_probabilities(self, current_phase: str) -> dict:
        """
        Get transition probabilities with intelligent fallbacks.
    
        Enhanced to handle:
        1. Exact state matches (best)
        2. Similar state matches (good)
        3. Base phase matches (acceptable)
        4. No match (return empty)
        """
        if not self.transition_matrix:
            return {
                'next_phase_probabilities': {},
                'confidence': 0.0,
                'observations': 0
            }
    
    # STRATEGY 1: Try exact match first
        if current_phase in self.transition_matrix:
            transitions = self.transition_matrix[current_phase]
            probabilities = {}
            total_observations = 0
        
            for next_state, data in transitions.items():
            # Extract just the phase from the detailed next_state key
                next_phase = next_state.split('_')[0]
            
                if next_phase not in probabilities:
                    probabilities[next_phase] = 0.0
                probabilities[next_phase] += data['probability']
            
                if total_observations == 0:
                    total_observations = data.get('sample_size', 0)
        
            observations = self.observation_counts.get(current_phase, total_observations)
        
            return {
                'next_phase_probabilities': probabilities,
                'confidence': self.confidence_score,
                'observations': observations
            }
    
    # STRATEGY 2: Fuzzy matching - look for similar states
    # Extract the base phase from the detailed state key
        base_phase = current_phase.split('_')[0]  # e.g., "early" from "early_rising_buy-pressure"
    
        # Find all states that start with this phase
        similar_states = [state for state in self.transition_matrix.keys() 
                         if state.startswith(base_phase + '_')]
    
        if similar_states:
            logger.debug(
                f"📊 No exact match for '{current_phase}', "
                f"using {len(similar_states)} similar '{base_phase}' states"
            )
        
        # Aggregate probabilities across all similar states
            combined_probs = {}
            total_weight = 0
        
            for similar_state in similar_states:
                state_weight = self.observation_counts.get(similar_state, 1)
                total_weight += state_weight
            
                for next_state, data in self.transition_matrix[similar_state].items():
                # Extract just the phase from next_state
                    next_phase = next_state.split('_')[0]
                
                    if next_phase not in combined_probs:
                        combined_probs[next_phase] = 0.0
                
                    combined_probs[next_phase] += data['probability'] * state_weight
        
        # Normalize
            if total_weight > 0:
                for phase in combined_probs:
                    combined_probs[phase] /= total_weight
        
            return {
                'next_phase_probabilities': combined_probs,
                'confidence': self.confidence_score * 0.7,  # Reduce confidence for fuzzy match
                'observations': total_weight
            }
    
    # STRATEGY 3: Look for the base phase without any modifiers
        if base_phase in self.transition_matrix:
            logger.debug(f"📊 Using base phase '{base_phase}' as fallback")
        
            transitions = self.transition_matrix[base_phase]
            probabilities = {}
        
            for next_state, data in transitions.items():
                next_phase = next_state.split('_')[0] if '_' in next_state else next_state
            
                if next_phase not in probabilities:
                    probabilities[next_phase] = 0.0
                probabilities[next_phase] += data['probability']
        
            return {
                'next_phase_probabilities': probabilities,
                'confidence': self.confidence_score * 0.5,
                'observations': self.observation_counts.get(base_phase, 1)
            }
    
    # STRATEGY 4: No match at all - return empty
        logger.debug(f"⚠️ No predictions available for phase '{current_phase}'")
        return {
            'next_phase_probabilities': {},
            'confidence': 0.0,
            'observations': 0
        }
    
    
    def get_debug_info(self) -> dict:
        """
        Get detailed debug information about transitions and matrix.
        This is helpful for troubleshooting.
        """
        transitions = self._load_transitions()
        
        # Analyze transitions
        phase_counter = Counter()
        from_phase_counter = Counter()
        to_phase_counter = Counter()
        tokens_with_transitions = set()
        
        for trans in transitions:
            from_phase = trans.get('from_phase')
            to_phase = trans.get('to_phase')
            token = trans.get('token_address')
            
            if from_phase:
                from_phase_counter[from_phase] += 1
            if to_phase:
                to_phase_counter[to_phase] += 1
            if from_phase and to_phase:
                phase_counter[f"{from_phase} → {to_phase}"] += 1
            if token:
                tokens_with_transitions.add(token)
        
        return {
            'total_transitions_logged': len(transitions),
            'unique_tokens': len(tokens_with_transitions),
            'matrix_states': len(self.transition_matrix),
            'matrix_confidence': self.confidence_score,
            'confidence_label': self._get_confidence_label(self.confidence_score),
            'from_phases': dict(from_phase_counter),
            'to_phases': dict(to_phase_counter),
            'top_transitions': dict(phase_counter.most_common(10)),
            'sample_transitions': transitions[:5] if transitions else [],
            'matrix_exists': self.matrix_file.exists(),
            'transitions_file_exists': self.transitions_file.exists()
                    }
