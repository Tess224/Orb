"""
State Transition Analysis System

This module processes historical snapshot data to calculate transition probabilities.
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
from collections import defaultdict, Counter
from pathlib import Path

logger = logging.getLogger(__name__)


class StateTransitionAnalyzer:
    """
    Analyzes historical data to build transition probability matrices.
    
    This is the brain that learns from history. After processing thousands of
    snapshots, it can tell you things like "tokens in early phase with high VTS
    stay in early phase seventy percent of the time over the next hour."
    """
    
    def __init__(self, data_directory: str = "historical_data"):
        """
        Initialize the analyzer.
        
        Args:
            data_directory: Where historical JSON files are stored
        """
        self.data_directory = data_directory
        
        # This will store our learned transition probabilities
        # Structure: {state_key: {next_state: probability}}
        self.transition_matrix = {}
        
        # Track how many observations went into each probability
        # This helps us know which probabilities are reliable
        self.observation_counts = {}
        
        logger.info("🧠 State Transition Analyzer initialized")
    
    
    def discretize_metrics(self, snapshot: Dict) -> Dict[str, str]:
        """
        Convert continuous metrics into discrete buckets.
        
        Instead of storing exact VTS values like 2.47, we bucket them into
        categories like "medium". This makes it possible to find patterns
        because we see the same bucket values many times.
        
        Think of it like rounding - instead of dealing with infinite possible
        temperatures, we say "cold", "warm", or "hot".
        
        Args:
            snapshot: A snapshot dictionary with metrics
            
        Returns:
            Dictionary mapping metric names to bucket labels
        """
        discretized = {}
        
        # VTS buckets
        vts = snapshot.get('vts', 1.0)
        if vts < 1.5:
            discretized['vts'] = 'low'
        elif vts < 3.0:
            discretized['vts'] = 'medium'
        else:
            discretized['vts'] = 'high'
        
        # PII buckets (pressure intensity)
        pii = snapshot.get('pii', 0.0)
        if abs(pii) < 0.3:
            discretized['pii'] = 'neutral'
        elif pii >= 0.3:
            discretized['pii'] = 'strong_buy'
        else:
            discretized['pii'] = 'strong_sell'
        
        # VEI buckets (exhaustion)
        vei = snapshot.get('vei', 1.0)
        if vei > 0.7:
            discretized['vei'] = 'fresh'
        elif vei > 0.4:
            discretized['vei'] = 'moderate'
        else:
            discretized['vei'] = 'exhausted'
        
        # Conviction buckets
        conviction = snapshot.get('conviction_multiplier', 1.0)
        if conviction < 0.8:
            discretized['conviction'] = 'weak'
        elif conviction < 1.2:
            discretized['conviction'] = 'neutral'
        else:
            discretized['conviction'] = 'strong'
        
        return discretized
    
    
    def create_state_key(self, snapshot: Dict) -> str:
        """
        Create a unique identifier for a state.
        
        A state is defined by the phase plus discretized metrics. This key
        is used to look up transition probabilities.
        
        Example: "early_high-vts_strong-buy_fresh_strong-conviction"
        
        Args:
            snapshot: Snapshot dictionary
            
        Returns:
            String key representing this state
        """
        phase = snapshot.get('phase', 'dormant')
        metrics = self.discretize_metrics(snapshot)
        
        # Build a composite key
        key_parts = [
            phase,
            f"{metrics['vts']}-vts",
            f"{metrics['pii']}-pii",
            f"{metrics['vei']}-vei",
            f"{metrics['conviction']}-conviction"
        ]
        
        return "_".join(key_parts)
    
    
    def analyze_token_history(self, snapshots: List[Dict], time_horizon_hours: int = 1) -> Dict:
        """
        Analyze one token's history to extract state transitions.
        
        We walk through the snapshots chronologically. For each snapshot, we
        record its current state and then look ahead to see what state it
        transitioned to after the specified time horizon.
        
        Args:
            snapshots: List of snapshot dictionaries for one token
            time_horizon_hours: How many hours ahead to look (default 1)
            
        Returns:
            Dictionary of observed transitions
        """
        transitions = defaultdict(list)
        
        # We need snapshots sorted by time
        snapshots = sorted(snapshots, key=lambda s: s['timestamp'])
        
        # Walk through each snapshot
        for i in range(len(snapshots) - 1):
            current_snapshot = snapshots[i]
            current_state = self.create_state_key(current_snapshot)
            current_time = current_snapshot['timestamp']
            
            # Find the snapshot that's approximately time_horizon_hours later
            target_time = current_time + (time_horizon_hours * 3600)
            
            # Find the closest snapshot to our target time
            # We look for a snapshot within +/- 10 minutes of the target
            best_match = None
            best_diff = float('inf')
            
            for j in range(i + 1, len(snapshots)):
                candidate = snapshots[j]
                time_diff = abs(candidate['timestamp'] - target_time)
                
                # If we're within 10 minutes, consider it
                if time_diff < 600:  # 600 seconds = 10 minutes
                    if time_diff < best_diff:
                        best_diff = time_diff
                        best_match = candidate
                
                # If we've gone past our target time by more than 10 minutes, stop looking
                if candidate['timestamp'] > target_time + 600:
                    break
            
            # If we found a matching future snapshot, record the transition
            if best_match:
                next_state = self.create_state_key(best_match)
                transitions[current_state].append(next_state)
        
        return transitions
    
    
    def build_transition_matrix(self, min_observations: int = 10):
        """
        Process all historical data to build the transition probability matrix.
        
        This is the main analysis function. It loads all token histories,
        extracts transitions, and calculates probabilities.
        
        Args:
            min_observations: Minimum number of observations needed to trust a probability
        """
        logger.info("🔬 Building transition matrix from historical data...")
        
        # Collect transitions from all tokens
        all_transitions = defaultdict(list)
        
        # Get all token files
        data_path = Path(self.data_directory)
        if not data_path.exists():
            logger.warning(f"⚠️ Data directory {self.data_directory} doesn't exist yet")
            return
        
        token_files = list(data_path.glob("*.json"))
        logger.info(f"📂 Found {len(token_files)} token history files")
        
        if len(token_files) == 0:
            logger.warning("⚠️ No historical data files found. Keep running to collect data.")
            return
        
        # Process each token's history
        tokens_processed = 0
        total_transitions = 0
        
        for file_path in token_files:
            try:
                with open(file_path, 'r') as f:
                    snapshots = json.load(f)
                
                if len(snapshots) < 10:
                    # Not enough data from this token yet
                    continue
                
                # Extract transitions from this token
                token_transitions = self.analyze_token_history(snapshots)
                
                # Merge into our overall collection
                for state, next_states in token_transitions.items():
                    all_transitions[state].extend(next_states)
                    total_transitions += len(next_states)
                
                tokens_processed += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing {file_path.name}: {e}")
        
        logger.info(f"✅ Processed {tokens_processed} tokens, found {total_transitions} transitions")
        
        # Now calculate probabilities from frequencies
        for state, next_states in all_transitions.items():
            observation_count = len(next_states)
            
            # Only calculate probabilities if we have enough observations
            if observation_count < min_observations:
                continue
            
            # Count frequency of each next state
            state_counts = Counter(next_states)
            
            # Convert counts to probabilities
            probabilities = {}
            for next_state, count in state_counts.items():
                probabilities[next_state] = count / observation_count
            
            # Store in our matrix
            self.transition_matrix[state] = probabilities
            self.observation_counts[state] = observation_count
        
        logger.info(f"📊 Built transition matrix with {len(self.transition_matrix)} states")
        
        # Log some example transitions
        if len(self.transition_matrix) > 0:
            example_state = list(self.transition_matrix.keys())[0]
            example_probs = self.transition_matrix[example_state]
            logger.info(f"📝 Example state: {example_state}")
            logger.info(f"   Transitions: {dict(list(example_probs.items())[:3])}")
    
    
    def get_transition_probabilities(self, current_snapshot: Dict) -> Optional[Dict]:
        """
        Get transition probabilities for a current state.
        
        This is what your real-time system will call. Given the current metrics,
        it returns probabilities for what will happen next.
        
        Args:
            current_snapshot: Current metrics snapshot
            
        Returns:
            Dictionary mapping next states to probabilities, or None if no data
        """
        state_key = self.create_state_key(current_snapshot)
        
        if state_key not in self.transition_matrix:
            # We haven't seen this state enough times to have reliable probabilities
            return None
        
        probabilities = self.transition_matrix[state_key]
        observation_count = self.observation_counts[state_key]
        
        return {
            'probabilities': probabilities,
            'confidence': min(observation_count / 50, 1.0),  # Confidence scales with observations
            'observations': observation_count
        }
    
    
    def save_matrix(self, filename: str = "transition_matrix.json"):
        """
        Save the transition matrix to a file so we don't have to recalculate it.
        
        Args:
            filename: Where to save the matrix
        """
        try:
            data = {
                'transition_matrix': self.transition_matrix,
                'observation_counts': self.observation_counts,
                'metadata': {
                    'num_states': len(self.transition_matrix),
                    'total_observations': sum(self.observation_counts.values())
                }
            }
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"💾 Saved transition matrix to {filename}")
            
        except Exception as e:
            logger.error(f"❌ Error saving matrix: {e}")
    
    
    def load_matrix(self, filename: str = "transition_matrix.json") -> bool:
        """
        Load a previously saved transition matrix.
        
        Args:
            filename: File to load from
            
        Returns:
            True if loaded successfully
        """
        try:
            if not Path(filename).exists():
                logger.warning(f"⚠️ Matrix file {filename} doesn't exist")
                return False
            
            with open(filename, 'r') as f:
                data = json.load(f)
            
            self.transition_matrix = data['transition_matrix']
            self.observation_counts = data['observation_counts']
            
            logger.info(f"📖 Loaded transition matrix from {filename}")
            logger.info(f"   States: {data['metadata']['num_states']}")
            logger.info(f"   Total observations: {data['metadata']['total_observations']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading matrix: {e}")
            return False
