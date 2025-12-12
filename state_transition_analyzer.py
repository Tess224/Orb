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
        
        logger.info("🔧 Initializing state transition analyzer...")
        self._load_matrix()
        logger.info("🧠 State Transition Analyzer initialized")
    
    def _load_matrix(self):
        """Load existing transition matrix from disk if it exists."""
        if self.matrix_file.exists():
            try:
                with open(self.matrix_file, 'r') as f:
                    data = json.load(f)
                    self.transition_matrix = data.get('matrix', {})
                    self.confidence_score = data.get('confidence', 0.0)
                    self.total_transitions = data.get('total_transitions', 0)
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
                'last_updated': datetime.utcnow().isoformat(),
                'states': list(self.transition_matrix.keys())
            }
            with open(self.matrix_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"💾 Saved transition matrix to {self.matrix_file}")
        except Exception as e:
            logger.error(f"❌ Error saving transition matrix: {e}")
    
    def log_transition(self, token_address: str, from_phase: str, to_phase: str, 
                      metrics: dict, duration_seconds: float):
        """
        Log a phase transition when it happens in real-time.
        This is called by MetricsManager when a phase change is detected.
        
        Args:
            token_address: The token that transitioned
            from_phase: Previous phase name
            to_phase: New phase name
            metrics: Current metrics snapshot
            duration_seconds: How long the token was in from_phase
        """
        # Create transition record with all relevant data
        transition = {
            'token_address': token_address,
            'from_phase': from_phase,
            'to_phase': to_phase,
            'timestamp': datetime.utcnow().isoformat(),
            'duration_seconds': duration_seconds,
            'metrics': {
                'bsr': metrics.get('bsr', 0),
                'vlr': metrics.get('vlr', 0),
                'pii': metrics.get('pii', 0),
                'vts': metrics.get('vts', 0),
                'vei': metrics.get('vei', 0),
                'token_age_hours': metrics.get('token_age_hours', 0)
            }
        }
        
        # Load existing transitions
        transitions = self._load_transitions()
        
        # Add new transition
        transitions.append(transition)
        
        # Save back to disk
        self._save_transitions(transitions)
        
        logger.info(f"📝 Logged transition: {from_phase} → {to_phase} for {token_address[:8]}...")
        logger.info(f"   Duration in {from_phase}: {duration_seconds:.0f}s ({duration_seconds/60:.1f}m)")
    
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
    
    def build_transition_matrix(self, min_observations: int = 5) -> dict:
        """
        Build the transition probability matrix from historical data.
        Now includes detailed debug logging and confidence scoring.
        
        Args:
            min_observations: Minimum number of transitions required to build matrix (default: 5)
        
        Returns:
            Dictionary with matrix, confidence, and diagnostic info
        """
        logger.info("🔬 Starting transition matrix analysis...")
        logger.info("📬 Building transition matrix from historical data...")
        
        # Load all transitions
        transitions = self._load_transitions()
        total_transitions = len(transitions)
        
        logger.info(f"📊 Found {total_transitions} total transition records in history file")
        
        # Debug: Show sample of transitions
        if total_transitions > 0:
            logger.info("🔍 Sample transitions (first 3):")
            for i, trans in enumerate(transitions[:3]):
                logger.info(f"   [{i+1}] {trans.get('from_phase', 'UNKNOWN')} → {trans.get('to_phase', 'UNKNOWN')} "
                          f"at {trans.get('timestamp', 'N/A')}")
        
        # Group transitions by token
        token_transitions = defaultdict(list)
        for trans in transitions:
            token_addr = trans.get('token_address')
            if token_addr:
                token_transitions[token_addr].append(trans)
        
        num_tokens = len(token_transitions)
        logger.info(f"🪙 Processed {num_tokens} unique tokens")
        
        # Debug: Show transitions per token
        for token_addr, trans_list in list(token_transitions.items())[:3]:
            logger.info(f"   Token {token_addr[:8]}... has {len(trans_list)} transitions")
        
        # Count transitions between states
        transition_counts = defaultdict(lambda: defaultdict(int))
        valid_transitions = 0
        invalid_transitions = 0
        
        for trans in transitions:
            from_phase = trans.get('from_phase')
            to_phase = trans.get('to_phase')
            
            # Validate transition has required fields
            if from_phase and to_phase and from_phase != to_phase:
                transition_counts[from_phase][to_phase] += 1
                valid_transitions += 1
            else:
                invalid_transitions += 1
                logger.debug(f"⚠️ Invalid transition: {from_phase} → {to_phase}")
        
        logger.info(f"✅ Valid transitions: {valid_transitions}")
        logger.info(f"❌ Invalid transitions (skipped): {invalid_transitions}")
        
        # Debug: Show what states were found
        unique_states = set(transition_counts.keys())
        logger.info(f"🎯 Found {len(unique_states)} distinct FROM states: {list(unique_states)}")
        
        # Show transition counts
        logger.info("📈 Transition count breakdown:")
        for from_state, to_states in transition_counts.items():
            total_from = sum(to_states.values())
            logger.info(f"   {from_state}: {total_from} transitions")
            for to_state, count in to_states.items():
                logger.info(f"      → {to_state}: {count} times")
        
        # Determine confidence based on number of valid transitions
        confidence = self._calculate_confidence(valid_transitions)
        confidence_label = self._get_confidence_label(confidence)
        
        logger.info(f"🎲 Calculated confidence: {confidence:.1%} ({confidence_label})")
        
        # Don't build matrix if we have too few transitions
        if valid_transitions < min_observations:
            logger.warning(f"⚠️ Only {valid_transitions} valid transitions - need at least {min_observations} to build matrix")
            logger.warning("   Continue tracking tokens to collect more transition data")
            return {
                'success': False,
                'reason': 'insufficient_data',
                'valid_transitions': valid_transitions,
                'required_minimum': min_observations,
                'message': f'Need at least {min_observations} valid transitions to build a matrix'
            }
        
        # Build probability matrix
        matrix = {}
        for from_state, to_states in transition_counts.items():
            total = sum(to_states.values())
            matrix[from_state] = {}
            
            for to_state, count in to_states.items():
                probability = count / total
                matrix[from_state][to_state] = {
                    'probability': probability,
                    'count': count,
                    'sample_size': total
                }
        
        # Store and save
        self.transition_matrix = matrix
        self.confidence_score = confidence
        self.total_transitions = valid_transitions
        self._save_matrix()
        
        logger.info(f"✅ Built transition matrix with {len(matrix)} states")
        logger.info(f"   Confidence: {confidence:.1%} ({confidence_label})")
        logger.info(f"   Based on {valid_transitions} valid transitions")
        
        return {
            'success': True,
            'states': len(matrix),
            'valid_transitions': valid_transitions,
            'invalid_transitions': invalid_transitions,
            'confidence': confidence,
            'confidence_label': confidence_label,
            'matrix': matrix
        }
    
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
