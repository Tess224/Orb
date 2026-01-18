"""
Historical Data Collection for State Transition Analysis

This module saves metric snapshots to JSON files so we can later analyze
patterns and build transition probability matrices.

The data structure is simple: each token gets its own JSON file where we
append snapshots as they occur. Later, we'll process these files to find
patterns like "when VTS was high and phase was early, what happened next?"
"""

import json
import os
import time
import logging
from typing import Dict, List
from pathlib import Path
from dataclasses import asdict

logger = logging.getLogger(__name__)


class HistoricalDataCollector:
    """
    Saves metric snapshots to disk for later analysis.
    
    Think of this as your data recorder. Every minute when a snapshot is taken,
    this class appends it to a JSON file. Over weeks of running, you'll build
    up a dataset of thousands of snapshots showing how different tokens behaved
    in different conditions.
    """
    
    def __init__(self, data_directory: str = "historical_data"):
        """
        Initialize the collector.
        
        Args:
            data_directory: Where to store the JSON files (default: "historical_data")
        """
        self.data_directory = data_directory
        
        # Create the directory if it doesn't exist
        Path(self.data_directory).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📁 Historical data collector initialized: {self.data_directory}")
    
    
    def save_snapshot(self, token_address: str, snapshot) -> bool:
        """
        Save a single snapshot to the token's historical file.
        
        We append to the file rather than overwriting, so each file becomes
        a time-series record of all snapshots for that token.
        
        Args:
            token_address: The token this snapshot is for
            snapshot: A MetricsSnapshot object
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Sanitize token_address to prevent path traversal
            # Only allow alphanumeric characters
            safe_token = ''.join(c for c in token_address if c.isalnum())
            if not safe_token or safe_token != token_address:
                logger.error(f"Invalid token address format: {token_address[:20]}")
                return False

            # Create filename: TOKEN_ADDRESS.json
            filename = os.path.join(self.data_directory, f"{safe_token}.json")

            # Verify the resolved path is still within data_directory
            if not os.path.abspath(filename).startswith(os.path.abspath(self.data_directory)):
                logger.error(f"Path traversal attempt detected: {token_address[:20]}")
                return False
            
            # Convert the snapshot dataclass to a dictionary
            snapshot_dict = asdict(snapshot)
            
            # Read existing data if file exists
            existing_data = []
            if os.path.exists(filename):
                try:
                    with open(filename, 'r') as f:
                        existing_data = json.load(f)
                except json.JSONDecodeError:
                    # File is corrupted, start fresh
                    logger.warning(f"⚠️ Corrupted data file for {token_address[:8]}, starting fresh")
                    existing_data = []
            
            # Append the new snapshot
            existing_data.append(snapshot_dict)
            
            # Write back to file
            with open(filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            # Only log occasionally to avoid spam (every 10th snapshot)
            if len(existing_data) % 10 == 0:
                logger.info(f"💾 Saved snapshot #{len(existing_data)} for {token_address[:8]}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving snapshot for {token_address[:8]}: {e}")
            return False
    
    
    def get_token_history(self, token_address: str) -> List[Dict]:
        """
        Load all historical snapshots for a token.
        
        This is used during analysis to read back the data we've collected.
        
        Args:
            token_address: The token to load history for
            
        Returns:
            List of snapshot dictionaries, empty list if no data exists
        """
        try:
            # Sanitize token_address to prevent path traversal
            safe_token = ''.join(c for c in token_address if c.isalnum())
            if not safe_token or safe_token != token_address:
                logger.error(f"Invalid token address format: {token_address[:20]}")
                return []

            filename = os.path.join(self.data_directory, f"{safe_token}.json")

            # Verify the resolved path is still within data_directory
            if not os.path.abspath(filename).startswith(os.path.abspath(self.data_directory)):
                logger.error(f"Path traversal attempt detected: {token_address[:20]}")
                return []

            if not os.path.exists(filename):
                return []
            
            with open(filename, 'r') as f:
                data = json.load(f)
            
            logger.info(f"📖 Loaded {len(data)} historical snapshots for {token_address[:8]}")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error loading history for {token_address[:8]}: {e}")
            return []
    
    
    def get_all_token_files(self) -> List[str]:
        """
        Get a list of all tokens we have historical data for.
        
        Returns:
            List of token addresses (without .json extension)
        """
        try:
            files = os.listdir(self.data_directory)
            # Filter for JSON files and remove the extension
            tokens = [f[:-5] for f in files if f.endswith('.json')]
            logger.info(f"📚 Found historical data for {len(tokens)} tokens")
            return tokens
        except Exception as e:
            logger.error(f"❌ Error listing token files: {e}")
            return []
