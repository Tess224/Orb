"""
Metric Alert System - Browser Notification Compatible
Detects real-time metrics and slippage changes, stores alerts for notifications.
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class MetricAlert:
    """Single alert for metric/slippage change."""
    timestamp: float
    token_address: str
    alert_type: str  # 'phase_change', 'signal_change', 'vts_spike', 'pii_change', 'vei_warning', 'slippage_change'
    severity: str  # 'low', 'medium', 'high', 'critical'
    message: str  # Brief notification message
    details: List[str]  # Detailed changes
    
    # Previous and new values for tracking
    changes: Dict = field(default_factory=dict)


class MetricAlertManager:
    """Manages per-token alert subscriptions and change detection."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.alerts_file = self.data_dir / "metric_alerts.json"
        
        # Track which tokens have alerts enabled (user preferences)
        self.enabled_tokens: set = set()
        
        # Track last known state for each token
        self.last_metrics_state: Dict[str, dict] = {}
        self.last_slippage_state: Dict[str, dict] = {}
        
        # Store alerts (in memory, limited to last 100 per token)
        self.alerts_per_token: Dict[str, List[MetricAlert]] = {}
        
        logger.info("✅ MetricAlertManager initialized")
    
    def enable_alerts(self, token_address: str):
        """Enable alerts for a specific token."""
        self.enabled_tokens.add(token_address)
        logger.info(f"🔔 Alerts enabled for {token_address[:8]}...")
    
    def disable_alerts(self, token_address: str):
        """Disable alerts for a specific token."""
        self.enabled_tokens.discard(token_address)
        logger.info(f"🔕 Alerts disabled for {token_address[:8]}...")
    
    def is_enabled(self, token_address: str) -> bool:
        """Check if alerts are enabled for this token."""
        return token_address in self.enabled_tokens
    
    def check_metrics_changes(self, token_address: str, current_snapshot) -> Optional[MetricAlert]:
        """
        Check for significant metric changes.
        Returns alert if changes detected and alerts are enabled.
        """
        # Skip if alerts not enabled for this token
        if not self.is_enabled(token_address):
            return None
        
        last_state = self.last_metrics_state.get(token_address)
        
        # Extract current state
        current_state = {
            'phase': current_snapshot.phase,
            'signal': self._classify_signal(
                current_snapshot.vts,
                current_snapshot.pii,
                current_snapshot.bsr_1h
            ),
            'vts': current_snapshot.vts,
            'pii': current_snapshot.pii,
            'vei': current_snapshot.vei
        }
        
        # First time seeing this token - just store state
        if not last_state:
            self.last_metrics_state[token_address] = current_state
            return None
        
        # Check for changes
        alert = None
        
        # 1. Phase change (HIGH priority)
        if current_state['phase'] != last_state['phase']:
            alert = MetricAlert(
                timestamp=current_snapshot.timestamp,
                token_address=token_address,
                alert_type='phase_change',
                severity='high',
                message=f"Phase: {last_state['phase']} → {current_state['phase']}",
                details=[
                    f"Phase transition: {last_state['phase']} → {current_state['phase']}",
                    f"VTS: {current_state['vts']:.2f}",
                    f"PII: {current_state['pii']:.2f}"
                ],
                changes={
                    'old_phase': last_state['phase'],
                    'new_phase': current_state['phase']
                }
            )
        
        # 2. Signal change (MEDIUM priority)
        elif current_state['signal'] != last_state['signal']:
            alert = MetricAlert(
                timestamp=current_snapshot.timestamp,
                token_address=token_address,
                alert_type='signal_change',
                severity='medium',
                message=f"Signal: {last_state['signal']} → {current_state['signal']}",
                details=[
                    f"Signal change: {last_state['signal']} → {current_state['signal']}",
                    f"VTS: {last_state['vts']:.2f} → {current_state['vts']:.2f}",
                    f"PII: {last_state['pii']:.2f} → {current_state['pii']:.2f}"
                ],
                changes={
                    'old_signal': last_state['signal'],
                    'new_signal': current_state['signal']
                }
            )
        
        # 3. VTS spike >50% (CRITICAL if >100%, MEDIUM otherwise)
        vts_change_pct = abs(current_state['vts'] - last_state['vts']) / (last_state['vts'] + 0.01) * 100
        if vts_change_pct > 50:
            severity = 'critical' if vts_change_pct > 100 else 'medium'
            alert = MetricAlert(
                timestamp=current_snapshot.timestamp,
                token_address=token_address,
                alert_type='vts_spike',
                severity=severity,
                message=f"VTS: {last_state['vts']:.2f} → {current_state['vts']:.2f} ({vts_change_pct:.0f}%)",
                details=[
                    f"Volume surge: {vts_change_pct:.0f}% change",
                    f"VTS: {last_state['vts']:.2f} → {current_state['vts']:.2f}",
                    f"Current phase: {current_state['phase']}"
                ],
                changes={
                    'old_vts': last_state['vts'],
                    'new_vts': current_state['vts'],
                    'change_pct': vts_change_pct
                }
            )
        
        # 4. PII significant change >0.2 (MEDIUM priority)
        elif abs(current_state['pii'] - last_state['pii']) > 0.2:
            alert = MetricAlert(
                timestamp=current_snapshot.timestamp,
                token_address=token_address,
                alert_type='pii_change',
                severity='medium',
                message=f"PII: {last_state['pii']:.2f} → {current_state['pii']:.2f}",
                details=[
                    f"Pressure shift: {last_state['pii']:.2f} → {current_state['pii']:.2f}",
                    f"Direction: {'Buy pressure' if current_state['pii'] > 0 else 'Sell pressure'}",
                    f"Phase: {current_state['phase']}"
                ],
                changes={
                    'old_pii': last_state['pii'],
                    'new_pii': current_state['pii']
                }
            )
        
        # 5. VEI exhaustion warning (HIGH priority)
        elif current_state['vei'] < 0.3 and last_state['vei'] >= 0.3:
            alert = MetricAlert(
                timestamp=current_snapshot.timestamp,
                token_address=token_address,
                alert_type='vei_warning',
                severity='high',
                message=f"Volume exhaustion: VEI {last_state['vei']:.2f} → {current_state['vei']:.2f}",
                details=[
                    "⚠️ Volume exhaustion detected",
                    f"VEI dropped: {last_state['vei']:.2f} → {current_state['vei']:.2f}",
                    f"Phase: {current_state['phase']}"
                ],
                changes={
                    'old_vei': last_state['vei'],
                    'new_vei': current_state['vei']
                }
            )
        
        # Update last known state
        self.last_metrics_state[token_address] = current_state
        
        # Store and return alert if generated
        if alert:
            self._add_alert(alert)
            logger.info(f"🚨 {alert.severity.upper()} Alert: {alert.message}")
        
        return alert
    
    def check_slippage_changes(self, token_address: str, current_slippage: Dict) -> Optional[MetricAlert]:
        """
        Check for slippage analysis changes (PRE_PUMP, PRE_DUMP, etc).
        Returns alert if significant changes detected.
        """
        # Skip if alerts not enabled
        if not self.is_enabled(token_address):
            return None
        
        if not current_slippage:
            return None
        
        last_state = self.last_slippage_state.get(token_address)
        
        # Extract current state
        current_state = {
            'state': current_slippage.get('state', 'UNKNOWN'),
            'severity': current_slippage.get('severity', 'UNKNOWN'),
            'is_honeypot': current_slippage.get('is_honeypot', False),
            'liquidity_health': current_slippage.get('slippage_signal', {}).get('liquidity_health', 'unknown')
        }
        
        # First time - just store
        if not last_state:
            self.last_slippage_state[token_address] = current_state
            return None
        
        alert = None
        
        # Check for state changes (PRE_PUMP, PRE_DUMP, etc)
        if current_state['state'] != last_state['state']:
            # Determine severity based on new state
            if 'HONEYPOT' in current_state['state'] or current_state['is_honeypot']:
                severity = 'critical'
                message = f"🚨 HONEYPOT DETECTED"
            elif 'PRE_DUMP' in current_state['state']:
                severity = 'critical'
                message = f"⚠️ PRE-DUMP: {last_state['state']} → {current_state['state']}"
            elif 'PRE_PUMP' in current_state['state']:
                severity = 'high'
                message = f"🚀 PRE-PUMP: {last_state['state']} → {current_state['state']}"
            else:
                severity = 'medium'
                message = f"Slippage: {last_state['state']} → {current_state['state']}"
            
            alert = MetricAlert(
                timestamp=datetime.now().timestamp(),
                token_address=token_address,
                alert_type='slippage_change',
                severity=severity,
                message=message,
                details=[
                    f"State: {last_state['state']} → {current_state['state']}",
                    f"Severity: {current_state['severity']}",
                    f"Liquidity: {current_state['liquidity_health']}"
                ],
                changes={
                    'old_state': last_state['state'],
                    'new_state': current_state['state']
                }
            )
        
        # Update state
        self.last_slippage_state[token_address] = current_state
        
        if alert:
            self._add_alert(alert)
            logger.info(f"🚨 Slippage Alert: {alert.message}")
        
        return alert
    
    def _classify_signal(self, vts: float, pii: float, bsr: float) -> str:
        """Classify as bullish, neutral, or bearish."""
        if vts > 1.5 and pii > 0.15 and bsr > 1.3:
            return 'bullish'
        elif (vts < 0.8 or pii < -0.15) and bsr < 0.8:
            return 'bearish'
        else:
            return 'neutral'
    
    def _add_alert(self, alert: MetricAlert):
        """Store alert in memory and disk."""
        token = alert.token_address
        
        if token not in self.alerts_per_token:
            self.alerts_per_token[token] = []
        
        self.alerts_per_token[token].append(alert)
        
        # Keep only last 100 alerts per token
        if len(self.alerts_per_token[token]) > 100:
            self.alerts_per_token[token].pop(0)
        
        # Persist to disk
        self._save_alert_to_disk(alert)
    
    def _save_alert_to_disk(self, alert: MetricAlert):
        """Append alert to disk file."""
        try:
            if self.alerts_file.exists():
                with open(self.alerts_file, 'r') as f:
                    all_alerts = json.load(f)
            else:
                all_alerts = []
            
            all_alerts.append({
                'timestamp': alert.timestamp,
                'token_address': alert.token_address,
                'alert_type': alert.alert_type,
                'severity': alert.severity,
                'message': alert.message,
                'details': alert.details,
                'changes': alert.changes
            })
            
            # Keep last 500 alerts
            if len(all_alerts) > 500:
                all_alerts = all_alerts[-500:]
            
            with open(self.alerts_file, 'w') as f:
                json.dump(all_alerts, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving alert: {e}")
    
    def get_alerts(self, token_address: str, limit: int = 20) -> List[dict]:
        """Get recent alerts for a token."""
        alerts = self.alerts_per_token.get(token_address, [])
        alerts = alerts[-limit:]
        
        return [{
            'timestamp': a.timestamp,
            'alert_type': a.alert_type,
            'severity': a.severity,
            'message': a.message,
            'details': a.details,
            'changes': a.changes,
            'time_ago': self._time_ago(a.timestamp)
        } for a in alerts]
    
    def clear_alerts(self, token_address: str):
        """Clear alerts for a token."""
        if token_address in self.alerts_per_token:
            self.alerts_per_token[token_address] = []
        logger.info(f"🧹 Cleared alerts for {token_address[:8]}...")
    
    def _time_ago(self, timestamp: float) -> str:
        """Convert to 'time ago' string."""
        seconds = datetime.now().timestamp() - timestamp
        if seconds < 60:
            return f"{int(seconds)}s ago"
        elif seconds < 3600:
            return f"{int(seconds/60)}m ago"
        elif seconds < 86400:
            return f"{int(seconds/3600)}h ago"
        else:
            return f"{int(seconds/86400)}d ago"