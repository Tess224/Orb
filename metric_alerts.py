"""
Metric Alert System - Enhanced Version
Detects ALL significant changes in real-time metrics and slippage.

Alert Types:
- VTS change > 50%
- PII change > 30%  
- Phase change (dormant, early, mid, late, etc.)
- Metrics direction change (bullish/bearish/neutral)
- Market state change (PRE_PUMP, PRE_DUMP, HOLDING, etc.)
- Asymmetry analysis change
- VEI exhaustion warning
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
    alert_type: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    message: str
    details: List[str]
    changes: Dict = field(default_factory=dict)


class MetricAlertManager:
    """Manages per-token alert subscriptions and change detection."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.alerts_file = self.data_dir / "metric_alerts.json"
        
        # Track which tokens have alerts enabled
        self.enabled_tokens: set = set()
        
        # Track last known state for each token
        self.last_metrics_state: Dict[str, dict] = {}
        self.last_slippage_state: Dict[str, dict] = {}
        
        # Store alerts (in memory, limited to last 100 per token)
        self.alerts_per_token: Dict[str, List[MetricAlert]] = {}
        
        logger.info("✅ MetricAlertManager initialized (Enhanced Version)")
    
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
    
    def check_metrics_changes(self, token_address: str, current_snapshot) -> List[MetricAlert]:
        """
        Check for ALL significant metric changes.
        Returns LIST of alerts (not just one).
        
        Checks:
        - VTS change > 50%
        - PII change > 30%
        - Phase change
        - Signal/direction change
        - VEI exhaustion
        """
        alerts = []
        
        # Skip if alerts not enabled for this token
        if not self.is_enabled(token_address):
            return alerts
        
        last_state = self.last_metrics_state.get(token_address)
        
        # Extract current state
        current_state = {
            'phase': current_snapshot.phase,
            'direction': self._classify_direction(
                current_snapshot.vts,
                current_snapshot.pii,
                current_snapshot.bsr_1h
            ),
            'vts': current_snapshot.vts,
            'pii': current_snapshot.pii,
            'vei': current_snapshot.vei,
            'bsr': current_snapshot.bsr_1h,
            'timestamp': current_snapshot.timestamp
        }
        
        # First time seeing this token - just store state
        if not last_state:
            self.last_metrics_state[token_address] = current_state
            logger.info(f"📊 Initial state for {token_address[:8]}: phase={current_state['phase']}, VTS={current_state['vts']:.2f}, PII={current_state['pii']:.2f}")
            return alerts
        
        # ========================================
        # CHECK 1 & 2: VTS and PII Changes (Always Reported Together)
        # ========================================
        old_vts = last_state['vts']
        new_vts = current_state['vts']
        old_pii = last_state['pii']
        new_pii = current_state['pii']
        
        # Calculate VTS change percentage
        vts_change_pct = 0
        if old_vts > 0.01:
            vts_change_pct = abs(new_vts - old_vts) / old_vts * 100
        
        # Calculate PII change percentage
        pii_denominator = max(abs(old_pii), 0.01)
        pii_change_pct = abs(new_pii - old_pii) / pii_denominator * 100
        
        # If EITHER VTS > 50% OR PII > 30%, report BOTH together
        vts_triggered = vts_change_pct > 50
        pii_triggered = pii_change_pct > 30
        
        if vts_triggered or pii_triggered:
            # Determine overall severity
            if vts_change_pct > 100 or (pii_triggered and (old_pii * new_pii < 0)):
                severity = 'critical'
            elif vts_triggered or (old_pii < 0 and new_pii > 0) or (old_pii > 0 and new_pii < 0):
                severity = 'high'
            else:
                severity = 'medium'
            
            # Build descriptive message showing both metrics
            vts_dir = "↑" if new_vts > old_vts else "↓"
            pii_dir = "↑" if new_pii > old_pii else "↓"
            
            # Determine pressure state
            if new_pii > 0.1:
                pressure = "🟢 BUY PRESSURE"
            elif new_pii < -0.1:
                pressure = "🔴 SELL PRESSURE"
            else:
                pressure = "⚪ NEUTRAL"
            
            # Create combined message
            message = f"📊 VTS: {old_vts:.2f}→{new_vts:.2f} ({vts_dir}{vts_change_pct:.0f}%) | PII: {old_pii:.2f}→{new_pii:.2f} ({pii_dir}{pii_change_pct:.0f}%) | {pressure}"
            
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='vts_pii_change',
                severity=severity,
                message=message,
                details=[
                    f"VTS: {old_vts:.2f} → {new_vts:.2f} ({vts_change_pct:.0f}% change)",
                    f"PII: {old_pii:.3f} → {new_pii:.3f} ({pii_change_pct:.0f}% change)",
                    f"Pressure: {pressure}",
                    f"Phase: {current_state['phase']}",
                    f"Trigger: {'VTS' if vts_triggered else ''}{' + ' if vts_triggered and pii_triggered else ''}{'PII' if pii_triggered else ''}"
                ],
                changes={
                    'vts_old': old_vts,
                    'vts_new': new_vts,
                    'vts_change_pct': vts_change_pct,
                    'pii_old': old_pii,
                    'pii_new': new_pii,
                    'pii_change_pct': pii_change_pct,
                    'vts_triggered': vts_triggered,
                    'pii_triggered': pii_triggered
                }
            ))
        
        # ========================================
        # CHECK 3: Phase Change
        # ========================================
        if current_state['phase'] != last_state['phase']:
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='phase_change',
                severity='high',
                message=f"⚡ PHASE: {last_state['phase']} → {current_state['phase']}",
                details=[
                    f"Token phase changed",
                    f"Old: {last_state['phase']}",
                    f"New: {current_state['phase']}",
                    f"VTS: {current_state['vts']:.2f}, PII: {current_state['pii']:.3f}"
                ],
                changes={
                    'metric': 'phase',
                    'old_value': last_state['phase'],
                    'new_value': current_state['phase']
                }
            ))
        
        # ========================================
        # CHECK 4: Metrics Direction Change
        # ========================================
        if current_state['direction'] != last_state['direction']:
            # Determine severity based on direction change
            if 'bullish' in current_state['direction'] and 'bearish' in last_state['direction']:
                severity = 'high'
                emoji = "🟢"
            elif 'bearish' in current_state['direction'] and 'bullish' in last_state['direction']:
                severity = 'high'
                emoji = "🔴"
            else:
                severity = 'medium'
                emoji = "🔄"
            
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='direction_change',
                severity=severity,
                message=f"{emoji} DIRECTION: {last_state['direction']} → {current_state['direction']}",
                details=[
                    f"Metrics direction changed",
                    f"Old: {last_state['direction']}",
                    f"New: {current_state['direction']}",
                    f"VTS: {current_state['vts']:.2f}, PII: {current_state['pii']:.3f}"
                ],
                changes={
                    'metric': 'direction',
                    'old_value': last_state['direction'],
                    'new_value': current_state['direction']
                }
            ))
        
        # ========================================
        # CHECK 5: VEI Exhaustion Warning
        # ========================================
        old_vei = last_state['vei']
        new_vei = current_state['vei']
        
        # Entering exhaustion
        if new_vei < 0.3 and old_vei >= 0.3:
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='vei_exhaustion',
                severity='high',
                message=f"⚠️ EXHAUSTION: VEI {old_vei:.2f} → {new_vei:.2f}",
                details=[
                    "Volume exhaustion detected",
                    f"VEI dropped below threshold",
                    f"Old: {old_vei:.2f} → New: {new_vei:.2f}"
                ],
                changes={
                    'metric': 'vei',
                    'old_value': old_vei,
                    'new_value': new_vei,
                    'event': 'exhaustion_entered'
                }
            ))
        
        # Recovery from exhaustion
        elif new_vei > 0.5 and old_vei <= 0.3:
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='vei_recovery',
                severity='medium',
                message=f"✅ RECOVERY: VEI {old_vei:.2f} → {new_vei:.2f}",
                details=[
                    "Volume recovered from exhaustion",
                    f"VEI rose above threshold",
                    f"Old: {old_vei:.2f} → New: {new_vei:.2f}"
                ],
                changes={
                    'metric': 'vei',
                    'old_value': old_vei,
                    'new_value': new_vei,
                    'event': 'recovery'
                }
            ))
        
        # Update last known state
        self.last_metrics_state[token_address] = current_state
        
        # Store all alerts
        for alert in alerts:
            self._add_alert(alert)
            logger.info(f"🚨 {alert.severity.upper()} Alert: {alert.message}")
        
        # Send push notification for each alert
        try:
            from web_push_service import get_push_service
            push_svc = get_push_service()
            if push_svc and push_svc.configured:
                for alert in alerts:
                    push_svc.send_alert_notification(alert)
        except Exception as e:
            logger.error(f"Push notification error: {e}")
        
        return alerts
    
    def check_slippage_changes(self, token_address: str, current_slippage: Dict) -> List[MetricAlert]:
        """
        Check for slippage/liquidity structure changes.
        Returns LIST of alerts.
        
        Checks:
        - Market state change (PRE_PUMP, PRE_DUMP, HOLDING, etc.)
        - Asymmetry ratio significant change
        - Honeypot detection
        """
        alerts = []
        
        # Skip if alerts not enabled
        if not self.is_enabled(token_address):
            return alerts
        
        if not current_slippage:
            return alerts
        
        last_state = self.last_slippage_state.get(token_address)
        
        # Extract current state
        slippage_data = current_slippage.get('slippage_data', {})
        asymmetry_data = slippage_data.get('asymmetry', {})
        
        current_state = {
            'state': current_slippage.get('state', 'UNKNOWN'),
            'severity': current_slippage.get('severity', 'UNKNOWN'),
            'is_honeypot': current_slippage.get('is_honeypot', False),
            'asymmetry_ratio': asymmetry_data.get('average_ratio', 1.0) if isinstance(asymmetry_data, dict) else 1.0,
            'pump_score': current_slippage.get('scores', {}).get('pre_pump_score', 0),
            'dump_score': current_slippage.get('scores', {}).get('pre_dump_score', 0),
            'timestamp': datetime.now().timestamp()
        }
        
        # First time - just store
        if not last_state:
            self.last_slippage_state[token_address] = current_state
            logger.info(f"📊 Initial slippage state for {token_address[:8]}: state={current_state['state']}, asymmetry={current_state['asymmetry_ratio']:.2f}")
            return alerts
        
        # ========================================
        # CHECK 1: Market State Change
        # ========================================
        if current_state['state'] != last_state['state']:
            # Determine severity and emoji based on new state
            if 'HONEYPOT' in current_state['state'] or current_state['is_honeypot']:
                severity = 'critical'
                message = f"🚨 HONEYPOT DETECTED!"
            elif 'PRE_DUMP' in current_state['state']:
                severity = 'critical'
                message = f"🔴 PRE-DUMP: {last_state['state']} → {current_state['state']}"
            elif 'PRE_PUMP' in current_state['state']:
                severity = 'high'
                message = f"🟢 PRE-PUMP: {last_state['state']} → {current_state['state']}"
            elif current_state['state'] == 'HOLDING':
                severity = 'medium'
                message = f"⏸️ HOLDING: {last_state['state']} → {current_state['state']}"
            else:
                severity = 'medium'
                message = f"🔄 STATE: {last_state['state']} → {current_state['state']}"
            
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='market_state_change',
                severity=severity,
                message=message,
                details=[
                    f"Market state changed",
                    f"Old: {last_state['state']}",
                    f"New: {current_state['state']}",
                    f"Pump score: {current_state['pump_score']}, Dump score: {current_state['dump_score']}"
                ],
                changes={
                    'metric': 'market_state',
                    'old_value': last_state['state'],
                    'new_value': current_state['state']
                }
            ))
        
        # ========================================
        # CHECK 2: Asymmetry Ratio Change > 30%
        # ========================================
        old_asym = last_state.get('asymmetry_ratio', 1.0)
        new_asym = current_state['asymmetry_ratio']
        
        if old_asym > 0.01:
            asym_change_pct = abs(new_asym - old_asym) / old_asym * 100
            
            if asym_change_pct > 30:
                # Determine if favorable or unfavorable
                if new_asym > old_asym:
                    direction = "⬆️ WORSE (harder to sell)"
                    severity = 'high' if new_asym > 2.0 else 'medium'
                else:
                    direction = "⬇️ BETTER (easier to sell)"
                    severity = 'medium'
                
                alerts.append(MetricAlert(
                    timestamp=current_state['timestamp'],
                    token_address=token_address,
                    alert_type='asymmetry_change',
                    severity=severity,
                    message=f"📊 ASYMMETRY {direction}: {old_asym:.2f}x → {new_asym:.2f}x ({asym_change_pct:.0f}%)",
                    details=[
                        f"Liquidity asymmetry changed {asym_change_pct:.0f}%",
                        f"Old: {old_asym:.2f}x → New: {new_asym:.2f}x",
                        f"Higher = harder to sell"
                    ],
                    changes={
                        'metric': 'asymmetry',
                        'old_value': old_asym,
                        'new_value': new_asym,
                        'change_pct': asym_change_pct
                    }
                ))
        
        # ========================================
        # CHECK 3: Honeypot Detection
        # ========================================
        if current_state['is_honeypot'] and not last_state.get('is_honeypot', False):
            alerts.append(MetricAlert(
                timestamp=current_state['timestamp'],
                token_address=token_address,
                alert_type='honeypot_detected',
                severity='critical',
                message=f"🚨 HONEYPOT DETECTED - CANNOT SELL!",
                details=[
                    "Token appears to be a honeypot",
                    "Selling may be impossible or heavily taxed",
                    "EXIT IMMEDIATELY if holding"
                ],
                changes={
                    'metric': 'honeypot',
                    'old_value': False,
                    'new_value': True
                }
            ))
        
        # Update state
        self.last_slippage_state[token_address] = current_state
        
        # Store all alerts
        for alert in alerts:
            self._add_alert(alert)
            logger.info(f"🚨 Slippage Alert: {alert.message}")
            # Send push notification for each alert
        try:
            from web_push_service import get_push_service
            push_svc = get_push_service()
            if push_svc and push_svc.configured:
                for alert in alerts:
                    push_svc.send_alert_notification(alert)
        except Exception as e:
            logger.error(f"Push notification error: {e}")
        
        
        return alerts
    
    def _classify_direction(self, vts: float, pii: float, bsr: float) -> str:
        """Classify overall metrics direction."""
        score = 0
        
        # VTS contribution
        if vts > 2.0:
            score += 2
        elif vts > 1.2:
            score += 1
        elif vts < 0.5:
            score -= 2
        elif vts < 0.8:
            score -= 1
        
        # PII contribution (buy/sell pressure)
        if pii > 0.2:
            score += 2
        elif pii > 0.05:
            score += 1
        elif pii < -0.2:
            score -= 2
        elif pii < -0.05:
            score -= 1
        
        # BSR contribution
        if bsr > 1.5:
            score += 1
        elif bsr < 0.7:
            score -= 1
        
        # Classify
        if score >= 3:
            return 'strong_bullish'
        elif score >= 1:
            return 'bullish'
        elif score <= -3:
            return 'strong_bearish'
        elif score <= -1:
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
