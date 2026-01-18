"""
Web Push Notification Service for ORB

This module handles sending push notifications to subscribed browsers,
even when the user doesn't have the app open.

HOW IT WORKS:
1. User visits your app and grants notification permission
2. Browser generates a unique "subscription" (endpoint + keys)
3. Frontend sends this subscription to your backend
4. Backend stores the subscription
5. When an alert triggers, backend sends a push to all subscribed browsers
6. Browser's Service Worker receives the push and shows notification

SETUP REQUIRED:
1. Generate VAPID keys (run: python -c "from pywebpush import webpush; import py_vapid; vapid = py_vapid.Vapid(); vapid.generate_keys(); print('Public:', vapid.public_key); print('Private:', vapid.private_key)")
2. Set environment variables:
   - VAPID_PUBLIC_KEY
   - VAPID_PRIVATE_KEY  
   - VAPID_EMAIL (your email for push service contact)
3. Install: pip install pywebpush

"""

import os
import json
import logging
from typing import Dict, List, Optional
from pathlib import Path
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)

# Try to import pywebpush - it's optional
try:
    from pywebpush import webpush, WebPushException
    WEBPUSH_AVAILABLE = True
except ImportError:
    WEBPUSH_AVAILABLE = False
    logger.warning("⚠️ pywebpush not installed. Run: pip install pywebpush")


@dataclass
class PushSubscription:
    """Represents a browser's push subscription."""
    endpoint: str
    keys: Dict[str, str]  # Contains 'p256dh' and 'auth' keys
    token_address: Optional[str] = None  # Which token this subscription is for
    created_at: float = 0


class WebPushService:
    """
    Manages web push subscriptions and sends notifications.
    
    This service maintains a list of browser subscriptions and can
    push notifications to them even when the browser tab is closed.
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.subscriptions_file = self.data_dir / "push_subscriptions.json"
        
        # Security: Load VAPID keys from environment (no defaults)
        self.vapid_public_key = os.environ.get('VAPID_PUBLIC_KEY', '')
        self.vapid_private_key = os.environ.get('VAPID_PRIVATE_KEY', '')
        self.vapid_email = os.environ.get('VAPID_EMAIL', '')

        # Store subscriptions: {subscription_id: PushSubscription}
        self.subscriptions: Dict[str, PushSubscription] = {}

        # Load existing subscriptions
        self._load_subscriptions()

        # Check if properly configured (all three required)
        if not self.vapid_public_key or not self.vapid_private_key or not self.vapid_email:
            logger.warning("⚠️ VAPID keys/email not configured. Web Push will not work.")
            logger.warning("⚠️ Set VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, and VAPID_EMAIL environment variables.")
            self.configured = False
        else:
            self.configured = True
            logger.info(f"✅ WebPushService initialized with {len(self.subscriptions)} subscriptions")
    
    def get_vapid_public_key(self) -> str:
        """Return the public VAPID key for frontend to use."""
        return self.vapid_public_key
    
    def add_subscription(self, subscription_data: Dict, token_address: Optional[str] = None) -> bool:
        """
        Add a new push subscription from a browser.
        
        Args:
            subscription_data: The PushSubscription object from browser's pushManager.subscribe()
            token_address: Optional - which token this subscription wants alerts for
        
        Returns:
            True if subscription was added successfully
        """
        try:
            endpoint = subscription_data.get('endpoint', '')
            keys = subscription_data.get('keys', {})
            
            if not endpoint or not keys.get('p256dh') or not keys.get('auth'):
                logger.error("Invalid subscription data - missing required fields")
                return False
            
            # Create subscription ID from endpoint hash
            sub_id = str(hash(endpoint))
            
            self.subscriptions[sub_id] = PushSubscription(
                endpoint=endpoint,
                keys=keys,
                token_address=token_address,
                created_at=time.time()
            )
            
            self._save_subscriptions()
            logger.info(f"✅ Added push subscription: {endpoint[:50]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error adding subscription: {e}")
            return False
    
    def remove_subscription(self, endpoint: str) -> bool:
        """Remove a subscription by its endpoint."""
        sub_id = str(hash(endpoint))
        if sub_id in self.subscriptions:
            del self.subscriptions[sub_id]
            self._save_subscriptions()
            logger.info(f"🗑️ Removed push subscription")
            return True
        return False
    
    def send_notification(self, title: str, body: str, token_address: Optional[str] = None, 
                         severity: str = 'medium', data: Optional[Dict] = None) -> int:
        """
        Send a push notification to all subscribed browsers.
        
        Args:
            title: Notification title
            body: Notification body text
            token_address: If set, only send to subscriptions for this token
            severity: Alert severity (low, medium, high, critical)
            data: Additional data to send with notification
        
        Returns:
            Number of successful sends
        """
        if not WEBPUSH_AVAILABLE:
            logger.warning("Cannot send push - pywebpush not installed")
            return 0
        
        if not self.configured:
            logger.warning("Cannot send push - VAPID keys not configured")
            return 0
        
        # Build notification payload
        payload = json.dumps({
            'title': title,
            'body': body,
            'severity': severity,
            'token_address': token_address,
            'timestamp': time.time(),
            'data': data or {}
        })
        
        # VAPID claims
        vapid_claims = {
            'sub': f'mailto:{self.vapid_email}'
        }
        
        successful = 0
        failed_endpoints = []
        
        for sub_id, subscription in list(self.subscriptions.items()):
            # Filter by token if specified
            if token_address and subscription.token_address:
                if subscription.token_address != token_address:
                    continue
            
            try:
                webpush(
                    subscription_info={
                        'endpoint': subscription.endpoint,
                        'keys': subscription.keys
                    },
                    data=payload,
                    vapid_private_key=self.vapid_private_key,
                    vapid_claims=vapid_claims
                )
                successful += 1
                logger.debug(f"✅ Push sent to {subscription.endpoint[:30]}...")
                
            except WebPushException as e:
                logger.error(f"Push failed: {e}")
                # If subscription is invalid (410 Gone), remove it
                if e.response and e.response.status_code in [404, 410]:
                    failed_endpoints.append(subscription.endpoint)
            except Exception as e:
                logger.error(f"Unexpected push error: {e}")
        
        # Clean up invalid subscriptions
        for endpoint in failed_endpoints:
            self.remove_subscription(endpoint)
        
        logger.info(f"📤 Push notification sent to {successful}/{len(self.subscriptions)} subscribers")
        return successful
    
    def send_alert_notification(self, alert) -> int:
        """
        Send a push notification for a MetricAlert.
        
        Args:
            alert: MetricAlert object
        
        Returns:
            Number of successful sends
        """
        # Map severity to emoji
        emoji = {
            'critical': '🚨',
            'high': '⚠️',
            'medium': '📊',
            'low': 'ℹ️'
        }.get(alert.severity, '📊')
        
        title = f"{emoji} ORB Alert: {alert.token_address[:8]}..."
        body = alert.message
        
        return self.send_notification(
            title=title,
            body=body,
            token_address=alert.token_address,
            severity=alert.severity,
            data={
                'alert_type': alert.alert_type,
                'details': alert.details,
                'changes': alert.changes
            }
        )
    
    def _load_subscriptions(self):
        """Load subscriptions from disk."""
        try:
            if self.subscriptions_file.exists():
                with open(self.subscriptions_file, 'r') as f:
                    data = json.load(f)
                    for sub_id, sub_data in data.items():
                        self.subscriptions[sub_id] = PushSubscription(
                            endpoint=sub_data['endpoint'],
                            keys=sub_data['keys'],
                            token_address=sub_data.get('token_address'),
                            created_at=sub_data.get('created_at', 0)
                        )
        except Exception as e:
            logger.error(f"Error loading subscriptions: {e}")
    
    def _save_subscriptions(self):
        """Save subscriptions to disk."""
        try:
            data = {}
            for sub_id, sub in self.subscriptions.items():
                data[sub_id] = {
                    'endpoint': sub.endpoint,
                    'keys': sub.keys,
                    'token_address': sub.token_address,
                    'created_at': sub.created_at
                }
            
            with open(self.subscriptions_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving subscriptions: {e}")


# Global instance - will be initialized in main.py
push_service: Optional[WebPushService] = None


def init_push_service(data_dir: str = "data") -> WebPushService:
    """Initialize the global push service."""
    global push_service
    push_service = WebPushService(data_dir)
    return push_service


def get_push_service() -> Optional[WebPushService]:
    """Get the global push service instance."""
    return push_service
