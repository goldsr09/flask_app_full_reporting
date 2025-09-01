# utils/notification_utils.py
# Notification system for alerts and monitoring

import smtplib
import requests
import json
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from datetime import datetime
import os

class NotificationManager:
    def __init__(self):
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'email_user': os.getenv('EMAIL_USER', ''),
            'email_password': os.getenv('EMAIL_PASSWORD', ''),
            'recipients': os.getenv('ALERT_RECIPIENTS', '').split(',')
        }
        
        self.slack_config = {
            'webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
            'channel': os.getenv('SLACK_CHANNEL', '#alerts')
        }
        
        self.webhook_config = {
            'url': os.getenv('WEBHOOK_URL', ''),
            'headers': {'Content-Type': 'application/json'}
        }
    
    def send_email_alert(self, alert, priority='normal'):
        """Send email notification for alerts"""
        if not self.email_config['email_user'] or not self.email_config['recipients']:
            return False
        
        try:
            # Create message
            msg = MimeMultipart()
            msg['From'] = self.email_config['email_user']
            msg['To'] = ', '.join(self.email_config['recipients'])
            msg['Subject'] = f"[{priority.upper()}] Performance Alert: {alert['tag_name']}"
            
            # Create HTML body
            html_body = self._create_email_html(alert)
            msg.attach(MimeText(html_body, 'html'))
            
            # Send email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email_user'], self.email_config['email_password'])
            server.send_message(msg)
            server.quit()
            
            print(f"✅ Email alert sent for {alert['tag_name']}")
            return True
            
        except Exception as e:
            print(f"❌ Email alert failed: {e}")
            return False
    
    def send_slack_alert(self, alert):
        """Send Slack notification for alerts"""
        if not self.slack_config['webhook_url']:
            return False
        
        try:
            # Create Slack message
            slack_message = self._create_slack_message(alert)
            
            response = requests.post(
                self.slack_config['webhook_url'],
                json=slack_message,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                print(f"✅ Slack alert sent for {alert['tag_name']}")
                return True
            else:
                print(f"❌ Slack alert failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Slack alert failed: {e}")
            return False
    
    def send_webhook_alert(self, alert):
        """Send webhook notification for alerts"""
        if not self.webhook_config['url']:
            return False
        
        try:
            webhook_data = {
                'timestamp': datetime.now().isoformat(),
                'alert': alert,
                'source': 'flask_analytics_app'
            }
            
            response = requests.post(
                self.webhook_config['url'],
                json=webhook_data,
                headers=self.webhook_config['headers']
            )
            
            if response.status_code in [200, 201, 202]:
                print(f"✅ Webhook alert sent for {alert['tag_name']}")
                return True
            else:
                print(f"❌ Webhook alert failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Webhook alert failed: {e}")
            return False
    
    def _create_email_html(self, alert):
        """Create HTML email body"""
        severity_color = {
            'high': '#dc3545',
            'medium': '#ffc107', 
            'low': '#17a2b8'
        }
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: {severity_color.get(alert['severity'], '#6c757d')}; color: white; padding: 20px; border-radius: 8px;">
                <h2>🚨 Performance Alert</h2>
                <p><strong>Tag:</strong> {alert['tag_name']}</p>
                <p><strong>Type:</strong> {alert.get('alert_type', 'Standard')}</p>
                <p><strong>Severity:</strong> {alert['severity'].title()}</p>
            </div>
            
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 20px;">
                <h3>Alert Details</h3>
                <p><strong>Message:</strong> {alert['message']}</p>
                <p><strong>Date:</strong> {alert['date']}</p>
                <p><strong>Change:</strong> {alert['previous_value']:,} → {alert['current_value']:,} ({alert['change_percent']:.1f}%)</p>
                {f"<p><strong>Comparison:</strong> {alert.get('comparison_date', '')}</p>" if alert.get('comparison_date') else ''}
            </div>
            
            <div style="margin-top: 20px; padding: 20px; background: #e9ecef; border-radius: 8px;">
                <p><small>This alert was generated automatically by the Flask Analytics App monitoring system.</small></p>
            </div>
        </body>
        </html>
        """
    
    def _create_slack_message(self, alert):
        """Create Slack message format"""
        severity_emoji = {
            'high': '🔴',
            'medium': '🟡',
            'low': '🔵'
        }
        
        return {
            "channel": self.slack_config['channel'],
            "text": f"{severity_emoji.get(alert['severity'], '⚪')} Performance Alert",
            "attachments": [{
                "color": {
                    'high': 'danger',
                    'medium': 'warning',
                    'low': 'good'
                }.get(alert['severity'], 'good'),
                "title": f"Alert: {alert['tag_name']}",
                "text": alert['message'],
                "fields": [
                    {
                        "title": "Severity",
                        "value": alert['severity'].title(),
                        "short": True
                    },
                    {
                        "title": "Type", 
                        "value": alert.get('alert_type', 'Standard'),
                        "short": True
                    },
                    {
                        "title": "Change",
                        "value": f"{alert['previous_value']:,} → {alert['current_value']:,} ({alert['change_percent']:.1f}%)",
                        "short": False
                    },
                    {
                        "title": "Date",
                        "value": alert['date'],
                        "short": True
                    }
                ],
                "footer": "Flask Analytics App",
                "ts": int(datetime.now().timestamp())
            }]
        }

# Global notification manager instance
notification_manager = NotificationManager()

def send_alert_notifications(alert, notification_types=None):
    """Send notifications for an alert via multiple channels"""
    if notification_types is None:
        notification_types = ['email', 'slack', 'webhook']
    
    results = {}
    
    if 'email' in notification_types:
        # Only send email for high/medium priority alerts
        if alert['severity'] in ['high', 'medium']:
            results['email'] = notification_manager.send_email_alert(alert, alert['severity'])
    
    if 'slack' in notification_types:
        results['slack'] = notification_manager.send_slack_alert(alert)
    
    if 'webhook' in notification_types:
        results['webhook'] = notification_manager.send_webhook_alert(alert)
    
    return results
