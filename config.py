# config.py
# Centralized configuration for the Flask application

import os
from datetime import datetime

# Database Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'query_cache.db')

# Auto-collection Configuration
AUTO_COLLECTION_ENABLED = True
AUTO_COLLECTION_TIME = "06:00"  # 6 AM daily
LOOKBACK_DAYS = 7  # How many days back to collect automatically

# Known IDs for auto-collection (can be updated dynamically)
KNOWN_SEAT_IDS = [
    '0011600001nYnDu',
    '00116000020nI8D',
    # Add more Seat IDs as discovered
]

KNOWN_PUBLISHER_IDS = [
    '58',
    '245',
    # Add more Publisher IDs as discovered
]

# Notification Configuration
NOTIFICATION_CONFIG = {
    'email': {
        'enabled': bool(os.getenv('EMAIL_USER') and os.getenv('EMAIL_PASSWORD')),
        'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
        'smtp_port': int(os.getenv('SMTP_PORT', '587')),
        'email_user': os.getenv('EMAIL_USER', ''),
        'email_password': os.getenv('EMAIL_PASSWORD', ''),
        'recipients': os.getenv('ALERT_RECIPIENTS', '').split(',') if os.getenv('ALERT_RECIPIENTS') else []
    },
    'slack': {
        'enabled': bool(os.getenv('SLACK_WEBHOOK_URL')),
        'webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
        'channel': os.getenv('SLACK_CHANNEL', '#alerts')
    },
    'webhook': {
        'enabled': bool(os.getenv('WEBHOOK_URL')),
        'url': os.getenv('WEBHOOK_URL', ''),
        'headers': {'Content-Type': 'application/json'}
    }
}

# Alert Configuration
ALERT_CONFIG = {
    'default_thresholds': {
        'day_over_day_drop': 35,
        'week_over_week_drop': 20,
        'week_over_week_increase': 25,
        'gap_tolerant_drop': 28,
        'minimum_impressions': 2500
    },
    'notification_rules': {
        'high_priority_always_notify': True,
        'medium_priority_email_only': True,
        'low_priority_dashboard_only': True,
        'business_hours_only': False,
        'weekend_alerts': True
    }
}

# Flask Configuration
class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-for-testing')
    DEBUG = os.getenv('FLASK_ENV', 'development') == 'development'
    
    # Database settings
    DATABASE_PATH = DB_PATH
    
    # Cache settings
    CACHE_DEFAULT_TIMEOUT = 3600  # 1 hour in seconds
    
    # API settings
    API_RATE_LIMIT = os.getenv('API_RATE_LIMIT', "100/hour")
    
    # Pagination
    ITEMS_PER_PAGE = 50
    MAX_ITEMS_PER_PAGE = 200

# Development Configuration
class DevelopmentConfig(Config):
    DEBUG = True

# Production Configuration
class ProductionConfig(Config):
    DEBUG = False
    SECRET_KEY = os.getenv('SECRET_KEY')

# Testing Configuration
class TestingConfig(Config):
    TESTING = True
    DEBUG = True

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}