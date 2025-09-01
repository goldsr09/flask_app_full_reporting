# app.py
# Main Flask application entry point

import os
import sqlite3
import threading
import schedule
import time
from flask import Flask
from config import config, DB_PATH

def create_app(config_name=None):
    """Application factory function"""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Initialize database
    init_db()
    
    # Register blueprints
    register_blueprints(app)
    
    # Initialize auto-collection system
    init_auto_collection()
    
    return app

def init_db():
    """Initialize the database with required tables"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        
        # Create query_cache table
        c.execute('''
            CREATE TABLE IF NOT EXISTS query_cache (
                cache_key TEXT PRIMARY KEY,
                result TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index for better performance
        c.execute('''
            CREATE INDEX IF NOT EXISTS idx_cache_created_at 
            ON query_cache(created_at)
        ''')
        
        conn.commit()
        print("✅ Database initialized successfully")

def register_blueprints(app):
    """Register all blueprints with the Flask app"""
    
    # Import blueprints here to avoid circular imports
    from blueprints.main import main_bp
    from blueprints.api import api_bp
    from blueprints.admin import admin_bp
    from blueprints.debug import debug_bp
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(debug_bp, url_prefix='/debug')
    
    print("✅ Blueprints registered successfully")

def init_auto_collection():
    """Initialize the auto-collection system"""
    from config import AUTO_COLLECTION_ENABLED, AUTO_COLLECTION_TIME
    
    if not AUTO_COLLECTION_ENABLED:
        print("🚫 Auto-collection disabled")
        return
    
    try:
        from utils.admin_utils import auto_collect_daily_data, run_scheduler
        
        # Schedule daily collection
        schedule.every().day.at(AUTO_COLLECTION_TIME).do(auto_collect_daily_data)
        
        # Start scheduler in background thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        print(f"🚀 Auto-collection enabled - scheduled for {AUTO_COLLECTION_TIME} daily")
        
    except Exception as e:
        print(f"⚠️ Auto-collection initialization failed: {e}")

# Create the application instance
app = create_app()

if __name__ == '__main__':
    print("🌐 Starting Flask web server...")
    app.run(host='127.0.0.1', port=5001, debug=True)