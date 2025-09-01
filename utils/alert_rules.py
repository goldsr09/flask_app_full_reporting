# utils/alert_rules.py
# Advanced alert rules and custom thresholds

import json
import os
from datetime import datetime, timedelta

class AlertRules:
    def __init__(self):
        self.rules_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'alert_rules.json')
        self.rules = self._load_rules()
    
    def _load_rules(self):
        """Load alert rules from JSON file"""
        default_rules = {
            "global_thresholds": {
                "day_over_day_drop": 35,
                "week_over_week_drop": 20,
                "week_over_week_increase": 25,
                "gap_tolerant_drop": 28,
                "minimum_impressions": 2500
            },
            "tag_specific_rules": {},
            "time_based_rules": {
                "business_hours_only": False,
                "weekend_alerts": True,
                "holiday_exceptions": []
            },
            "frequency_limits": {
                "max_alerts_per_tag_per_day": 3,
                "cooldown_hours": 6
            },
            "custom_conditions": []
        }
        
        try:
            if os.path.exists(self.rules_file):
                with open(self.rules_file, 'r') as f:
                    return json.load(f)
            else:
                # Create default rules file
                os.makedirs(os.path.dirname(self.rules_file), exist_ok=True)
                with open(self.rules_file, 'w') as f:
                    json.dump(default_rules, f, indent=2)
                return default_rules
        except Exception as e:
            print(f"Warning: Could not load alert rules: {e}")
            return default_rules
    
    def save_rules(self):
        """Save current rules to file"""
        try:
            with open(self.rules_file, 'w') as f:
                json.dump(self.rules, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving alert rules: {e}")
            return False
    
    def get_threshold_for_tag(self, tag_id, alert_type):
        """Get threshold for specific tag and alert type"""
        # Check for tag-specific rule first
        if tag_id in self.rules.get("tag_specific_rules", {}):
            tag_rule = self.rules["tag_specific_rules"][tag_id]
            if alert_type in tag_rule.get("thresholds", {}):
                return tag_rule["thresholds"][alert_type]
        
        # Fall back to global threshold
        global_thresholds = self.rules.get("global_thresholds", {})
        threshold_map = {
            "day_over_day": global_thresholds.get("day_over_day_drop", 35),
            "week_over_week": global_thresholds.get("week_over_week_drop", 20),
            "week_over_week_increase": global_thresholds.get("week_over_week_increase", 25),
            "gap_tolerant": global_thresholds.get("gap_tolerant_drop", 28)
        }
        
        return threshold_map.get(alert_type, 35)
    
    def should_send_alert(self, alert, tag_id):
        """Check if alert should be sent based on rules"""
        # Check frequency limits
        if not self._check_frequency_limit(alert, tag_id):
            return False
        
        # Check time-based rules
        if not self._check_time_rules(alert):
            return False
        
        # Check custom conditions
        if not self._check_custom_conditions(alert, tag_id):
            return False
        
        return True
    
    def _check_frequency_limit(self, alert, tag_id):
        """Check if we've exceeded frequency limits for this tag"""
        # This would need to be implemented with a database to track alert history
        # For now, return True (no frequency limiting)
        return True
    
    def _check_time_rules(self, alert):
        """Check time-based rules"""
        now = datetime.now()
        
        # Check business hours only
        if self.rules.get("time_based_rules", {}).get("business_hours_only", False):
            if now.weekday() >= 5:  # Weekend
                return False
            if now.hour < 9 or now.hour > 17:  # Outside business hours
                return False
        
        # Check weekend alerts
        if not self.rules.get("time_based_rules", {}).get("weekend_alerts", True):
            if now.weekday() >= 5:  # Weekend
                return False
        
        return True
    
    def _check_custom_conditions(self, alert, tag_id):
        """Check custom conditions"""
        custom_conditions = self.rules.get("custom_conditions", [])
        
        for condition in custom_conditions:
            if not self._evaluate_condition(condition, alert, tag_id):
                return False
        
        return True
    
    def _evaluate_condition(self, condition, alert, tag_id):
        """Evaluate a custom condition"""
        condition_type = condition.get("type")
        
        if condition_type == "tag_pattern":
            pattern = condition.get("pattern", "")
            return pattern in tag_id
        
        elif condition_type == "severity_minimum":
            min_severity = condition.get("minimum", "low")
            severity_order = {"low": 1, "medium": 2, "high": 3}
            return severity_order.get(alert.get("severity", "low"), 1) >= severity_order.get(min_severity, 1)
        
        elif condition_type == "change_threshold":
            threshold = condition.get("threshold", 0)
            return abs(alert.get("change_percent", 0)) >= threshold
        
        elif condition_type == "time_range":
            start_time = condition.get("start_time", "00:00")
            end_time = condition.get("end_time", "23:59")
            current_time = datetime.now().strftime("%H:%M")
            return start_time <= current_time <= end_time
        
        return True
    
    def add_tag_rule(self, tag_id, thresholds=None, conditions=None):
        """Add or update a tag-specific rule"""
        if tag_id not in self.rules["tag_specific_rules"]:
            self.rules["tag_specific_rules"][tag_id] = {}
        
        if thresholds:
            self.rules["tag_specific_rules"][tag_id]["thresholds"] = thresholds
        
        if conditions:
            self.rules["tag_specific_rules"][tag_id]["conditions"] = conditions
        
        return self.save_rules()
    
    def remove_tag_rule(self, tag_id):
        """Remove a tag-specific rule"""
        if tag_id in self.rules["tag_specific_rules"]:
            del self.rules["tag_specific_rules"][tag_id]
            return self.save_rules()
        return True
    
    def add_custom_condition(self, condition):
        """Add a custom condition"""
        self.rules["custom_conditions"].append(condition)
        return self.save_rules()
    
    def update_global_thresholds(self, thresholds):
        """Update global thresholds"""
        self.rules["global_thresholds"].update(thresholds)
        return self.save_rules()

# Global alert rules instance
alert_rules = AlertRules()

def get_alert_threshold(tag_id, alert_type):
    """Get threshold for alert type and tag"""
    return alert_rules.get_threshold_for_tag(tag_id, alert_type)

def should_send_alert(alert, tag_id):
    """Check if alert should be sent"""
    return alert_rules.should_send_alert(alert, tag_id)
