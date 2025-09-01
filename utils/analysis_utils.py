# utils/analysis_utils.py
# Trend analysis and alert functions

from datetime import datetime, timedelta

def analyze_trends_and_alerts(daily_data, columns, impression_cols=None, tag_id=None, tag_info=None):
    """
    Analyze trends and generate alerts from daily data
    
    Args:
        daily_data: List of rows from cache
        columns: Column names list
        impression_cols: List of impression column names to analyze
        tag_id: Tag ID for this analysis
        tag_info: Dictionary with tag information (name, etc.)
    
    Returns:
        alerts: List of alert dictionaries
        trends: Dictionary of trend data
    """
    alerts = []
    trends = {}
    
    # Default impression columns if not provided
    if impression_cols is None:
        impression_cols = [
            'total_impressions',
            'total_ad_query_requests',
            'total_ad_query_responses',
            'total_ad_slot_requests',
            'total_ad_slot_responses',
            'total_ad_creative_fetches',
            'total_ad_creative_responses'
        ]
    
    # Default tag_info if not provided
    if tag_info is None:
        tag_info = {'name': f'Tag {tag_id}' if tag_id else 'Unknown'}
    
    # Calculate trends (7-day moving average if we have enough data, excluding today)
    if len(daily_data) >= 7:
        for col in impression_cols:
            if col in columns:
                col_index = columns.index(col)
                values = [row[col_index] or 0 for row in daily_data[-7:]]
                avg_7day = sum(values) / len(values)

                # Compare to previous 7 days if available
                if len(daily_data) >= 14:
                    prev_values = [row[col_index] or 0 for row in daily_data[-14:-7]]
                    prev_avg_7day = sum(prev_values) / len(prev_values)

                    if prev_avg_7day > 0:
                        trend_change = ((avg_7day - prev_avg_7day) / prev_avg_7day) * 100

                        trends[f"{tag_id}_{col}"] = {
                            'tag_id': tag_id,
                            'tag_name': tag_info['name'],
                            'metric': col,
                            'current_7day_avg': avg_7day,
                            'previous_7day_avg': prev_avg_7day,
                            'trend_change_percent': trend_change,
                            'trend_direction': 'up' if trend_change > 0 else 'down' if trend_change < 0 else 'flat'
                        }

    return alerts, trends

def generate_impression_alerts(daily_data, columns, threshold_percent=35):
    """
    Generate alerts for significant drops in impression metrics
    
    Args:
        daily_data: List of rows sorted by date (newest first)
        columns: Column names list
        threshold_percent: Percentage drop threshold for alerts
    
    Returns:
        List of alert dictionaries
    """
    alerts = []
    
    if len(daily_data) < 2:
        return alerts
    
    # Get column indices
    try:
        date_key_index = columns.index('date_key')
        tag_id_index = columns.index('tag_id')
        tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
        impressions_index = columns.index('total_impressions') if 'total_impressions' in columns else None
    except ValueError:
        return alerts
    
    # Skip today's data - exclude from alerts
    today = datetime.now().strftime('%Y-%m-%d')
    filtered_data = [row for row in daily_data if str(row[date_key_index]) < today]
    
    if len(filtered_data) < 2:
        return alerts
    
    # Group by tag_id to analyze each tag separately
    tag_groups = {}
    for row in filtered_data:
        tag_id = row[tag_id_index]
        if tag_id not in tag_groups:
            tag_groups[tag_id] = []
        tag_groups[tag_id].append(row)
    
    # Analyze each tag separately
    for tag_id, tag_data in tag_groups.items():
        if len(tag_data) < 2:
            continue  # Need at least 2 days of data for this tag
        
        # Sort by date for this specific tag
        tag_data.sort(key=lambda x: x[date_key_index], reverse=True)
        
        # Check for day-over-day drops for this tag
        current_day = tag_data[0]  # Most recent day for this tag
        previous_day = tag_data[1]  # Previous day for this tag
        
        if impressions_index is not None:
            current_impressions = current_day[impressions_index] or 0
            previous_impressions = previous_day[impressions_index] or 0
            
            # Only alert if previous day had meaningful traffic
            if previous_impressions > 2500:
                if current_impressions < previous_impressions:
                    drop_percent = ((previous_impressions - current_impressions) / previous_impressions) * 100
                    
                    if drop_percent >= threshold_percent:
                        # Get the best available tag name
                        alert_tag_name = None
                        if tag_name_index is not None and current_day[tag_name_index]:
                            alert_tag_name = str(current_day[tag_name_index]).strip()
                        
                        if not alert_tag_name:
                            alert_tag_name = f"Tag {current_day[tag_id_index][:8]}..." if len(current_day[tag_id_index]) > 8 else f"Tag {current_day[tag_id_index]}"
                        
                        alert = {
                            'tag_id': current_day[tag_id_index],
                            'tag_name': alert_tag_name,
                            'metric': 'total_impressions',
                            'date': current_day[date_key_index],
                            'current_value': current_impressions,
                            'previous_value': previous_impressions,
                            'change_percent': -drop_percent,  # Negative for drop
                            'severity': 'high' if drop_percent >= 50 else 'medium' if drop_percent >= 35 else 'low',
                            'message': f"Impressions dropped {drop_percent:.1f}% day-over-day"
                        }
                        alerts.append(alert)
    
    return alerts

def generate_comprehensive_alerts(daily_data, columns, day_threshold=35, week_threshold=20, week_increase_threshold=25):
    """
    Generate comprehensive alerts including day-over-day and week-over-week comparisons
    
    Args:
        daily_data: List of data rows
        columns: Column names
        day_threshold: Minimum drop percentage for day-over-day alerts (default 35%)
        week_threshold: Minimum drop percentage for week-over-week alerts (default 20%)
        week_increase_threshold: Minimum increase percentage for week-over-week alerts (default 25%)
    
    Returns:
        List of alert dictionaries
    """
    alerts = []
    
    # Get column indices
    try:
        date_key_index = columns.index('date_key')
        tag_id_index = columns.index('tag_id')
        tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
        impressions_index = columns.index('total_impressions') if 'total_impressions' in columns else None
    except ValueError:
        return alerts
    
    # Skip today's data - exclude from alerts
    today = datetime.now().strftime('%Y-%m-%d')
    filtered_data = [row for row in daily_data if str(row[date_key_index]) < today]
    
    if len(filtered_data) < 2:
        return alerts
    
    # Group by tag_id to analyze each tag separately
    tag_groups = {}
    for row in filtered_data:
        tag_id = row[tag_id_index]
        if tag_id not in tag_groups:
            tag_groups[tag_id] = []
        tag_groups[tag_id].append(row)
    
    # Analyze each tag separately
    for tag_id, tag_data in tag_groups.items():
        if len(tag_data) < 2:
            continue  # Need at least 2 days of data for this tag
        
        # Sort by date for this specific tag
        tag_data.sort(key=lambda x: x[date_key_index], reverse=True)
        
        # Get the best available tag name
        alert_tag_name = None
        if tag_name_index is not None and tag_data[0][tag_name_index]:
            alert_tag_name = str(tag_data[0][tag_name_index]).strip()
        
        if not alert_tag_name:
            alert_tag_name = f"Tag {tag_data[0][tag_id_index][:8]}..." if len(tag_data[0][tag_id_index]) > 8 else f"Tag {tag_data[0][tag_id_index]}"
        
        if impressions_index is not None:
            current_impressions = tag_data[0][impressions_index] or 0
            current_date = tag_data[0][date_key_index]
            
            # 1. Day-over-day comparison (consecutive days)
            if len(tag_data) >= 2:
                previous_day_impressions = tag_data[1][impressions_index] or 0
                previous_day_date = tag_data[1][date_key_index]
                
                # Check if dates are consecutive
                current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                previous_dt = datetime.strptime(previous_day_date, '%Y-%m-%d')
                days_diff = (current_dt - previous_dt).days
                
                if days_diff == 1 and previous_day_impressions > 2500:  # Consecutive days
                    if current_impressions < previous_day_impressions:
                        drop_percent = ((previous_day_impressions - current_impressions) / previous_day_impressions) * 100
                        
                        if drop_percent >= day_threshold:
                            alert = {
                                'tag_id': tag_data[0][tag_id_index],
                                'tag_name': alert_tag_name,
                                'metric': 'total_impressions',
                                'date': current_date,
                                'current_value': current_impressions,
                                'previous_value': previous_day_impressions,
                                'change_percent': -drop_percent,
                                'severity': 'high' if drop_percent >= 50 else 'medium' if drop_percent >= 35 else 'low',
                                'message': f"Impressions dropped {drop_percent:.1f}% day-over-day",
                                'alert_type': 'day_over_day',
                                'comparison_date': previous_day_date
                            }
                            alerts.append(alert)
            
            # 2. Week-over-week comparison (cumulative weekly totals)
            # Calculate current week total (7 days ending on current_date)
            current_week_start = (current_dt - timedelta(days=6)).strftime('%Y-%m-%d')
            current_week_end = current_date
            
            current_week_data = [row for row in tag_data if current_week_start <= str(row[date_key_index]) <= current_week_end]
            current_week_total = sum(row[impressions_index] or 0 for row in current_week_data)
            
            # Calculate previous week total (7 days before current week)
            previous_week_start = (current_dt - timedelta(days=13)).strftime('%Y-%m-%d')
            previous_week_end = (current_dt - timedelta(days=7)).strftime('%Y-%m-%d')
            
            previous_week_data = [row for row in tag_data if previous_week_start <= str(row[date_key_index]) <= previous_week_end]
            previous_week_total = sum(row[impressions_index] or 0 for row in previous_week_data)
            
            if previous_week_total > 2500:  # Only alert if previous week had meaningful traffic
                # Check for drops
                if current_week_total < previous_week_total:
                    drop_percent = ((previous_week_total - current_week_total) / previous_week_total) * 100
                    
                    if drop_percent >= week_threshold:
                        alert = {
                            'tag_id': tag_data[0][tag_id_index],
                            'tag_name': alert_tag_name,
                            'metric': 'total_impressions',
                            'date': current_date,
                            'current_value': current_week_total,
                            'previous_value': previous_week_total,
                            'change_percent': -drop_percent,
                            'severity': 'high' if drop_percent >= 40 else 'medium' if drop_percent >= 25 else 'low',
                            'message': f"Impressions dropped {drop_percent:.1f}% week-over-week (cumulative)",
                            'alert_type': 'week_over_week',
                            'comparison_date': f"{previous_week_start} to {previous_week_end}",
                            'current_week_range': f"{current_week_start} to {current_week_end}",
                            'previous_week_range': f"{previous_week_start} to {previous_week_end}"
                        }
                        alerts.append(alert)
                
                # Check for increases (25% threshold)
                elif current_week_total > previous_week_total:
                    increase_percent = ((current_week_total - previous_week_total) / previous_week_total) * 100
                    
                    if increase_percent >= week_increase_threshold:  # Configurable increase threshold
                        alert = {
                            'tag_id': tag_data[0][tag_id_index],
                            'tag_name': alert_tag_name,
                            'metric': 'total_impressions',
                            'date': current_date,
                            'current_value': current_week_total,
                            'previous_value': previous_week_total,
                            'change_percent': increase_percent,
                            'severity': 'high' if increase_percent >= 50 else 'medium' if increase_percent >= 35 else 'low',
                            'message': f"Impressions increased {increase_percent:.1f}% week-over-week (cumulative)",
                            'alert_type': 'week_over_week_increase',
                            'comparison_date': f"{previous_week_start} to {previous_week_end}",
                            'current_week_range': f"{current_week_start} to {current_week_end}",
                            'previous_week_range': f"{previous_week_start} to {previous_week_end}"
                        }
                        alerts.append(alert)
            
            # 3. Gap-tolerant day-over-day (find nearest previous day with data)
            if len(tag_data) >= 2:
                # Find the most recent previous day with data (within 3 days)
                for i in range(1, min(4, len(tag_data))):
                    previous_day_impressions = tag_data[i][impressions_index] or 0
                    previous_day_date = tag_data[i][date_key_index]
                    
                    # Check if within 3 days
                    previous_dt = datetime.strptime(previous_day_date, '%Y-%m-%d')
                    days_diff = (current_dt - previous_dt).days
                    
                    if 1 <= days_diff <= 3 and previous_day_impressions > 2500:
                        if current_impressions < previous_day_impressions:
                            drop_percent = ((previous_day_impressions - current_impressions) / previous_day_impressions) * 100
                            
                            # Use a slightly lower threshold for gap-tolerant comparisons
                            gap_threshold = day_threshold * 0.8  # 80% of normal threshold
                            
                            if drop_percent >= gap_threshold:
                                alert = {
                                    'tag_id': tag_data[0][tag_id_index],
                                    'tag_name': alert_tag_name,
                                    'metric': 'total_impressions',
                                    'date': current_date,
                                    'current_value': current_impressions,
                                    'previous_value': previous_day_impressions,
                                    'change_percent': -drop_percent,
                                    'severity': 'high' if drop_percent >= 50 else 'medium' if drop_percent >= 35 else 'low',
                                    'message': f"Impressions dropped {drop_percent:.1f}% vs {days_diff} days ago",
                                    'alert_type': 'gap_tolerant',
                                    'comparison_date': previous_day_date,
                                    'days_gap': days_diff
                                }
                                alerts.append(alert)
                        break  # Only use the first valid comparison
    
    return alerts

def analyze_cache_trends(cache_data, columns):
    """
    Analyze trends across all cached data
    
    Args:
        cache_data: List of data rows
        columns: Column names
    
    Returns:
        Dictionary of trend analysis results
    """
    if not cache_data or len(cache_data) < 7:
        return {}
    
    # Group data by tag_id
    try:
        tag_id_index = columns.index('tag_id')
        date_key_index = columns.index('date_key')
        tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
    except ValueError:
        return {}
    
    # Group by tag_id
    tag_groups = {}
    for row in cache_data:
        tag_id = row[tag_id_index]
        if tag_id not in tag_groups:
            tag_groups[tag_id] = []
        tag_groups[tag_id].append(row)
    
    all_trends = {}
    
    # Analyze each tag separately
    for tag_id, tag_data in tag_groups.items():
        # Sort by date
        tag_data.sort(key=lambda x: x[date_key_index])
        
        # Get tag info - try to get the best available name
        tag_name = None
        if tag_name_index is not None:
            # Find the first non-empty tag name
            for row in tag_data:
                if row[tag_name_index] and str(row[tag_name_index]).strip():
                    tag_name = str(row[tag_name_index]).strip()
                    break
        
        # If no tag name found, use a more descriptive fallback
        if not tag_name:
            tag_name = f'Tag {tag_id[:8]}...' if len(tag_id) > 8 else f'Tag {tag_id}'
        
        tag_info = {
            'name': tag_name
        }
        
        # Analyze trends for this tag
        alerts, trends = analyze_trends_and_alerts(
            tag_data, 
            columns, 
            tag_id=tag_id, 
            tag_info=tag_info
        )
        
        # Merge into overall trends
        all_trends.update(trends)
    
    return all_trends

def get_performance_summary(cache_data, columns):
    """
    Get overall performance summary from cached data
    
    Args:
        cache_data: List of data rows
        columns: Column names
    
    Returns:
        Dictionary with performance metrics
    """
    if not cache_data:
        return {}
    
    try:
        impressions_index = columns.index('total_impressions') if 'total_impressions' in columns else None
        date_key_index = columns.index('date_key')
    except ValueError:
        return {}
    
    # Exclude today's data
    today = datetime.now().strftime('%Y-%m-%d')
    filtered_data = [row for row in cache_data if str(row[date_key_index]) < today]
    
    if not filtered_data or impressions_index is None:
        return {}
    
    # Calculate summary metrics
    total_impressions = sum(row[impressions_index] or 0 for row in filtered_data)
    
    # Get date range
    dates = [str(row[date_key_index]) for row in filtered_data]
    date_range = {
        'start': min(dates),
        'end': max(dates),
        'days': len(set(dates))
    }
    
    # Calculate daily average
    daily_avg = total_impressions / date_range['days'] if date_range['days'] > 0 else 0
    
    return {
        'total_impressions': total_impressions,
        'daily_average': daily_avg,
        'date_range': date_range,
        'total_records': len(filtered_data)
    }