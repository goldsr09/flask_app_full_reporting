# blueprints/api.py
# API endpoints for alerts, search, and data access

from flask import Blueprint, request, jsonify
import sqlite3
import json
from datetime import datetime, timedelta
from utils.analysis_utils import generate_impression_alerts
from utils.cache_utils import search_tags_in_cache
from config import DB_PATH

api_bp = Blueprint('api', __name__)

@api_bp.route('/alerts')
def api_alerts():
    """Get recent alerts from cached data"""
    limit = request.args.get('limit', 10, type=int)
    days_back = request.args.get('days', 7, type=int)  # Default to 7 days, configurable
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC", 
        )
        cache_entries = c.fetchall()
    
    all_alerts = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                # Generate alerts for this cache object using comprehensive analysis
                from utils.analysis_utils import generate_comprehensive_alerts
                alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                all_alerts.extend(alerts)
        except Exception as e:
            print(f"Error processing cache entry {cache_key} for alerts: {e}")
            continue
    
    # Filter for alerts from the past N days
    recent_alerts = []
    today = datetime.now().replace(hour=23, minute=59, second=59)  # End of today
    days_ago = (today - timedelta(days=days_back)).replace(hour=0, minute=0, second=0)  # Start of N days ago
    
    print(f"Debug: Filtering alerts from {days_ago.strftime('%Y-%m-%d %H:%M:%S')} to {today.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Debug: Total alerts found: {len(all_alerts)}")
    
    for alert in all_alerts:
        try:
            alert_date = datetime.strptime(alert.get('date'), '%Y-%m-%d')
            print(f"Debug: Alert date {alert.get('date')} -> {alert_date.strftime('%Y-%m-%d %H:%M:%S')}")
            if days_ago <= alert_date <= today:
                recent_alerts.append(alert)
                print(f"Debug: Added alert for {alert.get('date')}")
            else:
                print(f"Debug: Skipped alert for {alert.get('date')} (outside range)")
        except (ValueError, TypeError) as e:
            # Skip alerts with invalid dates
            print(f"Debug: Error parsing date {alert.get('date')}: {e}")
            continue
    
    # Sort by severity (high first) and date
    recent_alerts.sort(key=lambda x: (x.get('severity') == 'high', x.get('date', '')), reverse=True)
    
    return jsonify({
        'alerts': recent_alerts, 
        'count': len(recent_alerts),
        'time_range': {
            'days_back': days_back,
            'from_date': days_ago.strftime('%Y-%m-%d'),
            'to_date': today.strftime('%Y-%m-%d')
        }
    })

@api_bp.route('/search-tags')
def api_search_tags():
    """Search for tags across all cached data"""
    query = request.args.get('q', '').strip().lower()
    limit = request.args.get('limit', 20, type=int)
    max_results = request.args.get('max_results', 50, type=int)
    
    if len(query) < 2:
        return jsonify({
            'tags': [], 
            'message': 'Enter at least 2 characters', 
            'query': query
        })
    
    # Get all cache objects
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC LIMIT ?", (limit,))
        cache_entries = c.fetchall()
    
    found_tags = {}
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                # Determine query type and parent ID
                if cache_key.startswith('seat_id_'):
                    source = 'Query 1'
                    parent_id = cache_key.replace('seat_id_', '')
                elif cache_key.startswith('publisher_id_'):
                    source = 'Query 2'
                    parent_id = cache_key.replace('publisher_id_', '')
                else:
                    continue
                
                # Search within this cache object
                tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
                tag_id_index = columns.index('tag_id') if 'tag_id' in columns else None
                
                if tag_name_index is not None and tag_id_index is not None:
                    for row in data:
                        tag_name = str(row[tag_name_index] or '').strip()
                        tag_id = str(row[tag_id_index] or '').strip()
                        
                        # Check if query matches tag name or tag ID
                        if query in tag_name.lower() or query in tag_id.lower():
                            # Create unique key to avoid duplicates
                            composite_key = f"{tag_id}|{source}|{parent_id}"
                            
                            if composite_key not in found_tags:
                                found_tags[composite_key] = {
                                    'tag_id': tag_id,
                                    'tag_name': tag_name,
                                    'source': source,
                                    'parent_id': parent_id,
                                    'cache_key': cache_key[:12] + '...'
                                }
                                
                                # Stop if we've found enough results
                                if len(found_tags) >= max_results:
                                    break
                    
                    # Break outer loop if we have enough results
                    if len(found_tags) >= max_results:
                        break
                        
        except Exception as e:
            print(f"Error searching cache entry {cache_key}: {e}")
            continue
    
    # Convert to list and sort
    tags_list = list(found_tags.values())
    tags_list.sort(key=lambda x: x['tag_name'].lower())
    
    # Limit results
    if len(tags_list) > max_results:
        tags_list = tags_list[:max_results]
        message = f"Showing first {max_results} of {len(found_tags)} matches"
    else:
        message = f"Found {len(tags_list)} matches"
    
    return jsonify({
        'tags': tags_list, 
        'message': message, 
        'query': query
    })

@api_bp.route('/cache-stats')
def api_cache_stats():
    """Get cache statistics"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        
        # Get total cache objects
        c.execute('SELECT COUNT(*) FROM query_cache')
        total_entries = c.fetchone()[0]
        
        # Get Query 1 and Query 2 counts
        c.execute("SELECT COUNT(*) FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
        query1_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM query_cache WHERE cache_key LIKE 'publisher_id_%'")
        query2_count = c.fetchone()[0]
        
        # Get total records across all cache objects
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%'")
        cache_entries = c.fetchall()
    
    total_records = 0
    date_range = {'min': None, 'max': None}
    all_dates = set()
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'data' in cache_object:
                total_records += len(cache_object['data'])
                
                # Extract date range
                if 'columns' in cache_object and 'date_key' in cache_object['columns']:
                    date_index = cache_object['columns'].index('date_key')
                    for row in cache_object['data']:
                        try:
                            all_dates.add(str(row[date_index]))
                        except:
                            pass
        except:
            continue
    
    if all_dates:
        date_range['min'] = min(all_dates)
        date_range['max'] = max(all_dates)
    
    return jsonify({
        'total_cache_objects': total_entries,
        'query1_objects': query1_count,
        'query2_objects': query2_count,
        'total_records': total_records,
        'date_range': date_range,
        'unique_dates': len(all_dates)
    })

@api_bp.route('/entity-data/<query_type>/<entity_id>')
def api_entity_data(query_type, entity_id):
    """Get cached data for specific seat_id or publisher_id"""
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    if query_type not in ['query1', 'query2']:
        return jsonify({'error': 'Invalid query_type. Must be query1 or query2'}), 400
    
    # Get cache object
    cache_key = f"{'seat_id' if query_type == 'query1' else 'publisher_id'}_{entity_id}"
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT result FROM query_cache WHERE cache_key = ?', (cache_key,))
        row = c.fetchone()
        
        if not row:
            return jsonify({'error': f'No cached data found for {entity_id}'}), 404
        
        try:
            cache_object = json.loads(row[0])
        except:
            return jsonify({'error': 'Invalid cache data'}), 500
    
    columns = cache_object.get('columns', [])
    data = cache_object.get('data', [])
    
    # Filter by date range if provided
    if date_from and date_to and 'date_key' in columns:
        try:
            date_index = columns.index('date_key')
            filtered_data = []
            for row in data:
                row_date = str(row[date_index])
                if date_from <= row_date <= date_to:
                    filtered_data.append(row)
            data = filtered_data
        except:
            pass
    
    return jsonify({
        'entity_id': entity_id,
        'query_type': query_type,
        'columns': columns,
        'data': data,
        'record_count': len(data)
    })

@api_bp.route('/health')
def api_health():
    """API health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@api_bp.route('/debug-analysis')
def api_debug_analysis():
    """Debug endpoint to understand why analysis might not be showing results"""
    from utils.analysis_utils import generate_impression_alerts, analyze_cache_trends

@api_bp.route('/test-alerts')
def api_test_alerts():
    """Test endpoint to debug alerts filtering"""
    from utils.analysis_utils import generate_impression_alerts

@api_bp.route('/test-alerts-all')
def api_test_alerts_all():
    """Test endpoint to check all cache entries for alerts"""
    from utils.analysis_utils import generate_impression_alerts

@api_bp.route('/debug-dates')
def api_debug_dates():
    """Debug endpoint to check date ranges in cache data"""

@api_bp.route('/test-alerts-lower-threshold')
def api_test_alerts_lower_threshold():
    """Test alerts with lower threshold to see if more alerts would be generated"""

@api_bp.route('/test-comprehensive-alerts')
def api_test_comprehensive_alerts():
    """Test the new comprehensive alert system vs the old one"""
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 3")
        cache_entries = c.fetchall()
    
    comparison_results = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                # Test old system
                from utils.analysis_utils import generate_impression_alerts
                old_alerts = generate_impression_alerts(cache_object['data'], cache_object['columns'])
                
                # Test new system
                from utils.analysis_utils import generate_comprehensive_alerts
                new_alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                
                # Categorize new alerts by type
                alert_types = {}
                for alert in new_alerts:
                    alert_type = alert.get('alert_type', 'unknown')
                    if alert_type not in alert_types:
                        alert_types[alert_type] = []
                    alert_types[alert_type].append(alert)
                
                comparison_results.append({
                    'cache_key': cache_key,
                    'old_alerts_count': len(old_alerts),
                    'new_alerts_count': len(new_alerts),
                    'improvement': len(new_alerts) - len(old_alerts),
                    'alert_types': alert_types,
                    'new_alerts': new_alerts[:5]  # First 5 for preview
                })
                
        except Exception as e:
            comparison_results.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify(comparison_results)

@api_bp.route('/check-tag/<tag_id>')
def api_check_tag(tag_id):
    """Check specific tag for week-over-week analysis"""
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 5")
        cache_entries = c.fetchall()
    
    tag_analysis = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                if 'date_key' in columns and 'tag_id' in columns and 'total_impressions' in columns:
                    date_index = columns.index('date_key')
                    tag_index = columns.index('tag_id')
                    impressions_index = columns.index('total_impressions')
                    
                    # Find data for this specific tag
                    tag_data = []
                    for row in data:
                        if str(row[tag_index]) == tag_id:
                            tag_data.append({
                                'date': str(row[date_index]),
                                'impressions': row[impressions_index] or 0
                            })
                    
                    if tag_data:
                        # Sort by date
                        tag_data.sort(key=lambda x: x['date'], reverse=True)
                        
                        # Week-over-week analysis
                        from datetime import datetime, timedelta
                        if len(tag_data) > 0:
                            current_date = tag_data[0]['date']
                            current_impressions = tag_data[0]['impressions']
                            current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                            
                            # Calculate cumulative weekly totals
                            # Current week total (7 days ending on current_date)
                            current_week_start = (current_dt - timedelta(days=6)).strftime('%Y-%m-%d')
                            current_week_end = current_date
                            
                            current_week_data = [row for row in tag_data if current_week_start <= row['date'] <= current_week_end]
                            current_week_total = sum(row['impressions'] for row in current_week_data)
                            
                            # Previous week total (7 days before current week)
                            previous_week_start = (current_dt - timedelta(days=13)).strftime('%Y-%m-%d')
                            previous_week_end = (current_dt - timedelta(days=7)).strftime('%Y-%m-%d')
                            
                            previous_week_data = [row for row in tag_data if previous_week_start <= row['date'] <= previous_week_end]
                            previous_week_total = sum(row['impressions'] for row in previous_week_data)
                            
                            analysis = {
                                'cache_key': cache_key,
                                'tag_id': tag_id,
                                'current_date': current_date,
                                'current_impressions': current_impressions,
                                'current_week_total': current_week_total,
                                'previous_week_total': previous_week_total,
                                'current_week_range': f"{current_week_start} to {current_week_end}",
                                'previous_week_range': f"{previous_week_start} to {previous_week_end}",
                                'total_days': len(tag_data)
                            }
                            
                            if previous_week_total > 2500:
                                if current_week_total < previous_week_total:
                                    drop_percent = ((previous_week_total - current_week_total) / previous_week_total) * 100
                                    analysis['drop_percent'] = drop_percent
                                    analysis['meets_20_percent_threshold'] = drop_percent >= 20
                                    analysis['would_alert'] = drop_percent >= 20
                                    analysis['alert_type'] = 'drop'
                                elif current_week_total > previous_week_total:
                                    increase_percent = ((current_week_total - previous_week_total) / previous_week_total) * 100
                                    analysis['increase_percent'] = increase_percent
                                    analysis['meets_25_percent_threshold'] = increase_percent >= 25
                                    analysis['would_alert'] = increase_percent >= 25
                                    analysis['alert_type'] = 'increase'
                                    analysis['drop_percent'] = 0
                                else:
                                    analysis['drop_percent'] = 0
                                    analysis['increase_percent'] = 0
                                    analysis['meets_20_percent_threshold'] = False
                                    analysis['meets_25_percent_threshold'] = False
                                    analysis['would_alert'] = False
                                    analysis['alert_type'] = 'no_change'
                            else:
                                analysis['drop_percent'] = None
                                analysis['increase_percent'] = None
                                analysis['meets_20_percent_threshold'] = False
                                analysis['meets_25_percent_threshold'] = False
                                analysis['would_alert'] = False
                                analysis['alert_type'] = 'insufficient_data'
                            
                            tag_analysis.append(analysis)
                
        except Exception as e:
            tag_analysis.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify(tag_analysis)

@api_bp.route('/debug-day-over-day')
def api_debug_day_over_day():
    """Debug day-over-day comparisons to understand why so few alerts"""
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 3")
        cache_entries = c.fetchall()
    
    detailed_analysis = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                if 'date_key' in columns and 'tag_id' in columns and 'total_impressions' in columns:
                    date_index = columns.index('date_key')
                    tag_index = columns.index('tag_id')
                    impressions_index = columns.index('total_impressions')
                    
                    # Group by tag
                    tag_data = {}
                    for row in data:
                        tag_id = str(row[tag_index])
                        date = str(row[date_index])
                        impressions = row[impressions_index] or 0
                        
                        if tag_id not in tag_data:
                            tag_data[tag_id] = []
                        tag_data[tag_id].append({'date': date, 'impressions': impressions})
                    
                    # Analyze each tag
                    tag_analysis = []
                    for tag_id, tag_rows in tag_data.items():
                        if len(tag_rows) >= 2:
                            # Sort by date
                            tag_rows.sort(key=lambda x: x['date'], reverse=True)
                            
                            # Check consecutive days
                            consecutive_comparisons = []
                            for i in range(len(tag_rows) - 1):
                                current = tag_rows[i]
                                previous = tag_rows[i + 1]
                                
                                # Check if dates are consecutive
                                from datetime import datetime
                                current_date = datetime.strptime(current['date'], '%Y-%m-%d')
                                previous_date = datetime.strptime(previous['date'], '%Y-%m-%d')
                                days_diff = (current_date - previous_date).days
                                
                                if days_diff == 1:  # Consecutive days
                                    if previous['impressions'] > 2500:
                                        if current['impressions'] < previous['impressions']:
                                            drop_percent = ((previous['impressions'] - current['impressions']) / previous['impressions']) * 100
                                            consecutive_comparisons.append({
                                                'current_date': current['date'],
                                                'previous_date': previous['date'],
                                                'current_impressions': current['impressions'],
                                                'previous_impressions': previous['impressions'],
                                                'drop_percent': drop_percent,
                                                'would_alert_35': drop_percent >= 35,
                                                'would_alert_20': drop_percent >= 20,
                                                'would_alert_10': drop_percent >= 10
                                            })
                            
                            tag_analysis.append({
                                'tag_id': tag_id,
                                'total_days': len(tag_rows),
                                'consecutive_comparisons': len(consecutive_comparisons),
                                'comparisons': consecutive_comparisons
                            })
                    
                    detailed_analysis.append({
                        'cache_key': cache_key,
                        'total_tags': len(tag_data),
                        'tags_with_sufficient_data': len([t for t in tag_data.values() if len(t) >= 2]),
                        'tag_analysis': tag_analysis
                    })
                
        except Exception as e:
            detailed_analysis.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify(detailed_analysis)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 5")
        cache_entries = c.fetchall()
    
    all_alerts = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                # Test with lower threshold (10% instead of 35%)
                from utils.analysis_utils import generate_impression_alerts
                alerts_10 = generate_impression_alerts(cache_object['data'], cache_object['columns'], threshold_percent=10)
                alerts_20 = generate_impression_alerts(cache_object['data'], cache_object['columns'], threshold_percent=20)
                alerts_35 = generate_impression_alerts(cache_object['data'], cache_object['columns'], threshold_percent=35)
                
                all_alerts.append({
                    'cache_key': cache_key,
                    'alerts_10_percent': len(alerts_10),
                    'alerts_20_percent': len(alerts_20),
                    'alerts_35_percent': len(alerts_35),
                    'sample_alerts_10': alerts_10[:3] if alerts_10 else []
                })
                
        except Exception as e:
            all_alerts.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify(all_alerts)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 10")
        cache_entries = c.fetchall()
    
    date_analysis = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                if 'date_key' in columns:
                    date_index = columns.index('date_key')
                    dates = [str(row[date_index]) for row in data if row[date_index]]
                    
                    if dates:
                        date_analysis.append({
                            'cache_key': cache_key,
                            'date_range': {
                                'min': min(dates),
                                'max': max(dates),
                                'count': len(dates)
                            },
                            'unique_dates': sorted(list(set(dates))),
                            'today': datetime.now().strftime('%Y-%m-%d')
                        })
                
        except Exception as e:
            date_analysis.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify(date_analysis)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    all_alerts = []
    detailed_analysis = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                # Detailed analysis of this cache entry
                if 'date_key' in columns and 'tag_id' in columns and 'total_impressions' in columns:
                    date_index = columns.index('date_key')
                    tag_index = columns.index('tag_id')
                    impressions_index = columns.index('total_impressions')
                    
                    # Group by tag and analyze
                    tag_analysis = {}
                    for row in data:
                        tag_id = str(row[tag_index])
                        date = str(row[date_index])
                        impressions = row[impressions_index] or 0
                        
                        if tag_id not in tag_analysis:
                            tag_analysis[tag_id] = []
                        tag_analysis[tag_id].append({'date': date, 'impressions': impressions})
                    
                    # Analyze each tag
                    tag_details = []
                    for tag_id, tag_rows in tag_analysis.items():
                        if len(tag_rows) >= 2:
                            # Sort by date
                            tag_rows.sort(key=lambda x: x['date'], reverse=True)
                            
                            # Check if previous day had > 2500 impressions
                            previous_day_impressions = tag_rows[1]['impressions'] if len(tag_rows) > 1 else 0
                            current_day_impressions = tag_rows[0]['impressions']
                            
                            if previous_day_impressions > 2500:
                                # Calculate drop percentage
                                if current_day_impressions < previous_day_impressions:
                                    drop_percent = ((previous_day_impressions - current_day_impressions) / previous_day_impressions) * 100
                                    
                                    tag_details.append({
                                        'tag_id': tag_id,
                                        'current_date': tag_rows[0]['date'],
                                        'previous_date': tag_rows[1]['date'],
                                        'current_impressions': current_day_impressions,
                                        'previous_impressions': previous_day_impressions,
                                        'drop_percent': drop_percent,
                                        'would_alert': drop_percent >= 35
                                    })
                
                alerts = generate_impression_alerts(data, columns)
                if alerts:
                    all_alerts.extend(alerts)
                
                detailed_analysis.append({
                    'cache_key': cache_key,
                    'record_count': len(data),
                    'unique_tags': len(tag_analysis) if 'tag_id' in columns else 0,
                    'tags_with_sufficient_data': len([t for t in tag_analysis.values() if len(t) >= 2]) if 'tag_id' in columns else 0,
                    'tags_with_high_traffic': len([t for t in tag_analysis.values() if len(t) >= 2 and t[1]['impressions'] > 2500]) if 'tag_id' in columns else 0,
                    'potential_alerts': len([d for d in tag_details if d['would_alert']]) if 'tag_details' in locals() else 0,
                    'actual_alerts': len(alerts),
                    'tag_details': tag_details if 'tag_details' in locals() else []
                })
                
        except Exception as e:
            detailed_analysis.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify({
        'total_alerts': len(all_alerts),
        'alerts': all_alerts,
        'detailed_analysis': detailed_analysis
    })
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' ORDER BY updated_at DESC LIMIT 5")
        cache_entries = c.fetchall()
    
    all_alerts = []
    debug_info = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                # Check if we have the required columns
                has_required_columns = all(col in columns for col in ['date_key', 'tag_id', 'total_impressions'])
                
                # Get date range
                date_range = {'min': None, 'max': None}
                if 'date_key' in columns:
                    date_index = columns.index('date_key')
                    dates = [str(row[date_index]) for row in data if row[date_index]]
                    if dates:
                        date_range['min'] = min(dates)
                        date_range['max'] = max(dates)
                
                # Check unique tags
                unique_tags = set()
                if 'tag_id' in columns:
                    tag_index = columns.index('tag_id')
                    unique_tags = set(str(row[tag_index]) for row in data if row[tag_index])
                
                # Analyze data structure for debugging
                if 'date_key' in columns and 'tag_id' in columns and 'total_impressions' in columns:
                    date_index = columns.index('date_key')
                    tag_index = columns.index('tag_id')
                    impressions_index = columns.index('total_impressions')
                    
                    # Group by tag and check data
                    tag_data_analysis = {}
                    for row in data:
                        tag_id = str(row[tag_index])
                        date = str(row[date_index])
                        impressions = row[impressions_index] or 0
                        
                        if tag_id not in tag_data_analysis:
                            tag_data_analysis[tag_id] = []
                        tag_data_analysis[tag_id].append({'date': date, 'impressions': impressions})
                    
                    # Check each tag's data
                    tags_with_sufficient_data = 0
                    for tag_id, tag_rows in tag_data_analysis.items():
                        if len(tag_rows) >= 2:
                            # Sort by date
                            tag_rows.sort(key=lambda x: x['date'], reverse=True)
                            # Check if previous day had > 2500 impressions
                            if tag_rows[1]['impressions'] > 2500:
                                tags_with_sufficient_data += 1
                
                alerts = generate_impression_alerts(data, columns)
                all_alerts.extend(alerts)
                
                debug_info.append({
                    'cache_key': cache_key,
                    'record_count': len(data),
                    'has_required_columns': has_required_columns,
                    'date_range': date_range,
                    'unique_tags': len(unique_tags),
                    'tags_with_sufficient_data': tags_with_sufficient_data if 'date_key' in columns else 0,
                    'alerts_found': len(alerts)
                })
                
        except Exception as e:
            debug_info.append({
                'cache_key': cache_key,
                'error': str(e)
            })
            continue
    
    return jsonify({
        'total_alerts': len(all_alerts),
        'alerts': all_alerts,
        'debug_info': debug_info
    })
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    debug_info = {
        'total_cache_entries': len(cache_entries),
        'cache_entries': [],
        'analysis_results': {
            'total_alerts': 0,
            'total_trends': 0,
            'alerts_by_severity': {'high': 0, 'medium': 0, 'low': 0}
        },
        'data_requirements': {
            'min_days_for_trends': 7,
            'min_days_for_alerts': 2,
            'min_impressions_for_alerts': 2500,
            'excludes_today': True
        }
    }
    
    all_alerts = []
    all_trends = {}
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                columns = cache_object['columns']
                data = cache_object['data']
                
                # Analyze this cache entry
                alerts = generate_impression_alerts(data, columns)
                trends = analyze_cache_trends(data, columns)
                
                all_alerts.extend(alerts)
                all_trends.update(trends)
                
                # Get date range for this cache entry
                date_range = {'min': None, 'max': None}
                if 'date_key' in columns:
                    date_index = columns.index('date_key')
                    dates = [str(row[date_index]) for row in data if row[date_index]]
                    if dates:
                        date_range['min'] = min(dates)
                        date_range['max'] = max(dates)
                
                # Check if we have impression data
                has_impressions = 'total_impressions' in columns
                impression_count = 0
                if has_impressions:
                    impression_index = columns.index('total_impressions')
                    impression_count = sum(1 for row in data if row[impression_index] and row[impression_index] > 0)
                
                cache_entry_info = {
                    'cache_key': cache_key,
                    'record_count': len(data),
                    'date_range': date_range,
                    'has_impressions': has_impressions,
                    'impression_records': impression_count,
                    'alerts_found': len(alerts),
                    'trends_found': len(trends),
                    'columns': columns
                }
                
                debug_info['cache_entries'].append(cache_entry_info)
                
        except Exception as e:
            debug_info['cache_entries'].append({
                'cache_key': cache_key,
                'error': str(e)
            })
    
    # Count alerts by severity
    for alert in all_alerts:
        severity = alert.get('severity', 'unknown')
        debug_info['analysis_results']['alerts_by_severity'][severity] = debug_info['analysis_results']['alerts_by_severity'].get(severity, 0) + 1
    
    debug_info['analysis_results']['total_alerts'] = len(all_alerts)
    debug_info['analysis_results']['total_trends'] = len(all_trends)
    
    # Add sample alerts and trends
    debug_info['sample_alerts'] = all_alerts[:3]  # First 3 alerts
    debug_info['sample_trends'] = dict(list(all_trends.items())[:3])  # First 3 trends
    
    return jsonify(debug_info)

@api_bp.route('/alerts/analytics')
def api_alerts_analytics():
    """Get analytics about alerts over time"""
    days = request.args.get('days', 30, type=int)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    all_alerts = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                from utils.analysis_utils import generate_comprehensive_alerts
                alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                all_alerts.extend(alerts)
        except Exception as e:
            continue
    
    # Filter for recent alerts
    today = datetime.now()
    recent_alerts = [alert for alert in all_alerts 
                    if (today - datetime.strptime(alert['date'], '%Y-%m-%d')).days <= days]
    
    # Calculate analytics
    analytics = {
        'total_alerts': len(recent_alerts),
        'alerts_by_severity': {},
        'alerts_by_type': {},
        'alerts_by_tag': {},
        'daily_trend': {},
        'most_affected_tags': [],
        'alert_frequency': {}
    }
    
    for alert in recent_alerts:
        # Severity breakdown
        severity = alert.get('severity', 'unknown')
        analytics['alerts_by_severity'][severity] = analytics['alerts_by_severity'].get(severity, 0) + 1
        
        # Type breakdown
        alert_type = alert.get('alert_type', 'unknown')
        analytics['alerts_by_type'][alert_type] = analytics['alerts_by_type'].get(alert_type, 0) + 1
        
        # Tag breakdown
        tag_id = alert.get('tag_id', 'unknown')
        analytics['alerts_by_tag'][tag_id] = analytics['alerts_by_tag'].get(tag_id, 0) + 1
        
        # Daily trend
        date = alert['date']
        analytics['daily_trend'][date] = analytics['daily_trend'].get(date, 0) + 1
    
    # Get most affected tags
    tag_counts = sorted(analytics['alerts_by_tag'].items(), key=lambda x: x[1], reverse=True)
    analytics['most_affected_tags'] = tag_counts[:10]
    
    # Calculate alert frequency
    if recent_alerts:
        avg_alerts_per_day = len(recent_alerts) / days
        analytics['alert_frequency'] = {
            'avg_per_day': round(avg_alerts_per_day, 2),
            'total_days': days,
            'total_alerts': len(recent_alerts)
        }
    
    return jsonify(analytics)

@api_bp.route('/alerts/history')
def api_alerts_history():
    """Get historical alert data"""
    tag_id = request.args.get('tag_id')
    days = request.args.get('days', 30, type=int)
    alert_type = request.args.get('type')
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    all_alerts = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                from utils.analysis_utils import generate_comprehensive_alerts
                alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                all_alerts.extend(alerts)
        except Exception as e:
            continue
    
    # Filter alerts
    today = datetime.now()
    filtered_alerts = []
    
    for alert in all_alerts:
        alert_date = datetime.strptime(alert['date'], '%Y-%m-%d')
        if (today - alert_date).days <= days:
            if tag_id and alert.get('tag_id') != tag_id:
                continue
            if alert_type and alert.get('alert_type') != alert_type:
                continue
            filtered_alerts.append(alert)
    
    # Sort by date
    filtered_alerts.sort(key=lambda x: x['date'], reverse=True)
    
    return jsonify(filtered_alerts)

@api_bp.route('/alerts/summary')
def api_alerts_summary():
    """Get summary of current alert status"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    all_alerts = []
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                from utils.analysis_utils import generate_comprehensive_alerts
                alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                all_alerts.extend(alerts)
        except Exception as e:
            continue
    
    # Filter for recent alerts (last 7 days)
    today = datetime.now()
    recent_alerts = [alert for alert in all_alerts 
                    if (today - datetime.strptime(alert['date'], '%Y-%m-%d')).days <= 7]
    
    summary = {
        'total_active_alerts': len(recent_alerts),
        'high_priority_alerts': len([a for a in recent_alerts if a.get('severity') == 'high']),
        'medium_priority_alerts': len([a for a in recent_alerts if a.get('severity') == 'medium']),
        'low_priority_alerts': len([a for a in recent_alerts if a.get('severity') == 'low']),
        'alerts_by_type': {
            'day_over_day': len([a for a in recent_alerts if a.get('alert_type') == 'day_over_day']),
            'week_over_week': len([a for a in recent_alerts if a.get('alert_type') == 'week_over_week']),
            'week_over_week_increase': len([a for a in recent_alerts if a.get('alert_type') == 'week_over_week_increase']),
            'gap_tolerant': len([a for a in recent_alerts if a.get('alert_type') == 'gap_tolerant'])
        },
        'unique_tags_affected': len(set(a.get('tag_id') for a in recent_alerts)),
        'last_updated': datetime.now().isoformat()
    }
    
    return jsonify(summary)

@api_bp.route('/alerts/rules', methods=['GET', 'POST', 'PUT', 'DELETE'])
def api_alert_rules():
    """Manage alert rules"""
    from utils.alert_rules import alert_rules
    
    if request.method == 'GET':
        return jsonify(alert_rules.rules)
    
    elif request.method == 'POST':
        data = request.get_json()
        action = data.get('action')
        
        if action == 'add_tag_rule':
            tag_id = data.get('tag_id')
            thresholds = data.get('thresholds')
            conditions = data.get('conditions')
            success = alert_rules.add_tag_rule(tag_id, thresholds, conditions)
            return jsonify({'success': success})
        
        elif action == 'add_custom_condition':
            condition = data.get('condition')
            success = alert_rules.add_custom_condition(condition)
            return jsonify({'success': success})
        
        elif action == 'update_global_thresholds':
            thresholds = data.get('thresholds')
            success = alert_rules.update_global_thresholds(thresholds)
            return jsonify({'success': success})
    
    elif request.method == 'DELETE':
        data = request.get_json()
        tag_id = data.get('tag_id')
        if tag_id:
            success = alert_rules.remove_tag_rule(tag_id)
            return jsonify({'success': success})
    
    return jsonify({'error': 'Invalid request'})

@api_bp.route('/alerts/test-notification')
def api_test_notification():
    """Test notification system"""
    from utils.notification_utils import send_alert_notifications
    
    test_alert = {
        'tag_id': 'TEST_TAG_001',
        'tag_name': 'Test Tag',
        'metric': 'total_impressions',
        'date': datetime.now().strftime('%Y-%m-%d'),
        'current_value': 1000,
        'previous_value': 2000,
        'change_percent': -50.0,
        'severity': 'medium',
        'message': 'Test alert - Impressions dropped 50.0% day-over-day',
        'alert_type': 'day_over_day'
    }
    
    notification_types = request.args.get('types', 'email,slack,webhook').split(',')
    results = send_alert_notifications(test_alert, notification_types)
    
    return jsonify({
        'test_alert': test_alert,
        'notification_results': results,
        'timestamp': datetime.now().isoformat()
    })