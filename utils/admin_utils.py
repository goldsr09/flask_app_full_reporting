# utils/admin_utils.py
# Admin utilities for auto-collection, deduplication, and maintenance

import sqlite3
import json
import threading
import time
import schedule
from datetime import datetime, timedelta
from config import DB_PATH, KNOWN_SEAT_IDS, KNOWN_PUBLISHER_IDS, LOOKBACK_DAYS
from utils.cache_utils import cache_get_unified, generate_cache_key
from utils.superset_utils import fetch_from_superset, fetch_from_superset_query2_with_fallback

def get_date_range_for_auto_collection():
    """Get date range for automatic collection (yesterday + lookback days)"""
    today = datetime.now()
    end_date = today - timedelta(days=1)  # Yesterday (complete data)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)  # Last N days
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def fetch_data_for_seat_id(seat_id, date_from, date_to):
    """Fetch data for a specific seat ID and cache it"""
    try:
        print(f"🔄 Auto-collecting data for Seat ID: {seat_id} ({date_from} to {date_to})")
        
        # Check if already cached
        cache_object = cache_get_unified('query1', seat_id)
        if cache_object:
            # Check if we have all dates
            try:
                date_key_index = cache_object['columns'].index('date_key')
                cached_dates = set(str(row[date_key_index]) for row in cache_object['data'])
                
                # Generate requested dates
                start = datetime.strptime(date_from, '%Y-%m-%d')
                end = datetime.strptime(date_to, '%Y-%m-%d')
                requested_dates = set()
                current = start
                while current <= end:
                    requested_dates.add(current.strftime('%Y-%m-%d'))
                    current += timedelta(days=1)
                
                if requested_dates.issubset(cached_dates):
                    print(f"✅ Seat ID {seat_id} already cached for {date_from} to {date_to}")
                    return True
            except (ValueError, KeyError):
                pass
        
        # Fetch fresh data using the optimized function
        columns, data = fetch_from_superset(date_from, date_to, seat_id)
        
        if len(data) > 0:
            print(f"✅ Auto-collected {len(data)} rows for Seat ID {seat_id}")
            return True
        else:
            print(f"⚠️ No data found for Seat ID {seat_id}")
            return False
            
    except Exception as e:
        print(f"❌ Error fetching data for Seat ID {seat_id}: {str(e)}")
        return False

def fetch_data_for_publisher_id(publisher_id, date_from, date_to):
    """Fetch data for a specific publisher ID and cache it"""
    try:
        print(f"🔄 Auto-collecting data for Publisher ID: {publisher_id} ({date_from} to {date_to})")
        
        # Check if already cached
        cache_object = cache_get_unified('query2', publisher_id)
        if cache_object:
            # Check if we have all dates
            try:
                date_key_index = cache_object['columns'].index('date_key')
                cached_dates = set(str(row[date_key_index]) for row in cache_object['data'])
                
                # Generate requested dates
                start = datetime.strptime(date_from, '%Y-%m-%d')
                end = datetime.strptime(date_to, '%Y-%m-%d')
                requested_dates = set()
                current = start
                while current <= end:
                    requested_dates.add(current.strftime('%Y-%m-%d'))
                    current += timedelta(days=1)
                
                if requested_dates.issubset(cached_dates):
                    print(f"✅ Publisher ID {publisher_id} already cached for {date_from} to {date_to}")
                    return True
            except (ValueError, KeyError):
                pass
        
        # Fetch fresh data using the optimized function
        columns, data = fetch_from_superset_query2_with_fallback(date_from, date_to, publisher_id)
        
        if len(data) > 0:
            print(f"✅ Auto-collected {len(data)} rows for Publisher ID {publisher_id}")
            return True
        else:
            print(f"⚠️ No data found for Publisher ID {publisher_id}")
            return False
            
    except Exception as e:
        print(f"❌ Error fetching data for Publisher ID {publisher_id}: {str(e)}")
        return False

def auto_collect_daily_data():
    """Main function to collect daily data for all known IDs"""
    from config import AUTO_COLLECTION_ENABLED
    
    if not AUTO_COLLECTION_ENABLED:
        print("🚫 Auto-collection is disabled")
        return
    
    print("🚀 Starting automatic daily data collection...")
    start_time = datetime.now()
    
    date_from, date_to = get_date_range_for_auto_collection()
    print(f"📅 Collecting data for date range: {date_from} to {date_to}")
    
    successful_collections = 0
    failed_collections = 0
    
    # Collect Query 1 data (Seat IDs)
    print(f"🎯 Collecting Query 1 data for {len(KNOWN_SEAT_IDS)} Seat IDs...")
    for seat_id in KNOWN_SEAT_IDS:
        if fetch_data_for_seat_id(seat_id, date_from, date_to):
            successful_collections += 1
        else:
            failed_collections += 1
        
        # Small delay to avoid overwhelming Superset
        time.sleep(2)
    
    # Collect Query 2 data (Publisher IDs)
    print(f"🎯 Collecting Query 2 data for {len(KNOWN_PUBLISHER_IDS)} Publisher IDs...")
    for publisher_id in KNOWN_PUBLISHER_IDS:
        if fetch_data_for_publisher_id(publisher_id, date_from, date_to):
            successful_collections += 1
        else:
            failed_collections += 1
        
        # Small delay to avoid overwhelming Superset
        time.sleep(2)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"✅ Auto-collection completed in {duration:.1f} seconds")
    print(f"📊 Results: {successful_collections} successful, {failed_collections} failed")
    
    # Store collection summary
    summary = {
        'timestamp': end_time.isoformat(),
        'date_range': f"{date_from} to {date_to}",
        'successful': successful_collections,
        'failed': failed_collections,
        'duration_seconds': duration,
        'seat_ids_processed': len(KNOWN_SEAT_IDS),
        'publisher_ids_processed': len(KNOWN_PUBLISHER_IDS)
    }
    
    # Cache the summary for status endpoint
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            'REPLACE INTO query_cache (cache_key, result, updated_at) VALUES (?, ?, ?)',
            ('auto_collection_last_run', json.dumps(summary), datetime.now().isoformat())
        )
        conn.commit()
    
    return summary

def daily_bulk_collection():
    """Daily bulk collection for Query 1 - collects data for all seat_ids"""
    from datetime import datetime, timedelta
    
    print("🚀 Starting daily bulk collection for Query 1...")
    start_time = datetime.now()
    
    # Get yesterday's date for collection
    yesterday = datetime.now() - timedelta(days=1)
    date_from = yesterday.strftime('%Y-%m-%d')
    date_to = yesterday.strftime('%Y-%m-%d')
    
    print(f"📅 Collecting data for date: {date_from}")
    
    try:
        from utils.superset_utils import fetch_all_seat_ids_bulk
        
        success = fetch_all_seat_ids_bulk(date_from, date_to)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if success:
            print(f"✅ Daily bulk collection completed successfully in {duration:.1f} seconds")
            return {
                'status': 'success',
                'message': f'Daily bulk collection completed in {duration:.1f}s',
                'date_collected': date_from,
                'duration_seconds': duration
            }
        else:
            print(f"❌ Daily bulk collection failed after {duration:.1f} seconds")
            return {
                'status': 'error',
                'message': f'Daily bulk collection failed after {duration:.1f}s',
                'date_collected': date_from,
                'duration_seconds': duration
            }
            
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"❌ Daily bulk collection error: {e}")
        return {
            'status': 'error',
            'message': f'Daily bulk collection error: {str(e)}',
            'date_collected': date_from,
            'duration_seconds': duration
        }

def run_scheduler():
    """Background thread function to run the scheduler"""
    from config import AUTO_COLLECTION_TIME
    print(f"🕐 Scheduler started - auto-collection at {AUTO_COLLECTION_TIME} daily")
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"❌ Scheduler error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

def extract_all_ids_from_cache():
    """Extract all unique Seat IDs and Publisher IDs from cached data"""
    all_seat_ids = set()
    all_publisher_ids = set()
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT cache_key, result FROM query_cache')
        cache_entries = c.fetchall()
    
    print(f"🔍 Analyzing {len(cache_entries)} cache entries to discover IDs...")
    
    for cache_key, result_json in cache_entries:
        try:
            if cache_key.startswith('seat_id_'):
                # Extract seat_id from cache key
                seat_id = cache_key.replace('seat_id_', '')
                if seat_id and seat_id != 'None' and len(seat_id) > 5:
                    all_seat_ids.add(seat_id)
            elif cache_key.startswith('publisher_id_'):
                # Extract publisher_id from cache key
                publisher_id = cache_key.replace('publisher_id_', '')
                if publisher_id and publisher_id != 'None' and publisher_id.isdigit():
                    all_publisher_ids.add(publisher_id)
        except Exception as e:
            print(f"Error processing cache entry {cache_key}: {e}")
            continue
    
    seat_ids_list = sorted(list(all_seat_ids))
    publisher_ids_list = sorted(list(all_publisher_ids), key=int)
    
    print(f"✅ Discovered {len(seat_ids_list)} unique Seat IDs")
    print(f"✅ Discovered {len(publisher_ids_list)} unique Publisher IDs")
    
    return seat_ids_list, publisher_ids_list

def clear_cache_by_tag(tag_id):
    """Clear cache entries that contain a specific tag_id"""
    removed_count = 0
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT cache_key, result FROM query_cache')
        cache_entries = c.fetchall()
    
    for cache_key, result_json in cache_entries:
        try:
            if cache_key.startswith(('seat_id_', 'publisher_id_')):
                cache_object = json.loads(result_json)
                if 'columns' in cache_object and 'data' in cache_object:
                    columns = cache_object['columns']
                    data = cache_object['data']
                    
                    if 'tag_id' in columns:
                        tag_id_index = columns.index('tag_id')
                        
                        # Check if this cache object contains the tag
                        has_tag = any(str(row[tag_id_index]) == str(tag_id) for row in data)
                        
                        if has_tag:
                            # Remove rows with this tag_id
                            filtered_data = [row for row in data if str(row[tag_id_index]) != str(tag_id)]
                            
                            if len(filtered_data) != len(data):
                                print(f"Removing {len(data) - len(filtered_data)} rows with tag_id {tag_id} from {cache_key}")
                                
                                if filtered_data:
                                    # Update cache with remaining data
                                    cache_object['data'] = filtered_data
                                    with sqlite3.connect(DB_PATH) as conn:
                                        c = conn.cursor()
                                        c.execute(
                                            'REPLACE INTO query_cache (cache_key, result, updated_at) VALUES (?, ?, ?)',
                                            (cache_key, json.dumps(cache_object), datetime.now().isoformat())
                                        )
                                        conn.commit()
                                else:
                                    # No data left, remove entire cache entry
                                    with sqlite3.connect(DB_PATH) as conn:
                                        c = conn.cursor()
                                        c.execute('DELETE FROM query_cache WHERE cache_key = ?', (cache_key,))
                                        conn.commit()
                                
                                removed_count += 1
        except Exception as e:
            print(f"Error processing cache entry {cache_key}: {e}")
            continue
    
    if removed_count > 0:
        print(f"Successfully modified {removed_count} cache entries containing tag_id '{tag_id}'")
    else:
        print(f"No cache entries found containing tag_id '{tag_id}'")
    
    return removed_count

def get_auto_collection_status():
    """Get status of auto-collection system"""
    from config import AUTO_COLLECTION_ENABLED, AUTO_COLLECTION_TIME
    
    # Get last run info
    last_run = None
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT result FROM query_cache WHERE cache_key = ?', ('auto_collection_last_run',))
            row = c.fetchone()
            if row:
                last_run = json.loads(row[0])
    except:
        pass
    
    # Get discovered IDs
    discovered_seat_ids, discovered_publisher_ids = extract_all_ids_from_cache()
    
    return {
        'enabled': AUTO_COLLECTION_ENABLED,
        'schedule_time': AUTO_COLLECTION_TIME,
        'lookback_days': LOOKBACK_DAYS,
        'known_seat_ids': KNOWN_SEAT_IDS,
        'known_publisher_ids': KNOWN_PUBLISHER_IDS,
        'discovered_seat_ids': discovered_seat_ids,
        'discovered_publisher_ids': discovered_publisher_ids,
        'last_run': last_run,
        'next_run': schedule.next_run().isoformat() if schedule.jobs else None
    }

def diagnose_cache_health():
    """Diagnose cache health and identify potential issues"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT cache_key, result FROM query_cache')
        cache_entries = c.fetchall()
    
    stats = {
        'total_cache_objects': len(cache_entries),
        'query1_objects': 0,
        'query2_objects': 0,
        'total_records': 0,
        'corrupted_objects': 0,
        'empty_objects': 0,
        'date_range': {'min': None, 'max': None}
    }
    
    all_dates = set()
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            
            if cache_key.startswith('seat_id_'):
                stats['query1_objects'] += 1
            elif cache_key.startswith('publisher_id_'):
                stats['query2_objects'] += 1
            
            if 'data' in cache_object:
                data_count = len(cache_object['data'])
                stats['total_records'] += data_count
                
                if data_count == 0:
                    stats['empty_objects'] += 1
                
                # Extract dates
                if 'columns' in cache_object and 'date_key' in cache_object['columns']:
                    date_index = cache_object['columns'].index('date_key')
                    for row in cache_object['data']:
                        try:
                            all_dates.add(str(row[date_index]))
                        except:
                            pass
            
        except Exception:
            stats['corrupted_objects'] += 1
    
    if all_dates:
        stats['date_range']['min'] = min(all_dates)
        stats['date_range']['max'] = max(all_dates)
    
    return stats

def get_cache_size_info():
    """Get information about cache size and storage"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        
        # Get total cache entries
        c.execute('SELECT COUNT(*) FROM query_cache')
        total_entries = c.fetchone()[0]
        
        # Get database file size
        import os
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        db_size_mb = db_size / (1024 * 1024)
        
        # Get oldest and newest entries
        c.execute('SELECT MIN(created_at), MAX(created_at) FROM query_cache')
        date_range = c.fetchone()
    
    return {
        'total_entries': total_entries,
        'database_size_mb': round(db_size_mb, 2),
        'date_range': {
            'oldest': date_range[0],
            'newest': date_range[1]
        }
    }