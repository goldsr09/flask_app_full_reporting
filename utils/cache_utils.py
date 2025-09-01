# utils/cache_utils.py
# Cache management utilities with unified storage and search functionality

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from config import DB_PATH

def get_yesterday_date():
    """Get yesterday's date string (exclude today's data everywhere)"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def ensure_date_not_today(date_str):
    """Ensure date is not today - if it is, return yesterday"""
    today = datetime.now().strftime('%Y-%m-%d')
    if date_str >= today:
        return get_yesterday_date()
    return date_str

def generate_cache_key(query_type, entity_id):
    """Generate cache key for unified storage: query_type + entity_id"""
    if query_type == 'query1':
        return f"seat_id_{entity_id}"
    elif query_type == 'query2':
        return f"publisher_id_{entity_id}"
    else:
        raise ValueError(f"Invalid query_type: {query_type}")

def cache_get_unified(query_type, entity_id):
    """Retrieve unified cache object for seat_id or publisher_id"""
    cache_key = generate_cache_key(query_type, entity_id)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        try:
            c.execute('SELECT result FROM query_cache WHERE cache_key = ?', (cache_key,))
            row = c.fetchone()
            if row:
                return json.loads(row[0])
            return None
        except Exception as e:
            print(f"❌ Cache get error for key {cache_key}: {e}")
            return None

def cache_set_unified(query_type, entity_id, columns, new_data):
    """Store unified cache object with deduplication"""
    cache_key = generate_cache_key(query_type, entity_id)
    
    print(f"🔧 Cache set: {len(columns)} columns, {len(new_data)} rows")
    print(f"🔧 Columns: {columns}")
    
    # Validate data structure first
    if not validate_data_structure(columns, new_data):
        print(f"❌ Data validation failed for {cache_key}")
        return False
    
    # Get existing cache object
    existing_cache = cache_get_unified(query_type, entity_id)
    
    if existing_cache is None:
        # Create new cache object
        cache_object = {
            'columns': columns,
            'data': []
        }
    else:
        # Use existing cache object
        cache_object = existing_cache
        # Ensure columns match
        if cache_object['columns'] != columns:
            print(f"⚠️ Column mismatch for {cache_key}, updating columns")
            cache_object['columns'] = columns
    
    # Find column indices for deduplication
    try:
        tag_id_index = columns.index('tag_id')
        date_key_index = columns.index('date_key')
        print(f"🔧 Using indices - tag_id: {tag_id_index}, date_key: {date_key_index}")
    except ValueError as e:
        print(f"❌ Required columns missing: {e}")
        print(f"❌ Available columns: {columns}")
        return False
    
    # Create lookup set of existing date_key + tag_id combinations
    existing_combinations = set()
    for row in cache_object['data']:
        try:
            if len(row) > max(date_key_index, tag_id_index):
                combo_key = f"{row[date_key_index]}|{row[tag_id_index]}"
                existing_combinations.add(combo_key)
        except (IndexError, TypeError) as e:
            print(f"⚠️ Skipping invalid existing row: {e}")
            continue
    
    # Add only new unique combinations
    new_records_added = 0
    today = datetime.now().strftime('%Y-%m-%d')
    
    for i, row in enumerate(new_data):
        try:
            # Validate row has enough columns
            if len(row) <= max(date_key_index, tag_id_index):
                print(f"⚠️ Skipping row {i}: insufficient columns ({len(row)} < {max(date_key_index, tag_id_index) + 1})")
                continue
            
            # Skip today's data everywhere
            row_date = str(row[date_key_index])
            if row_date >= today:
                print(f"🚫 Skipping today's data: {row_date}")
                continue
                
            combo_key = f"{row[date_key_index]}|{row[tag_id_index]}"
            if combo_key not in existing_combinations:
                cache_object['data'].append(row)
                existing_combinations.add(combo_key)
                new_records_added += 1
            else:
                print(f"🔧 Skipping duplicate: {combo_key}")
                
        except (IndexError, TypeError) as e:
            print(f"❌ Error processing row {i}: {e}")
            print(f"❌ Row content: {row}")
            continue
    
    # Store updated cache object
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        try:
            c.execute(
                'REPLACE INTO query_cache (cache_key, result, updated_at) VALUES (?, ?, ?)',
                (cache_key, json.dumps(cache_object), datetime.now().isoformat())
            )
            conn.commit()
            print(f"✅ Cached {new_records_added} new records for {cache_key} (total: {len(cache_object['data'])})")
            return True
        except Exception as e:
            print(f"❌ Cache set error for {cache_key}: {e}")
            return False

def validate_data_structure(columns, data):
    """Validate that data structure is consistent"""
    if not columns:
        print("❌ No columns provided")
        return False
    
    if not data:
        print("⚠️ No data provided (empty dataset)")
        return True  # Empty data is valid
    
    expected_column_count = len(columns)
    
    # Check first few rows to validate structure
    for i, row in enumerate(data[:5]):  # Check first 5 rows
        if not isinstance(row, (list, tuple)):
            print(f"❌ Row {i} is not a list/tuple: {type(row)}")
            return False
        
        if len(row) != expected_column_count:
            print(f"❌ Row {i} has {len(row)} columns, expected {expected_column_count}")
            print(f"❌ Row content: {row}")
            print(f"❌ Expected columns: {columns}")
            return False
    
    print(f"✅ Data structure validated: {len(data)} rows, {expected_column_count} columns")
    return True

def find_missing_dates(query_type, entity_id, date_from, date_to):
    """Find missing dates in cache for the specified entity and date range"""
    # Ensure dates don't include today
    date_to = ensure_date_not_today(date_to)
    
    if date_from > date_to:
        print(f"⚠️ Invalid date range after today exclusion: {date_from} to {date_to}")
        return []
    
    # Get existing cache
    cache_object = cache_get_unified(query_type, entity_id)
    
    if cache_object is None:
        # No cache exists, need all dates
        return get_date_ranges_to_query(date_from, date_to)
    
    # Find which dates we already have
    try:
        columns = cache_object['columns']
        date_key_index = columns.index('date_key')
        cached_dates = set()
        for row in cache_object['data']:
            try:
                if len(row) > date_key_index:
                    cached_dates.add(str(row[date_key_index]))
            except (IndexError, TypeError):
                continue
    except (ValueError, KeyError):
        print(f"❌ Invalid cache structure for {entity_id}")
        return get_date_ranges_to_query(date_from, date_to)
    
    # Generate requested date range
    requested_dates = set()
    current = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.strptime(date_to, '%Y-%m-%d')
    
    while current <= end:
        requested_dates.add(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    # Find missing dates
    missing_dates = requested_dates - cached_dates
    
    if not missing_dates:
        print(f"✅ All dates cached for {entity_id}")
        return []
    
    print(f"🔍 Found {len(missing_dates)} missing dates for {entity_id}: {sorted(missing_dates)}")
    
    # Convert missing dates to date ranges
    return get_date_ranges_to_query_from_dates(sorted(missing_dates))

def get_date_ranges_to_query(date_from, date_to):
    """Convert date range to list of ranges (for chunking)"""
    start = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.strptime(date_to, '%Y-%m-%d')
    
    # If range is <= 21 days, return single range
    if (end - start).days <= 21:
        return [(date_from, date_to)]
    
    # Split into 14-day chunks
    ranges = []
    current = start
    
    while current <= end:
        chunk_end = min(current + timedelta(days=13), end)  # 14-day chunks (0-13 = 14 days)
        ranges.append((
            current.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        ))
        current = chunk_end + timedelta(days=1)
    
    print(f"📊 Split into {len(ranges)} chunks: {ranges}")
    return ranges

def get_date_ranges_to_query_from_dates(missing_dates):
    """Convert list of missing dates to contiguous ranges"""
    if not missing_dates:
        return []
    
    ranges = []
    start_date = missing_dates[0]
    end_date = missing_dates[0]
    
    for i in range(1, len(missing_dates)):
        current_date = missing_dates[i]
        prev_date = missing_dates[i-1]
        
        # Check if dates are consecutive
        current_dt = datetime.strptime(current_date, '%Y-%m-%d')
        prev_dt = datetime.strptime(prev_date, '%Y-%m-%d')
        
        if (current_dt - prev_dt).days == 1:
            # Consecutive, extend current range
            end_date = current_date
        else:
            # Gap found, close current range and start new one
            ranges.append((start_date, end_date))
            start_date = current_date
            end_date = current_date
    
    # Add final range
    ranges.append((start_date, end_date))
    
    # Apply chunking to each range if needed
    chunked_ranges = []
    for range_start, range_end in ranges:
        chunked_ranges.extend(get_date_ranges_to_query(range_start, range_end))
    
    return chunked_ranges

def search_tags_in_cache(query_type, entity_id, search_term, date_from, date_to):
    """Search for tags by name within cache for specific entity and date range"""
    # Ensure dates don't include today
    date_to = ensure_date_not_today(date_to)
    
    cache_object = cache_get_unified(query_type, entity_id)
    if cache_object is None:
        return [], []
    
    try:
        columns = cache_object['columns']
        tag_name_index = columns.index('tag_name')
        tag_id_index = columns.index('tag_id') if 'tag_id' in columns else None
        date_key_index = columns.index('date_key')
        
        matching_rows = []
        search_term_lower = search_term.lower()
        
        for row in cache_object['data']:
            try:
                # Validate row has enough columns
                max_index = max(tag_name_index, date_key_index)
                if tag_id_index is not None:
                    max_index = max(max_index, tag_id_index)
                if len(row) <= max_index:
                    continue
                    
                # Check tag name match
                tag_name = str(row[tag_name_index] or '').lower()
                tag_id_str = str(row[tag_id_index] or '').lower() if tag_id_index is not None else ''
                
                if search_term_lower in tag_name or search_term_lower in tag_id_str:
                    # Check date range
                    row_date = str(row[date_key_index])
                    if date_from <= row_date <= date_to:
                        matching_rows.append(row)
            except (IndexError, TypeError):
                continue
        
        return columns, matching_rows
        
    except (ValueError, KeyError) as e:
        print(f"❌ Search error: {e}")
        return [], []

def clear_cache():
    """Clear all cache entries"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('DELETE FROM query_cache')
        conn.commit()
        print("🗑️ All cache cleared successfully!")

def get_cache_stats():
    """Get cache statistics"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM query_cache')
        total_entries = c.fetchone()[0]
        
        c.execute('SELECT cache_key, result FROM query_cache')
        entries = c.fetchall()
    
    stats = {
        'total_cache_objects': total_entries,
        'query1_objects': 0,
        'query2_objects': 0,
        'total_records': 0
    }
    
    for cache_key, result_json in entries:
        try:
            cache_object = json.loads(result_json)
            record_count = len(cache_object.get('data', []))
            stats['total_records'] += record_count
            
            if cache_key.startswith('seat_id_'):
                stats['query1_objects'] += 1
            elif cache_key.startswith('publisher_id_'):
                stats['query2_objects'] += 1
                
        except:
            continue
    
    return stats

def get_all_cache_keys():
    """Get all cache keys from the database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT cache_key FROM query_cache")
        keys = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return keys
    except Exception as e:
        print(f"❌ Error getting cache keys: {e}")
        return []