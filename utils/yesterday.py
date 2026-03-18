# utils/yesterday.py
# Functions for fetching and caching yesterday's data

import sqlite3
import json
from datetime import datetime, timedelta
from config import DB_PATH
from utils.superset_utils import SUPERSET_DB_ID, SUPERSET_EXECUTE_URL, SUPERSET_HEADERS
from utils.cache_utils import cache_get_unified, cache_set_unified, generate_cache_key
import requests


sql_test = """
    SELECT
        t.name AS tag_name,
        m.seat_id,
        m.tag_id,
        m.channel_id,
        SUM(m.query_requests) AS total_type_a_requests,
        SUM(m.query_responses) AS total_type_a_responses,
        SUM(m.slot_requests) AS total_type_b_requests,
        SUM(m.slot_responses) AS total_type_b_responses,
        SUM(m.creative_fetches) AS total_type_c_fetches,
        SUM(m.creative_responses) AS total_type_c_responses,
        CASE
            WHEN SUM(m.slot_requests) > 0
            THEN (SUM(m.deliveries) * 100.0 / SUM(m.slot_requests))
            ELSE 0
        END AS completion_rate,
        CASE
            WHEN SUM(m.creative_responses) > 0
            THEN (SUM(m.deliveries) * 100.0 / SUM(m.creative_responses))
            ELSE 0
        END AS avg_success_rate,
        SUM(m.deliveries) AS total_deliveries,
        m.date_key
    FROM analytics.agg_metrics_daily m
    LEFT JOIN analytics.dim_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
    WHERE m.date_key = 'YESTERDAY_DATE'
      AND m.seat_id IN ('SEAT_ID_LIST')
      AND m.date_id IS NOT NULL
    GROUP BY
        t.name, m.seat_id, m.tag_id,
        m.date_key, m.channel_id
    ORDER BY m.seat_id, m.date_key DESC
    """




def fetch_from_superset_api_test(sql_test):
    
    with sqlite3.connect(DB_PATH) as conn:
                    c = conn.cursor()
                    c.execute("SELECT cache_key FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
                    publisher_cache_keys = [row[0] for row in c.fetchall()]
                    seat_ids = [key.replace('seat_id_', '') for key in publisher_cache_keys]
                    print(f"Seat IDs: {seat_ids}")
    
    # Create seat_id list for SQL
    seat_id_list = "', '".join(seat_ids)
    
    # Generate dynamic SQL
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    sql_test = sql_test.replace('YESTERDAY_DATE', yesterday_date)
    sql_test = sql_test.replace('SEAT_ID_LIST', seat_id_list)
    
    print(f"🔍 Executing bulk Query1_test")
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql_test,
        "schema": "analytics"
    }
    
    try:
        print(f"🔄 Executing Superset API call...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload)
        ) 
        print(f"Status: {response.status_code}")
        print(f"Response length: {len(response.text)} characters")
        print(f"Response preview: {response.text[:500]}...")
        
        # Always try to parse the response regardless of status
        try:
            data = response.text
            print(f"Data type: {type(data)}")
            if isinstance(data, list):
                print(f"Number of rows: {len(data)}")
        except Exception as e:
            print(f"❌ Error parsing JSON: {e}")
        
        # Parse and print seat_id and deliveries
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Handle different response formats
                if isinstance(data, dict) and 'data' in data:
                    # Nested dictionary with 'data' key
                    rows = data['data']
                    if isinstance(rows, list):
                        print(f"\n📊 All Data (Seat ID, Tag Name, Deliveries):")
                        for i, row in enumerate(rows):
                            seat_id = row.get('seat_id', 'N/A')
                            tag_name = row.get('tag_name', 'N/A')
                            deliveries = row.get('total_deliveries', 'N/A')
                            print(f"  {i+1}. Seat ID: {seat_id}, Tag: {tag_name}, Deliveries: {deliveries}")
                    else:
                        print(f"❌ 'data' is not a list: {type(rows)}")
                elif isinstance(data, list):
                    # Direct list of dictionaries
                    rows = data
                    print(f"\n📊 Seat ID and Deliveries (showing first 5):")
                    for i, row in enumerate(rows[:5]):  # Only show first 5
                        seat_id = row.get('seat_id', 'N/A')
                        deliveries = row.get('total_deliveries', 'N/A')
                        print(f"  {i+1}. Seat ID: {seat_id}, Deliveries: {deliveries}")
                    if len(rows) > 5:
                        print(f"  ... and {len(rows) - 5} more rows")
                else:
                    print(f"❌ Unexpected data format: {type(data)}")
                        
            except Exception as e:
                print(f"❌ Error parsing response: {e}")
    except Exception as e:
        print(f"❌ API test error: {str(e)}")





def check_cache_for_yesterday():
    """Check what seat_ids already have yesterday's data cached"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # Get all cached seat_ids
        c.execute("SELECT cache_key FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
        cached_keys = [row[0] for row in c.fetchall()]
        
        print(f"🔍 Found {len(cached_keys)} cached seat_id entries")
        print(f"🔍 Cached seat_ids: {[key.replace('seat_id_', '') for key in cached_keys[:5]]}{'...' if len(cached_keys) > 5 else ''}")
        
        # Check which ones have yesterday's data
        missing_seat_ids = []
        for cache_key in cached_keys:
            seat_id = cache_key.replace('seat_id_', '')
            cache_object = cache_get_unified('query1', seat_id)
            
            if cache_object and 'data' in cache_object and 'columns' in cache_object:
                # Find date_key column index
                try:
                    date_key_index = cache_object['columns'].index('date_key')
                    # Check if yesterday's data exists in cache
                    has_yesterday = any(
                        str(row[date_key_index]) == yesterday 
                        for row in cache_object['data']
                        if len(row) > date_key_index
                    )
                    if not has_yesterday:
                        missing_seat_ids.append(seat_id)
                except ValueError:
                    # date_key column not found, consider it missing
                    missing_seat_ids.append(seat_id)
            else:
                missing_seat_ids.append(seat_id)
        
        return missing_seat_ids


def fetch_missing_yesterday_data():
    """Only fetch data for seat_ids missing yesterday's data"""
    missing_seat_ids = check_cache_for_yesterday()
    
    if not missing_seat_ids:
        print("✅ All seat_ids already have yesterday's data cached")
        return None
    
    print(f"🔄 Fetching yesterday's data for {len(missing_seat_ids)} missing seat_ids")
    print(f"📅 Yesterday's date: {(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')}")
    print(f"🔍 Missing seat_ids: {missing_seat_ids[:5]}{'...' if len(missing_seat_ids) > 5 else ''}")
    
    # Build SQL query for missing seat_ids
    seat_id_list = "', '".join(missing_seat_ids)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    sql = f"""
    SELECT 
        t.name AS tag_name,
        m.seat_id,
        m.tag_id,
        SUM(m.query_requests) AS total_type_a_requests,
        SUM(m.query_responses) AS total_type_a_responses,
        SUM(m.slot_requests) AS total_type_b_requests,
        SUM(m.slot_responses) AS total_type_b_responses,
        SUM(m.creative_fetches) AS total_type_c_fetches,
        SUM(m.creative_responses) AS total_type_c_responses,
        CASE
            WHEN SUM(m.slot_requests) > 0
            THEN (SUM(m.deliveries) * 100.0 / SUM(m.slot_requests))
            ELSE 0
        END AS completion_rate,
        CASE
            WHEN SUM(m.creative_responses) > 0
            THEN (SUM(m.deliveries) * 100.0 / SUM(m.creative_responses))
            ELSE 0
        END AS avg_success_rate,
        SUM(m.deliveries) AS total_deliveries,
        m.date_key
    FROM analytics.agg_metrics_daily m
    LEFT JOIN analytics.dim_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
    WHERE m.date_key = '{yesterday}'
      AND m.seat_id IN ('{seat_id_list}')
      AND m.date_id IS NOT NULL
    GROUP BY 
        t.name, m.seat_id, m.tag_id, m.date_key
    ORDER BY m.seat_id, m.date_key DESC
    """
    
    # Execute API call
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "schema": "analytics"
    }
    
    try:
        print(f"🔄 Executing Superset API call...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload)
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"🔍 API Response status: {response.status_code}")
            print(f"🔍 API Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            if isinstance(data, dict) and 'data' in data:
                api_data = data['data']
                print(f"✅ Fetched {len(api_data)} rows from API")
                if len(api_data) == 0:
                    print(f"⚠️ No data returned - this could mean:")
                    print(f"   - No data exists for yesterday ({yesterday})")
                    print(f"   - Seat IDs don't have data for that date")
                    print(f"   - API query returned empty results")
                return api_data
            else:
                print(f"❌ Unexpected API response format: {type(data)}")
                print(f"❌ Response content: {data}")
                return None
        else:
            print(f"❌ API call failed with status {response.status_code}")
            print(f"❌ Response text: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None



def store_yesterday_data_to_cache(api_data):
    """Store the bulk API response back to individual seat_id caches"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Convert API data from list of dicts to list of lists format
    if api_data and isinstance(api_data[0], dict):
        # Define column order to match cache format
        columns = ['tag_name', 'seat_id', 'tag_id', 
                  'total_type_a_requests', 'total_type_a_responses', 
                  'total_type_b_requests', 'total_type_b_responses', 
                  'total_type_c_fetches', 'total_type_c_responses', 
                  'completion_rate', 'avg_success_rate', 'total_deliveries', 'date_key']
        
        # Convert each row from dict to list
        converted_data = []
        for row in api_data:
            converted_row = [row.get(col, '') for col in columns]
            converted_data.append(converted_row)
        
        api_data = converted_data
        print(f"🔄 Converted {len(converted_data)} rows from dict to list format")
    
    # Group data by seat_id
    seat_id_groups = {}
    for row in api_data:
        # Now api_data is list of lists, so access by index
        seat_id = row[1] if len(row) > 1 else None  # seat_id is still at index 1
        if seat_id:
            if seat_id not in seat_id_groups:
                seat_id_groups[seat_id] = []
            seat_id_groups[seat_id].append(row)
    
    # Store each seat_id's data to its own cache
    for seat_id, seat_data in seat_id_groups.items():
        if seat_data:
            # Define columns for new data
            columns = ['tag_name', 'seat_id', 'tag_id', 
                      'total_type_a_requests', 'total_type_a_responses', 
                      'total_type_b_requests', 'total_type_b_responses', 
                      'total_type_c_fetches', 'total_type_c_responses', 
                      'completion_rate', 'avg_success_rate', 'total_deliveries', 'date_key']
            
            # Get existing cache or create new
            existing_cache = cache_get_unified('query1', seat_id) or {'data': [], 'columns': columns}
            
            # Add yesterday's data (avoid duplicates)
            if 'columns' in existing_cache:
                try:
                    date_key_index = existing_cache['columns'].index('date_key')
                    existing_dates = {str(row[date_key_index]) for row in existing_cache['data'] if len(row) > date_key_index}
                    if yesterday not in existing_dates:
                        existing_cache['data'].extend(seat_data)
                        cache_set_unified('query1', seat_id, existing_cache['columns'], existing_cache['data'])
                        print(f"✅ Cached {len(seat_data)} rows for seat_id {seat_id}")
                    else:
                        print(f"⚠️ Yesterday's data already exists for seat_id {seat_id}")
                except ValueError:
                    # date_key column not found, just add the data
                    existing_cache['data'].extend(seat_data)
                    cache_set_unified('query1', seat_id, existing_cache['columns'], existing_cache['data'])
                    print(f"✅ Cached {len(seat_data)} rows for seat_id {seat_id}")


def remove_channel_id_from_cache():
    """Remove channel_id column from existing cached data"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
        cache_entries = c.fetchall()
        
        updated_count = 0
        for cache_key, result_json in cache_entries:
            try:
                cache_object = json.loads(result_json)
                if 'columns' in cache_object and 'data' in cache_object:
                    # Find channel_id column index
                    try:
                        provider_index = cache_object['columns'].index('channel_id')
                        
                        # Remove channel_id from columns
                        cache_object['columns'].pop(provider_index)
                        
                        # Remove channel_id from each data row
                        for row in cache_object['data']:
                            if len(row) > provider_index:
                                row.pop(provider_index)
                        
                        # Update cache with modified data
                        c.execute(
                            'UPDATE query_cache SET result = ?, updated_at = ? WHERE cache_key = ?',
                            (json.dumps(cache_object), datetime.now().isoformat(), cache_key)
                        )
                        updated_count += 1
                        print(f"✅ Updated {cache_key} - removed channel_id column")
                        
                    except ValueError:
                        # channel_id column not found, skip
                        print(f"⚠️ {cache_key} - no channel_id column found")
                        continue
                        
            except Exception as e:
                print(f"❌ Error updating {cache_key}: {e}")
                continue
        
        conn.commit()
        print(f"🔄 Updated {updated_count} cache entries to remove channel_id")
        return updated_count


def clear_cache():
    """Clear all cached data to force fresh queries with new column structure"""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
        deleted_count = c.rowcount
        conn.commit()
        print(f"🗑️ Cleared {deleted_count} cached seat_id entries")
        return deleted_count


def fetch_and_cache_yesterday_data():
    """Main function to fetch and cache yesterday's data"""
    # 1. Check what's missing
    missing_seat_ids = check_cache_for_yesterday()
    
    # 2. Fetch missing data
    if missing_seat_ids:
        api_data = fetch_missing_yesterday_data()
        
        # 3. Store to cache
        if api_data:
            store_yesterday_data_to_cache(api_data)
    
    return missing_seat_ids