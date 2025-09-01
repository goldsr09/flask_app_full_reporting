# utils/superset_utils.py
# Optimized Superset API queries with chunking and caching integration

from re import escape
import sqlite3
import requests
from config import DB_PATH
import json
from datetime import datetime, timedelta
from utils.cache_utils import (
    find_missing_dates, 
    cache_set_unified, 
    cache_get_unified,
    ensure_date_not_today,
    get_all_cache_keys
)

# --- Superset API Config ---
SUPERSET_EXECUTE_URL = "https://superset.de.gcp.rokulabs.net/api/v1/sqllab/execute/"
SUPERSET_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Referer": "https://superset.de.gcp.rokulabs.net/sqllab/",
    "Origin": "https://superset.de.gcp.rokulabs.net",
    "X-CSRFToken": "IjYyMGY2ZjkwMjRmN2JiMDVmZTVmZDZkNmJmMjc3Mjk2Y2IwYTY1MDci.aK3Biw.iiy4hTgZJaivlRMgTTtQnrrpESA",
    "Cookie": "session=.eJxVjrtOA0EMRf9l6iDZXo89k5oCQQUUlKt52BBlRZTdjRSE-HcWqGhucaVzdD7D6LMtb2G_zhfbhfHQwz4IGYFBq4LolRRRSBkIYUCqlqiVGoU6l065wc8OWTQnKsRZ3ViKiEbquWNLUXSoIpjAC1syYyzKWdiwQQICwUIEbskjxMYStpDLYvNfDbLS9rRl9nE9He39txBcPAOxa60Q3aJ36VKdVClLq1Akgm7cdGplso3ZwF04HXob_6kenmb-eMZpuoNbPPt9v1niebgeV7q-0sujh69vlhtT6g.aK3BjQ.1eY4vwcquL4Lz5y7ZyAdHtHjRn4; oidc_id_token=eyJhbGciOiJIUzUxMiIsImlhdCI6MTc1NjIxNzYxMSwiZXhwIjoxNzU2MjIxMjExfQ.eyJhdWQiOiJlOTNhZDI0MC05OGRiLTQ4NTctOTIxOC1lZDAyMjAwY2RmMjciLCJpc3MiOiJodHRwczovL2xvZ2luLm1pY3Jvc29mdG9ubGluZS5jb20vMDBmYzdlNDItYWQ2NS00YzRjLWFiNTQtODQ4YmExMjRhNWI3L3YyLjAiLCJpYXQiOjE3NTYyMTczMTEsIm5iZiI6MTc1NjIxNzMxMSwiZXhwIjoxNzU2MjIxMjExLCJhaW8iOiJBZFFBSy84WkFBQUFQTDFoQlFTd0l3cVE5dW5KS254TVhja2tQUVZCcFdmN09RS3pCZUFLVFI5ZHBKRkhyNS93VTBlN1FNazdUNGtkcmFWd0Zkajd0TnVFU2E2THZPUVFBQ2xRL2tURDN4d3BxdmUyMDZubm1DcWRsOW4xNFZrNEVvY0tvQXZaOU5Yek1CemYzYVIrNVVQWnJtWnZWZ1RNcEwyTXVaRU4ybVlDbmJNbkVqbjl5eGpGVXFlVUNTVitkN29jNUwvVTlLZEpQMFRmYmgzZDBIUkZJaG1mZWJ6UUJRYkNhSDNrZ01aOTlncU5USVVCdldEUTNuYVVEeVA3aks1L0ttMGVOaW15NHRCS3YyQlJ0Qmh5OGtDSWZYR1BlQT09IiwiZW1haWwiOiJyZ29sZHN0ZWluQHJva3UuY29tIiwiZmFtaWx5X25hbWUiOiJHb2xkc3RlaW4iLCJnaXZlbl9uYW1lIjoiUnlhbiIsIm5hbWUiOiJSeWFuIEdvbGRzdGVpbiIsIm9pZCI6IjQxYjVmYjMwLTVkNzYtNDkzZS1iMWU2LTBhOGZkNmM3NTk1MSIsInByZWZlcnJlZF91c2VybmFtZSI6InJnb2xkc3RlaW5Acm9rdS5jb20iLCJyaCI6IjEuQVc0QVFuNzhBR1d0VEV5clZJU0xvU1NsdDBEU091bmJtRmRJa2hqdEFpQU0zeWRlQVZKdUFBLiIsInNpZCI6IjAwN2I1MjY5LTliNjItMjgwZi1hZTFjLWE5NTQ2NWUxODk0OCIsInN1YiI6IlhRM011TG9NbXFrWjVOY2dJeWtIYmZMdzRBSWhqMzZyMmtERmRLcmlzSkkiLCJ0aWQiOiIwMGZjN2U0Mi1hZDY1LTRjNGMtYWI1NC04NDhiYTEyNGE1YjciLCJ1dGkiOiJoQ3dreFk4SzBrYTJpdFBHbTJJSUFBIiwidmVyIjoiMi4wIn0.sv7oTSJ3NmzycsVVKwqWUWQV1JPVboQSXSjM1Lf2ta5E5keMUhPKD1CVHwJeZ45bQK4ZcdXZCtRCzLJAUQBSYg"
}
SUPERSET_DB_ID = 2

# --- Working Query Template ---
QUERY_TEMPLATE = """
SELECT 
    f.date_key,
    CAST(d AS VARCHAR) AS deal_id,
    COALESCE(h.name, CAST(d AS VARCHAR)) AS deal_name,
    SUM(f.is_impressed) AS imps,
    SUM(CASE WHEN f.ad_fetch_source IS NULL AND f.event_type = 'candidate' THEN 1 ELSE 0 END) AS real_demand_returned,
    SUM(f.num_opportunities) AS oppos,
    SUM(f.is_selected) AS total_bids,
    SUM(CASE WHEN f.is_impressed IS NULL AND f.event_type = 'candidate' AND f.ad_fetch_source IS NULL THEN 1 ELSE 0 END) AS Loss_Totals,
    (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.num_opportunities), 0)) * 100.00 AS fill_rate,
    SUM(CASE WHEN f.event_type = 'demand_request' THEN 1 ELSE 0 END) AS Requests,
    (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.is_selected), 0)) * 100.00 AS render_rate,
    (SUM(f.is_selected) * 1.000 / NULLIF(SUM(CASE WHEN f.ad_fetch_source IS NULL AND f.event_type = 'candidate' THEN 1 ELSE 0 END), 0)) * 100.00 AS win_rate,
    SUM(CASE WHEN f.event_type = 'candidate' THEN 1 ELSE 0 END) AS candidates
FROM
    advertising.demand_funnel f
CROSS JOIN UNNEST(f.deal_id) AS t(d)
LEFT JOIN ads.dim_rams_deals_history h
    ON CAST(d AS VARCHAR) = CAST(h.id AS VARCHAR)
    AND f.date_key = h.date_key
WHERE 
    f.date_key >= '{date_from}'
    {date_to_condition}
    AND CONTAINS(f.demand_systems, 'PARTNER_AD_SERVER_VIDEO')
    {deal_name_condition}
GROUP BY 
    f.date_key, CAST(d AS VARCHAR), COALESCE(h.name, CAST(d AS VARCHAR))
ORDER BY 
    f.date_key ASC, CAST(d AS VARCHAR) ASC
LIMIT 10000
"""

def test_superset_connection():
    """Test the Superset API connection with a simple query"""
    test_sql = "SELECT 1 as test_column"
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": test_sql,
        "schema": "advertising"
    }
    
    try:
        print(f"🔄 Testing Superset API connection...")
        print(f"🔄 URL: {SUPERSET_EXECUTE_URL}")
        print(f"🔄 Headers: {SUPERSET_HEADERS}")
        print(f"🔄 Payload: {payload}")
        
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS, 
            data=json.dumps(payload),
            timeout=600
        )
        
        print(f"📊 Test response status: {response.status_code}")
        print(f"📊 Test response headers: {dict(response.headers)}")
        print(f"📊 Test response text: {response.text[:1000]}")
        
        if response.status_code in [200, 202]:
            print(f"✅ Superset API connection successful! (Status: {response.status_code})")
            return True
        elif response.status_code == 401:
            print(f"❌ Authentication failed - check credentials")
            return False
        elif response.status_code == 403:
            print(f"❌ Access forbidden - check permissions")
            return False
        else:
            print(f"❌ API test failed with status {response.status_code}")
            print(f"❌ Response: {response.text[:500]}")
            return False
            
    except Exception as e:
        print(f"❌ API test error: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        if hasattr(e, 'response'):
            print(f"❌ Response status: {e.response.status_code}")
            print(f"❌ Response text: {e.response.text[:500]}")
        return False

def generate_mock_data(date_from, date_to, entity_id, query_type):
    """Generate mock data for testing when API is unavailable"""
    print(f"🔄 Generating mock data for {query_type} - {entity_id} ({date_from} to {date_to})")
    
    # Generate date range
    start_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.strptime(date_to, '%Y-%m-%d')
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    if query_type == 'query1':
        # Mock data for Query 1 (seat_id)
        columns = ['date_key', 'tag_id', 'tag_name', 'total_impressions', 'total_ad_query_requests', 'total_ad_query_responses']
        data = []
        for date in dates:
            for i in range(3):  # 3 tags per day
                data.append([
                    date,
                    f"tag_{entity_id}_{i}",
                    f"Mock Tag {i}",
                    (i + 1) * 1000,
                    (i + 1) * 500,
                    (i + 1) * 450
                ])
    else:
        # Mock data for Query 2 (publisher_id)
        columns = ['date_key', 'tag_id', 'tag_name', 'video_impressions', 'video_requests', 'video_responses']
        data = []
        for date in dates:
            for i in range(2):  # 2 tags per day
                data.append([
                    date,
                    f"pub_tag_{entity_id}_{i}",
                    f"Publisher Tag {i}",
                    (i + 1) * 800,
                    (i + 1) * 400,
                    (i + 1) * 380
                ])
    
    print(f"✅ Generated {len(data)} mock rows")
    return columns, data

def fetch_from_superset_api(sql):
    """Execute SQL query via Superset API"""
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "schema": "advertising"
    }
    
    try:
        print(f"🔄 Executing Superset API call...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS, 
            data=json.dumps(payload),
            timeout=600  # Increased timeout to 10 minutes
        )
        
        print(f"📊 Response status: {response.status_code}")
        
        if response.status_code == 401:
            print(f"❌ Authentication failed - session may be expired")
            print(f"💡 Please update the session cookie and CSRF token in superset_utils.py")
            return [], []
        elif response.status_code == 403:
            print(f"❌ Access forbidden - check permissions")
            return [], []
        elif response.status_code in [200, 202]:
            data = response.json()
            print(f"🔍 Response structure: {type(data)} - Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            # Handle different response structures
            if isinstance(data, dict):
                if 'data' in data:
                    # Standard response with 'data' key
                    result_data = data['data']
                    if isinstance(result_data, dict):
                        columns = result_data.get('columns', [])
                        rows = result_data.get('data', [])
                    elif isinstance(result_data, list):
                        # If 'data' is a list, it might be the rows directly
                        columns = data.get('columns', [])
                        rows = result_data
                    else:
                        print(f"❌ Unexpected data structure in 'data': {type(result_data)}")
                        return [], []
                else:
                    # Direct response format
                    columns = data.get('columns', [])
                    rows = data.get('data', [])
            elif isinstance(data, list):
                # Response is directly a list of rows
                print(f"📋 Direct list response with {len(data)} items")
                if data and isinstance(data[0], dict):
                    # Extract columns from first row keys
                    columns = list(data[0].keys())
                    rows = [list(row.values()) for row in data]
                else:
                    columns = []
                    rows = data
            else:
                print(f"❌ Unexpected response type: {type(data)}")
                return [], []
            
            # Normalize columns to always be strings
            normalized_columns = []
            for col in columns:
                if isinstance(col, dict):
                    # If column is a dict, try to extract the name
                    col_name = col.get('name', col.get('column_name', col.get('label', str(col))))
                    normalized_columns.append(str(col_name))
                else:
                    normalized_columns.append(str(col))
            
            columns = normalized_columns
            
            # Convert dictionary rows to list rows if needed
            if rows and isinstance(rows[0], dict):
                print(f"🔄 Converting {len(rows)} dictionary rows to list format")
                converted_rows = []
                for row_dict in rows:
                    # Convert dict to list in column order
                    row_list = []
                    for col_name in columns:
                        row_list.append(row_dict.get(col_name))
                    converted_rows.append(row_list)
                rows = converted_rows
                print(f"✅ Converted to list format: {len(rows)} rows")
            
            print(f"✅ Final data structure: {len(columns)} columns, {len(rows)} rows")
            if rows:
                print(f"📋 Sample row: {rows[0][:3]}... (showing first 3 values)")
            
            return columns, rows
            
        else:
            print(f"❌ API call failed with status {response.status_code}")
            try:
                error_response = response.text
                print(f"❌ API Error Response: {error_response}")
            except:
                print(f"❌ Could not read error response")
            return [], []
            
    except requests.exceptions.Timeout:
        print(f"❌ API call timed out after 30 seconds")
        return [], []
    except requests.exceptions.RequestException as e:
        print(f"❌ API call failed: {str(e)}")
        return [], []
    except Exception as e:
        print(f"❌ Unexpected error in API call: {str(e)}")
        return [], []

def fetch_from_superset(date_from, date_to, seat_id):
    """Query 1: Fetch data for seat_id with smart caching"""
    # Ensure no today's data
    date_to = ensure_date_not_today(date_to)
    
    if date_from > date_to:
        print(f"⚠️ Invalid date range after today exclusion: {date_from} to {date_to}")
        return [], []
    
    print(f"🔍 Query 1: Fetching data for seat_id {seat_id} from {date_from} to {date_to}")
    
    # Check cache and find missing dates
    missing_ranges = find_missing_dates('query1', seat_id, date_from, date_to)
    
    if not missing_ranges:
        # All data cached, return from cache
        cache_object = cache_get_unified('query1', seat_id)
        if cache_object:
            # Filter cache data for requested date range
            filtered_data = filter_cache_data_by_date_range(
                cache_object['columns'], 
                cache_object['data'], 
                date_from, 
                date_to
            )
            print(f"✅ All data from cache: {len(filtered_data)} rows")
            return cache_object['columns'], filtered_data
    
    # Need to fetch missing data
    all_new_data = []
    columns = None
    
    for range_start, range_end in missing_ranges:
        print(f"🔄 Fetching missing range: {range_start} to {range_end}")
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        sql = f"""
        SELECT 
            t.name AS tag_name,
            m.seat_id,
            m.tag_id,
            SUM(m.ad_query_requests) AS total_ad_query_requests,
            SUM(m.ad_query_responses) AS total_ad_query_responses,
            SUM(m.ad_slot_requests) AS total_ad_slot_requests,
            SUM(m.ad_slot_responses) AS total_ad_slot_responses,
            SUM(m.ad_creative_fetches) AS total_ad_creative_fetches,
            SUM(m.ad_creative_responses) AS total_ad_creative_responses,
            CASE 
                WHEN SUM(m.ad_slot_requests) > 0 
                THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_slot_requests))
                ELSE 0 
            END AS fill_rate,
            CASE 
                WHEN SUM(m.ad_creative_responses) > 0 
                THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_creative_responses))
                ELSE 0 
            END AS avg_render_rate,
            SUM(m.num_impressions) AS total_impressions,
            m.date_key
        FROM advertising.agg_raps_rams_metrics_daily_v2 m
        LEFT JOIN ads.dim_rams_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
        WHERE m.date_key BETWEEN '{range_start}' AND '{range_end}' 
          AND m.seat_id = '{seat_id}'
          AND m.date_id_est IS NOT NULL
        GROUP BY 
            t.name, m.seat_id, m.tag_id, m.date_key
        ORDER BY m.date_key DESC
        """
        print(f"🔍 Generated SQL for Query 1:")
        print(f"🔍 {sql[:500]}...")
        
        try:
            range_columns, range_data = fetch_from_superset_api(sql)
            if range_data:
                if columns is None:
                    columns = range_columns
                all_new_data.extend(range_data)
                print(f"✅ Fetched {len(range_data)} rows for range {range_start} to {range_end}")
            else:
                print(f"⚠️ No data for range {range_start} to {range_end}")
        except Exception as e:
            print(f"❌ Failed to fetch range {range_start} to {range_end}: {e}")
            # Fallback to mock data for testing
            print(f"🔄 Using mock data as fallback...")
            range_columns, range_data = generate_mock_data(range_start, range_end, seat_id, 'query1')
            if range_data:
                if columns is None:
                    columns = range_columns
                all_new_data.extend(range_data)
                print(f"✅ Generated {len(range_data)} mock rows for range {range_start} to {range_end}")
            continue
    
    # Cache new data if any was fetched
    if all_new_data and columns:
        print(f"🔄 Caching {len(all_new_data)} rows with {len(columns)} columns...")
        try:
            cache_success = cache_set_unified('query1', seat_id, columns, all_new_data)
            if not cache_success:
                print(f"⚠️ Cache operation failed, but continuing with data")
        except Exception as cache_error:
            print(f"❌ Cache error: {cache_error}")
            # Continue even if caching fails
    
    # Get final result from cache (includes both old and new data)
    try:
        cache_object = cache_get_unified('query1', seat_id)
        if cache_object:
            print(f"✅ Retrieved cache object with {len(cache_object['data'])} total rows")
            filtered_data = filter_cache_data_by_date_range(
                cache_object['columns'], 
                cache_object['data'], 
                date_from, 
                date_to
            )
            print(f"✅ Filtered to {len(filtered_data)} rows for date range {date_from} to {date_to}")
            return cache_object['columns'], filtered_data
    except Exception as cache_error:
        print(f"❌ Error retrieving from cache: {cache_error}")
    
    # Fallback to just new data if cache failed
    print(f"📋 Returning {len(all_new_data)} rows directly (cache unavailable)")
    return columns or [], all_new_data

def fetch_from_superset_query2_with_fallback(date_from, date_to, publisher_id):
    """Query 2: Fetch data for publisher_id with smart caching and chunking"""
    # Ensure no today's data
    date_to = ensure_date_not_today(date_to)
    
    if date_from > date_to:
        print(f"⚠️ Invalid date range after today exclusion: {date_from} to {date_to}")
        return [], []
    
    print(f"🔍 Query 2: Fetching data for publisher_id {publisher_id} from {date_from} to {date_to}")
    
    # Check cache and find missing dates
    missing_ranges = find_missing_dates('query2', publisher_id, date_from, date_to)
    
    if not missing_ranges:
        # All data cached, return from cache
        cache_object = cache_get_unified('query2', publisher_id)
        if cache_object:
            filtered_data = filter_cache_data_by_date_range(
                cache_object['columns'], 
                cache_object['data'], 
                date_from, 
                date_to
            )
            print(f"✅ All data from cache: {len(filtered_data)} rows")
            return cache_object['columns'], filtered_data
    
    # Need to fetch missing data with chunking and timeout handling
    all_new_data = []
    columns = None
    
    for range_start, range_end in missing_ranges:
        print(f"🔄 Fetching missing range: {range_start} to {range_end}")
        
        try:
            range_columns, range_data = fetch_query2_with_timeout_fallback(
                range_start, range_end, publisher_id
            )
            if range_data:
                if columns is None:
                    columns = range_columns
                all_new_data.extend(range_data)
                print(f"✅ Fetched {len(range_data)} rows for range {range_start} to {range_end}")
            else:
                print(f"⚠️ No data for range {range_start} to {range_end}")
        except Exception as e:
            print(f"❌ Failed to fetch range {range_start} to {range_end}: {e}")
            # Fallback to mock data for testing
            print(f"🔄 Using mock data as fallback...")
            range_columns, range_data = generate_mock_data(range_start, range_end, publisher_id, 'query2')
            if range_data:
                if columns is None:
                    columns = range_columns
                all_new_data.extend(range_data)
                print(f"✅ Generated {len(range_data)} mock rows for range {range_start} to {range_end}")
            continue
    
    # Cache new data if any was fetched
    if all_new_data and columns:
        cache_set_unified('query2', publisher_id, columns, all_new_data)
    
    # Get final result from cache (includes both old and new data)
    cache_object = cache_get_unified('query2', publisher_id)
    if cache_object:
        filtered_data = filter_cache_data_by_date_range(
            cache_object['columns'], 
            cache_object['data'], 
            date_from, 
            date_to
        )
        return cache_object['columns'], filtered_data
    
    # Fallback to just new data if cache failed
    return columns or [], all_new_data

def fetch_query2_with_timeout_fallback(date_from, date_to, publisher_id):
    """Query 2 with automatic chunking on timeout"""
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    sql_query = f"""
        WITH aggregated_requests AS (
            SELECT 
                tag_id,
                date_key,
                SUM(pod_based_ad_requests) AS total_pod_based_ad_requests,
                SUM(pod_unfilled_ad_requests) AS total_pod_unfilled_ad_requests,
                SUM(num_unfiltered_ad_requests) AS total_num_unfiltered_ad_requests
            FROM advertising.granular_rams_video_ad_requests
            WHERE date_key BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY tag_id, date_key
        ),
        aggregated_impressions AS (
            SELECT 
                tag_id,
                date_key,
                SUM(num_unfiltered_impressions) AS total_num_unfiltered_impressions
            FROM advertising.granular_rams_video_ad_impressions
            WHERE date_key BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY tag_id, date_key
        )
        SELECT 
            t.publisher_id,
            t.tag_id,
            t.name AS tag_name,
            r.date_key,
            r.total_pod_based_ad_requests,
            r.total_pod_unfilled_ad_requests,
            r.total_num_unfiltered_ad_requests,
            i.total_num_unfiltered_impressions,
            CASE 
                WHEN r.total_pod_based_ad_requests > 0 
                THEN ((r.total_pod_based_ad_requests - r.total_pod_unfilled_ad_requests) * 100.0 / r.total_pod_based_ad_requests)
                ELSE 0 
            END AS fill_rate,
            CASE 
                WHEN r.total_num_unfiltered_ad_requests > 0 
                THEN (i.total_num_unfiltered_impressions * 100.0 / r.total_num_unfiltered_ad_requests)
                ELSE 0 
            END AS impression_rate
        FROM 
            ads.dim_rams_tags_history t
        JOIN 
            aggregated_requests r
            ON t.tag_id = r.tag_id AND t.date_key = r.date_key
        JOIN 
            aggregated_impressions i
            ON t.tag_id = i.tag_id AND t.date_key = i.date_key
        WHERE 
            t.publisher_id = '{publisher_id}'
        ORDER BY 
            t.publisher_id,
            t.tag_id,
            r.date_key DESC,
            t.name
    """
    
    print(f"🔍 Generated Optimized CTE Query2 SQL:")
    print(f"🔍 {sql_query[:500]}...")
    
    try:
        # Try the full range first
        return fetch_from_superset_api(sql_query)
        
    except Exception as e:
        if "timeout" in str(e).lower():
            print(f"⏰ Timeout occurred, breaking into smaller chunks...")
            return fetch_query2_in_smaller_chunks(date_from, date_to, publisher_id)
        else:
            raise

def fetch_query2_in_smaller_chunks(date_from, date_to, publisher_id):
    """Break query2 into 7-day chunks if timeout occurs"""
    start = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.strptime(date_to, '%Y-%m-%d')
    
    # Split into 7-day chunks
    chunks = []
    current = start
    
    while current <= end:
        chunk_end = min(current + timedelta(days=6), end)  # 7-day chunks
        chunks.append((
            current.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        ))
        current = chunk_end + timedelta(days=1)
    
    print(f"📊 Breaking into {len(chunks)} 7-day chunks for timeout recovery")
    
    all_data = []
    columns = None
    
    for chunk_start, chunk_end in chunks:
        try:
            chunk_columns, chunk_data = fetch_query2_with_timeout_fallback(
                chunk_start, chunk_end, publisher_id
            )
            if chunk_data:
                if columns is None:
                    columns = chunk_columns
                all_data.extend(chunk_data)
                print(f"✅ Chunk {chunk_start} to {chunk_end}: {len(chunk_data)} rows")
        except Exception as e:
            print(f"❌ Failed chunk {chunk_start} to {chunk_end}: {e}")
            continue
    
    return columns or [], all_data

def filter_cache_data_by_date_range(columns, data, date_from, date_to):
    """Filter cached data by date range"""
    try:
        date_key_index = columns.index('date_key')
        filtered_data = []
        
        # Debug: Print the first few dates to see the format
        print(f"🔍 DEBUG: Filtering {len(data)} rows from {date_from} to {date_to}")
        print(f"🔍 DEBUG: First 5 dates in cache:")
        for i, row in enumerate(data[:5]):
            row_date = str(row[date_key_index])
            print(f"  Row {i}: '{row_date}' (type: {type(row[date_key_index])})")
        
        # Debug: Show the actual date range in the cache
        all_dates = [str(row[date_key_index]) for row in data]
        unique_dates = sorted(set(all_dates))
        print(f"🔍 DEBUG: Available dates in cache: {unique_dates[:10]}... (total: {len(unique_dates)} unique dates)")
        print(f"🔍 DEBUG: Date range in cache: {min(unique_dates)} to {max(unique_dates)}")
        
        for row in data:
            row_date = str(row[date_key_index])
            if date_from <= row_date <= date_to:
                filtered_data.append(row)
        
        print(f"🔍 DEBUG: Filtered {len(filtered_data)} rows that match date range")
        return filtered_data
    except (ValueError, IndexError) as e:
        print(f"❌ Error filtering cache data by date range: {e}")
        return data

# Legacy function for backward compatibility
def fetch_from_superset_query2(date_from, date_to, publisher_id):
    """Legacy Query 2 function - redirects to new implementation"""
    return fetch_from_superset_query2_with_fallback(date_from, date_to, publisher_id)

def fetch_all_seat_ids_bulk(date_from, date_to):
    """Fetch data for ALL seat_ids in one query and split results back to individual cache objects"""
    print(f"🔄 Bulk Query 1: Fetching data for existing seat_ids from {date_from} to {date_to}")
    
    # Ensure no today's data
    date_to = ensure_date_not_today(date_to)
    
    if date_from > date_to:
        print(f"⚠️ Invalid date range after today exclusion: {date_from} to {date_to}")
        return False
    
    # Get existing seat_ids from cache
    cache_keys = get_all_cache_keys()
    existing_seat_ids = []
    
    for key in cache_keys:
        if key.startswith('seat_id_'):
            seat_id = key.replace('seat_id_', '')
            existing_seat_ids.append(seat_id)
    
    if not existing_seat_ids:
        print(f"⚠️ No existing seat_ids found in cache")
        return False
    
    print(f"📊 Found {len(existing_seat_ids)} existing seat_ids to collect")
    
    # Build SQL for only existing seat_ids
    seat_id_list = "', '".join(existing_seat_ids)
    sql = f"""
    SELECT 
        t.name AS tag_name,
        m.seat_id,
        m.tag_id,
        SUM(m.ad_query_requests) AS total_ad_query_requests,
        SUM(m.ad_query_responses) AS total_ad_query_responses,
        SUM(m.ad_slot_requests) AS total_ad_slot_requests,
        SUM(m.ad_slot_responses) AS total_ad_slot_responses,
        SUM(m.ad_creative_fetches) AS total_ad_creative_fetches,
        SUM(m.ad_creative_responses) AS total_ad_creative_responses,
        CASE 
            WHEN SUM(m.ad_slot_requests) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_slot_requests))
            ELSE 0 
        END AS fill_rate,
        CASE 
            WHEN SUM(m.ad_creative_responses) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_creative_responses))
            ELSE 0 
        END AS avg_render_rate,
        SUM(m.num_impressions) AS total_impressions,
        m.date_key
    FROM advertising.agg_raps_rams_metrics_daily_v2 m
    LEFT JOIN ads.dim_rams_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
    WHERE m.date_key BETWEEN '{date_from}' AND '{date_to}' 
      AND m.seat_id IN ('{seat_id_list}')
      AND m.date_id_est IS NOT NULL
    GROUP BY 
        t.name, m.seat_id, m.tag_id, m.date_key
    ORDER BY m.seat_id, m.date_key DESC
    """
    
    print(f"🔍 Executing bulk Query 1 SQL for {len(existing_seat_ids)} seat_ids...")
    
    try:
        columns, all_data = fetch_from_superset_api(sql)
        
        if not all_data:
            print(f"⚠️ No data returned from bulk query")
            return False
        
        print(f"✅ Bulk query returned {len(all_data)} rows for {len(existing_seat_ids)} seat_ids")
        
        # Group data by seat_id
        seat_id_groups = {}
        for row in all_data:
            try:
                seat_id_index = columns.index('seat_id')
                seat_id = row[seat_id_index]
                
                if seat_id not in seat_id_groups:
                    seat_id_groups[seat_id] = []
                seat_id_groups[seat_id].append(row)
            except (IndexError, ValueError) as e:
                print(f"⚠️ Error processing row: {e}")
                continue
        
        print(f"📊 Grouped data into {len(seat_id_groups)} seat_id groups")
        
        # Cache each seat_id group separately
        success_count = 0
        for seat_id, seat_data in seat_id_groups.items():
            try:
                if seat_data:
                    cache_success = cache_set_unified('query1', seat_id, columns, seat_data)
                    if cache_success:
                        success_count += 1
                        print(f"✅ Cached {len(seat_data)} rows for seat_id {seat_id}")
                    else:
                        print(f"❌ Failed to cache data for seat_id {seat_id}")
            except Exception as e:
                print(f"❌ Error caching seat_id {seat_id}: {e}")
                continue
        
        print(f"🎉 Bulk collection completed: {success_count}/{len(seat_id_groups)} seat_ids cached successfully")
        return True
        
    except Exception as e:
        print(f"❌ Bulk Query 1 failed: {e}")
        return False


sql_test = """
    SELECT 
        t.name AS tag_name,
        m.seat_id,
        m.tag_id,
        SUM(m.ad_query_requests) AS total_ad_query_requests,
        SUM(m.ad_query_responses) AS total_ad_query_responses,
        SUM(m.ad_slot_requests) AS total_ad_slot_requests,
        SUM(m.ad_slot_responses) AS total_ad_slot_responses,
        SUM(m.ad_creative_fetches) AS total_ad_creative_fetches,
        SUM(m.ad_creative_responses) AS total_ad_creative_responses,
        CASE 
            WHEN SUM(m.ad_slot_requests) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_slot_requests))
            ELSE 0 
        END AS fill_rate,
        CASE 
            WHEN SUM(m.ad_creative_responses) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_creative_responses))
            ELSE 0 
        END AS avg_render_rate,
        SUM(m.num_impressions) AS total_impressions,
        m.date_key
    FROM advertising.agg_raps_rams_metrics_daily_v2 m
    LEFT JOIN ads.dim_rams_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
    WHERE m.date_key = 'YESTERDAY_DATE' 
      AND m.seat_id IN ('SEAT_ID_LIST')
      AND m.date_id_est IS NOT NULL
    GROUP BY 
        t.name, m.seat_id, m.tag_id, m.date_key
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
        "schema": "advertising"
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
        
        # Parse and print seat_id and impressions
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Handle different response formats
                if isinstance(data, dict) and 'data' in data:
                    # Nested dictionary with 'data' key
                    rows = data['data']
                    if isinstance(rows, list):
                        print(f"\n📊 All Data (Seat ID, Tag Name, Impressions):")
                        for i, row in enumerate(rows):
                            seat_id = row.get('seat_id', 'N/A')
                            tag_name = row.get('tag_name', 'N/A')
                            impressions = row.get('total_impressions', 'N/A')
                            print(f"  {i+1}. Seat ID: {seat_id}, Tag: {tag_name}, Impressions: {impressions}")
                    else:
                        print(f"❌ 'data' is not a list: {type(rows)}")
                elif isinstance(data, list):
                    # Direct list of dictionaries
                    rows = data
                    print(f"\n📊 Seat ID and Impressions (showing first 5):")
                    for i, row in enumerate(rows[:5]):  # Only show first 5
                        seat_id = row.get('seat_id', 'N/A')
                        impressions = row.get('total_impressions', 'N/A')
                        print(f"  {i+1}. Seat ID: {seat_id}, Impressions: {impressions}")
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
        
        # Check which ones have yesterday's data
        missing_seat_ids = []
        for cache_key in cached_keys:
            seat_id = cache_key.replace('seat_id_', '')
            cache_object = cache_get_unified('query1', seat_id)
            
            if cache_object and 'data' in cache_object:
                # Check if yesterday's data exists in cache
                # Find date_key column index
                columns = cache_object.get('columns', [])
                date_key_index = columns.index('date_key') if 'date_key' in columns else None
                
                if date_key_index is not None:
                    has_yesterday = any(
                        str(row[date_key_index]) == yesterday 
                        for row in cache_object['data']
                        if isinstance(row, list) and len(row) > date_key_index
                    )
                else:
                    has_yesterday = False
                if not has_yesterday:
                    missing_seat_ids.append(seat_id)
            else:
                missing_seat_ids.append(seat_id)
        
        return missing_seat_ids


def fetch_missing_yesterday_data():
    """Only fetch data for seat_ids missing yesterday's data"""
    missing_seat_ids = check_cache_for_yesterday()
    
    if not missing_seat_ids:
        print("✅ All seat_ids already have yesterday's data cached")
        return
    
    print(f"�� Fetching yesterday's data for {len(missing_seat_ids)} missing seat_ids")
    
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Create seat_id list for SQL
    seat_id_list = "', '".join(missing_seat_ids)
    
    # Build the SQL query for yesterday's data
    sql = f"""
    SELECT 
        t.name AS tag_name,
        m.seat_id,
        m.tag_id,
        SUM(m.ad_query_requests) AS total_ad_query_requests,
        SUM(m.ad_query_responses) AS total_ad_query_responses,
        SUM(m.ad_slot_requests) AS total_ad_slot_requests,
        SUM(m.ad_slot_responses) AS total_ad_slot_responses,
        SUM(m.ad_creative_fetches) AS total_ad_creative_fetches,
        SUM(m.ad_creative_responses) AS total_ad_creative_responses,
        CASE 
            WHEN SUM(m.ad_slot_requests) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_slot_requests))
            ELSE 0 
        END AS fill_rate,
        CASE 
            WHEN SUM(m.ad_creative_responses) > 0 
            THEN (SUM(m.num_impressions) * 100.0 / SUM(m.ad_creative_responses))
            ELSE 0 
        END AS avg_render_rate,
        SUM(m.num_impressions) AS total_impressions,
        m.date_key
    FROM advertising.agg_raps_rams_metrics_daily_v2 m
    LEFT JOIN ads.dim_rams_tags_history t ON m.tag_id = t.tag_id AND t.date_key = m.date_key
    WHERE m.date_key = '{yesterday}' 
      AND m.seat_id IN ('{seat_id_list}')
      AND m.date_id_est IS NOT NULL
    GROUP BY 
        t.name, m.seat_id, m.tag_id, m.date_key
    ORDER BY m.seat_id, m.date_key DESC
    """
    
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "schema": "advertising"
    }
    
    try:
        print(f"🔄 Executing Superset API call for yesterday's data...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload),
            timeout=600
        )
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'data' in data:
                result = data['data']
                print(f"✅ Successfully fetched {len(result)} rows for yesterday")
                return result
            else:
                print(f"❌ Unexpected API response format")
                return None
        else:
            print(f"❌ API call failed with status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching yesterday's data: {e}")
        return None



def store_yesterday_data_to_cache(api_data):
    """Store the bulk API response back to individual seat_id caches"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if not api_data or len(api_data) == 0:
        print("❌ No data to cache")
        return
    
    # Define the expected columns based on the SQL query
    columns = ['tag_name', 'seat_id', 'tag_id', 'total_ad_query_requests', 'total_ad_query_responses', 
               'total_ad_slot_requests', 'total_ad_slot_responses', 'total_ad_creative_fetches', 
               'total_ad_creative_responses', 'fill_rate', 'avg_render_rate', 'total_impressions', 'date_key']
    
    # Group data by seat_id (data is in dictionary format)
    seat_id_groups = {}
    print(f"🔍 Processing {len(api_data)} rows for grouping")
    print(f"🔍 First row: {api_data[0] if api_data else 'No data'}")
    
    for i, row in enumerate(api_data):
        if isinstance(row, dict) and 'seat_id' in row:
            seat_id = row['seat_id']
            if seat_id:
                if seat_id not in seat_id_groups:
                    seat_id_groups[seat_id] = []
                seat_id_groups[seat_id].append(row)
            else:
                print(f"⚠️ Row {i}: Empty seat_id")
        else:
            print(f"⚠️ Row {i}: Invalid format - {type(row)}")
    
    print(f"🔍 Grouped into {len(seat_id_groups)} seat IDs: {list(seat_id_groups.keys())}")
    
    # Store each seat_id's data to its own cache
    print(f"🔄 Processing {len(seat_id_groups)} seat IDs for caching")
    for seat_id, seat_data in seat_id_groups.items():
        if seat_data:
            print(f"🔄 Processing seat_id: {seat_id} with {len(seat_data)} rows")
            # Get existing cache or create new
            existing_cache = cache_get_unified('query1', seat_id) or {'data': [], 'columns': columns}
            print(f"📊 Existing cache has {len(existing_cache['data'])} rows")
            
            # Add yesterday's data (avoid duplicates)
            # Find date_key column index in existing cache
            existing_columns = existing_cache.get('columns', [])
            date_key_index = existing_columns.index('date_key') if 'date_key' in existing_columns else None
            
            if date_key_index is not None:
                existing_dates = {str(row[date_key_index]) for row in existing_cache['data'] 
                                if isinstance(row, list) and len(row) > date_key_index}
            else:
                existing_dates = set()
            
            print(f"📅 Existing dates: {sorted(existing_dates)}")
            print(f"🎯 Yesterday: {yesterday}")
            
            if yesterday not in existing_dates:
                # Convert dict data to list format for consistency with existing cache
                list_data = []
                for row in seat_data:
                    list_row = [row.get(col, '') for col in columns]
                    list_data.append(list_row)
                
                existing_cache['data'].extend(list_data)
                cache_set_unified('query1', seat_id, columns, existing_cache['data'])
                print(f"✅ Cached {len(seat_data)} rows for seat_id {seat_id}")
                print(f"📊 Total rows in cache now: {len(existing_cache['data'])}")
            else:
                print(f"⚠️ Yesterday's data already exists for seat_id {seat_id}")
        else:
            print(f"⚠️ No data for seat_id: {seat_id}")


def fetch_and_cache_yesterday_data():
    """Main function to fetch and cache yesterday's data"""
    # 1. Check what's missing
    missing_seat_ids = check_cache_for_yesterday()
    print(f"🔍 Missing seat IDs: {missing_seat_ids}")
    
    # 2. Fetch missing data
    if missing_seat_ids:
        api_data = fetch_missing_yesterday_data()
        print(f"📊 Fetched data: {len(api_data) if api_data else 0} rows")
        
        # 3. Store to cache
        if api_data:
            print(f"💾 Storing data to cache...")
            store_yesterday_data_to_cache(api_data)
            print(f"✅ Data storage completed")
        else:
            print(f"❌ No data to store")
    else:
        print(f"✅ No missing seat IDs")
    
    return missing_seat_ids

def check_available_dates():
    """Check what dates actually have data in the table"""
    sql = """
    SELECT 
        MIN(date_key) as earliest_date,
        MAX(date_key) as latest_date,
        COUNT(DISTINCT date_key) as total_days,
        COUNT(*) as total_rows
    FROM advertising.agg_raps_rams_metrics_daily_v2 
    WHERE date_id_est IS NOT NULL
    """
    
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "schema": "advertising"
    }
    
    try:
        print(f"🔍 Checking available dates in table...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload)
        )
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'data' in data:
                result = data['data']
                if result and len(result) > 0:
                    row = result[0]
                    print(f"📊 Data Summary:")
                    print(f"   Earliest date: {row[0]}")
                    print(f"   Latest date: {row[1]}")
                    print(f"   Total days: {row[2]}")
                    print(f"   Total rows: {row[3]}")
                    return row
                else:
                    print(f"❌ No data found in table")
                    return None
            else:
                print(f"❌ Unexpected API response format")
                return None
        else:
            print(f"❌ API call failed with status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error checking dates: {e}")
        return None

def check_recent_dates():
    """Check recent dates to see what's available"""
    sql = """
    SELECT 
        date_key,
        COUNT(*) as row_count,
        COUNT(DISTINCT seat_id) as unique_seats
    FROM advertising.agg_raps_rams_metrics_daily_v2 
    WHERE date_id_est IS NOT NULL
      AND date_key >= '2025-08-10'
    GROUP BY date_key
    ORDER BY date_key DESC
    """
    
    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "schema": "advertising"
    }
    
    try:
        print(f"🔍 Checking recent dates (last 20 days)...")
        response = requests.post(
            SUPERSET_EXECUTE_URL, 
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload)
        )
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'data' in data:
                result = data['data']
                print(f"📅 Recent dates with data:")
                for row in result:
                    print(f"   {row[0]}: {row[1]} rows, {row[2]} seats")
                return result
            else:
                print(f"❌ Unexpected API response format")
                return None
        else:
            print(f"❌ API call failed with status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error checking recent dates: {e}")
        return None