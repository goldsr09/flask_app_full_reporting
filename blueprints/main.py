# blueprints/main.py
# Main routes for Query 1, Query 2, trends, and search

from flask import Blueprint, render_template, request
import sqlite3
import json
import traceback
from datetime import datetime
from utils.superset_utils import fetch_from_superset, fetch_from_superset_query2_with_fallback
from utils.cache_utils import cache_get_unified, search_tags_in_cache
from utils.analysis_utils import analyze_cache_trends, generate_impression_alerts
from config import DB_PATH

main_bp = Blueprint('main', __name__)

@main_bp.route('/', methods=['GET', 'POST'])
@main_bp.route('/query1', methods=['GET', 'POST'])
def query1():
    columns = []
    data = []
    date_from = date_to = seat_id = tag_search = None
    cache_hit = False
    search_method = None

    if request.method == 'POST':
        date_from = request.form.get('date_from')
        date_to = request.form.get('date_to')
        seat_id = request.form.get('seat_id')
        tag_search = request.form.get('tag_search')
        
        print(f"Query1 POST request - Date from: {date_from}, Date to: {date_to}, Seat ID: {seat_id}, Tag search: {tag_search}")

        # Smart search logic: Check what the user provided
        if tag_search and tag_search.strip():
            # User wants to search by tag name/string
            print(f"Query1 Searching by tag name: '{tag_search}' within date range {date_from} to {date_to}")
            
            found_in_cache = False
            
            # If seat_id provided, search within that specific cache
            if seat_id and seat_id.strip():
                columns, data = search_tags_in_cache('query1', seat_id, tag_search, date_from, date_to)
                if data:
                    cache_hit = True
                    found_in_cache = True
                    search_method = f"Found in cache (Seat ID: {seat_id}) - Tag search returned {len(data)} records"
                    print(f"Query1 Found {len(data)} matching rows in seat_id {seat_id} cache")
            
            if not found_in_cache:
                # Search across ALL Query 1 cache objects for this tag
                with sqlite3.connect(DB_PATH) as conn:
                    c = conn.cursor()
                    c.execute("SELECT cache_key FROM query_cache WHERE cache_key LIKE 'seat_id_%'")
                    seat_cache_keys = [row[0] for row in c.fetchall()]
                
                all_matching_rows = []
                found_seat_id = None
                master_columns = None
                search_term = tag_search.strip().lower()
                
                for cache_key in seat_cache_keys:
                    current_seat_id = cache_key.replace('seat_id_', '')
                    search_columns, search_data = search_tags_in_cache('query1', current_seat_id, tag_search, date_from, date_to)
                    
                    if search_data:
                        print(f"Query1 Found {len(search_data)} matching rows in cache {cache_key}")
                        
                        if master_columns is None:
                            master_columns = search_columns
                            found_seat_id = current_seat_id
                        
                        if search_columns == master_columns:
                            all_matching_rows.extend(search_data)
                
                if all_matching_rows:
                    columns = master_columns
                    data = all_matching_rows
                    seat_id = found_seat_id
                    cache_hit = True
                    found_in_cache = True
                    search_method = f"Found in cache (Seat ID: {found_seat_id}) - Tag search across all caches returned {len(data)} records"
                    print(f"Query1 SUCCESS: Found {len(data)} total matching rows across caches")
            
            if not found_in_cache:
                # Not found in cache - need seat_id to run new query
                if seat_id and seat_id.strip():
                    print(f"Query1 Tag not found in cache, running new query with seat_id: {seat_id}")
                    try:
                        # Fetch data using optimized function
                        all_columns, all_data = fetch_from_superset(date_from, date_to, seat_id)
                        
                        if all_data:
                            # Filter results by tag search
                            tag_name_index = all_columns.index('tag_name') if 'tag_name' in all_columns else None
                            tag_id_index = all_columns.index('tag_id') if 'tag_id' in all_columns else None
                            
                            if tag_name_index is not None or tag_id_index is not None:
                                filtered_data = []
                                search_term = tag_search.strip().lower()
                                
                                for row in all_data:
                                    tag_name = str(row[tag_name_index] or '').lower() if tag_name_index is not None else ''
                                    tag_id_str = str(row[tag_id_index] or '').lower() if tag_id_index is not None else ''
                                    
                                    if search_term in tag_name or search_term in tag_id_str:
                                        filtered_data.append(row)
                                
                                columns = all_columns
                                data = filtered_data
                                search_method = f"New query + filtered (Seat ID: {seat_id}) - {len(data)} matching records"
                                print(f"Query1 Filtered to {len(data)} rows matching '{tag_search}'")
                            else:
                                columns = all_columns
                                data = all_data
                                search_method = f"New query (Seat ID: {seat_id}) - {len(data)} records"
                        else:
                            search_method = "❌ No data returned from query"
                    except Exception as e:
                        search_method = f"❌ Error: {str(e)}"
                        print(f"Query1 Error during tag search: {e}")
                        print(f"Query1 Error traceback: {traceback.format_exc()}")
                else:
                    search_method = "❌ Tag not found in cache. Please provide Seat ID to run new query."
                    print("Query1 Tag not found in cache and no seat_id provided")
        
        elif seat_id and seat_id.strip():
            # User wants to search by seat_id (traditional method)
            print(f"Query1 Searching by seat_id: {seat_id}")
            
            try:
                print(f"🔄 Calling fetch_from_superset with parameters: {date_from}, {date_to}, {seat_id}")
                columns, data = fetch_from_superset(date_from, date_to, seat_id)
                print(f"✅ fetch_from_superset returned: {len(columns)} columns, {len(data)} rows")
                
                if data:
                    # Check if data came from cache by looking for cache hit indicators
                    cache_object = cache_get_unified('query1', seat_id)
                    cache_hit = cache_object is not None and len(cache_object.get('data', [])) > 0
                    
                    search_method = f"{'Loaded from cache' if cache_hit else 'Fresh query'} (Seat ID: {seat_id}) - {len(data)} records"
                    print(f"Query1 {'Cache hit' if cache_hit else 'Fresh query'}: {len(data)} rows")
                else:
                    search_method = "❌ No data found for this Seat ID and date range"
                    print(f"Query1 No data returned for seat_id {seat_id}")
                    
            except Exception as e:
                search_method = f"❌ Error: {str(e)}"
                print(f"Query1 Error during seat_id search: {e}")
                print(f"Query1 Error traceback: {traceback.format_exc()}")
        else:
            search_method = "❌ Please provide either a tag name to search or a Seat ID"
            print("Query1 No search criteria provided")
    
    else:
        # Set default date_from for GET requests
        date_from = '2025-07-01'
        print(f"Query1 GET request - setting default date_from: {date_from}")

    print(f"Query1 Rendering template with {len(data)} rows")
    return render_template('query1.html', 
                         columns=columns, 
                         data=data, 
                         date_from=date_from, 
                         date_to=date_to, 
                         seat_id=seat_id, 
                         tag_search=tag_search, 
                         cache_hit=cache_hit,
                         search_method=search_method)

@main_bp.route('/query2', methods=['GET', 'POST'])
def query2():
    columns = []
    data = []
    date_from = date_to = publisher_id = tag_search = None
    cache_hit = False
    error_message = None
    search_method = None

    if request.method == 'POST':
        date_from = request.form.get('date_from')
        date_to = request.form.get('date_to')
        publisher_id = request.form.get('publisher_id')
        tag_search = request.form.get('tag_search')
        
        print(f"Query2 POST request - Date from: {date_from}, Date to: {date_to}, Publisher ID: {publisher_id}, Tag search: {tag_search}")

        # Smart search logic: Check what the user provided
        if tag_search and tag_search.strip():
            # User wants to search by tag name/string
            print(f"Query2 Searching by tag name: '{tag_search}' within date range {date_from} to {date_to}")
            
            found_in_cache = False
            
            # If publisher_id provided, search within that specific cache
            if publisher_id and publisher_id.strip():
                columns, data = search_tags_in_cache('query2', publisher_id, tag_search, date_from, date_to)
                if data:
                    cache_hit = True
                    found_in_cache = True
                    search_method = f"Found in cache (Publisher ID: {publisher_id}) - Tag search returned {len(data)} records"
                    print(f"Query2 Found {len(data)} matching rows in publisher_id {publisher_id} cache")
            
            if not found_in_cache:
                # Search across ALL Query 2 cache objects for this tag
                with sqlite3.connect(DB_PATH) as conn:
                    c = conn.cursor()
                    c.execute("SELECT cache_key FROM query_cache WHERE cache_key LIKE 'publisher_id_%'")
                    publisher_cache_keys = [row[0] for row in c.fetchall()]
                
                all_matching_rows = []
                found_publisher_id = None
                master_columns = None
                
                for cache_key in publisher_cache_keys:
                    current_publisher_id = cache_key.replace('publisher_id_', '')
                    search_columns, search_data = search_tags_in_cache('query2', current_publisher_id, tag_search, date_from, date_to)
                    
                    if search_data:
                        print(f"Query2 Found {len(search_data)} matching rows in cache {cache_key}")
                        
                        if master_columns is None:
                            master_columns = search_columns
                            found_publisher_id = current_publisher_id
                        
                        if search_columns == master_columns:
                            all_matching_rows.extend(search_data)
                
                if all_matching_rows:
                    columns = master_columns
                    data = all_matching_rows
                    publisher_id = found_publisher_id
                    cache_hit = True
                    found_in_cache = True
                    search_method = f"Found in cache (Publisher ID: {found_publisher_id}) - Tag search across all caches returned {len(data)} records"
                    print(f"Query2 SUCCESS: Found {len(data)} total matching rows across caches")
            
            if not found_in_cache:
                # Not found in cache - need publisher_id to run new query
                if publisher_id and publisher_id.strip():
                    print(f"Query2 Tag not found in cache, running new query with publisher_id: {publisher_id}")
                    try:
                        # Fetch data using optimized function with chunking
                        all_columns, all_data = fetch_from_superset_query2_with_fallback(date_from, date_to, publisher_id)
                        
                        if all_data:
                            # Filter results by tag search
                            tag_name_index = all_columns.index('tag_name') if 'tag_name' in all_columns else None
                            tag_id_index = all_columns.index('tag_id') if 'tag_id' in all_columns else None
                            
                            if tag_name_index is not None or tag_id_index is not None:
                                filtered_data = []
                                search_term = tag_search.strip().lower()
                                
                                for row in all_data:
                                    tag_name = str(row[tag_name_index] or '').lower() if tag_name_index is not None else ''
                                    tag_id_str = str(row[tag_id_index] or '').lower() if tag_id_index is not None else ''
                                    
                                    if search_term in tag_name or search_term in tag_id_str:
                                        filtered_data.append(row)
                                
                                columns = all_columns
                                data = filtered_data
                                search_method = f"New query + filtered (Publisher ID: {publisher_id}) - {len(data)} matching records"
                                print(f"Query2 Filtered to {len(data)} rows matching '{tag_search}'")
                            else:
                                columns = all_columns
                                data = all_data
                                search_method = f"New query (Publisher ID: {publisher_id}) - {len(data)} records"
                        else:
                            search_method = "❌ No data returned from query"
                    except Exception as e:
                        error_message = f"Error fetching data: {str(e)}"
                        search_method = f"❌ Error: {str(e)}"
                        print(f"Query2 Error: {e}")
                        print(f"Query2 Error traceback: {traceback.format_exc()}")
                else:
                    search_method = "❌ Tag not found in cache. Please provide Publisher ID to run new query."
                    print("Query2 Tag not found in cache and no publisher_id provided")
        
        elif publisher_id and publisher_id.strip():
            # User wants to search by publisher_id (traditional method)
            print(f"Query2 Searching by publisher_id: {publisher_id}")
            
            try:
                columns, data = fetch_from_superset_query2_with_fallback(date_from, date_to, publisher_id)
                
                if data:
                    # Check if data came from cache
                    cache_object = cache_get_unified('query2', publisher_id)
                    cache_hit = cache_object is not None and len(cache_object.get('data', [])) > 0
                    
                    search_method = f"{'Loaded from cache' if cache_hit else 'Fresh query'} (Publisher ID: {publisher_id}) - {len(data)} records"
                    print(f"Query2 {'Cache hit' if cache_hit else 'Fresh query'}: {len(data)} rows")
                else:
                    search_method = "❌ No data found for this Publisher ID and date range"
                    print(f"Query2 No data returned for publisher_id {publisher_id}")
                    
            except Exception as e:
                error_message = f"Error fetching data: {str(e)}"
                search_method = f"❌ Error: {str(e)}"
                print(f"Query2 Error: {e}")
                print(f"Query2 Error traceback: {traceback.format_exc()}")
        else:
            search_method = "❌ Please provide either a tag name to search or a Publisher ID"
            print("Query2 No search criteria provided")
    
    else:
        # Set default date_from for GET requests
        date_from = '2025-07-01'
        print(f"Query2 GET request - setting default date_from: {date_from}")

    print(f"Query2 Rendering template with {len(data)} rows")
    return render_template('query2.html', 
                         columns=columns, 
                         data=data, 
                         date_from=date_from, 
                         date_to=date_to, 
                         publisher_id=publisher_id, 
                         tag_search=tag_search, 
                         cache_hit=cache_hit,
                         error_message=error_message,
                         search_method=search_method)

@main_bp.route('/trends')
def trends_dashboard():
    """Trend Analysis Dashboard"""
    # Get recent data from cache for analysis
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT cache_key, result FROM query_cache WHERE cache_key LIKE 'seat_id_%' OR cache_key LIKE 'publisher_id_%' ORDER BY updated_at DESC")
        cache_entries = c.fetchall()
    
    all_alerts = []
    all_trends = {}
    
    for cache_key, result_json in cache_entries:
        try:
            cache_object = json.loads(result_json)
            if 'columns' in cache_object and 'data' in cache_object:
                # Generate comprehensive alerts including week-over-week and gap-tolerant comparisons
                from utils.analysis_utils import generate_comprehensive_alerts
                alerts = generate_comprehensive_alerts(cache_object['data'], cache_object['columns'])
                all_alerts.extend(alerts)
                
                # Analyze trends
                trends = analyze_cache_trends(cache_object['data'], cache_object['columns'])
                all_trends.update(trends)
        except Exception as e:
            print(f"Error analyzing cache entry {cache_key}: {e}")
            continue
    
    # Sort alerts by severity and date
    all_alerts.sort(key=lambda x: (x.get('severity') == 'high', x.get('date', '')), reverse=True)
    
    # Get today's date for template
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    return render_template('trends.html', 
                         alerts=all_alerts, 
                         trends=all_trends,
                         today_date=today_date)

@main_bp.route('/search')
def search_page():
    """Dedicated search page for tags"""
    return render_template('search.html')

@main_bp.route('/forecast')
def forecast_dashboard():
    """Forecast tracking dashboard"""
    from utils.forecast_tracking import get_all_publishers_delivery_status, get_delivery_summary, get_cached_publishers
    
    # Get delivery status for all publishers
    delivery_status = get_all_publishers_delivery_status()
    
    # Get overall summary
    summary = get_delivery_summary()
    
    # Get cached publishers for reference
    cached_publishers = get_cached_publishers()
    
    return render_template('forecast.html', 
                         delivery_status=delivery_status,
                         summary=summary,
                         cached_publishers=cached_publishers)

@main_bp.route('/forecast/debug')
def forecast_debug():
    """Debug route to show tag mapping analysis"""
    from utils.forecast_tracking import get_all_publishers_mapping_analysis, get_cached_publishers
    
    # Get mapping analysis for all publishers
    mapping_analysis = get_all_publishers_mapping_analysis()
    
    # Get cached publishers
    cached_publishers = get_cached_publishers()
    return render_template('forecast_debug.html', 
                         mapping_analysis=mapping_analysis,
                         cached_publishers=cached_publishers)


@main_bp.route('/test')
def test():
    from utils.superset_utils import fetch_from_superset_api_test
    
    result = []
    result.append("🔍 INVESTIGATING DATA AVAILABILITY")
    result.append("=" * 50)
    
    # Run actual data collection for yesterday
    result.append("🔄 Running yesterday's data collection...")
    try:
        from utils.superset_utils import fetch_and_cache_yesterday_data
        missing_seat_ids = fetch_and_cache_yesterday_data()
        if missing_seat_ids is not None:
            result.append("✅ Data collection completed successfully")
            if len(missing_seat_ids) == 0:
                result.append("📊 All data was already cached")
            else:
                result.append(f"📊 Processed {len(missing_seat_ids)} missing seat IDs")
        else:
            result.append("❌ Data collection failed")
    except Exception as e:
        result.append(f"❌ Data collection error: {str(e)}")
    
    result.append("\n" + "=" * 50)
    

    

    
    result.append("\n" + "=" * 50)
    result.append("🔍 END INVESTIGATION")
    
    return "<br>".join(result)