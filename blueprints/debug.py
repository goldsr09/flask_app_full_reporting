# blueprints/debug.py
# Debug endpoints for troubleshooting and development

from flask import Blueprint, request, jsonify
import sqlite3
import json
from datetime import datetime
from utils.cache_utils import get_cache_stats
from utils.admin_utils import diagnose_cache_health
from config import DB_PATH

debug_bp = Blueprint('debug', __name__)

@debug_bp.route('/cache-status')
def cache_status():
    """Get detailed cache status information"""
    try:
        cache_info = []
        
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT cache_key, result, created_at, updated_at FROM query_cache ORDER BY updated_at DESC')
            cache_entries = c.fetchall()
        
        query1_count = 0
        query2_count = 0
        total_rows = 0
        
        for cache_key, result_json, created_at, updated_at in cache_entries:
            try:
                cache_object = json.loads(result_json)
                columns = cache_object.get('columns', [])
                data = cache_object.get('data', [])
                
                # Determine query type
                query_type = 'Unknown'
                if cache_key.startswith('seat_id_'):
                    query_type = 'Query 1'
                    query1_count += 1
                elif cache_key.startswith('publisher_id_'):
                    query_type = 'Query 2'
                    query2_count += 1
                
                total_rows += len(data)
                
                # Get sample data and date range
                sample_row = data[0] if data else []
                date_range = {'min': None, 'max': None}
                
                if 'date_key' in columns and data:
                    try:
                        date_index = columns.index('date_key')
                        dates = [str(row[date_index]) for row in data]
                        date_range = {'min': min(dates), 'max': max(dates)}
                    except:
                        pass
                
                cache_info.append({
                    'cache_key': cache_key,
                    'query_type': query_type,
                    'columns': columns,
                    'row_count': len(data),
                    'sample_row': sample_row[:3] if sample_row else [],
                    'date_range': date_range,
                    'created_at': created_at,
                    'updated_at': updated_at
                })
            except Exception as e:
                cache_info.append({
                    'cache_key': cache_key,
                    'error': f'Failed to parse: {str(e)}',
                    'created_at': created_at,
                    'updated_at': updated_at
                })
        
        return jsonify({
            'status': 'success',
            'cache_entries': cache_info,
            'summary': {
                'total_cache_entries': len(cache_entries),
                'query1_entries': query1_count,
                'query2_entries': query2_count,
                'total_rows_cached': total_rows
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting cache status: {str(e)}"
        }), 500

@debug_bp.route('/cache/<cache_key>')
def get_cache_entry(cache_key):
    """Get specific cache entry details"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT result, created_at, updated_at FROM query_cache WHERE cache_key = ?', (cache_key,))
            row = c.fetchone()
            
            if not row:
                return jsonify({
                    'status': 'error',
                    'message': f'Cache entry {cache_key} not found'
                }), 404
            
            result_json, created_at, updated_at = row
            cache_object = json.loads(result_json)
            
            # Add metadata
            cache_details = {
                'cache_key': cache_key,
                'created_at': created_at,
                'updated_at': updated_at,
                'columns': cache_object.get('columns', []),
                'data_sample': cache_object.get('data', [])[:5],  # First 5 rows only
                'total_rows': len(cache_object.get('data', [])),
                'cache_size_kb': round(len(result_json) / 1024, 2)
            }
            
            return jsonify({
                'status': 'success',
                'cache_entry': cache_details
            })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting cache entry: {str(e)}"
        }), 500

@debug_bp.route('/database/info')
def database_info():
    """Get database information and statistics"""
    try:
        import os
        
        # Get database file info
        db_info = {
            'path': DB_PATH,
            'exists': os.path.exists(DB_PATH),
            'size_bytes': 0,
            'size_mb': 0
        }
        
        if db_info['exists']:
            db_info['size_bytes'] = os.path.getsize(DB_PATH)
            db_info['size_mb'] = round(db_info['size_bytes'] / (1024 * 1024), 2)
        
        # Get table info
        table_info = []
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            
            # Get table list
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = c.fetchall()
            
            for (table_name,) in tables:
                c.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = c.fetchone()[0]
                
                # Get table schema
                c.execute(f"PRAGMA table_info({table_name})")
                schema = c.fetchall()
                
                table_info.append({
                    'name': table_name,
                    'row_count': row_count,
                    'columns': [{'name': col[1], 'type': col[2], 'nullable': not col[3]} for col in schema]
                })
        
        return jsonify({
            'status': 'success',
            'database': db_info,
            'tables': table_info
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting database info: {str(e)}"
        }), 500

@debug_bp.route('/query-test', methods=['POST'])
def query_test():
    """Test query execution (for debugging)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'Request body required'
            }), 400
        
        query_type = data.get('query_type')  # 'query1' or 'query2'
        entity_id = data.get('entity_id')    # seat_id or publisher_id
        date_from = data.get('date_from')
        date_to = data.get('date_to')
        
        if not all([query_type, entity_id, date_from, date_to]):
            return jsonify({
                'status': 'error',
                'message': 'query_type, entity_id, date_from, and date_to required'
            }), 400
        
        if query_type not in ['query1', 'query2']:
            return jsonify({
                'status': 'error',
                'message': 'query_type must be query1 or query2'
            }), 400
        
        # Import query functions
        if query_type == 'query1':
            from utils.superset_utils import fetch_from_superset
            columns, data = fetch_from_superset(date_from, date_to, entity_id)
        else:
            from utils.superset_utils import fetch_from_superset_query2_with_fallback
            columns, data = fetch_from_superset_query2_with_fallback(date_from, date_to, entity_id)
        
        return jsonify({
            'status': 'success',
            'query_info': {
                'query_type': query_type,
                'entity_id': entity_id,
                'date_from': date_from,
                'date_to': date_to
            },
            'results': {
                'columns': columns,
                'row_count': len(data),
                'sample_data': data[:3] if data else []  # First 3 rows
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Query test failed: {str(e)}"
        }), 500

@debug_bp.route('/cache/validate')
def validate_cache():
    """Validate cache integrity and consistency"""
    try:
        validation_results = {
            'total_objects': 0,
            'valid_objects': 0,
            'invalid_objects': 0,
            'duplicate_records': 0,
            'missing_columns': 0,
            'issues': []
        }
        
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('SELECT cache_key, result FROM query_cache')
            cache_entries = c.fetchall()
        
        validation_results['total_objects'] = len(cache_entries)
        
        for cache_key, result_json in cache_entries:
            try:
                cache_object = json.loads(result_json)
                
                # Check required fields
                if 'columns' not in cache_object or 'data' not in cache_object:
                    validation_results['invalid_objects'] += 1
                    validation_results['issues'].append({
                        'cache_key': cache_key,
                        'issue': 'Missing columns or data field'
                    })
                    continue
                
                columns = cache_object['columns']
                data = cache_object['data']
                
                # Check for required columns
                required_columns = ['tag_id', 'date_key']
                missing_cols = [col for col in required_columns if col not in columns]
                if missing_cols:
                    validation_results['missing_columns'] += 1
                    validation_results['issues'].append({
                        'cache_key': cache_key,
                        'issue': f'Missing required columns: {missing_cols}'
                    })
                    continue
                
                # Check for duplicates within this cache object
                if 'tag_id' in columns and 'date_key' in columns:
                    tag_id_index = columns.index('tag_id')
                    date_key_index = columns.index('date_key')
                    
                    seen_combinations = set()
                    duplicates_found = 0
                    
                    for row in data:
                        combo = f"{row[tag_id_index]}|{row[date_key_index]}"
                        if combo in seen_combinations:
                            duplicates_found += 1
                        else:
                            seen_combinations.add(combo)
                    
                    if duplicates_found > 0:
                        validation_results['duplicate_records'] += duplicates_found
                        validation_results['issues'].append({
                            'cache_key': cache_key,
                            'issue': f'Found {duplicates_found} duplicate date_key+tag_id combinations'
                        })
                
                validation_results['valid_objects'] += 1
                
            except Exception as e:
                validation_results['invalid_objects'] += 1
                validation_results['issues'].append({
                    'cache_key': cache_key,
                    'issue': f'JSON parsing error: {str(e)}'
                })
        
        # Calculate health score
        if validation_results['total_objects'] > 0:
            health_score = (validation_results['valid_objects'] / validation_results['total_objects']) * 100
        else:
            health_score = 0
        
        validation_results['health_score'] = round(health_score, 2)
        
        return jsonify({
            'status': 'success',
            'validation': validation_results
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Cache validation failed: {str(e)}"
        }), 500

@debug_bp.route('/test-superset')
def test_superset_connection():
    """Test Superset API connection"""
    try:
        from utils.superset_utils import test_superset_connection
        
        success = test_superset_connection()
        
        return jsonify({
            'status': 'success' if success else 'error',
            'connection_successful': success,
            'message': 'Superset API connection test completed'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'connection_successful': False,
            'message': f"Superset API test failed: {str(e)}"
        }), 500

@debug_bp.route('/system/health')
def system_health():
    """Overall system health check"""
    try:
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'database': {'status': 'unknown'},
            'cache': {'status': 'unknown'},
            'auto_collection': {'status': 'unknown'},
            'overall': 'unknown'
        }
        
        # Check database
        try:
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute('SELECT COUNT(*) FROM query_cache')
                cache_count = c.fetchone()[0]
                health_status['database'] = {
                    'status': 'healthy',
                    'cache_entries': cache_count
                }
        except Exception as e:
            health_status['database'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Check cache health
        try:
            cache_stats = get_cache_stats()
            health_status['cache'] = {
                'status': 'healthy',
                'stats': cache_stats
            }
        except Exception as e:
            health_status['cache'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Check auto-collection
        try:
            from config import AUTO_COLLECTION_ENABLED
            health_status['auto_collection'] = {
                'status': 'enabled' if AUTO_COLLECTION_ENABLED else 'disabled',
                'enabled': AUTO_COLLECTION_ENABLED
            }
        except Exception as e:
            health_status['auto_collection'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Overall health
        component_statuses = [
            health_status['database']['status'],
            health_status['cache']['status'],
            health_status['auto_collection']['status']
        ]
        
        if all(status in ['healthy', 'enabled', 'disabled'] for status in component_statuses):
            health_status['overall'] = 'healthy'
        elif any(status == 'error' for status in component_statuses):
            health_status['overall'] = 'error'
        else:
            health_status['overall'] = 'warning'
        
        return jsonify({
            'status': 'success',
            'health': health_status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Health check failed: {str(e)}"
        }), 500
    
# Add this to blueprints/debug.py

@debug_bp.route('/inspect-data/<seat_id>')
def inspect_data(seat_id):
    """Inspect raw cached data for debugging"""
    try:
        from utils.cache_utils import cache_get_unified
        
        cache_object = cache_get_unified('query1', seat_id)
        if not cache_object:
            return jsonify({
                'status': 'error',
                'message': f'No cached data found for seat_id {seat_id}'
            })
        
        columns = cache_object.get('columns', [])
        data = cache_object.get('data', [])
        
        # Get first few rows for inspection
        sample_data = data[:3] if data else []
        
        return jsonify({
            'status': 'success',
            'seat_id': seat_id,
            'columns': columns,
            'column_count': len(columns),
            'total_rows': len(data),
            'sample_rows': sample_data,
            'sample_row_lengths': [len(row) for row in sample_data],
            'first_row_details': {
                'row': sample_data[0] if sample_data else None,
                'length': len(sample_data[0]) if sample_data else 0,
                'types': [type(val).__name__ for val in sample_data[0]] if sample_data else []
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error inspecting data: {str(e)}"
        }), 500