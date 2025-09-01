# blueprints/admin.py
# Admin endpoints for cache management and auto-collection

from flask import Blueprint, request, jsonify
import threading
from utils.admin_utils import (
    extract_all_ids_from_cache,
    auto_collect_daily_data,
    clear_cache_by_tag,
    get_auto_collection_status,
    diagnose_cache_health,
    get_cache_size_info
)
from utils.cache_utils import clear_cache, get_cache_stats
from config import KNOWN_SEAT_IDS, KNOWN_PUBLISHER_IDS

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/discover-ids')
def discover_ids_endpoint():
    """Discover all seat IDs and publisher IDs from cached data"""
    try:
        seat_ids, publisher_ids = extract_all_ids_from_cache()
        
        return jsonify({
            'status': 'success',
            'discovered_seat_ids': seat_ids,
            'discovered_publisher_ids': publisher_ids,
            'seat_id_count': len(seat_ids),
            'publisher_id_count': len(publisher_ids),
            'current_known_seat_ids': KNOWN_SEAT_IDS,
            'current_known_publisher_ids': KNOWN_PUBLISHER_IDS,
            'message': f"Found {len(seat_ids)} Seat IDs and {len(publisher_ids)} Publisher IDs in cache"
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error discovering IDs: {str(e)}"
        }), 500

@admin_bp.route('/auto-collect/status')
def auto_collect_status():
    """Get status of auto-collection system"""
    try:
        status = get_auto_collection_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting auto-collection status: {str(e)}"
        }), 500

@admin_bp.route('/auto-collect/run-now', methods=['POST'])
def auto_collect_run_now():
    """Trigger auto-collection manually"""
    try:
        def run_in_background():
            try:
                result = auto_collect_daily_data()
                print(f"Background auto-collection completed: {result}")
            except Exception as e:
                print(f"Background auto-collection failed: {e}")
        
        # Start collection in background thread
        thread = threading.Thread(target=run_in_background, daemon=True)
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Auto-collection started in background. Check /admin/auto-collect/status for progress.'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error starting auto-collection: {str(e)}"
        }), 500

@admin_bp.route('/bulk-collect/run-now', methods=['POST'])
def bulk_collect_run_now():
    """Trigger bulk collection manually"""
    try:
        from utils.admin_utils import daily_bulk_collection
        
        def run_in_background():
            try:
                result = daily_bulk_collection()
                print(f"Background bulk collection completed: {result}")
            except Exception as e:
                print(f"Background bulk collection failed: {e}")
        
        # Start collection in background thread
        thread = threading.Thread(target=run_in_background, daemon=True)
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Bulk collection started in background. Check logs for progress.'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error starting bulk collection: {str(e)}"
        }), 500

@admin_bp.route('/bulk-collect/status')
def bulk_collect_status():
    """Get status of bulk collection system"""
    try:
        # For now, just return basic status
        # In the future, this could track collection history
        return jsonify({
            'status': 'success',
            'bulk_collection': {
                'enabled': True,
                'last_run': None,  # Could be stored in database
                'next_run': None,   # Could be scheduled
                'message': 'Bulk collection system ready'
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting bulk collection status: {str(e)}"
        }), 500

@admin_bp.route('/cache/clear', methods=['POST'])
def clear_cache_endpoint():
    """Clear all cache entries"""
    try:
        clear_cache()
        return jsonify({
            'status': 'success',
            'message': 'All cache cleared successfully'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error clearing cache: {str(e)}"
        }), 500

@admin_bp.route('/cache/clear-tag', methods=['POST'])
def clear_cache_tag_endpoint():
    """Clear cache entries containing specific tag ID"""
    data = request.get_json()
    if not data or 'tag_id' not in data:
        return jsonify({
            'status': 'error',
            'message': 'tag_id required in request body'
        }), 400
    
    tag_id = data['tag_id']
    
    try:
        removed_count = clear_cache_by_tag(tag_id)
        return jsonify({
            'status': 'success',
            'message': f'Modified {removed_count} cache entries containing tag_id {tag_id}',
            'tag_id': tag_id,
            'removed_count': removed_count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error clearing cache for tag {tag_id}: {str(e)}"
        }), 500

@admin_bp.route('/cache/stats')
def cache_stats_endpoint():
    """Get detailed cache statistics"""
    try:
        # Get basic cache stats
        stats = get_cache_stats()
        
        # Get cache health info
        health = diagnose_cache_health()
        
        # Get cache size info
        size_info = get_cache_size_info()
        
        # Combine all information
        combined_stats = {
            'cache_statistics': stats,
            'cache_health': health,
            'cache_size': size_info,
            'status': 'success'
        }
        
        return jsonify(combined_stats)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting cache stats: {str(e)}"
        }), 500

@admin_bp.route('/cache/health')
def cache_health_endpoint():
    """Diagnose cache health"""
    try:
        health_info = diagnose_cache_health()
        return jsonify({
            'status': 'success',
            'health': health_info
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error diagnosing cache health: {str(e)}"
        }), 500

@admin_bp.route('/system/info')
def system_info_endpoint():
    """Get system information"""
    try:
        from config import AUTO_COLLECTION_ENABLED, AUTO_COLLECTION_TIME, LOOKBACK_DAYS
        import os
        
        # Get database file info
        db_size = 0
        db_exists = False
        try:
            from config import DB_PATH
            if os.path.exists(DB_PATH):
                db_size = os.path.getsize(DB_PATH)
                db_exists = True
        except:
            pass
        
        system_info = {
            'auto_collection': {
                'enabled': AUTO_COLLECTION_ENABLED,
                'schedule_time': AUTO_COLLECTION_TIME,
                'lookback_days': LOOKBACK_DAYS
            },
            'known_entities': {
                'seat_ids': KNOWN_SEAT_IDS,
                'publisher_ids': KNOWN_PUBLISHER_IDS,
                'seat_id_count': len(KNOWN_SEAT_IDS),
                'publisher_id_count': len(KNOWN_PUBLISHER_IDS)
            },
            'database': {
                'exists': db_exists,
                'size_bytes': db_size,
                'size_mb': round(db_size / (1024 * 1024), 2) if db_size > 0 else 0
            }
        }
        
        return jsonify({
            'status': 'success',
            'system_info': system_info
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error getting system info: {str(e)}"
        }), 500

@admin_bp.route('/entities/list')
def list_entities_endpoint():
    """List all cached entities (seat IDs and publisher IDs)"""
    try:
        # Get discovered IDs from cache
        seat_ids, publisher_ids = extract_all_ids_from_cache()
        
        # Get cache stats for each entity
        entity_stats = []
        
        # Process seat IDs (Query 1)
        for seat_id in seat_ids:
            try:
                from utils.cache_utils import cache_get_unified
                cache_object = cache_get_unified('query1', seat_id)
                if cache_object:
                    record_count = len(cache_object.get('data', []))
                    
                    # Get date range
                    date_range = {'min': None, 'max': None}
                    if 'columns' in cache_object and 'date_key' in cache_object['columns']:
                        date_index = cache_object['columns'].index('date_key')
                        dates = [str(row[date_index]) for row in cache_object['data']]
                        if dates:
                            date_range = {'min': min(dates), 'max': max(dates)}
                    
                    entity_stats.append({
                        'entity_id': seat_id,
                        'entity_type': 'seat_id',
                        'query_type': 'Query 1',
                        'record_count': record_count,
                        'date_range': date_range
                    })
            except:
                continue
        
        # Process publisher IDs (Query 2)
        for publisher_id in publisher_ids:
            try:
                from utils.cache_utils import cache_get_unified
                cache_object = cache_get_unified('query2', publisher_id)
                if cache_object:
                    record_count = len(cache_object.get('data', []))
                    
                    # Get date range
                    date_range = {'min': None, 'max': None}
                    if 'columns' in cache_object and 'date_key' in cache_object['columns']:
                        date_index = cache_object['columns'].index('date_key')
                        dates = [str(row[date_index]) for row in cache_object['data']]
                        if dates:
                            date_range = {'min': min(dates), 'max': max(dates)}
                    
                    entity_stats.append({
                        'entity_id': publisher_id,
                        'entity_type': 'publisher_id',
                        'query_type': 'Query 2',
                        'record_count': record_count,
                        'date_range': date_range
                    })
            except:
                continue
        
        # Sort by record count (highest first)
        entity_stats.sort(key=lambda x: x['record_count'], reverse=True)
        
        return jsonify({
            'status': 'success',
            'entities': entity_stats,
            'summary': {
                'total_entities': len(entity_stats),
                'seat_ids': len(seat_ids),
                'publisher_ids': len(publisher_ids),
                'total_records': sum(e['record_count'] for e in entity_stats)
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error listing entities: {str(e)}"
        }), 500