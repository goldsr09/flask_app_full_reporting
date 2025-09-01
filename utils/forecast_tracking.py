# utils/forecast_tracking.py
# Forecast tracking and delivery analysis

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import DB_PATH

# Tag to Country mapping based on the provided table
TAG_TO_COUNTRY_MAPPING = {
    # Canela Media
    "Canela_InventorySplit_FAST MX": "MX",
    "Canela_InventorySplit_FAST US (Spanish)": "US",
    "CanelaTV_MX_InventorySplit_VOD (SSAI)": "MX",
    "CanelaTV_US_InventorySplit_VOD (SSAI)": "US",
    
    # Viki
    "Viki_48537 (Inventory Split)": "US",
    "Viki_48537_InventorySplit_CA": "CA",
    "Viki_48537_InventorySplit_MX": "MX",
    "Viki_BR_InventorySplit": "BR",
    
    # Runtime
    "Runtime_BR_InventorySplit_FAST": "BR",
    "Runtime_BR_InventorySplit_VOD": "BR",
    "Runtime_MX_InventorySplit_FAST": "MX",
    "Runtime_MX_InventorySplit_VOD": "MX",
    
    # SBT
    "SBT_Inventory Split_LIVE": "BR",
    "SBT_Inventory Split_VOD": "BR",
    
    # TV Azteca
    "TV Azteca_InventorySplit_MX (VOD) SS": "MX",
    "TV Azteca_InventorySplit_MX_Midroll (Live) SS": "MX",
    "TV Azteca_InventorySplit_MX_Preroll (Live) SS": "MX",
}

def get_country_from_tag_name(tag_name: str) -> Optional[str]:
    """
    Extract country from tag name using the mapping table.
    Returns country code (US, MX, BR, CA) or None if not found.
    """
    if not tag_name:
        return None
    
    # First, try exact match
    if tag_name in TAG_TO_COUNTRY_MAPPING:
        return TAG_TO_COUNTRY_MAPPING[tag_name]
    
    # Try partial matching for variations
    tag_upper = tag_name.upper()
    
    # Check for country codes in tag name
    if " MX" in tag_upper or "_MX" in tag_upper or "MEXICO" in tag_upper:
        return "MX"
    elif " US" in tag_upper or "_US" in tag_upper or "UNITED STATES" in tag_upper:
        return "US"
    elif " BR" in tag_upper or "_BR" in tag_upper or "BRAZIL" in tag_upper:
        return "BR"
    elif " CA" in tag_upper or "_CA" in tag_upper or "CANADA" in tag_upper:
        return "CA"
    
    # Try publisher-based mapping
    if "CANELA" in tag_upper:
        if "MX" in tag_upper:
            return "MX"
        elif "US" in tag_upper:
            return "US"
    elif "VIKI" in tag_upper:
        if "CA" in tag_upper:
            return "CA"
        elif "MX" in tag_upper:
            return "MX"
        elif "BR" in tag_upper:
            return "BR"
        else:
            return "US"  # Default for Viki
    elif "RUNTIME" in tag_upper:
        if "BR" in tag_upper:
            return "BR"
        elif "MX" in tag_upper:
            return "MX"
    elif "SBT" in tag_upper:
        return "BR"
    elif "TV AZTECA" in tag_upper or "AZTECA" in tag_upper:
        return "MX"
    
    return None

def get_publisher_from_tag_name(tag_name: str) -> Optional[str]:
    """
    Extract publisher name from tag name.
    """
    if not tag_name:
        return None
    
    tag_upper = tag_name.upper()
    
    if "CANELA" in tag_upper:
        return "Canela Media"
    elif "VIKI" in tag_upper:
        return "Viki"
    elif "RUNTIME" in tag_upper:
        return "Runtime"
    elif "SBT" in tag_upper:
        return "SBT"
    elif "TV AZTECA" in tag_upper or "AZTECA" in tag_upper:
        return "TV Azteca"
    elif "SOPLAY" in tag_upper:
        return "Soplay"
    
    return None

# Q3 IS Forecast data from the image
Q3_FORECAST = {
    "Canela Media": {
        "publisher_id": "222",  # Corrected publisher ID
        "integration_type": "RAMS",
        "forecasts": {
            "US": 11403000,
            "MX": None,
            "BR": None,
            "CA": None
        }
    },
    "Runtime": {
        "publisher_id": "143",  # Corrected publisher ID
        "integration_type": "RAMS", 
        "forecasts": {
            "US": None,
            "MX": 2600000,
            "BR": 5000000,
            "CA": None
        }
    },
    "SBT": {
        "publisher_id": "245",  # Corrected publisher ID
        "integration_type": "RAMS",
        "forecasts": {
            "US": None,
            "MX": None,
            "BR": 75000000,
            "CA": None
        }
    },
    "Soplay": {
        "publisher_id": "227",  # Assuming based on cached data
        "integration_type": "RAMS",
        "forecasts": {
            "US": None,
            "MX": None,
            "BR": 180000,
            "CA": None
        }
    },
    "TV Azteca": {
        "publisher_id": "228",  # Corrected publisher ID
        "integration_type": "RAMS",
        "forecasts": {
            "US": None,
            "MX": 15800000,
            "BR": None,
            "CA": None
        }
    },
    "Viki": {
        "publisher_id": "25",  # Corrected publisher ID
        "integration_type": "RAMS",
        "forecasts": {
            "US": 4000000,
            "MX": 3500000,
            "BR": 2500000,
            "CA": 250000
        }
    }
}

# Q3 date range (July 1 - September 30, 2025)
Q3_START = "2025-07-01"
Q3_END = "2025-09-30"

def get_publisher_forecast(publisher_id: str) -> Optional[Dict]:
    """Get forecast data for a specific publisher ID"""
    for publisher_name, data in Q3_FORECAST.items():
        if data["publisher_id"] == publisher_id:
            return {
                "publisher_name": publisher_name,
                "publisher_id": publisher_id,
                "forecasts": data["forecasts"]
            }
    return None

def get_actual_delivery(publisher_id: str, geo: str = None, start_date: str = Q3_START, end_date: str = Q3_END) -> Dict:
    """Get actual delivery data for a publisher from cache, optionally filtered by geo"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT result FROM query_cache WHERE cache_key = ?", (f"publisher_id_{publisher_id}",))
            result = c.fetchone()
            
            if not result:
                return {"error": f"No cached data found for publisher_id_{publisher_id}"}
            
            cache_data = json.loads(result[0])
            if 'data' not in cache_data or 'columns' not in cache_data:
                return {"error": "Invalid cache data format"}
            
            # Find column indices
            columns = cache_data['columns']
            try:
                date_key_index = columns.index('date_key')
                impressions_index = columns.index('total_num_unfiltered_impressions')
                tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
            except ValueError as e:
                return {"error": f"Missing required column: {e}"}
            
            # Filter data by date range and geo (if specified)
            filtered_data = []
            geo_mapping_stats = {"total_tags": 0, "mapped_tags": 0, "unmapped_tags": []}
            
            for row in cache_data['data']:
                if start_date <= row[date_key_index] <= end_date:
                    # If geo is specified, filter by tag name using proper mapping
                    if geo and tag_name_index is not None:
                        tag_name = str(row[tag_name_index])
                        geo_mapping_stats["total_tags"] += 1
                        
                        # Get country from tag name using our mapping
                        tag_country = get_country_from_tag_name(tag_name)
                        
                        if tag_country:
                            geo_mapping_stats["mapped_tags"] += 1
                            if tag_country == geo.upper():
                                filtered_data.append(row)
                        else:
                            geo_mapping_stats["unmapped_tags"].append(tag_name)
                    else:
                        filtered_data.append(row)
            
            # Calculate totals
            total_impressions = sum(row[impressions_index] or 0 for row in filtered_data)
            total_days = len(set(row[date_key_index] for row in filtered_data))
            daily_average = total_impressions / total_days if total_days > 0 else 0
            
            # Get unique tags
            tags = list(set(row[tag_name_index] for row in filtered_data if tag_name_index is not None and row[tag_name_index]))
            
            result = {
                "publisher_id": publisher_id,
                "geo": geo,
                "total_impressions": total_impressions,
                "total_days": total_days,
                "daily_average": daily_average,
                "date_range": {"start": start_date, "end": end_date},
                "tags": tags,
                "data_points": len(filtered_data),
                "geo_mapping_stats": geo_mapping_stats
            }
            
            # Add debug info for unmapped tags
            if geo_mapping_stats["unmapped_tags"]:
                result["unmapped_tags"] = list(set(geo_mapping_stats["unmapped_tags"]))
            
            return result
            
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

def calculate_delivery_vs_forecast(publisher_id: str, geo: str = None) -> Dict:
    """Calculate delivery vs forecast for a publisher"""
    forecast_data = get_publisher_forecast(publisher_id)
    if not forecast_data:
        return {"error": f"No forecast data found for publisher_id {publisher_id}"}
    
    # Get forecast for specific geo or total
    if geo and geo in forecast_data["forecasts"]:
        forecast_value = forecast_data["forecasts"][geo]
        if forecast_value is None:
            return {"error": f"No forecast for {geo} region"}
    else:
        # Sum all non-None forecasts
        forecast_value = sum(v for v in forecast_data["forecasts"].values() if v is not None)
    
    if forecast_value == 0:
        return {"error": "Forecast value is zero"}
    
    # Get actual delivery data, filtered by geo if specified
    actual_data = get_actual_delivery(publisher_id, geo)
    if "error" in actual_data:
        return actual_data
    
    # Calculate metrics
    actual_value = actual_data["total_impressions"]
    variance = actual_value - forecast_value
    variance_percent = (variance / forecast_value) * 100
    delivery_percent = (actual_value / forecast_value) * 100
    
    # Calculate days remaining in Q3
    today = datetime.now()
    q3_end = datetime.strptime(Q3_END, "%Y-%m-%d")
    days_remaining = (q3_end - today).days if today < q3_end else 0
    
    # Projected delivery based on current daily average
    projected_total = actual_data["daily_average"] * 92  # Total Q3 days
    projected_variance = projected_total - forecast_value
    projected_variance_percent = (projected_variance / forecast_value) * 100
    
    return {
        "publisher_name": forecast_data["publisher_name"],
        "publisher_id": publisher_id,
        "geo": geo or "Total",
        "forecast": forecast_value,
        "actual": actual_value,
        "variance": variance,
        "variance_percent": variance_percent,
        "delivery_percent": delivery_percent,
        "days_remaining": days_remaining,
        "daily_average": actual_data["daily_average"],
        "projected_total": projected_total,
        "projected_variance": projected_variance,
        "projected_variance_percent": projected_variance_percent,
        "status": "On Track" if abs(variance_percent) < 10 else "Behind" if variance_percent < -10 else "Ahead",
        "date_range": actual_data["date_range"],
        "tags": actual_data["tags"]
    }

def get_all_publishers_delivery_status() -> List[Dict]:
    """Get delivery status for all publishers with forecasts"""
    results = []
    
    for publisher_name, forecast_data in Q3_FORECAST.items():
        publisher_id = forecast_data["publisher_id"]
        
        # Get all geos with forecasts for this publisher
        geos_with_forecasts = [geo for geo, value in forecast_data["forecasts"].items() if value is not None]
        
        if len(geos_with_forecasts) > 1:
            # Publisher has multiple geos - group them
            publisher_group = {
                "publisher_name": publisher_name,
                "publisher_id": publisher_id,
                "type": "grouped",
                "geos": []
            }
            
            # Add each geo
            for geo in geos_with_forecasts:
                result = calculate_delivery_vs_forecast(publisher_id, geo)
                if "error" not in result:
                    publisher_group["geos"].append(result)
            
            # Add total for the publisher
            total_result = calculate_delivery_vs_forecast(publisher_id)
            if "error" not in total_result:
                publisher_group["total"] = total_result
            
            results.append(publisher_group)
        else:
            # Single geo publisher - show as individual entry with total delivery
            # For single geo publishers, show total delivery against the single geo forecast
            result = calculate_delivery_vs_forecast(publisher_id)  # No geo filter = total delivery
            if "error" not in result:
                # Update the geo label to show it's the total
                result["geo"] = "Total"
                results.append(result)
    
    return results

def get_delivery_summary() -> Dict:
    """Get overall delivery summary across all publishers"""
    all_results = get_all_publishers_delivery_status()
    
    if not all_results:
        return {"error": "No delivery data available"}
    
    # Flatten the results to handle both grouped and individual items
    flattened_results = []
    for result in all_results:
        if "error" not in result:
            if result.get("type") == "grouped":
                # For grouped items, use the total
                if "total" in result:
                    flattened_results.append(result["total"])
            else:
                # For individual items, use as is
                flattened_results.append(result)
    
    if not flattened_results:
        return {"error": "No valid delivery data available"}
    
    total_forecast = sum(r["forecast"] for r in flattened_results)
    total_actual = sum(r["actual"] for r in flattened_results)
    total_variance = total_actual - total_forecast
    total_variance_percent = (total_variance / total_forecast) * 100 if total_forecast > 0 else 0
    
    # Count statuses
    status_counts = {}
    for result in flattened_results:
        status = result["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return {
        "total_forecast": total_forecast,
        "total_actual": total_actual,
        "total_variance": total_variance,
        "total_variance_percent": total_variance_percent,
        "overall_status": "On Track" if abs(total_variance_percent) < 10 else "Behind" if total_variance_percent < -10 else "Ahead",
        "status_breakdown": status_counts,
        "publisher_count": len(set(r["publisher_id"] for r in flattened_results)),
        "geo_count": len(set(r["geo"] for r in flattened_results if r["geo"] != "Total"))
    }

def get_cached_publishers() -> List[str]:
    """Get list of all cached publisher IDs"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT cache_key FROM query_cache WHERE cache_key LIKE 'publisher_id_%'")
            results = c.fetchall()
            return [row[0].replace('publisher_id_', '') for row in results]
    except Exception as e:
        print(f"Error getting cached publishers: {e}")
        return []

def analyze_tag_mapping_for_publisher(publisher_id: str) -> Dict:
    """
    Analyze tag mapping for a specific publisher to help debug geo filtering.
    Returns detailed mapping statistics and unmapped tags.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT result FROM query_cache WHERE cache_key = ?", (f"publisher_id_{publisher_id}",))
            result = c.fetchone()
            
            if not result:
                return {"error": f"No cached data found for publisher_id_{publisher_id}"}
            
            cache_data = json.loads(result[0])
            if 'data' not in cache_data or 'columns' not in cache_data:
                return {"error": "Invalid cache data format"}
            
            # Find column indices
            columns = cache_data['columns']
            try:
                tag_name_index = columns.index('tag_name') if 'tag_name' in columns else None
                impressions_index = columns.index('total_num_unfiltered_impressions')
            except ValueError as e:
                return {"error": f"Missing required column: {e}"}
            
            if tag_name_index is None:
                return {"error": "No tag_name column found in data"}
            
            # Analyze all tags
            tag_analysis = {}
            total_impressions = 0
            mapped_impressions = 0
            
            for row in cache_data['data']:
                tag_name = str(row[tag_name_index])
                impressions = row[impressions_index] or 0
                total_impressions += impressions
                
                country = get_country_from_tag_name(tag_name)
                publisher = get_publisher_from_tag_name(tag_name)
                
                if tag_name not in tag_analysis:
                    tag_analysis[tag_name] = {
                        "country": country,
                        "publisher": publisher,
                        "total_impressions": 0,
                        "mapped": country is not None
                    }
                
                tag_analysis[tag_name]["total_impressions"] += impressions
                
                if country:
                    mapped_impressions += impressions
            
            # Group by country
            country_stats = {}
            for tag_name, analysis in tag_analysis.items():
                country = analysis["country"]
                if country:
                    if country not in country_stats:
                        country_stats[country] = {
                            "tags": [],
                            "total_impressions": 0
                        }
                    country_stats[country]["tags"].append(tag_name)
                    country_stats[country]["total_impressions"] += analysis["total_impressions"]
            
            # Find unmapped tags
            unmapped_tags = [tag for tag, analysis in tag_analysis.items() if not analysis["mapped"]]
            
            return {
                "publisher_id": publisher_id,
                "total_tags": len(tag_analysis),
                "mapped_tags": len([t for t in tag_analysis.values() if t["mapped"]]),
                "unmapped_tags": len(unmapped_tags),
                "total_impressions": total_impressions,
                "mapped_impressions": mapped_impressions,
                "mapping_coverage": (mapped_impressions / total_impressions * 100) if total_impressions > 0 else 0,
                "country_breakdown": country_stats,
                "unmapped_tag_list": unmapped_tags,
                "tag_analysis": tag_analysis
            }
            
    except Exception as e:
        return {"error": f"Analysis error: {str(e)}"}

def get_all_publishers_mapping_analysis() -> Dict:
    """Get mapping analysis for all cached publishers"""
    cached_publishers = get_cached_publishers()
    analysis_results = {}
    
    for publisher_id in cached_publishers:
        analysis = analyze_tag_mapping_for_publisher(publisher_id)
        if "error" not in analysis:
            analysis_results[publisher_id] = analysis
    
    return analysis_results
