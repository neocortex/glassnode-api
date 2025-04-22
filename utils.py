"""
Utility functions for Glassnode API client
"""
import datetime
from typing import Union


def convert_to_unix_timestamp(date_value: Union[int, str, datetime.datetime]) -> int:
    """
    Convert various date formats to Unix timestamp.

    Args:
        date_value: Date in Unix timestamp (int), string format, or datetime object

    Returns:
        int: Unix timestamp (seconds since epoch)

    Raises:
        ValueError: If the date format cannot be parsed
    """
    if date_value is None:
        return None
        
    # If already an integer, assume it's already a Unix timestamp
    if isinstance(date_value, int):
        return date_value
        
    # If it's a datetime object, convert to timestamp
    if isinstance(date_value, datetime.datetime):
        return int(date_value.timestamp())
        
    # If it's a string, try different formats
    if isinstance(date_value, str):
        # Check if it's already a numeric timestamp
        if date_value.isdigit():
            return int(date_value)
            
        try:
            # Try ISO format (YYYY-MM-DD, YYYY-MM-DDTHH:MM:SSZ, etc.)
            return int(datetime.datetime.fromisoformat(date_value.replace('Z', '+00:00')).timestamp())
        except ValueError:
            pass
            
        # Try common date formats
        date_formats = [
            "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",  # Various slash formats
            "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",  # Various dash formats
            "%Y.%m.%d", "%d.%m.%Y", "%m.%d.%Y",  # Various dot formats
            "%Y%m%d", "%d%m%Y", "%m%d%Y",        # No separator formats
            "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",  # With time
            "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",  # With time
        ]
        
        for fmt in date_formats:
            try:
                return int(datetime.datetime.strptime(date_value, fmt).timestamp())
            except ValueError:
                continue
                
    # If we get here, the format wasn't recognized
    raise ValueError(f"Could not parse date value: {date_value}. Please provide a Unix timestamp or a recognized date format.")

def merge_bulk_data(combined_data, chunk_data, direction="forward"):
    """
    Merge arrays of timestamped data with bulk arrays, focusing only on the data array.
    
    Args:
        combined_data: The existing combined data array
        chunk_data: The new chunk data array to merge
        direction: The pagination direction ('forward' or 'backward')
        
    Returns:
        The updated combined data array
    """
    if not combined_data:
        return chunk_data
        
    # Create a mapping of timestamps to existing data points
    time_map = {item['t']: item for item in combined_data}
    
    # Process new items that need to be added (not merged)
    new_items = []
    
    for item in chunk_data:
        if item['t'] in time_map:
            # For existing timestamps, merge the bulk arrays
            existing_item = time_map[item['t']]
            # Create a map of assets in the existing bulk array
            existing_assets = {asset_data['a']: asset_data for asset_data in existing_item.get('bulk', [])}
            
            # Add new assets or update existing ones
            for asset_data in item.get('bulk', []):
                existing_assets[asset_data['a']] = asset_data
                
            # Update the bulk array with all assets
            existing_item['bulk'] = list(existing_assets.values())
        else:
            # For new timestamps, add to new_items for later processing
            new_items.append(item)
    
    # Handle direction-specific addition of new items
    if direction == "forward":
        # Append new items at the end
        combined_data.extend(new_items)
    else:  # backward
        # Prepend new items at the beginning (preserve chronological order)
        combined_data = new_items + combined_data
    
    return combined_data 