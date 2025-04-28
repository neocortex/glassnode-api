"""
Utility functions for Glassnode API client
"""
import datetime
from typing import Union, List, Dict, Any, Tuple, Set
import pandas as pd
from io import StringIO
import time


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


def calculate_since_for_limit(interval: str = '24h', limit: int = 100) -> int:
    """
    Calculate the 'since' timestamp that will return 'limit' number of data points 
    for a given interval, using the current time as the end point.
    
    This function is designed to be used when retrieving the most recent data points.
    
    Args:
        interval: Resolution interval (defaults to '24h')
        limit: Number of data points to retrieve (defaults to 100)
        
    Returns:
        int: The 'since' timestamp
        
    Raises:
        ValueError: If interval format is not recognized
    """
    if limit <= 0:
        raise ValueError("Limit must be a positive integer")
        
    # Always use current time as the until timestamp
    until_ts = int(time.time())
    
    # Define interval seconds mapping
    interval_seconds = {
        '10m': 10 * 60,
        '1h': 60 * 60,
        '24h': 24 * 60 * 60,
        '1d': 24 * 60 * 60,
        '1w': 7 * 24 * 60 * 60,
        '1month': 30 * 24 * 60 * 60,  # Approximation
    }
    
    # Get seconds per interval (use 24h as default if not recognized)
    seconds_per_interval = interval_seconds.get(interval, interval_seconds['24h'])
    
    # Calculate the since timestamp by subtracting 'limit' intervals from until
    # We use 'limit' instead of (limit-1) to go back one additional datapoint
    # This ensures the since timestamp is always earlier than until
    since_ts = until_ts - limit * seconds_per_interval
    
    return since_ts

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

# --- Helper Functions ---

def _get_column_name_from_path(path: str) -> str:
    """Extract a column name from the metric path."""
    try:
        return path.strip('/').split('/')[-1] if '/' in path else 'value'
    except Exception:
        return 'value' # Fallback column name

def _dataframe_from_csv(csv_string: str, path: str) -> pd.DataFrame:
    """Convert CSV string data to a pandas DataFrame."""
    try:
        df = pd.read_csv(StringIO(csv_string))
        timestamp_col = 'timestamp'

        if timestamp_col not in df.columns:
            raise ValueError(f"CSV data missing expected timestamp column '{timestamp_col}'")

        # Convert timestamp column to datetime
        if pd.api.types.is_numeric_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='s')
        else:
            try:
                df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            except ValueError as e:
                 raise ValueError(f"Failed to parse timestamp column '{timestamp_col}' in CSV: {e}")

        df = df.set_index(timestamp_col)

        # Handle single vs multi-column CSV
        if len(df.columns) == 1:
            col_name = _get_column_name_from_path(path)
            df = df.rename(columns={df.columns[0]: col_name})
        elif len(df.columns) > 1:
            # Keep original columns for multi-column data (e.g., HODL waves)
            pass
        else:
            raise ValueError("CSV data has timestamp column but no data columns.")

        return df
    except Exception as e:
        # Catch potential pd.errors.EmptyDataError from read_csv if string is empty/invalid
        # and other parsing errors
        if isinstance(e, ValueError): # Keep specific ValueErrors
            raise e
        raise ValueError(f"Failed to parse CSV data into DataFrame: {e}")

def _dataframe_from_json_standard(json_list: List[Dict[str, Any]], path: str) -> pd.DataFrame:
    """Convert standard JSON format [{'t': ts, 'v': val}, ...] to DataFrame."""
    try:
        df = pd.DataFrame(json_list)
        if not all('v' in item for item in json_list):
             raise ValueError("Inconsistent JSON format: some items missing 'v' key.")

        df['t'] = pd.to_datetime(df['t'], unit='s')
        df = df.set_index('t')

        column_name = _get_column_name_from_path(path)
        df = df.rename(columns={'v': column_name})

        if column_name in df.columns:
            df = df[[column_name]] # Keep only the renamed value column
        else:
            # This case should ideally not happen if 'v' exists and rename succeeds
            raise ValueError(f"Failed to find or rename column 'v' to '{column_name}'.")
        return df
    except Exception as e:
        if isinstance(e, ValueError): raise e
        raise ValueError(f"Failed to convert standard JSON [{'t': ..., 'v': ...}] to DataFrame: {e}")


def _dataframe_from_json_nested(json_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert nested JSON format [{'t': ts, 'o': {'k': v}}, ...] to DataFrame."""
    try:
        records = []
        for item in json_list:
            if isinstance(item, dict) and 't' in item and 'o' in item and isinstance(item['o'], dict):
                record = {'t': item['t'], **item['o']}
                records.append(record)
            else:
                print(f"Warning: Skipping item with unexpected structure in nested JSON: {item}")

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df = df.set_index('t')
        # Columns are automatically named from the keys in 'o'
        return df
    except Exception as e:
        raise ValueError(f"Failed to convert nested JSON [{'t': ..., 'o': {...}}] to DataFrame: {e}")


def _dataframe_from_json(json_list: List[Dict[str, Any]], path: str) -> pd.DataFrame:
    """Convert JSON list data to a pandas DataFrame, detecting the format."""
    if not json_list:
        return pd.DataFrame()

    if not isinstance(json_list[0], dict) or 't' not in json_list[0]:
        raise ValueError("JSON list items must be dictionaries with a 't' timestamp key.")

    first_item = json_list[0]

    if 'v' in first_item:
        return _dataframe_from_json_standard(json_list, path)
    elif 'o' in first_item and isinstance(first_item['o'], dict):
        return _dataframe_from_json_nested(json_list)
    else:
        raise ValueError("JSON data does not match expected formats: [{'t':..., 'v':...}] or [{'t':..., 'o':{...}}]")


def _flatten_bulk_response(data_list: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Set[str], Set[str]]:
    """Parse and flatten the bulk API response data."""
    flat_data = []
    all_metric_keys = set()
    all_assets = set()

    for timestamp_entry in data_list:
        if not isinstance(timestamp_entry, dict) or 't' not in timestamp_entry or 'bulk' not in timestamp_entry:
            print(f"Warning: Skipping invalid timestamp entry: {timestamp_entry}")
            continue

        timestamp = timestamp_entry['t']
        bulk_items = timestamp_entry['bulk']

        if not isinstance(bulk_items, list):
            print(f"Warning: Skipping invalid bulk entry (not a list) for timestamp {timestamp}: {bulk_items}")
            continue

        for item in bulk_items:
            if not isinstance(item, dict) or 'v' not in item:
                print(f"Warning: Skipping invalid item within bulk list for timestamp {timestamp}: {item}")
                continue

            value = item['v']
            identifiers = {k: str(v) for k, v in item.items() if k != 'v'}

            asset = identifiers.pop('a', None)
            all_assets.add(asset) # Add asset (even if None)

            # Generate the metric_key from remaining identifiers (sorted for consistency)
            metric_parts = [f"{k}_{identifiers[k]}" for k in sorted(identifiers.keys())]
            metric_key = "_".join(metric_parts) if metric_parts else (str(asset) if asset else 'value') # Fallback naming
            all_metric_keys.add(metric_key)

            flat_data.append({
                't': timestamp,
                'asset': asset,
                'metric_key': metric_key,
                'value': value
            })

    if not flat_data:
        return pd.DataFrame(), all_assets, all_metric_keys

    flat_df = pd.DataFrame(flat_data)
    flat_df['t'] = pd.to_datetime(flat_df['t'], unit='s')
    return flat_df, all_assets, all_metric_keys


def _structure_bulk_dataframe(
    flat_df: pd.DataFrame,
    output_structure: str,
    all_assets: Set[str],
    all_metric_keys: Set[str]
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Pivot and structure the flattened bulk data according to the desired output."""
    try:
        if output_structure == "wide":
            def create_wide_col_name(row):
                asset_str = str(row['asset']) if row['asset'] is not None else 'None'
                # Avoid 'None_' prefix if asset is None
                if row['asset'] is None:
                    return row['metric_key']
                # Avoid double asset name if metric_key *is* the asset name
                if row['metric_key'] == asset_str:
                     return asset_str
                return f"{asset_str}_{row['metric_key']}"

            if flat_df.empty: return pd.DataFrame() # Handle empty case before applying
            flat_df['wide_col'] = flat_df.apply(create_wide_col_name, axis=1)
            wide_df = flat_df.pivot_table(index='t', columns='wide_col', values='value')
            # Ensure consistent column order? Maybe not strictly necessary for 'wide'
            # wide_df = wide_df.reindex(columns=sorted(wide_df.columns))
            return wide_df

        elif output_structure == "dict_by_asset":
            result_dict = {}
            if flat_df.empty: return result_dict # Handle empty case before grouping
            # Use pd.Categorical for potentially better grouping performance
            flat_df['asset_cat'] = pd.Categorical(flat_df['asset'].apply(lambda x: str(x) if x is not None else 'None'))
            sorted_metric_keys = sorted(list(all_metric_keys))
            # Use observed=True if pandas >= 1.5, False otherwise for older versions
            # Using observed=False to maintain compatibility across versions and handle potential empty categories
            for asset, group_df in flat_df.groupby('asset_cat', observed=False):
                # Use asset's original type (before categorization) as dict key if possible
                original_asset = next((a for a in all_assets if str(a) == asset), asset) # Find original None/value
                asset_df = group_df.pivot_table(index='t', columns='metric_key', values='value')
                # Reindex to ensure all metric keys are present as columns, filled with NaN
                asset_df = asset_df.reindex(columns=sorted_metric_keys)
                result_dict[original_asset] = asset_df
            return result_dict

        elif output_structure == "dict_by_metric":
            result_dict = {}
            if flat_df.empty: return result_dict # Handle empty case before grouping
            flat_df['metric_key_cat'] = pd.Categorical(flat_df['metric_key'])
            # Ensure assets used for columns are strings or handle None appropriately
            asset_cols = sorted([str(a) if a is not None else 'None' for a in all_assets])
            # Use observed=False for consistent behavior
            for metric_key, group_df in flat_df.groupby('metric_key_cat', observed=False):
                metric_df = group_df.pivot_table(index='t', columns='asset', values='value')
                # Standardize column names (including handling None asset) before reindexing
                metric_df.columns = [str(c) if c is not None else 'None' for c in metric_df.columns]
                # Reindex to ensure all assets are present as columns, filled with NaN
                metric_df = metric_df.reindex(columns=asset_cols)
                result_dict[metric_key] = metric_df
            return result_dict

    except Exception as e:
        # Add more specific error context if possible
        raise ValueError(f"Failed to create output structure '{output_structure}' from bulk data: {e}")


# --- Original Functions (Refactored) ---

def convert_to_dataframe(data: Union[List[Dict[str, Any]], str], path: str) -> pd.DataFrame:
    """
    Convert Glassnode API response (JSON or CSV) to a pandas DataFrame.

    Args:
        data: The raw data from the API (list of dicts for JSON, string for CSV).
        path: The metric path used for the request (used for column naming).

    Returns:
        pd.DataFrame: A DataFrame with a datetime index and appropriate columns.

    Raises:
        ValueError: If the input data format is unexpected or conversion fails.
    """
    if not data:
        return pd.DataFrame() # Return empty DataFrame for empty data

    if isinstance(data, str):
        return _dataframe_from_csv(data, path)
    elif isinstance(data, list):
        return _dataframe_from_json(data, path)
    else:
        raise ValueError(f"Unexpected data type for DataFrame conversion: {type(data)}")


def convert_bulk_to_dataframe(
    bulk_response: Dict[str, Any],
    output_structure: str = "wide"
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Convert a Glassnode bulk API response into a pandas DataFrame or a dictionary of DataFrames.

    Args:
        bulk_response: The raw dictionary response from a bulk API endpoint.
        output_structure: The desired output format ('wide', 'dict_by_asset', 'dict_by_metric').
            - "wide" (default): Returns a single DataFrame with timestamps as index and
              combined identifiers (e.g., 'BTC_network_eth') as columns.
            - "dict_by_asset": Returns a dictionary where keys are asset symbols and
              values are DataFrames (index=timestamp, columns=metric_identifiers).
            - "dict_by_metric": Returns a dictionary where keys are metric identifiers
              (e.g., 'network_eth') and values are DataFrames (index=timestamp, columns=asset).

    Returns:
        Union[pd.DataFrame, Dict[str, pd.DataFrame]]: The processed data in the specified structure.

    Raises:
        ValueError: If the input format is invalid, the output_structure is unknown,
                    or conversion fails.
    """
    # --- Input Validation ---
    if output_structure not in ["wide", "dict_by_asset", "dict_by_metric"]:
        raise ValueError(f"Invalid output_structure: '{output_structure}'. Must be 'wide', 'dict_by_asset', or 'dict_by_metric'.")

    if not isinstance(bulk_response, dict) or 'data' not in bulk_response:
        raise ValueError("Input must be a dictionary with a 'data' key containing the bulk list.")

    data_list = bulk_response.get('data') # Use .get for slightly safer access
    if not isinstance(data_list, list):
        # Allow empty list, but raise if 'data' exists and is not a list
        if data_list is not None:
             raise ValueError("The 'data' key must contain a list.")
        else:
             data_list = [] # Treat missing 'data' as empty list

    # Handle empty data early
    if not data_list:
        if output_structure == "wide":
            return pd.DataFrame()
        else: # dict_by_asset or dict_by_metric
            return {}

    # --- Processing Steps ---
    # 1. Flatten the response
    flat_df, all_assets, all_metric_keys = _flatten_bulk_response(data_list)

    # Handle case where flattening resulted in empty DataFrame (e.g., all items were invalid)
    if flat_df.empty:
         if output_structure == "wide":
            return pd.DataFrame()
         else:
            return {}

    # 2. Structure the flattened data
    return _structure_bulk_dataframe(flat_df, output_structure, all_assets, all_metric_keys)