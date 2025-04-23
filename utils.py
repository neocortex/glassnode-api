"""
Utility functions for Glassnode API client
"""
import datetime
from typing import Union, List, Dict, Any
import pandas as pd
from io import StringIO


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

def convert_to_dataframe(data: Union[List[Dict[str, Any]], str], path: str) -> pd.DataFrame:
    """
    Convert Glassnode API response (JSON or CSV) to a pandas DataFrame.

    Args:
        data: The raw data from the API (list of dicts for JSON, string for CSV).
        path: The metric path used for the request (used for column naming).

    Returns:
        pd.DataFrame: A DataFrame with a datetime index and a value column.

    Raises:
        ValueError: If the input data format is unexpected or conversion fails.
        ImportError: If pandas is not installed.
    """
    if not data:
        return pd.DataFrame() # Return empty DataFrame for empty data

    try:
        # Attempt to extract a meaningful column name from the path
        column_name = path.strip('/').split('/')[-1] if '/' in path else 'value'
    except Exception:
        column_name = 'value' # Fallback column name

    if isinstance(data, str): # Handle CSV data
        try:
            # Use StringIO to treat the string data as a file
            df = pd.read_csv(StringIO(data))
            if 't' not in df.columns:
                 raise ValueError("CSV data missing expected timestamp column 't'")
                 
            # Determine the value column name (often 'v', but could be different)
            value_col = 'v' # Default assumption
            if value_col not in df.columns:
                # If 'v' isn't present, try to infer it (e.g., the first non-'t' column)
                potential_v_cols = [col for col in df.columns if col != 't']
                if not potential_v_cols:
                    raise ValueError("CSV data missing a value column ('v' or other).")
                value_col = potential_v_cols[0] # Use the first other column as value
                print(f"Warning: CSV data missing 'v' column, using '{value_col}' as value.")


            # Convert timestamp 't' to datetime and set as index
            df['t'] = pd.to_datetime(df['t'], unit='s')
            df = df.set_index('t')

            # Rename the determined value column to the desired column_name
            df = df.rename(columns={value_col: column_name})

            # Keep only the renamed value column
            if column_name in df.columns:
                df = df[[column_name]]
            else:
                 raise ValueError(f"Could not find or rename value column '{value_col}' to '{column_name}' from CSV.")

            return df

        except Exception as e:
            raise ValueError(f"Failed to parse CSV data into DataFrame: {e}")

    elif isinstance(data, list): # Handle JSON data
        if not data:
            return pd.DataFrame()

        if not isinstance(data[0], dict) or 't' not in data[0]:
             raise ValueError("JSON list items must be dictionaries with a 't' timestamp key.")

        first_item = data[0]

        # Case 1: Standard format [{'t': timestamp, 'v': value}, ...]
        if 'v' in first_item:
            try:
                df = pd.DataFrame(data)
                # Ensure 'v' exists in all items if present in the first
                if not all('v' in item for item in data):
                     raise ValueError("Inconsistent JSON format: some items missing 'v' key.")
                     
                df['t'] = pd.to_datetime(df['t'], unit='s')
                df = df.set_index('t')
                
                # Use path for column name if possible
                try:
                    column_name = path.strip('/').split('/')[-1] if '/' in path else 'value'
                except Exception:
                    column_name = 'value'
                    
                df = df.rename(columns={'v': column_name})
                # Keep only the renamed value column
                if column_name in df.columns:
                    df = df[[column_name]]
                else:
                    raise ValueError(f"Failed to find or rename column 'v' to '{column_name}'.")
                return df
            except Exception as e:
                raise ValueError(f"Failed to convert standard JSON [{'t': ..., 'v': ...}] to DataFrame: {e}")

        # Case 2: Nested object format [{'t': timestamp, 'o': {'k1': v1, 'k2': v2}}, ...]
        elif 'o' in first_item and isinstance(first_item['o'], dict):
            try:
                records = []
                for item in data:
                    if isinstance(item, dict) and 't' in item and 'o' in item and isinstance(item['o'], dict):
                        # Flatten the structure: combine 't' with keys from 'o'
                        record = {'t': item['t'], **item['o']}
                        records.append(record)
                    else:
                        # Handle items that don't match the expected nested structure
                        print(f"Warning: Skipping item with unexpected structure in nested JSON: {item}")
                
                if not records:
                    return pd.DataFrame() # Return empty if no valid records found
                    
                df = pd.DataFrame(records)
                df['t'] = pd.to_datetime(df['t'], unit='s')
                df = df.set_index('t')
                # Columns are automatically named from the keys in 'o'
                return df
            except Exception as e:
                raise ValueError(f"Failed to convert nested JSON [{'t': ..., 'o': {...}}] to DataFrame: {e}")

        # Add more cases here if other JSON structures need handling

        else:
            # If the first item doesn't match known structures ('v' or 'o')
            raise ValueError("JSON data does not match expected formats: [{'t':..., 'v':...}] or [{'t':..., 'o':{...}}]")

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
        ImportError: If pandas is not installed.
    """
    if output_structure not in ["wide", "dict_by_asset", "dict_by_metric"]:
        raise ValueError(f"Invalid output_structure: '{output_structure}'. Must be 'wide', 'dict_by_asset', or 'dict_by_metric'.")
        
    if not isinstance(bulk_response, dict) or 'data' not in bulk_response:
        raise ValueError("Input must be a dictionary with a 'data' key containing the bulk list.")

    data_list = bulk_response['data']
    if not isinstance(data_list, list):
        raise ValueError("The 'data' key must contain a list.")

    if not data_list:
        # Return empty based on requested structure
        if output_structure == "wide":
            return pd.DataFrame()
        else:
            return {}

    # --- Step 1: Parse into a flat list of records --- 
    flat_data = []
    all_metric_keys = set() # Keep track of unique metric identifiers
    all_assets = set()      # Keep track of unique assets

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
            identifiers = {k: str(v) for k, v in item.items() if k != 'v'} # Ensure keys are strings for sorting
            
            asset = identifiers.pop('a', None) # Use None if 'a' is not present
            all_assets.add(asset) # Add asset (even if None)

            # Generate the metric_key from remaining identifiers
            metric_parts = []
            for key in sorted(identifiers.keys()):
                metric_parts.append(f"{key}_{identifiers[key]}")
            metric_key = "_".join(metric_parts) if metric_parts else (asset if asset else 'value') # Fallback naming
            all_metric_keys.add(metric_key)

            flat_data.append({
                't': timestamp,
                'asset': asset,
                'metric_key': metric_key,
                'value': value
            })
    # --- End Step 1 --- 
    
    if not flat_data:
        if output_structure == "wide":
            return pd.DataFrame()
        else:
            return {}
            
    # --- Step 2: Convert flat list to desired output structure --- 
    try:
        # Create a DataFrame from the flat list for easier manipulation
        flat_df = pd.DataFrame(flat_data)
        flat_df['t'] = pd.to_datetime(flat_df['t'], unit='s')

        if output_structure == "wide":
            # Combine asset and metric_key for wide format columns
            def create_wide_col_name(row):
                if row['asset'] is None:
                    return row['metric_key']
                # Avoid double asset name if metric_key already is the asset (e.g. only 'a' and 'v' present)
                if row['metric_key'] == row['asset']:
                     return str(row['asset'])
                return f"{row['asset']}_{row['metric_key']}"
            
            flat_df['wide_col'] = flat_df.apply(create_wide_col_name, axis=1)
            
            wide_df = flat_df.pivot_table(index='t', columns='wide_col', values='value')
            return wide_df

        elif output_structure == "dict_by_asset":
            result_dict = {}
            # Use pd.Categorical for potentially better grouping performance
            flat_df['asset'] = pd.Categorical(flat_df['asset'])
            # Add observed=False to groupby to silence FutureWarning
            for asset, group_df in flat_df.groupby('asset', observed=False):
                # Pivot each group: index=time, columns=metric_key
                asset_df = group_df.pivot_table(index='t', columns='metric_key', values='value')
                # Reindex to ensure all metric keys are present as columns, filled with NaN
                # Remove axis=1 as 'columns' implies the axis
                asset_df = asset_df.reindex(columns=sorted(list(all_metric_keys)))
                result_dict[asset] = asset_df
            return result_dict
            
        elif output_structure == "dict_by_metric":
            result_dict = {}
            flat_df['metric_key'] = pd.Categorical(flat_df['metric_key'])
            # Add observed=False to groupby to silence FutureWarning
            for metric_key, group_df in flat_df.groupby('metric_key', observed=False):
                # Pivot each group: index=time, columns=asset
                metric_df = group_df.pivot_table(index='t', columns='asset', values='value')
                 # Reindex to ensure all assets are present as columns, filled with NaN
                # Ensure assets used for columns are strings or handle None appropriately
                asset_cols = sorted([str(a) if a is not None else 'None' for a in all_assets]) # Use 'None' string for None asset
                metric_df.columns = [str(c) if c is not None else 'None' for c in metric_df.columns] # Rename existing columns
                # Remove axis=1 as 'columns' implies the axis
                metric_df = metric_df.reindex(columns=asset_cols)
                result_dict[metric_key] = metric_df
            return result_dict
            
    except Exception as e:
        raise ValueError(f"Failed to create output structure '{output_structure}' from bulk data: {e}")
    # --- End Step 2 --- 