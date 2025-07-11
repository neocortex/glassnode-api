"""
Glassnode API Client

A simple wrapper for the Glassnode API.
"""
from typing import Dict, List, Optional, Any, Union
import requests
import json
import time
import datetime
import re
import os # Added for cache file path handling
from .utils import (
    convert_to_unix_timestamp, merge_bulk_data, convert_to_dataframe, 
    convert_bulk_to_dataframe, calculate_since_for_limit,
    load_json_cache, save_json_cache  # Import cache utils
)
import pandas as pd


class GlassnodeAPIClient:
    """
    Client for interacting with the Glassnode API.

    This class provides methods for fetching data from various
    Glassnode API endpoints including metadata and metrics.
    """

    BASE_URL = "https://api.glassnode.com/v1"

    # Maximum allowed timerange in days for bulk endpoints
    BULK_MAX_DAYS = {
        "10m": 10,
        "1h": 10,
        "24h": 31,
        "1w": 93,
        "1month": 93
    }

    def __init__(self, api_key: str, return_format: str = "raw"):
        """
        Initialize the Glassnode API client.

        Args:
            api_key: API key for Glassnode API authentication
            
        Attributes:
            return_format_default (str): Default format for fetch_metric ('raw' or 'pandas').
                                       Can be changed after initialization.
        """
        self.api_key = api_key
        self.session = requests.Session()
        self.session.params = {"api_key": self.api_key} # Add API key globally for the session
        self.session.timeout = 30.0 # Set a timeout for requests
        self.return_format_default = return_format # Default return format for fetch_metric

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict:
        """
        Make a request to the Glassnode API.

        Args:
            endpoint: API endpoint path
            params: Optional query parameters

        Returns:
            Dict: Response data as a dictionary

        Raises:
            requests.exceptions.HTTPError: If the HTTP request returns an error status code
            ValueError: If the response is not valid JSON
        """
        if params is None:
            params = {}

        # Remove leading slash from endpoint if present
        endpoint = endpoint.lstrip("/")

        # Make request using the session
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status() # Raise exception for HTTP errors (4xx or 5xx)
            # Need to handle potential JSON decode error even on success for certain endpoints
            try:
                return response.json()
            except json.JSONDecodeError:
                 # If JSON fails, check if it's likely CSV (text/csv content type) or just return text
                if 'text/csv' in response.headers.get('Content-Type', ''):
                    return response.text # Return raw text for CSV
                else: # Otherwise, raise the JSON error
                    raise ValueError(f"Invalid JSON response: {response.text}")

        except requests.exceptions.RequestException as e:
            # Catch potential request errors (network, timeout, etc.)
            print(f"Request failed: {e}")
            raise


    # Metadata endpoints

    def get_assets_list(self) -> List[Dict]:
        """
        Get a list of all supported assets.

        Returns:
            List[Dict]: List of asset metadata objects
        """
        return self._make_request("metadata/assets")

    def get_metrics_list(self) -> List[Dict]:
        """
        Get a list of all available metrics.

        Returns:
            List[Dict]: List of available metric objects
        """
        return self._make_request("metadata/metrics")

    def get_metric_metadata(self, path: str, asset: Optional[str] = None) -> Dict:
        """
        Get metadata for a specific metric.

        Args:
            path: The metric path (e.g., "market/price_usd_close")
            asset: Optional asset ID to retrieve asset-specific metadata

        Returns:
            Dict: Metric metadata

        Raises:
            requests.exceptions.HTTPError: If the endpoint returns an error (e.g., invalid path)
            ValueError: If the response is not valid JSON
        """
        params = {"path": path}
        if asset:
            params["a"] = asset

        return self._make_request("metadata/metric", params)
        
    def _build_asset_metrics_map(self) -> Dict[str, List[str]]:
        """
        Internal helper to build the map of assets to their supported metrics.
        Fetches metadata for all metrics and extracts asset support.
        Includes rate limiting.

        Returns:
            Dict[str, List[str]]: A dictionary mapping asset symbols to lists of metric paths.
        """
        print("Generating asset-to-metrics mapping from API (this may take a while)...")
        all_metrics = self.get_metrics_list()
        asset_to_metrics_map = {}

        for i, metric_path in enumerate(all_metrics):
              # Print progress indicator
            if (i + 1) % 50 == 0 or i == len(all_metrics) - 1:
                 print(f"  Processed {i + 1}/{len(all_metrics)} metrics...")

            try:
                # Fetch metadata for the specific metric path
                metadata = self.get_metric_metadata(path=metric_path)
                # Extract assets under parameters -> 'a'
                supported_assets_list = metadata.get("parameters", {}).get("a", [])
                
                # Add a longer pause after every 500 requests (consider if needed with other rate limiting)
                if (i + 1) % 500 == 0:
                     print(f"  Pausing for 45 seconds after {i + 1} requests to respect rate limits...")
                     time.sleep(45)

                # Iterate through the list of asset symbols (strings)
                for asset_symbol in supported_assets_list:
                    if asset_symbol not in asset_to_metrics_map:
                        asset_to_metrics_map[asset_symbol] = []
                    if metric_path not in asset_to_metrics_map[asset_symbol]:
                        asset_to_metrics_map[asset_symbol].append(metric_path)

            except requests.exceptions.HTTPError as e:
                # Log error for a specific metric but continue with others
                print(f"Warning: Failed to fetch metadata for metric '{metric_path}'. Skipping. Error: {e}")
            except Exception as e: # Catch other potential errors
                print(f"Warning: An unexpected error occurred while processing metric '{metric_path}'. Skipping. Error: {e}")

        print(f"Finished processing all metrics. Found metrics for {len(asset_to_metrics_map)} assets.")
        return asset_to_metrics_map

    def get_asset_metrics(
        self,
        asset: str,
        use_cache: bool = True,
        cache_file: str = "asset_metrics_cache.json"
    ) -> List[str]:
        """
        Get a list of metric paths available for a specific asset.

        Uses caching to store the asset-to-metric mapping locally.

        Args:
            asset: The asset symbol (e.g., "BTC") to find metrics for.
            use_cache: If True (default), try to load from the cache file first.
                       If False or the file doesn't exist/is invalid, it regenerates
                       the mapping by calling the API and saves it to the cache file.
            cache_file: The path to the JSON file used for caching the mapping.

        Returns:
            List[str]: A list of metric paths available for the specified asset.
                       Returns an empty list if the asset is not found or has no metrics.

        Raises:
            requests.exceptions.HTTPError: If API requests fail during mapping generation.
            IOError: If there are issues reading or writing the cache file (warnings printed).
        """
        asset_to_metrics_map = None

        # Attempt to load from cache if requested
        if use_cache:
            asset_to_metrics_map = load_json_cache(cache_file)

        # If cache not used, loading failed, or file didn't exist, regenerate
        if asset_to_metrics_map is None:
            asset_to_metrics_map = self._build_asset_metrics_map()
            # Save the newly generated map (even if empty)
            save_json_cache(asset_to_metrics_map, cache_file)

        # Return the data for the requested asset from the map (loaded or generated)
        return asset_to_metrics_map.get(asset, [])


    # Data endpoints

    def fetch_metric(
        self,
        path: str,
        asset: str,
        since: Optional[Union[int, str, datetime.datetime]] = None,
        until: Optional[Union[int, str, datetime.datetime]] = None,
        interval: Optional[str] = "24h",
        format: Optional[str] = "json",
        currency: Optional[str] = None,
        return_format: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> Union[List[Dict], str, 'pd.DataFrame']:
        """
        Fetch data for a specific metric.

        Args:
            path: The metric path (e.g., "market/price_usd_close")
            asset: The asset symbol (e.g., "BTC")
            since: Optional start date as Unix timestamp, string date format, or datetime object
            until: Optional end date as Unix timestamp, string date format, or datetime object
            interval: Optional resolution interval (defaults to "24h")
            format: Response format ('json' or 'csv'). Note: 'pandas' return format works best with 'json'.
            currency: Optional currency for metrics that support it ("native" or "USD")
            return_format: Desired format for the returned data. Options:
                - "raw": Returns the raw response (List[Dict] for JSON, str for CSV).
                - "pandas": Returns a pandas DataFrame with a datetime index and a value column.
                  Requires pandas to be installed.
                - None (default): Uses the client's `return_format_default` attribute.
            limit: Optional number of data points to retrieve. If specified, it will calculate
                  the appropriate 'since' parameter to get exactly this many data points.
                  Always returns the most recent data points. Will be enforced to be at least 3.
            **kwargs: Additional parameters to pass to the API

        Returns:
            Union[List[Dict], str, pd.DataFrame]: Metric data in the specified format.
            - If the effective format is "raw", returns List[Dict] (for format="json") or str (for format="csv").
            - If the effective format is "pandas", returns a pandas DataFrame.
            
        Raises:
            ValueError: If the effective return_format is invalid, or if data conversion fails.
            
        Note:
            The effective return format is determined by the `return_format` parameter.
            If `return_format` is None, `self.return_format_default` is used.
            You can set `client.return_format_default = 'pandas'` to change the default behavior.
        """
        # Determine the effective return format
        effective_format = return_format if return_format is not None else self.return_format_default
        
        params = {"a": asset, "f": format, **kwargs}

        # Calculate the 'since' parameter based on 'limit', if provided
        if limit is not None and limit > 0:
            # When limit is specified, always use current time as 'until'
            until = int(time.time())
            
            # Calculate 'since' based on 'limit' to get the most recent datapoints
            since = calculate_since_for_limit(interval, limit)

        if since is not None:
            params["s"] = convert_to_unix_timestamp(since)

        if until is not None:
            params["u"] = convert_to_unix_timestamp(until)

        if interval is not None:
            params["i"] = interval

        if currency is not None:
            params["c"] = currency

        # Fetch the raw data
        path = path.lstrip("/")
        endpoint = f"metrics/{path}"
        raw_response = self._make_request(endpoint, params)

        # Process based on the effective format
        if effective_format == "pandas":
            try:
                # Attempt conversion using the utility function
                return convert_to_dataframe(raw_response, path)
            except ValueError as e:
                # Re-raise conversion errors with more context
                raise ValueError(f"Failed to convert data to DataFrame: {e}")
        elif effective_format == "raw":
            # Return the raw response as is
            return raw_response
        else:
            raise ValueError(f"Invalid effective return_format: '{effective_format}'. Must be 'raw' or 'pandas'.")

    def _paginated_bulk_fetch(
        self,
        path: str,
        params: Dict[str, Any],
        since: Optional[int] = None,
        until: int = None,
        interval: str = "24h"
    ) -> Dict:
        """
        Helper function to fetch bulk metric data with pagination. Handles both
        forward (since provided) and backward (since not provided) pagination.

        Args:
            path: The metric path (including "/bulk")
            params: Base parameters for the API request (excluding s and u)
            since: Optional start date as Unix timestamp
            until: End date as Unix timestamp
            interval: Resolution interval

        Returns:
            Dict: Combined results from multiple API calls
        """
        max_days = self.BULK_MAX_DAYS[interval]
        max_timerange = max_days * 24 * 60 * 60

        combined_result = {}
        combined_data = []
        empty_chunks_count = 0

        direction = "forward" if since is not None else "backward"

        if direction == "forward":
            current_since = since
            current_until = min(since + max_timerange, until)
        else: # backward
            current_until = until
            current_since = current_until - max_timerange

        while True:
            # Set time parameters for the current chunk
            params["s"] = current_since
            params["u"] = current_until

            try:
                chunk_result = self._make_request(f"metrics{path}", params)

                # Check if the result is empty or lacks data
                is_empty = not chunk_result or 'data' not in chunk_result or not chunk_result['data']

                if is_empty:
                    empty_chunks_count += 1
                    if empty_chunks_count >= 2: # Stop after two consecutive empty chunks
                        break
                else:
                    empty_chunks_count = 0 # Reset counter on successful fetch

                    # Store metadata from the first non-empty chunk
                    if not combined_result:
                        for key, value in chunk_result.items():
                            if key != 'data':
                                combined_result[key] = value

                    # Merge the data using the appropriate direction
                    combined_data = merge_bulk_data(combined_data, chunk_result['data'], direction)

            except Exception as e:
                print(f"Error fetching data chunk ({current_since}-{current_until}): {e}")
                # Optionally decide whether to break or continue on error
                break # Stop pagination on error

            # Update time window for the next iteration based on direction
            if direction == "forward":
                if current_until >= until: # Reached the final target date
                    break
                current_since = current_until + 1 # Use second after previous until
                current_until = min(current_since + max_timerange, until)
            else: # backward
                if current_since <= 0: # Arbitrary early cutoff or adjust as needed
                   break
                current_until = current_since - 1 # Use second before previous since
                current_since = current_until - max_timerange
                # Ensure we don't go negative if not desired, though API might handle it
                if current_since < 0: current_since = 0


        # Add the combined data to the final result
        combined_result['data'] = combined_data
        return combined_result

    def fetch_bulk_metric(
        self,
        path: str,
        assets: Optional[List[str]] = None,
        since: Optional[Union[int, str, datetime.datetime]] = None,
        until: Optional[Union[int, str, datetime.datetime]] = None,
        interval: str = "24h",
        currency: Optional[str] = "native",
        paginate: bool = False,
        return_format: Optional[str] = None,
        bulk_output_structure: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> Union[Dict, pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Fetch data for a metric using Glassnode's bulk endpoint.

        The bulk endpoint allows fetching data for multiple parameter combinations
        (like multiple assets) in a single API call. Note: Bulk endpoint always returns JSON.

        Args:
            path: The metric path without the "/bulk" suffix (e.g., "market/price_usd_close")
            assets: Optional list of asset symbols (e.g., ["BTC", "ETH"])
            since: Optional start date as Unix timestamp, string date format, or datetime object
            until: Optional end date as Unix timestamp, string date format, or datetime object
            interval: Resolution interval (defaults to "24h")
            currency: Optional currency for metrics that support it ("native" or "USD")
            paginate: If True, automatically handle pagination for large time ranges.
                      The returned DataFrame/Dict will contain combined data from all pages.
            return_format: Desired format for the returned data. Options:
                - "raw": Returns the raw JSON response as a dictionary.
                - "pandas": Returns processed data as pandas object(s). The specific structure
                          is determined by `bulk_output_structure`.
                - None (default): Uses the client's `return_format_default` attribute.
            bulk_output_structure: Specifies the structure when `return_format` is "pandas". Options:
                - "wide": Returns a single DataFrame with combined identifiers as columns.
                - "dict_by_asset": Returns a Dict[asset, DataFrame].
                - "dict_by_metric": Returns a Dict[metric_key, DataFrame].
                - None (default): Defaults to "wide".
            limit: Optional number of data points to retrieve. If specified, it will calculate
                  the appropriate 'since' parameter to get exactly this many data points.
                  Always returns the most recent data points. Will be enforced to be at least 3.
            **kwargs: Additional parameters to pass to the API

        Returns:
            Union[Dict, pd.DataFrame, Dict[str, pd.DataFrame]]: Bulk metric data.
            - If effective format is "raw", returns raw Dict.
            - If effective format is "pandas", returns pd.DataFrame or Dict[str, pd.DataFrame]
              based on `bulk_output_structure`.

        Raises:
            ValueError: If the metric does not support bulk operations, if the effective return_format
                      or bulk_output_structure is invalid, or if DataFrame conversion fails.
            requests.exceptions.HTTPError: For API request errors.
            ImportError: If return_format="pandas" is requested but pandas is not installed.

        Note:
            The response format differs from regular metrics. See:
            https://docs.glassnode.com/basic-api/bulk-metrics

            Timerange constraints by interval apply when paginate=False:
            - 10m and 1h resolutions: 10 days
            - 24h resolution: 31 days
            - 1w and 1month resolutions: 93 days
            
            The effective return format is determined by the `return_format` parameter.
            If `return_format` is None, `self.return_format_default` is used.
        """
        # Determine the effective return format
        effective_format = return_format if return_format is not None else self.return_format_default
        # Determine the effective bulk output structure (only relevant if returning pandas)
        effective_bulk_structure = bulk_output_structure if bulk_output_structure is not None else "wide"

        # Check if the metric supports bulk operations
        metadata = self.get_metric_metadata(path)
        if not metadata.get("bulk_supported", False):
            raise ValueError(f"Metric '{path}' does not support bulk operations")

        # Construct the bulk path
        bulk_path = f"{path}/bulk"

        params = {"i": interval, **kwargs}

        # Add multiple asset parameters if provided
        if assets:
            params["a"] = assets

        if currency is not None:
            params["c"] = currency

        # Calculate the 'since' parameter based on 'limit', if provided
        if limit is not None and limit > 0:
            # When limit is specified, always use current time as 'until'
            until_ts = int(time.time())
            
            # Calculate 'since' based on 'limit' to get the most recent datapoints
            since = calculate_since_for_limit(interval, limit)
            # When using limit, we'll disable pagination as we know exactly what range we want
            paginate = False
        else:
            # Set until timestamp
            until_ts = int(time.time()) if until is None else convert_to_unix_timestamp(until)

        # --- Fetch Raw Data (Handle Pagination) --- 
        raw_response: Dict
        if paginate:
            # Convert since to timestamp if provided
            since_ts = convert_to_unix_timestamp(since) if since is not None else None
            
            # Paginated fetch always returns a dictionary (combined raw data)
            raw_response = self._paginated_bulk_fetch(
                bulk_path,
                params,
                since=since_ts,
                until=until_ts,
                interval=interval
            )
        else:
            # Non-paginated request
            max_days = self.BULK_MAX_DAYS[interval]
            max_timerange = max_days * 24 * 60 * 60

            # Calculate appropriate since timestamp
            if since is None:
                since_ts = until_ts - max_timerange
            else:
                since_ts = convert_to_unix_timestamp(since)
                if (until_ts - since_ts) > max_timerange:
                    since_ts = until_ts - max_timerange
                    print(f"Warning: Requested timerange for interval '{interval}' exceeds {max_days} days. Adjusting 'since' timestamp.")

            params["s"] = since_ts
            params["u"] = until_ts

            # Single request returns a dictionary
            raw_response = self._make_request(f"metrics{bulk_path}", params)
        # --- End Fetch Raw Data --- 

        # --- Process based on effective format --- 
        if effective_format == "pandas":
            try:
                # Use the dedicated bulk conversion function with the specified structure
                return convert_bulk_to_dataframe(
                    raw_response,
                    output_structure=effective_bulk_structure
                )
            except ValueError as e:
                # Add context about the requested structure to the error
                raise ValueError(f"Failed to convert bulk data to DataFrame (structure='{effective_bulk_structure}'): {e}")
        elif effective_format == "raw":
            return raw_response
        else:
             raise ValueError(f"Invalid effective return_format: '{effective_format}'. Must be 'raw' or 'pandas'.") 
