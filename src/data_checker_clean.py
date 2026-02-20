import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

def dtype_map(df):
    dtype_mapping = {"Gen_Bear_Temp_Avg": float,
                 "Gen_Phase1_Temp_Avg": float,
                 "Gen_Phase2_Temp_Avg": float,
                 "Gen_Phase3_Temp_Avg": float,
                 "Amb_Temp_Avg":float,
                 "Cont_VCP_ChokcoilTemp_Avg":float}
    df= df.astype(dtype_mapping)
    return df

# Dedup timestamps basic based on turbine
# Remove duplicates within each turbine group
def remove_duplicate_timestamps(df, timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                               keep='first'):
    """
    Remove duplicate timestamps within each turbine group.
    
    Parameters:
    -----------
    df : pandas DataFrame
        Your dataset
    timestamp_col : str
        Name of the timestamp column
    turbine_col : str
        Name of the turbine ID column
    keep : str
        Which duplicate to keep: 'first', 'last', or False (remove all duplicates)
    
    Returns:
    --------
    df_cleaned : DataFrame with duplicates removed
    duplicate_info : DataFrame with information about removed duplicates
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    print(f"Total rows before cleaning: {len(df)}")
    
    # Find duplicates within each turbine group
    duplicate_info = []
    df_cleaned_list = []
    
    for turbine_id in sorted(df[turbine_col].unique()):
        turbine_data = df[df[turbine_col] == turbine_id].copy()
        
        # Find duplicates for this turbine
        duplicates_mask = turbine_data.duplicated(subset=[timestamp_col], keep=False)
        n_duplicates = duplicates_mask.sum()
        
        if n_duplicates > 0:
            print(f"Turbine {turbine_id}: {n_duplicates} duplicate timestamp rows")
            duplicate_info.append({
                'turbine_id': turbine_id,
                'duplicate_rows': n_duplicates,
                'unique_duplicate_timestamps': turbine_data[duplicates_mask][timestamp_col].nunique()
            })
        
        # Remove duplicates for this turbine
        turbine_cleaned = turbine_data.drop_duplicates(subset=[timestamp_col], keep=keep)
        df_cleaned_list.append(turbine_cleaned)
    
    # Combine all cleaned turbine data
    df_cleaned = pd.concat(df_cleaned_list, ignore_index=True)
    df_cleaned = df_cleaned.sort_values([turbine_col, timestamp_col])
    
    print(f"\nTotal rows after cleaning: {len(df_cleaned)}")
    print(f"Rows removed: {len(df) - len(df_cleaned)}")
    
    duplicate_summary = pd.DataFrame(duplicate_info) if duplicate_info else None
    
    return df_cleaned, duplicate_summary

# Helper function: Find gaps (consecutive missing periods)
def find_timestamp_gaps(df, timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                       freq='10T', min_gap_size=2):
    """
    Find gaps (consecutive missing timestamps) in the time series.
    
    Parameters:
    -----------
    min_gap_size : int
        Minimum number of consecutive missing timestamps to report as a gap
    
    Returns:
    --------
    gaps_df : DataFrame with start_time, end_time, and duration of each gap
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df = df.sort_values([turbine_col, timestamp_col])
    
    gaps_list = []
    
    for turbine_id in sorted(df[turbine_col].unique()):
        turbine_data = df[df[turbine_col] == turbine_id].copy()
        turbine_data = turbine_data.sort_values(timestamp_col)
        
        # Get time range
        min_time = turbine_data[timestamp_col].min()
        max_time = turbine_data[timestamp_col].max()
        
        # Create expected range
        expected_timestamps = pd.date_range(start=min_time, end=max_time, freq=freq)
        existing_timestamps = set(turbine_data[timestamp_col])
        missing_timestamps = sorted([ts for ts in expected_timestamps if ts not in existing_timestamps])
        
        # Find consecutive gaps
        if len(missing_timestamps) > 0:
            gap_start = missing_timestamps[0]
            gap_end = missing_timestamps[0]
            gap_count = 1
            
            for i in range(1, len(missing_timestamps)):
                # Check if this timestamp is consecutive to the previous
                expected_next = gap_end + pd.Timedelta(freq)
                if missing_timestamps[i] == expected_next:
                    gap_end = missing_timestamps[i]
                    gap_count += 1
                else:
                    # Gap ended, record it if it meets minimum size
                    if gap_count >= min_gap_size:
                        gaps_list.append({
                            'turbine_id': turbine_id,
                            'gap_start': gap_start,
                            'gap_end': gap_end,
                            'missing_records': gap_count,
                            'duration': gap_end - gap_start
                        })
                    # Start new gap
                    gap_start = missing_timestamps[i]
                    gap_end = missing_timestamps[i]
                    gap_count = 1
            
            # Record the last gap if it meets minimum size
            if gap_count >= min_gap_size:
                gaps_list.append({
                    'turbine_id': turbine_id,
                    'gap_start': gap_start,
                    'gap_end': gap_end,
                    'missing_records': gap_count,
                    'duration': gap_end - gap_start
                })
    
    gaps_df = pd.DataFrame(gaps_list) if gaps_list else pd.DataFrame()

    if len(gaps_df) > 0:
        print("=" * 80)
        print(f"DATA GAPS (consecutive missing periods of {min_gap_size}+ records)")
        print("=" * 80)
        print(gaps_df.to_string(index=False))
        print(f"\nTotal gaps found: {len(gaps_df)}")
    else:
        print(f"No gaps of {min_gap_size}+ consecutive missing records found.")
    
    return gaps_df


def detect_missing_timestamps(df, timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                             freq='10min', fill_missing=False, 
                             show_summary=True, show_details=False):
    """
    Detect missing timestamps in time series data grouped by turbine.
    
    Parameters:
    -----------
    df : pandas DataFrame
        Your dataset
    timestamp_col : str
        Name of the timestamp column
    turbine_col : str
        Name of the turbine ID column
    freq : str
        Expected frequency of timestamps using pandas offset aliases:
        - '10s', '30s' (seconds)
        - '5min', '10min' (minutes)
        - '1h' (hours)
        - '1D' (calendar day)
    fill_missing : bool
        If True, fill in missing timestamps with null values for other columns
    show_summary : bool
        If True, print summary of missing timestamps
    show_details : bool
        If True, print detailed list of missing timestamps for each turbine
    
    Returns:
    --------
    df_result : DataFrame (original or with filled timestamps)
    missing_summary : DataFrame with summary of missing timestamps per turbine
    missing_details : dict with detailed missing timestamp info per turbine
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df = df.sort_values([turbine_col, timestamp_col])
    
    missing_summary_list = []
    missing_details_dict = {}
    df_filled_list = []
    
    for turbine_id in sorted(df[turbine_col].unique()):
        turbine_data = df[df[turbine_col] == turbine_id].copy()
        turbine_data = turbine_data.sort_values(timestamp_col)
        
        # Get the time range for this turbine
        min_time = turbine_data[timestamp_col].min()
        max_time = turbine_data[timestamp_col].max()
        
        # Create complete timestamp range
        expected_timestamps = pd.date_range(start=min_time, end=max_time, freq=freq)
        
        # Find missing timestamps
        existing_timestamps = set(turbine_data[timestamp_col])
        missing_timestamps = [ts for ts in expected_timestamps if ts not in existing_timestamps]
        
        n_expected = len(expected_timestamps)
        n_existing = len(existing_timestamps)
        n_missing = len(missing_timestamps)
        pct_missing = (n_missing / n_expected * 100) if n_expected > 0 else 0
        
        # Store summary
        missing_summary_list.append({
            'turbine_id': turbine_id,
            'start_time': min_time,
            'end_time': max_time,
            'expected_records': n_expected,
            'actual_records': n_existing,
            'missing_records': n_missing,
            'missing_pct': round(pct_missing, 2)
        })
        
        # Store details
        missing_details_dict[turbine_id] = sorted(missing_timestamps)
        
        # Fill missing timestamps if requested
        if fill_missing and n_missing > 0:
            # Create dataframe with missing timestamps
            missing_df = pd.DataFrame({timestamp_col: missing_timestamps})
            missing_df[turbine_col] = turbine_id
            
            # Add null columns for all other columns in original data
            for col in turbine_data.columns:
                if col not in [timestamp_col, turbine_col]:
                    missing_df[col] = np.nan
            
            # Combine with existing data
            turbine_filled = pd.concat([turbine_data, missing_df], ignore_index=True)
            turbine_filled = turbine_filled.sort_values(timestamp_col)
            df_filled_list.append(turbine_filled)
        else:
            df_filled_list.append(turbine_data)
    
    # Create summary dataframe
    missing_summary = pd.DataFrame(missing_summary_list)
    
    # Print summary if requested
    if show_summary:
        print("=" * 80)
        print("MISSING TIMESTAMPS SUMMARY")
        print("=" * 80)
        print(f"Expected frequency: {freq}")
        print()
        print(missing_summary.to_string(index=False))
        print()
        print(f"Total expected records across all turbines: {missing_summary['expected_records'].sum()}")
        print(f"Total actual records: {missing_summary['actual_records'].sum()}")
        print(f"Total missing records: {missing_summary['missing_records'].sum()}")
        print(f"Overall missing percentage: {missing_summary['missing_records'].sum() / missing_summary['expected_records'].sum() * 100:.2f}%")
    
    # Print details if requested
    if show_details:
        print("\n" + "=" * 80)
        print("DETAILED MISSING TIMESTAMPS")
        print("=" * 80)
        for turbine_id, missing_times in missing_details_dict.items():
            if len(missing_times) > 0:
                print(f"\nTurbine {turbine_id}: {len(missing_times)} missing timestamps")
                if len(missing_times) <= 20:
                    for ts in missing_times:
                        print(f"  {ts}")
                else:
                    print(f"  First 10:")
                    for ts in missing_times[:10]:
                        print(f"    {ts}")
                    print(f"  ...")
                    print(f"  Last 10:")
                    for ts in missing_times[-10:]:
                        print(f"    {ts}")
    
    # Combine all turbine data
    df_result = pd.concat(df_filled_list, ignore_index=True)
    df_result = df_result.sort_values([turbine_col, timestamp_col])
    
    if fill_missing:
        print(f"\n✓ Missing timestamps filled. DataFrame now has {len(df_result)} rows (was {len(df)})")
    
    return df_result, missing_summary, missing_details_dict
