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