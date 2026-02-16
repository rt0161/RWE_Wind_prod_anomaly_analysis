import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

def add_difference_column(df, column1, column2, new_column_name='diff_pwr',
                         difference_type='absolute', turbine_col='TURBINE_ID'):
    """
    Add a difference column to the dataframe, preserving grouping by turbine.
    
    Parameters:
    -----------
    difference_type : str
        - 'absolute': column1 - column2
        - 'relative': (column1 - column2) / column2 * 100
        - 'ratio': column1 / column2
    """
    df = df.copy()
    
    if difference_type == 'absolute':
        df[new_column_name] = df[column1] - df[column2]
    elif difference_type == 'relative':
        df[new_column_name] = (df[column1] - df[column2]) / df[column2] * 100
    elif difference_type == 'ratio':
        df[new_column_name] = df[column1] / df[column2]
    else:
        raise ValueError("difference_type must be 'absolute', 'relative', or 'ratio'")
    
    return df


def scan_turbine_anomalies(df, timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                          value_col='diff_pwr', abs_threshold=None, 
                          deviation_threshold=2.0, deviation_method='std'):
    """
    Scan time series data to detect anomalies using sequential criteria:
    1. FIRST: Check if |diff_pwr| > abs_threshold
    2. THEN: If step 1 passes, check if value deviates from other turbines at same timestamp
    3. BOTH must be true to mark as anomaly
    
    Parameters:
    -----------
    df : pandas DataFrame
        dataset with columns: TURBINE_ID, TTimeStamp, diff_pwr
    timestamp_col : str
        Name of the timestamp column
    turbine_col : str
        Name of the turbine ID column
    value_col : str
        Name of the value column to analyze (e.g., 'diff_pwr')
    abs_threshold : float, optional
        Absolute value threshold. Values where |diff_pwr| > threshold are flagged.
        If None, only deviation-based detection is used.
    deviation_threshold : float
        Number of standard deviations (or MAD) from median to consider anomalous.
        Default: 2.0
    deviation_method : str
        Method for deviation calculation:
        - 'std': Use standard deviation from mean
        - 'mad': Use Median Absolute Deviation from median (more robust)
    
    Returns:
    --------
    Returns:
    --------
    df_detection : DataFrame with columns [TURBINE_ID, TTimeStamp, detection]
        - detection = 0 if either criterion not met
        - detection = diff_pwr value if BOTH criteria met
    anomaly_stats : dict with statistics about detected anomalies
    """
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df = df.sort_values([timestamp_col, turbine_col])
    
    # Initialize detection column
    df['detection'] = 0.0
    
    anomaly_count = 0
    abs_threshold_pass = 0
    deviation_pass = 0
    both_criteria_met = 0
    
    # Group by timestamp to compare across turbines
    for timestamp, group in df.groupby(timestamp_col):
        turbine_values = group.set_index(turbine_col)[value_col]
        abs_values = turbine_values.abs()
        
        for turbine_id in turbine_values.index:
            value = turbine_values.loc[turbine_id]
            abs_value = abs(value)
            
            # STEP 1: Check absolute threshold
            if abs_value <= abs_threshold:
                # Failed first check, set detection to 0 and continue
                mask = (df[timestamp_col] == timestamp) & (df[turbine_col] == turbine_id)
                df.loc[mask, 'detection'] = 0.0
                continue
            
            # Passed step 1
            abs_threshold_pass += 1
            
            # STEP 2: Check deviation from other turbines at same timestamp
            passes_deviation = False
            
            if len(abs_values) > 1:  # Need at least 2 turbines to compare
                if deviation_method == 'mad':
                    # Use median and Median Absolute Deviation (more robust)
                    median_val = abs_values.median()
                    mad = (abs_values - median_val).abs().median()
                    if mad > 0:
                        # Modified z-score
                        modified_z = abs((abs_value - median_val) / (1.4826 * mad))
                        if modified_z > deviation_threshold:
                            passes_deviation = True
                    elif abs_value > median_val:
                        # If MAD is 0 (all values same), check if this one is different
                        passes_deviation = True
                
                elif deviation_method == 'std':
                    # Use mean and standard deviation
                    mean_val = abs_values.mean()
                    std_val = abs_values.std()
                    if std_val > 0:
                        z_score = abs((abs_value - mean_val) / std_val)
                        if z_score > deviation_threshold:
                            passes_deviation = True
                    elif abs_value > mean_val:
                        passes_deviation = True
                
            else:
                # Only one turbine at this timestamp, can't compare
                # You can choose to either:
                # Option A: Mark as anomaly since absolute threshold is met
                passes_deviation = True
                # Option B: Don't mark as anomaly since we can't verify deviation
                # passes_deviation = False
            
            if passes_deviation:
                deviation_pass += 1
            
            # BOTH criteria must be met
            if passes_deviation:
                mask = (df[timestamp_col] == timestamp) & (df[turbine_col] == turbine_id)
                df.loc[mask, 'detection'] = value
                both_criteria_met += 1
            else:
                mask = (df[timestamp_col] == timestamp) & (df[turbine_col] == turbine_id)
                df.loc[mask, 'detection'] = 0.0
    
    # Create output dataframe
    df_detection = df[[turbine_col, timestamp_col, 'detection']].copy()
    
    # Calculate statistics
    total_records = len(df)
    anomaly_stats = {
        'total_records': total_records,
        'abs_threshold_pass': abs_threshold_pass,
        'deviation_pass': deviation_pass,
        'both_criteria_met': both_criteria_met,
        'anomaly_percentage': (both_criteria_met / total_records * 100) if total_records > 0 else 0,
        'parameters': {
            'abs_threshold': abs_threshold,
            'deviation_threshold': deviation_threshold,
            'deviation_method': deviation_method
        }
    }
    
    # Print summary
    print("=" * 80)
    print("ANOMALY DETECTION SUMMARY (Sequential Logic)")
    print("=" * 80)
    print(f"Total records analyzed: {total_records}")
    print()
    print(f"Step 1 - Absolute threshold (|diff_pwr| > {abs_threshold}):")
    print(f"  Records passing: {abs_threshold_pass} ({abs_threshold_pass/total_records*100:.2f}%)")
    print()
    print(f"Step 2 - Deviation check (on records from Step 1):")
    print(f"  Records passing: {deviation_pass} ({deviation_pass/abs_threshold_pass*100:.2f}% of Step 1)" if abs_threshold_pass > 0 else "  Records passing: 0")
    print()
    print(f"BOTH criteria met (Final anomalies): {both_criteria_met} ({both_criteria_met/total_records*100:.2f}% of total)")
    print()
    print(f"Parameters:")
    print(f"  - Absolute threshold: {abs_threshold}")
    print(f"  - Deviation threshold: {deviation_threshold}")
    print(f"  - Deviation method: {deviation_method}")
    
    # Anomalies by turbine
    anomalies_by_turbine = df_detection[df_detection['detection'] != 0].groupby(turbine_col).size()
    if len(anomalies_by_turbine) > 0:
        print()
        print("Anomalies by turbine:")
        for turbine_id, count in anomalies_by_turbine.sort_values(ascending=False).items():
            pct = (count / len(df[df[turbine_col] == turbine_id]) * 100)
            print(f"  Turbine {turbine_id}: {count} anomalies ({pct:.2f}%)")
    else:
        print()
        print("No anomalies detected with current thresholds.")
    
    return df_detection, anomaly_stats


# Helper function: Visualize anomalies for specific turbines
def visualize_anomalies(df_original, df_detection, turbine_ids,
                       timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                       value_col='diff_pwr', figsize=(14, 10)):
    """
    Visualize the original values and detected anomalies for selected turbines.
    """
    
    n_turbines = len(turbine_ids)
    n_cols = 1
    n_rows = n_turbines
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize, sharex=True)
    if n_turbines == 1:
        axes = [axes]
    
    # Get consistent colors
    colors = plt.cm.tab10(range(len(turbine_ids)))
    color_map = dict(zip(sorted(turbine_ids), colors))
    
    for idx, turbine_id in enumerate(sorted(turbine_ids)):
        # Get data for this turbine
        turbine_original = df_original[df_original[turbine_col] == turbine_id].sort_values(timestamp_col)
        turbine_detection = df_detection[df_detection[turbine_col] == turbine_id].sort_values(timestamp_col)
        
        # Plot original values
        axes[idx].plot(turbine_original[timestamp_col], turbine_original[value_col],
                      color=color_map[turbine_id], linewidth=1, alpha=0.7,
                      label='diff_pwr')
        
        # Highlight anomalies
        anomalies = turbine_detection[turbine_detection['detection'] != 0]
        if len(anomalies) > 0:
            axes[idx].scatter(anomalies[timestamp_col], anomalies['detection'],
                            color='red', s=5, zorder=5, alpha=0.8,
                            label=f'Anomalies ({len(anomalies)})')
        
        axes[idx].axhline(y=0, color='gray', linestyle='--', alpha=0.3, linewidth=1)
        axes[idx].set_title(f'Turbine {turbine_id}', fontsize=12, fontweight='bold')
        axes[idx].set_ylabel(value_col, fontsize=10)
        axes[idx].grid(True, alpha=0.3)
        axes[idx].legend(loc='upper right')
    
    axes[-1].set_xlabel(timestamp_col, fontsize=10)
    fig.suptitle('Anomaly Detection Results', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()


# Helper function: Get detailed anomaly report
def get_anomaly_report(df_original, df_detection, 
                      timestamp_col='TTimeStamp', turbine_col='TURBINE_ID',
                      value_col='diff_pwr', top_n=20):
    """
    Get detailed report of detected anomalies.
    """
    # Merge to get original values
    anomalies = df_detection[df_detection['detection'] != 0].copy()
    anomalies = anomalies.merge(
        df_original[[turbine_col, timestamp_col, value_col]],
        on=[turbine_col, timestamp_col],
        how='left'
    )
    
    # Sort by absolute value of detection
    anomalies['abs_detection'] = anomalies['detection'].abs()
    anomalies = anomalies.sort_values('abs_detection', ascending=False)
    
    print("=" * 80)
    print(f"TOP {top_n} ANOMALIES (by absolute value)")
    print("=" * 80)
    
    display_cols = [turbine_col, timestamp_col, value_col, 'detection']
    print(anomalies[display_cols].head(top_n).to_string(index=False))
    
    return anomalies


# Helper function: Find timestamps with multiple turbine anomalies
def find_synchronized_anomalies(df_detection, timestamp_col='TTimeStamp',
                               turbine_col='TURBINE_ID', min_turbines=2):
    """
    Find timestamps where multiple turbines have anomalies simultaneously.
    """
    anomalies = df_detection[df_detection['detection'] != 0].copy()
    
    # Count anomalies per timestamp
    anomaly_counts = anomalies.groupby(timestamp_col).size()
    synchronized = anomaly_counts[anomaly_counts >= min_turbines]
    
    if len(synchronized) > 0:
        print("=" * 80)
        print(f"SYNCHRONIZED ANOMALIES ({min_turbines}+ turbines)")
        print("=" * 80)
        
        for timestamp in synchronized.index:
            turbines_at_time = anomalies[anomalies[timestamp_col] == timestamp]
            print(f"\nTimestamp: {timestamp}")
            print(f"  Turbines affected: {len(turbines_at_time)}")
            print(turbines_at_time[[turbine_col, 'detection']].to_string(index=False))
    else:
        print(f"No synchronized anomalies found ({min_turbines}+ turbines)")
    
    return synchronized


# Usage examples:

if __name__ == "__main__":
    # Load your data
    df = pd.read_csv('your_data.csv')
    
    print("=" * 80)
    print("Absolute threshold and deviation")
    print("=" * 80)
    df_detection, stats = scan_turbine_anomalies(
        df,
        abs_threshold=100.0,      # Flag if |diff_pwr| > 100
        deviation_threshold=2.0,  # Flag if 2+ std deviations from mean
        deviation_method='std'
    )


    # Visualize results for selected turbines
    visualize_anomalies(df, df_detection, turbine_ids=turbine_ids) #turbine_ids=["T001", "T003", "T004", "T005", "T008", "T013"]

    # Get detailed report
    anomaly_details = get_anomaly_report(df, df_detection, top_n=20)

    # Find synchronized anomalies
    synchronized = find_synchronized_anomalies(df_detection, min_turbines=3)

    #Save results
    df_detection.to_csv('anomaly_detection_results.csv', index=False)
    print("\n Detection results saved to 'anomaly_detection_results.csv'")

    #Export only anomalies
    anomalies_only = df_detection[df_detection['detection'] != 0]
    anomalies_only.to_csv('anomalies_only.csv', index=False)
    print("\n Anomalies saved to 'anomalies_only.csv'")