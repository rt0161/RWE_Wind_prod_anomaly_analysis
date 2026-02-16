import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

def plot_timeseries(df, column_to_plot, timestamp_col='TTimeStamp', 
                    turbine_col='TURBINE_ID', figsize=(12, 6)):
    """
    Simple overlay plot of time series data grouped by turbine ID.
    """
    # Prepare data
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    df_sorted = df.sort_values(timestamp_col)
    
    # Get sorted unique turbine IDs
    turbine_ids = sorted(df_sorted[turbine_col].unique())
    
    
    # Create plot
    plt.figure(figsize=figsize)

    # Create color map - consistent colors for each turbine
    colors = plt.cm.tab10(range(len(turbine_ids)))
    color_map = dict(zip(turbine_ids, colors))
    
    for turbine_id in turbine_ids:
        turbine_data = df_sorted[df_sorted[turbine_col] == turbine_id]
        plt.plot(turbine_data[timestamp_col], 
                 turbine_data[column_to_plot],
                 label=f'Turbine {turbine_id}',
                 color=color_map[turbine_id])
    
    plt.xlabel('Timestamp')
    plt.ylabel(column_to_plot)
    plt.title(f'{column_to_plot} over Time')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
