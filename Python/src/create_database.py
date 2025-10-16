#!/usr/bin/env python3
"""
Fly Sleep Behavior Database Creator

This script creates a RELATIONAL database structure to avoid redundancy when storing
millions of time-series measurements. The design separates:

1. TIME_SERIES_DATA: Raw measurements (millions of rows)
   - Only stores: datetime, monitor, channel, mt, ct, pn
   - No fly metadata (genotype, sex, treatment) to avoid duplication

2. FLY_METADATA: Fly information (64 rows)
   - Stores: monitor, channel, fly_id, genotype, sex, treatment
   - One row per fly, not per timepoint

The tables are linked via (monitor, channel) foreign key relationship.
When analysis is needed, JOIN the tables to get complete information.

Benefits:
- No redundancy: genotype stored once, not millions of times
- Small file sizes: time_series stays lean
- Easy updates: change fly info in one place
- Professional database design
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys


def parse_details(filepath):
    """
    Parse details.txt to extract fly metadata.
    
    Creates TABLE 2: fly_metadata
    - One row per fly (not per timepoint)
    - Stores genotype, sex, treatment ONCE per fly
    - Links to time_series via (monitor, channel)
    
    Args:
        filepath (str): Path to details.txt file
        
    Returns:
        pd.DataFrame: fly_metadata with columns:
            monitor, channel, fly_id, genotype, sex, treatment
    """
    print(f"📋 Parsing metadata from {filepath}...")
    
    # Read the details file
    df = pd.read_csv(filepath, sep='\t')
    
    # Clean up the data
    df['monitor'] = df['Monitor'].astype(int)
    df['channel'] = df['Channel'].str.replace('ch', '').astype(int)
    df['genotype'] = df['Genotype']
    df['sex'] = df['Sex']
    df['treatment'] = df['Treatment']
    
    # Create fly_id: M{monitor}_Ch{channel:02d}
    df['fly_id'] = df.apply(lambda row: f"M{row['monitor']}_Ch{row['channel']:02d}", axis=1)
    
    # Select and reorder columns
    fly_metadata = df[['monitor', 'channel', 'fly_id', 'genotype', 'sex', 'treatment']].copy()
    
    # Remove rows with NA values (empty channels)
    fly_metadata = fly_metadata[fly_metadata['genotype'] != 'NA'].copy()
    
    print(f"✅ Parsed {len(fly_metadata)} flies from metadata")
    print(f"   Monitors: {sorted(fly_metadata['monitor'].unique())}")
    print(f"   Genotypes: {list(fly_metadata['genotype'].unique())}")
    print(f"   Treatments: {list(fly_metadata['treatment'].unique())}")
    
    return fly_metadata


def parse_monitor_file(filepath, monitor_num):
    """
    Parse one Monitor*.txt file to extract time-series data.
    
    Creates TABLE 1: time_series_data (for one monitor)
    - Millions of rows of measurements
    - Only stores: datetime, monitor, channel, mt, ct, pn
    - No fly metadata to avoid redundancy
    
    Args:
        filepath (str): Path to Monitor*.txt file
        monitor_num (int): Monitor number (5 or 6)
        
    Returns:
        pd.DataFrame: time_series_data with columns:
            datetime, monitor, channel, mt, ct, pn
    """
    print(f"📊 Parsing time-series data from {filepath} (Monitor {monitor_num})...")
    
    # Read the monitor file
    df = pd.read_csv(filepath, sep='\t', header=None)
    
    # Define column names based on the data structure
    # Columns: ID, date, time, port, [unknowns], movement_type, 0, 0, [32 channel values]
    columns = ['id', 'date', 'time', 'port', 'unknown1', 'unknown2', 'unknown3', 'movement_type', 'zero1', 'zero2']
    # Add 32 channel columns (channels 1-32)
    for i in range(1, 33):
        columns.append(f'ch{i}')
    
    df.columns = columns
    
    # Parse datetime
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], format='%d %b %y %H:%M:%S')
    
    # Filter for the three movement types: MT, CT, Pn
    movement_types = ['MT', 'CT', 'Pn']
    df_filtered = df[df['movement_type'].isin(movement_types)].copy()
    
    print(f"   Found {len(df_filtered)} rows with movement data")
    print(f"   Date range: {df_filtered['datetime'].min()} to {df_filtered['datetime'].max()}")
    print(f"   Movement types: {df_filtered['movement_type'].unique()}")
    
    # Reshape from wide to long format
    # Each timestamp has 3 rows (MT, CT, Pn), we want one row per channel per timestamp
    time_series_list = []
    
    # Group by timestamp (id, date, time)
    for (timestamp_id, date, time), group in df_filtered.groupby(['id', 'date', 'time']):
        datetime_val = group['datetime'].iloc[0]
        
        # Get the three movement types for this timestamp
        mt_data = group[group['movement_type'] == 'MT'].iloc[0] if 'MT' in group['movement_type'].values else None
        ct_data = group[group['movement_type'] == 'CT'].iloc[0] if 'CT' in group['movement_type'].values else None
        pn_data = group[group['movement_type'] == 'Pn'].iloc[0] if 'Pn' in group['movement_type'].values else None
        
        # Extract channel values (columns 10-41, which are ch1-ch32)
        for channel in range(1, 33):
            channel_col = f'ch{channel}'
            
            # Get values for each movement type
            mt_val = mt_data[channel_col] if mt_data is not None else 0
            ct_val = ct_data[channel_col] if ct_data is not None else 0
            pn_val = pn_data[channel_col] if pn_data is not None else 0
            
            # Only include if at least one value is non-zero (active channel)
            if mt_val > 0 or ct_val > 0 or pn_val > 0:
                time_series_list.append({
                    'datetime': datetime_val,
                    'monitor': monitor_num,
                    'channel': channel,
                    'mt': int(mt_val),
                    'ct': int(ct_val),
                    'pn': int(pn_val)
                })
    
    time_series_df = pd.DataFrame(time_series_list)
    
    print(f"✅ Created {len(time_series_df)} time-series records for Monitor {monitor_num}")
    print(f"   Channels with data: {list(time_series_df['channel'].unique())}")
    print(f"   Date range: {time_series_df['datetime'].min()} to {time_series_df['datetime'].max()}")
    
    return time_series_df


def main():
    """
    Main function to create the relational database.
    
    Creates two separate CSV files:
    1. data/processed/fly_metadata.csv - Fly information (TABLE 2)
    2. data/processed/time_series_data.csv - Time-series measurements (TABLE 1)
    
    Demonstrates how the tables connect via JOIN operation.
    """
    print("🚀 Starting Fly Sleep Behavior Database Creation")
    print("=" * 60)
    
    # Ensure output directory exists
    os.makedirs('../data/processed', exist_ok=True)
    
    # PART 1: Parse metadata (TABLE 2)
    print("\n📋 PART 1: Creating fly_metadata table")
    print("-" * 40)
    
    fly_metadata = parse_details('../details.txt')
    
    # Save metadata table
    metadata_path = '../data/processed/fly_metadata.csv'
    fly_metadata.to_csv(metadata_path, index=False)
    print(f"💾 Saved fly_metadata to {metadata_path}")
    
    # PART 2: Parse time-series data (TABLE 1)
    print("\n📊 PART 2: Creating time_series_data table")
    print("-" * 40)
    
    # Parse both monitor files
    monitor5_data = parse_monitor_file('../Monitor5.txt', 5)
    monitor6_data = parse_monitor_file('../Monitor6.txt', 6)
    
    # Combine into single time-series table
    time_series_data = pd.concat([monitor5_data, monitor6_data], ignore_index=True)
    
    # Sort by datetime for better organization
    time_series_data = time_series_data.sort_values(['datetime', 'monitor', 'channel']).reset_index(drop=True)
    
    # Save time-series table
    timeseries_path = '../data/processed/time_series_data.csv'
    time_series_data.to_csv(timeseries_path, index=False)
    print(f"💾 Saved time_series_data to {timeseries_path}")
    
    # PART 3: Summary statistics
    print("\n📈 PART 3: Database Summary")
    print("-" * 40)
    
    print(f"Fly metadata table:")
    print(f"  - Rows: {len(fly_metadata)}")
    print(f"  - Columns: {list(fly_metadata.columns)}")
    print(f"  - File size: {os.path.getsize(metadata_path) / 1024:.1f} KB")
    
    print(f"\nTime-series data table:")
    print(f"  - Rows: {len(time_series_data):,}")
    print(f"  - Columns: {list(time_series_data.columns)}")
    print(f"  - File size: {os.path.getsize(timeseries_path) / (1024*1024):.1f} MB")
    print(f"  - Date range: {time_series_data['datetime'].min()} to {time_series_data['datetime'].max()}")
    print(f"  - Monitors: {list(time_series_data['monitor'].unique())}")
    print(f"  - Channels per monitor: {time_series_data.groupby('monitor')['channel'].nunique().to_dict()}")
    
    # PART 4: Demonstrate relational JOIN
    print("\n🔗 PART 4: Demonstrating Relational JOIN")
    print("-" * 40)
    print("This shows how the two tables connect to provide complete information:")
    
    print("\nTime series table (first 10 rows):")
    print(time_series_data.head(10))
    
    print("\nMetadata table:")
    print(fly_metadata.head(10))
    
    print("\nJoined data (first 10 rows):")
    joined = time_series_data.merge(fly_metadata, on=['monitor', 'channel'])
    print(joined.head(10))
    
    print(f"\nJoin statistics:")
    print(f"  - Time series rows: {len(time_series_data):,}")
    print(f"  - Metadata rows: {len(fly_metadata)}")
    print(f"  - Joined rows: {len(joined):,} (same as time series - each measurement gets fly info)")
    
    # Show some analysis examples
    print(f"\n📊 Analysis Examples:")
    print(f"  - Total measurements per genotype:")
    genotype_counts = joined.groupby('genotype').size().sort_values(ascending=False)
    for genotype, count in genotype_counts.items():
        print(f"    {genotype}: {count:,} measurements")
    
    print(f"\n  - Measurements per treatment:")
    treatment_counts = joined.groupby('treatment').size().sort_values(ascending=False)
    for treatment, count in treatment_counts.items():
        print(f"    {treatment}: {count:,} measurements")
    
    print(f"\n✅ Database creation complete!")
    print(f"   Files created:")
    print(f"   - {metadata_path}")
    print(f"   - {timeseries_path}")
    print(f"\n💡 To use in analysis:")
    print(f"   time_series = pd.read_csv('{timeseries_path}')")
    print(f"   metadata = pd.read_csv('{metadata_path}')")
    print(f"   full_data = time_series.merge(metadata, on=['monitor', 'channel'])")


if __name__ == "__main__":
    main()
