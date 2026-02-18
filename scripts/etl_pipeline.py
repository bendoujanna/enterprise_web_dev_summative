import pandas as pd
import os
import sqlite3
import numpy as np

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "database.db")
TRIPS_FILE = os.path.join(PROJECT_ROOT, 'data', 'yellow_tripdata_2019-01.parquet')
# TRIPS_FILE = os.path.join(PROJECT_ROOT, 'data', 'yellow_tripdata_2019-01.csv')
ZONE_FILE = os.path.join(PROJECT_ROOT, 'data', 'taxi_zone_lookup.csv')
TRIPS_FILE = os.path.join(PROJECT_ROOT, 'data', 'yellow_tripdata_2019-01.parquet')
LOG_DIR = os.path.join(PROJECT_ROOT, 'output')
LOG_FILE = os.path.join(LOG_DIR, 'suspicious_records.log')


def run_pipeline():
    print(f"Starting ETL Pipeline...")
    print(f"Database Path: {DB_PATH}")

    # Ensure output directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    conn = sqlite3.connect(DB_PATH)

    # 1. Load Zones
    valid_zones = set()
    try:
        print("Loading zones...")
        if os.path.exists(ZONE_FILE):
            zones_df = pd.read_csv(ZONE_FILE)
            # Create zones table if it doesn't exist
            zones_df.to_sql('zones', conn, if_exists='replace', index=False)
            valid_zones = set(zones_df['LocationID'].unique())
            print(f"Loaded {len(zones_df)} zones.")
        else:
            print(f"Warning: Zone file not found at {ZONE_FILE}. Skipping zone validation.")
    except Exception as e:
        print(f"Zone Error: {e}")

    # 2. Process Trips
    print("Processing Data...")
    try:
        # Load Data (Adjust for CSV or Parquet)
        if TRIPS_FILE.endswith('.parquet'):
            df = pd.read_parquet(TRIPS_FILE)
        else:
            df = pd.read_csv(TRIPS_FILE)

        # Precalculations
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Calculate Duration (Seconds)
        df['trip_duration_seconds'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds()

        # Calculate Speed (MPH)
        # Handle division by zero using numpy to avoid crash, then fill NA
        df['speed_mph'] = (df['trip_distance'] / (df['trip_duration_seconds'] / 3600))
        df['speed_mph'] = df['speed_mph'].replace([np.inf, -np.inf], 0).fillna(0)
        df['average_speed_mph'] = df['speed_mph']

        # suspicious data

        # 1. Fare Outlier / Price Gouging (Fixes $185/0.4mi bug)
        # Rejects trips that cost more than $50 but went less than 0.5 miles
        mask_price_anomaly = (df['total_amount'] > 50) & (df['trip_distance'] < 0.5)

        # 2. Impossible Short-Distance Speed
        # Rejects trips < 1.0 mile with speeds > 30 mph
        mask_short_speed = (df['trip_distance'] < 1.0) & (df['average_speed_mph'] > 30)

        # 3. Standard Zero Distance/High Fare
        mask_distance = (df['trip_distance'] <= 0.1) & (df['total_amount'] > 10.0)

        # 4. Negative/Zero Fares
        mask_fare = df['total_amount'] <= 0

        # 5. Invalid Duration (Negative time or > 12 hours)
        mask_time = (df['trip_duration_seconds'] <= 0) | (df['trip_duration_seconds'] > 43200)

        # 6. Extreme Speed (> 100 mph overall)
        mask_speed = (df['average_speed_mph'] > 100) | (df['average_speed_mph'] < 0)

        # 7. Unknown Zones
        if valid_zones:
            mask_zone = (~df['PULocationID'].isin(valid_zones)) | \
                        (~df['DOLocationID'].isin(valid_zones))
        else:
            mask_zone = pd.Series([False] * len(df))

        # Combine all masks including the rules
        mask_suspicious = (mask_price_anomaly | mask_short_speed | mask_distance |
                           mask_fare | mask_time | mask_speed | mask_zone)

        # Log suspicious records
        bad_count = mask_suspicious.sum()
        if bad_count > 0:
            print(f"Found {bad_count} suspicious records.")
            bad_df = df[mask_suspicious].copy()

            # Updated labels to reflect the logic
            conditions = [
                mask_price_anomaly[mask_suspicious],
                mask_short_speed[mask_suspicious],
                mask_distance[mask_suspicious],
                mask_fare[mask_suspicious],
                mask_speed[mask_suspicious],
                mask_time[mask_suspicious],
                mask_zone[mask_suspicious]
            ]

            choices = [
                'Fare Outlier (Short Trip)',
                'Impossible Short Speed',
                'Zero Distance/High Fare',
                'Negative/Zero Fare',
                'Extreme Speed',
                'Invalid Duration',
                'Unknown Zone'
            ]

            bad_df['rejection_reason'] = np.select(conditions, choices, default='Unknown')
            bad_df.to_csv(LOG_FILE, index=False)
            print(f"  - Logged to {LOG_FILE}")

        # --- D. Filter & Save Clean Data ---
        df_clean = df[~mask_suspicious].copy()

        # E. Feature Engineering
        hours = df_clean['tpep_pickup_datetime'].dt.hour
        df_clean['time_of_day'] = pd.cut(hours,
                                         bins=[-1, 5, 11, 16, 20, 24],
                                         labels=['Night', 'Morning', 'Afternoon', 'Evening', 'Night'],
                                         ordered=False)

        # Ensure columns match DB schema
        cols_to_save = [
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
            'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
            'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
            'improvement_surcharge', 'total_amount', 'congestion_surcharge',
            'trip_duration_seconds', 'average_speed_mph', 'time_of_day'
        ]

        # Handle missing columns safely
        for col in cols_to_save:
            if col not in df_clean.columns:
                df_clean[col] = 0  # Default value if missing
        if 'congestion_surcharge' in df_clean.columns:
            df_clean['congestion_surcharge'] = df_clean['congestion_surcharge'].fillna(0.00)


        print("Saving clean data to Database...")

        # Clear old data to verify the filter works
        conn.execute("DELETE FROM trips")

        # Insert new clean data
        df_clean[cols_to_save].to_sql('trips', conn, if_exists='append', index=False, chunksize=10000)

        conn.commit()
        print(f"Success! ETL Completed.")
        print(f"Total Rows Processed: {len(df)}")
        print(f"Clean Rows Inserted:  {len(df_clean)}")
        print(f"Rejected Rows:        {bad_count}")

    except Exception as e:
        print(f"Pipeline Critical Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()