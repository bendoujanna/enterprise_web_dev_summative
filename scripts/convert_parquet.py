import pandas as pd
import os

# paths
scripts_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(scripts_dir)
csv_path = os.path.join(project_root, 'data', 'yellow_tripdata_2019-01.csv')
parquet_path = os.path.join(project_root, 'data', 'yellow_tripdata_2019-01.parquet')

print(f"Looking for file at: {csv_path}")

# convertion
if os.path.exists(csv_path):
    print("File found! Starting conversion... (This might take some time)")
    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows.")

        # Save as Parquet
        df.to_parquet(parquet_path, index=False)
        print(f"Success! Parquet file saved at: {parquet_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
else:
    print("Error: File still not found.")
    print(f"Please check if 'yellow_tripdata_2019-01.csv' is actually in '{os.path.join(project_root, 'data')}'")