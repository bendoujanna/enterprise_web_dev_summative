import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join('output', 'suspicious_records.log')

try:
    print(f"Reading log file from: {log_path}...")

    # Read the log file
    log_df = pd.read_csv(log_path)

    # Count the occurrences of each specific reason
    counts = log_df['rejection_reason'].value_counts()

    print(f"\n--- Data Quality Report (Real-Time) ---")

    print(f"Negative/Zero Fares:      {counts.get('Negative/Zero Fare', 0)}")
    print(f"Zero Dist/High Fares:     {counts.get('Zero Distance/High Fare', 0)}")
    print(f"Unknown Zones:            {counts.get('Unknown Zone', 0)}")
    print(f"Invalid Durations:        {counts.get('Invalid Duration', 0)}")
    print(f"Extreme Speeds (>100mph): {counts.get('Extreme Speed', 0)}")

    print(f"---------------------------------------")
    print(f"Total Suspicious Records: {len(log_df)}")
    print(f"---------------------------------------")

except FileNotFoundError:
    print(f"Error: Log file not found at {log_path}. Did you run the ETL pipeline?")
except Exception as e:
    print(f"An error occurred: {e}")