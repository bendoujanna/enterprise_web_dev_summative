import sqlite3
import os

# cONFIGURATION

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output')
# Temporary Safe Path
DB_PATH = os.path.expanduser("database.db")


def create_schema():
    # create the output folder if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory at {OUTPUT_DIR}")

    print(f"Initializing database at {DB_PATH}...")

    # connect to the db
    # URI=True allows us to pass special parameters
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # enable foreigh keys
    cursor.execute("PRAGMA foreign_keys = ON;")

 # TABLES

    # 1. vendors

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
           VendorID INTEGER PRIMARY KEY,
           vendor_name TEXT NOT NULL
        );
    """)

 # 2. Zones

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS zones (
            LocationID INTEGER PRIMARY KEY,
            Borough TEXT,
            Zone TEXT,
            service_zone TEXT       
        );
    """)

