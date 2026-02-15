from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

app = Flask(__name__)
CORS(app)

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'database.db')


def get_db_connection():
    """Establish a connection to the sqlite database with row mapping"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# utilities and metadata
@app.route('/api/health', methods=['GET'])
def health_check():
    """Returns the API and Database status"""
    return jsonify({
        "status": "online",
        "database_found": os.path.exists(DB_PATH)
    })


@app.route('/api/zones', methods=['GET'])
def get_zones():
    """Provides spatial metadata mapping LocationIDs to Borough/Zone names"""
    conn = get_db_connection()
    zones = conn.execute("SELECT LocationID, Borough, Zone FROM zones").fetchall()
    conn.close()
    # Returns a dictionary for O(1) lookup on the frontend
    return jsonify({row['LocationID']: {"Borough": row['Borough'], "Zone": row['Zone']} for row in zones})

#

@app.route('/api/stats/summary', methods=['GET'])
def get_summary():
    """KPIs for the dashboard header"""
    conn = get_db_connection()
    # Pulling total count and average revenue
    stats = conn.execute("""
                         SELECT COUNT(*)                    as total_trips,
                                ROUND(AVG(total_amount), 2) as avg_fare
                         FROM trips
                         """).fetchone()
    conn.close()
    return jsonify(dict(stats))


@app.route('/api/stats/charts/boroughs', methods=['GET'])
def get_borough_distribution():
    """Returns trip counts per Borough for the Bar Chart"""
    conn = get_db_connection()
    query = """
            SELECT z.Borough, COUNT(*) as trip_count
            FROM trips t
                     JOIN zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Borough
            ORDER BY trip_count DESC \
            """
    data = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])


@app.route('/api/stats/charts/efficiency', methods=['GET'])
def get_time_efficiency():
    """Returns average speed per time of day for the Line Chart"""
    conn = get_db_connection()
    query = """
            SELECT time_of_day, ROUND(AVG(average_speed_mph), 2) as avg_speed
            FROM trips
            GROUP BY time_of_day \
            """
    data = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])


# raw data
@app.route('/api/trips', methods=['GET'])
def get_trips():
    """Returns a list of trips with optional borough filtering and pagination"""
    limit = request.args.get('limit', 200, type=int)
    offset = request.args.get('offset', 0, type=int)
    borough = request.args.get('borough', None)

    conn = get_db_connection()
    query = """
            SELECT t.*, p.Borough as Pickup_Borough, d.Borough as Dropoff_Borough
            FROM trips t
                     JOIN zones p ON t.PULocationID = p.LocationID
                     JOIN zones d ON t.DOLocationID = d.LocationID \
            """
    params = []
    if borough:
        query += " WHERE p.Borough = ?"
        params.append(borough)

    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    trips = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(row) for row in trips])


@app.route('/api/analytics/summary', methods=['GET'])
def get_analytics_summary():
    conn = get_db_connection()
    try:
        # Calculate Revenue and Duration
        stats = conn.execute("""
            SELECT 
                SUM(total_amount) as total_rev,
                AVG(trip_distance / (NULLIF(average_speed_mph, 0) / 60.0)) as avg_dur
            FROM trips
        """).fetchone()

        # Peak Hours Analysis
        hourly_data = conn.execute("""
            SELECT strftime('%H', tpep_pickup_datetime) as hr, COUNT(*) as count 
            FROM trips GROUP BY hr ORDER BY hr ASC
        """).fetchall()

        return jsonify({
            "kpis": {
                "total_revenue": f"${round((stats['total_rev'] or 0) / 1000000, 1)}M",
                "avg_trip_duration": f"{round(stats['avg_dur'] or 0, 1)} min"
            },
            "chart_data": [{"hour": f"{row['hr']}:00", "trips": row['count']} for row in hourly_data]
        })
    finally:
        conn.close()


@app.route('/api/stats/quality', methods=['GET'])
def get_data_quality():
    conn = get_db_connection()
    try:
        valid_records = conn.execute("SELECT COUNT(*) FROM trips").fetchone()[0]

        rejected_records = 66498

        issues = [
            {"issue": "Time Reversal", "count": 53662, "status": "critical"},
            {"issue": "Negative Fare", "count": 7131, "status": "critical"},
            {"issue": "Extreme Speed (>100MPH)", "count": 5705, "status": "warning"},
            {"issue": "Unknown Zones", "count": 0, "status": "success"}
        ]

        # Calculate the real-time quality score
        total_attempted = valid_records + rejected_records
        quality_score = round((valid_records / total_attempted) * 100, 2) if total_attempted > 0 else 0

        return jsonify({
            "overall_score": f"{quality_score}%",
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "detailed_issues": issues,
            "last_updated": "Feb 15, 2026"
        })
    finally:
        conn.close()


if __name__ == '__main__':
    print(f"API Server running at http://127.0.0.1:5000")
    app.run(debug=True, port=5000)