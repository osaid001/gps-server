from flask import Flask, request, jsonify
from datetime import datetime
import sqlite3
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key'

DB_PATH = os.path.join(os.path.dirname(__file__), 'gps_data.db')

# === SQLite DB Setup ===
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Create table if not exists
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS GPSData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DriverID TEXT,
            Timestamp TEXT,
            Latitude REAL,
            Longitude REAL,
            Speed REAL
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/')
def home():
    return "✅ GPS Tracking Server with SQLite is running."

@app.route('/upload', methods=['POST'])
def receive_gps_data():
    try:
        data = request.get_json(force=True)
        print("📥 JSON Received:", data)

        driver_id = data.get('driver_id', 'unknown')
        gps_points = data.get('points', [])

        if not gps_points:
            return jsonify({'status': 'error', 'message': 'No GPS points received'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        for point in gps_points:
            cursor.execute('''
                INSERT INTO GPSData (DriverID, Timestamp, Latitude, Longitude, Speed)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                driver_id,
                point.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                point.get('lat', 0.0),
                point.get('lon', 0.0),
                point.get('speed', 0.0)
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Stored {len(gps_points)} point(s) for driver {driver_id}")
        return jsonify({'status': 'success', 'message': f'{len(gps_points)} point(s) stored successfully'})

    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
