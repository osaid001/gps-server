from flask import Flask, request, jsonify
from datetime import datetime
import pyodbc

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# === SQL Server Connection ===
def get_db_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=TABLET-5GM9SJA7\\SQLEXPRESS;'
        'DATABASE=temp;'
        'Trusted_Connection=yes;'
    )

@app.route('/')
def home():
    return "✅ GPS Tracking Server is running."

@app.route('/upload', methods=['POST'])
def receive_gps_data():
    try:
        data = request.get_json(force=True)
        print("📥 Raw JSON Received:", data)

        driver_id = data.get('driver_id', 'unknown')
        gps_points = data.get('points', [])

        if not gps_points:
            return jsonify({'status': 'error', 'message': 'No GPS points received'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        for point in gps_points:
            cursor.execute("""
                INSERT INTO GPSData (DriverID, Timestamp, Latitude, Longitude, Speed)
                VALUES (?, ?, ?, ?, ?)
            """, (
                driver_id,
                point.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                point.get('lat', 0.0),
                point.get('lon', 0.0),
                point.get('speed', 0.0)
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Received {len(gps_points)} points from {driver_id} and saved to DB")
        return jsonify({'status': 'success', 'message': f'{len(gps_points)} points stored successfully'})

    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':  # ✅ Fix: correct Python main guard
    app.run(host='0.0.0.0', port=5000, debug=True)
