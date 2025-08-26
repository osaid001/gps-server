from flask import Flask, request, jsonify
<<<<<<< HEAD
import psycopg2
import os
import json
import paho.mqtt.client as mqtt
import threading
import time
from datetime import datetime, timedelta
import pytz
import requests
from geopy.distance import geodesic
import statistics
from collections import deque
import sys

app = Flask(__name__)
data_sessions = {}
PKT = pytz.timezone('Asia/Karachi')

road_type_cache = {}

# ------------------ LOG CAPTURE ------------------
logs_buffer = deque(maxlen=500)  # store last 500 logs

class LogCapture:
    def write(self, message):
        if message.strip():
            logs_buffer.append(message.strip())
        sys.__stdout__.write(message)  # still print to console
    def flush(self):
        pass

# Redirect stdout & stderr
sys.stdout = LogCapture()
sys.stderr = LogCapture()
# -------------------------------------------------

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "require")  # enforce SSL for Render
    )

def send_notifications(device_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch notifications for this specific vehicle ID
        cursor.execute("""
            SELECT message FROM notifications 
            WHERE vehicle_id = %s 
            ORDER BY created_at DESC
        """, device_id)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if rows:
            # Join all notification messages with newlines
            notification_text = '\n'.join([row.message.strip() for row in rows])
            print(f"📤 Sending {len(rows)} notifications to device {device_id}")
        else:
            notification_text = ""
            print(f"📤 No notifications found for device {device_id}")

        # Send notifications to device
        topic = f"device/{device_id}/notifications"
        payload = json.dumps({"notifications": notification_text})
        result = mqtt_client.publish(topic, payload, qos=1, retain=False)

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✅ Notifications sent to {device_id}")
            result.wait_for_publish(timeout=5)
            return True
        else:
            print(f"❌ Failed to send notifications to {device_id}: RC={result.rc}")
            return False

    except Exception as e:
        print(f"❌ Error sending notifications to {device_id}: {e}")
        return False

def get_road_type_from_osm(lat, lon):
    """Get road type from OpenStreetMap Overpass API with caching"""
    cache_key = f"{lat:.4f},{lon:.4f}"  # Round to ~11m precision for caching

    if cache_key in road_type_cache:
        return road_type_cache[cache_key]

    try:
        # Overpass API query to find nearest road
        overpass_url = "http://overpass-api.de/api/interpreter"
        overpass_query = f"""
        [out:json][timeout:10];
        (
          way["highway"](around:20,{lat},{lon});
        );
        out tags;
        """

        response = requests.post(overpass_url, data=overpass_query, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if data['elements']:
                # Get the first road found
                highway_tag = data['elements'][0]['tags'].get('highway', '')

                # Map OSM highway tags to our road categories
                road_type = map_highway_to_road_type(highway_tag)

                # Cache the result
                road_type_cache[cache_key] = road_type
                return road_type

        # Default to "Other Roads" if no road found or API error
        road_type_cache[cache_key] = "Other Roads"
        return "Other Roads"

    except Exception as e:
        print(f"❌ Error getting road type for {lat},{lon}: {e}")
        road_type_cache[cache_key] = "Other Roads"
        return "Other Roads"

def map_highway_to_road_type(highway_tag):
    """Map OSM highway tags to our 4 road categories"""
    highway_tag = highway_tag.lower()

    # Motorways - high speed, controlled access
    if highway_tag in ['motorway', 'motorway_link']:
        return "Motorway"

    # Expressways - high speed roads, often divided
    if highway_tag in ['trunk', 'trunk_link'] and 'expressway' in highway_tag:
        return "Expressways"

    # National Highways - major roads
    if highway_tag in ['trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link']:
        return "National Highways"

    # Everything else - local roads, residential, etc.
    return "Other Roads"

def get_speed_limit_for_vehicle(vehicle_id, road_type):
    """Get speed limit for a vehicle on a specific road type"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT speed_limit FROM settings 
            WHERE vehicle_id = %s AND road_type = %s
        """, vehicle_id, road_type)

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return float(result[0])
        else:
            # Default speed limits if not set in settings
            defaults = {
                "Motorway": 120.0,
                "Expressways": 100.0,
                "National Highways": 80.0,
                "Other Roads": 50.0
            }
            return defaults.get(road_type, 50.0)

    except Exception as e:
        print(f"❌ Error getting speed limit: {e}")
        return 50.0  # Default fallback

def check_event_exists(vehicle_id, driver_id, timestamp, lat, lon, event_type):
    """Check if an event already exists to prevent duplicates"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM events 
            WHERE vehicle_id = %s AND driver_id = %s AND timestamp = %s 
            AND ABS(lat - %s) < 0.0001 AND ABS(lon - %s) < 0.0001 
            AND event_type = %s
        """, vehicle_id, driver_id, timestamp, lat, lon, event_type)

        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return count > 0

    except Exception as e:
        print(f"❌ Error checking event existence: {e}")
        return False

def insert_event(vehicle_id, driver_id, timestamp, lat, lon, event_type):
    """Insert an event into the events table"""
    try:
        # Check if event already exists
        if check_event_exists(vehicle_id, driver_id, timestamp, lat, lon, event_type):
            return False

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO events (vehicle_id, driver_id, timestamp, lat, lon, event_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, vehicle_id, driver_id, timestamp, lat, lon, event_type)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Event inserted: {event_type} for vehicle {vehicle_id}")
        return True

    except Exception as e:
        print(f"❌ Error inserting event: {e}")
        return False

def smooth_speeds(speeds, window_size=3):
    """Apply median smoothing to speeds to reduce noise"""
    if len(speeds) < window_size:
        return speeds

    smoothed = []
    half_window = window_size // 2

    for i in range(len(speeds)):
        start = max(0, i - half_window)
        end = min(len(speeds), i + half_window + 1)
        window = speeds[start:end]
        smoothed.append(statistics.median(window))

    return smoothed

def detect_harsh_events(gps_points, vehicle_id, driver_id):
    """Detect harsh braking and acceleration events"""
    if len(gps_points) < 2:
        return

    # Extract speeds and smooth them
    speeds = [point['speed'] for point in gps_points]
    smoothed_speeds = smooth_speeds(speeds)

    events_detected = 0

    for i in range(1, len(gps_points)):
        current_speed = smoothed_speeds[i]
        prev_speed = smoothed_speeds[i - 1]

        # Skip if speed is too low (parking/creep)
        if current_speed < 10 and prev_speed < 10:
            continue

        # Parse timestamps and calculate actual time_diff
        try:
            prev_time = datetime.strptime(gps_points[i - 1]['timestamp'], '%Y-%m-%d %H:%M:%S')
            curr_time = datetime.strptime(gps_points[i]['timestamp'], '%Y-%m-%d %H:%M:%S')
            time_diff = (curr_time - prev_time).total_seconds()
            if time_diff <= 0:
                continue  # Skip duplicates or invalid timestamps
        except ValueError as e:
            print(f"❌ Timestamp parse error: {e} - assuming 1s interval for this point")
            time_diff = 1.0

        speed_diff = current_speed - prev_speed
        accel = speed_diff / time_diff  # Acceleration in km/h per second

        point = gps_points[i]
        timestamp = point['timestamp']
        if 'T' in timestamp or 'Z' in timestamp:
            timestamp = convert_to_pkt(timestamp)

        # Check for harsh braking (adjusted for time_diff)
        if accel <= -15:  # -15 km/h/s threshold
            insert_event(vehicle_id, driver_id, timestamp,
                         point['lat'], point['lon'], 'harsh_brake')
            events_detected += 1

        # Check for harsh acceleration
        elif accel >= 19:  # +19 km/h/s threshold
            insert_event(vehicle_id, driver_id, timestamp,
                         point['lat'], point['lon'], 'harsh_acceleration')
            events_detected += 1

        curr_time = datetime.strptime(gps_points[i]['timestamp'], '%Y-%m-%d %H:%M:%S')

        # Optional: Multi-second patterns with normalized accel
        if i >= 2:
            # 2-second window
            time_i_minus_2 = datetime.strptime(gps_points[i - 2]['timestamp'], '%Y-%m-%d %H:%M:%S')
            delta2 = (curr_time - time_i_minus_2).total_seconds()
            if delta2 > 0:
                two_sec_diff = current_speed - smoothed_speeds[i - 2]
                accel2 = two_sec_diff / delta2
                if accel2 <= -13.5:  # Adjusted threshold (~ -27 km/h over 2s)
                    insert_event(vehicle_id, driver_id, timestamp,
                                 point['lat'], point['lon'], 'harsh_brake')
                    events_detected += 1
                elif accel2 >= 13.5:  # Adjusted (~ +27 km/h over 2s)
                    insert_event(vehicle_id, driver_id, timestamp,
                                 point['lat'], point['lon'], 'harsh_acceleration')
                    events_detected += 1

        if i >= 3:
            # 3-second window
            time_i_minus_3 = datetime.strptime(gps_points[i - 3]['timestamp'], '%Y-%m-%d %H:%M:%S')
            delta3 = (curr_time - time_i_minus_3).total_seconds()
            if delta3 > 0:
                three_sec_diff = current_speed - smoothed_speeds[i - 3]
                accel3 = three_sec_diff / delta3
                if accel3 <= -13.3:  # Adjusted (~ -40 km/h over 3s)
                    insert_event(vehicle_id, driver_id, timestamp,
                                 point['lat'], point['lon'], 'harsh_brake')
                    events_detected += 1
                elif accel3 >= 11.7:  # Adjusted (~ +35 km/h over 3s)
                    insert_event(vehicle_id, driver_id, timestamp,
                                 point['lat'], point['lon'], 'harsh_acceleration')
                    events_detected += 1

    if events_detected > 0:
        print(f"🚨 Detected {events_detected} harsh driving events")

def calculate_distance_and_check_events(gps_points, vehicle_id, driver_id):
    """Calculate distance traveled and check for speeding events"""
    if len(gps_points) < 2:
        return 0.0

    total_distance = 0.0

    # Calculate distance between consecutive points
    for i in range(1, len(gps_points)):
        prev_point = gps_points[i - 1]
        curr_point = gps_points[i]

        prev_lat, prev_lon = prev_point['lat'], prev_point['lon']
        curr_lat, curr_lon = curr_point['lat'], curr_point['lon']

        # Calculate distance
        distance = geodesic((prev_lat, prev_lon), (curr_lat, curr_lon)).kilometers
        total_distance += distance

        # Check for overspeeding at current point
        curr_speed = curr_point['speed']

        # Get road type for current point
        road_type = get_road_type_from_osm(curr_lat, curr_lon)

        # Get speed limit for this vehicle and road type
        speed_limit = get_speed_limit_for_vehicle(vehicle_id, road_type)

        # Check if overspeeding
        if curr_speed > speed_limit:
            timestamp = curr_point['timestamp']
            if 'T' in timestamp or 'Z' in timestamp:
                timestamp = convert_to_pkt(timestamp)

            insert_event(vehicle_id, driver_id, timestamp,
                         curr_lat, curr_lon, 'overspeeding')

    return total_distance

def update_vehicle_mileage(vehicle_id, distance_km):
    """Update the total mileage of a vehicle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE vehicles 
            SET total_milage = total_milage + %s 
            WHERE id = %s
        """, distance_km, vehicle_id)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Updated vehicle {vehicle_id} mileage by {distance_km:.2f} km")
        return True

    except Exception as e:
        print(f"❌ Error updating vehicle mileage: {e}")
        return False

def send_driver_list(device_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM drivers")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        driver_list = [{"id": str(r.id), "name": r.name.strip()} for r in rows]
        payload = json.dumps({"drivers": driver_list})
        topic = f"device/{device_id}/config"
        mqtt_client.publish(topic, payload)
        print(f"📤 Sent {len(driver_list)} drivers to {device_id}")
    except Exception as e:
        print(f"❌ Error sending driver list: {e}")

def send_confirmation(device_id, driver_id, status="success", message=""):
    topic = f"device/{device_id}/confirmation"
    payload = json.dumps({
        "driver_id": driver_id,
        "status": status,
        "message": message,
        "timestamp": datetime.now(PKT).isoformat()
    })
    result = mqtt_client.publish(topic, payload, qos=1, retain=False)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"📤 Sent confirmation to {device_id}: {status} - {message}")
        result.wait_for_publish(timeout=5)
        return True
    else:
        print(f"❌ Failed to send confirmation to {device_id}: RC={result.rc}")
        return False

def validate_gps_data(data):
    required_fields = ['driver_id', 'lat', 'lon', 'speed', 'timestamp', 'device_id']
    for field in required_fields:
        if field not in data:
            return False, f"Missing field: {field}"
    try:
        lat = float(data['lat'])
        lon = float(data['lon'])
        speed = float(data['speed'])
        if not (-90 <= lat <= 90): return False, f"Invalid latitude: {lat}"
        if not (-180 <= lon <= 180): return False, f"Invalid longitude: {lon}"
        if speed < 0: return False, f"Invalid speed: {speed}"
    except:
        return False, "Invalid numeric values"
    return True, "Valid"

def validate_batch_gps_data(data):
    required_fields = ['driver_id', 'lat', 'lon', 'speed', 'time', 'device_id']
    for field in required_fields:
        if field not in data:
            return False, f"Missing field: {field}"
    try:
        lat = float(data['lat'])
        lon = float(data['lon'])
        speed = float(data['speed'])
        if not (-90 <= lat <= 90): return False, f"Invalid latitude: {lat}"
        if not (-180 <= lon <= 180): return False, f"Invalid longitude: {lon}"
        if speed < 0: return False, f"Invalid speed: {speed}"
    except:
        return False, "Invalid numeric values"
    return True, "Valid"

def check_gps_data_exists(device_id, driver_id, timestamp, lat, lon, speed):
    """Check if GPS data point already exists in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM GPSData 
            WHERE vehicle_id = %s AND driver_id = %s AND timestamp = %s 
            AND ABS(lat - %s) < 0.000001 AND ABS(lon - %s) < 0.000001 AND ABS(speed - %s) < 0.01
        """, device_id, driver_id, timestamp, lat, lon, speed)

        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return count > 0
    except Exception as e:
        print(f"❌ Error checking GPS data existence: {e}")
        return False

def handle_live_gps_data(data):
    """Handle live GPS data - stores in GPSData_live table"""
    try:
        device_id = data.get('device_id', '').strip()
        driver_id = data.get('driver_id', '').strip()
        timestamp = data.get('timestamp', '')
        lat = float(data.get('lat', 0))
        lon = float(data.get('lon', 0))
        speed = float(data.get('speed', 0))

        print(
            f"📡 Live GPS - Device: {device_id}, Driver: {driver_id}, Location: {lat:.6f},{lon:.6f}, Speed: {speed:.2f}")

        is_valid, error_msg = validate_gps_data(data)
        if not is_valid:
            print(f"❌ Invalid live GPS data: {error_msg}")
            return False

        if not verify_device_exists(device_id):
            print(f"⚠️ Live GPS: Device {device_id} not verified, but continuing...")

        if not verify_driver_exists(driver_id):
            print(f"⚠️ Live GPS: Driver {driver_id} not verified, but continuing...")

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Check for duplicates in GPSData_live table
            cursor.execute("""
                SELECT COUNT(*) FROM GPSData_live 
                WHERE vehicle_id = %s AND driver_id = %s AND timestamp = %s
            """, device_id, driver_id, timestamp)

            existing_count = cursor.fetchone()[0]
            if existing_count > 0:
                print(f"⚠️ Live GPS data with same timestamp already exists, skipping")
                cursor.close()
                conn.close()
                return True

            speed = max(0.0, speed)

            # Insert into GPSData_live table instead of GPSData
            cursor.execute("""
                INSERT INTO GPSData_live (vehicle_id, driver_id, timestamp, lat, lon, speed)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, device_id, driver_id, timestamp, lat, lon, speed)

            conn.commit()
            cursor.close()
            conn.close()

            print(f"✅ Live GPS data saved to GPSData_live successfully")
            return True

        except Exception as e:
            print(f"❌ Error saving live GPS data to database: {e}")
            return False

    except Exception as e:
        print(f"❌ Error handling live GPS data: {e}")
        return False

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        data = json.loads(msg.payload.decode())
        print(f"📥 MQTT Topic: {topic}, Data: {data}")

        device_id = data.get('device_id', '').strip()
        if not device_id:
            print("❌ Missing device_id")
            return

        if topic.endswith("/boot"):
            print(f"🔔 {device_id} booted. Sending driver list...")
            send_driver_list(device_id)
            return

        if topic.startswith("live/gps/"):
            print(f"📡 Processing live GPS data from {device_id}")
            handle_live_gps_data(data)
            return

        if topic == "gps/driver001":
            driver_id = data.get('driver_id', '').strip()
            time_stamp = data.get('time', '')
            session_key = f"{device_id}_{driver_id}"

            if time_stamp == "START":
                data_sessions[session_key] = {
                    'start_time': datetime.now(),
                    'points': [],
                    'complete': False
                }
                print(f"🟢 Started batch data session for {device_id} - {driver_id}")
                return

            if time_stamp == "END":
                print(f"📥 Received END marker for {device_id} - {driver_id}")

                if session_key in data_sessions:
                    session = data_sessions[session_key]
                    session['complete'] = True
                    points_count = len(session['points'])

                    print(f"🔚 Batch session ended for {device_id} - {driver_id} with {points_count} points")

                    try:
                        if points_count == 0:
                            print(f"📤 Empty batch session - sending success confirmation")
                            success = send_confirmation(device_id, driver_id, "success", "empty_session_confirmed")
                            if success:
                                del data_sessions[session_key]
                            return

                        print(f"🔄 Starting to process {points_count} batch GPS points...")
                        success, msg = save_gps_data_to_db(device_id, driver_id, session['points'])
                        print(f"🔄 Processing batch data - Success: {success}, Message: {msg}")

                        confirmation_sent = send_confirmation(device_id, driver_id, "success" if success else "error",
                                                              msg)
                        print(f"📤 Confirmation sent: {confirmation_sent}")

                        if confirmation_sent:
                            del data_sessions[session_key]
                            print(f"🧹 Cleaned up batch session: {session_key}")
                        else:
                            print(f"❌ Failed to send confirmation, keeping session: {session_key}")

                    except Exception as e:
                        print(f"❌ Error processing END marker: {e}")
                        send_confirmation(device_id, driver_id, "error", f"processing_error: {str(e)}")

                else:
                    print(f"⚠️ No active batch session found for {device_id} - {driver_id}")
                    send_confirmation(device_id, driver_id, "success", "no_session_but_confirmed")
                return

            if session_key not in data_sessions:
                print(f"⚠️ No active batch session for GPS data: {session_key}")
                return

            is_valid, error_msg = validate_batch_gps_data(data)
            if not is_valid:
                print(f"❌ Invalid batch GPS data: {error_msg}")
                return

            point = {
                'timestamp': time_stamp,
                'lat': float(data['lat']),
                'lon': float(data['lon']),
                'speed': float(data['speed'])
            }
            data_sessions[session_key]['points'].append(point)
            print(
                f"📍 Added GPS point to batch session {session_key} (Total: {len(data_sessions[session_key]['points'])})")

    except json.JSONDecodeError:
        print("❌ Invalid JSON received")
    except Exception as e:
        print(f"❌ MQTT Error: {e}")

def verify_device_exists(device_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, ba_number, make FROM vehicles WHERE id = %s", device_id)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result:
            return True
        else:
            print(f"❌ Device '{device_id}' not found in vehicles table")
            return False
    except Exception as e:
        print(f"❌ Error verifying device: {e}")
        return False

def verify_driver_exists(driver_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM drivers WHERE id = %s", driver_id)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result:
            return True
        else:
            print(f"❌ Driver '{driver_id}' not found in drivers table")
            return False
    except Exception as e:
        print(f"❌ Error verifying driver: {e}")
        return False

def convert_to_pkt(utc_time_str):
    try:
        utc_dt = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        pkt_dt = utc_dt.astimezone(PKT)
        return pkt_dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return datetime.now(PKT).strftime('%Y-%m-%d %H:%M:%S')

def save_gps_data_to_db(device_id, driver_id, gps_points):
    """Save batch GPS data with distance calculation and event detection"""
    try:
        print(f"🔄 Starting to save {len(gps_points)} GPS points to database...")

        if not verify_device_exists(device_id):
            print(f"❌ Device verification failed: {device_id}")
            return False, "error_invalid_device"
        if not verify_driver_exists(driver_id):
            print(f"❌ Driver verification failed: {driver_id}")
            return False, "error_invalid_driver"

        # Calculate distance traveled in this batch
        distance_traveled = calculate_distance_and_check_events(gps_points, device_id, driver_id)
        print(f"📏 Distance traveled in batch: {distance_traveled:.2f} km")

        # Detect harsh driving events
        detect_harsh_events(gps_points, device_id, driver_id)

        # Update vehicle mileage
        if distance_traveled > 0:
            update_vehicle_mileage(device_id, distance_traveled)

        conn = get_db_connection()
        cursor = conn.cursor()
        success_count = 0
        error_count = 0

        for i, point in enumerate(gps_points):
            try:
                pkt_timestamp = point['timestamp']
                if 'T' in pkt_timestamp or 'Z' in pkt_timestamp:
                    pkt_timestamp = convert_to_pkt(pkt_timestamp)

                speed = max(0.0, point['speed'])
                lat = point['lat']
                lon = point['lon']

                cursor.execute("""
                    SELECT COUNT(*) FROM GPSData 
                    WHERE vehicle_id = %s AND driver_id = %s AND timestamp = %s
                """, device_id, driver_id, pkt_timestamp)

                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    print(f"⚠️ Skipping duplicate point {i + 1}: {pkt_timestamp}")
                    success_count += 1
                    continue

                cursor.execute("""
                    INSERT INTO GPSData (vehicle_id, driver_id, timestamp, lat, lon, speed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, device_id, driver_id, pkt_timestamp, lat, lon, speed)
                success_count += 1

                if (i + 1) % 10 == 0:
                    print(f"📍 Processed {i + 1}/{len(gps_points)} points...")

            except Exception as e:
                error_count += 1
                print(f"❌ Error inserting GPS point {i + 1}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Database operation completed: {success_count} saved, {error_count} errors")
        print(f"📊 Batch summary: {distance_traveled:.2f} km traveled, events detected and vehicle mileage updated")

        if success_count > 0:
            return True, f"success_saved_{success_count}_points_distance_{distance_traveled:.2f}km"
        else:
            return False, "error_no_valid_points"

    except Exception as e:
        print(f"❌ Critical database error in save_gps_data_to_db: {e}")
        return False, "error_database"

def cleanup_old_sessions():
    while True:
        try:
            now = datetime.now()
            expired = [k for k, v in data_sessions.items() if (now - v['start_time']).seconds > 3600]
            for key in expired:
                print(f"🧹 Cleaning expired session: {key}")
                del data_sessions[key]
        except Exception as e:
            print(f"❌ Cleanup error: {e}")
        time.sleep(300)

mqtt_client = mqtt.Client(client_id=f"GPSServer_{int(time.time())}", clean_session=True)
mqtt_client.on_message = on_message

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT broker")
        client.subscribe("gps/driver001")
        client.subscribe("gps/+/boot")
        client.subscribe("live/gps/+")
        print("✅ Subscribed to MQTT topics (including live GPS)")
    else:
        print(f"❌ Failed to connect to MQTT broker, return code {rc}")

def on_disconnect(client, userdata, rc):
    print("❌ MQTT client disconnected" if rc != 0 else "✅ MQTT client disconnected")

def start_mqtt():
    try:
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.connect("broker.hivemq.com", 1883, 60)
        mqtt_client.loop_start()
        threading.Thread(target=cleanup_old_sessions, daemon=True).start()
    except Exception as e:
        print(f"❌ MQTT connection failed: {e}")

@app.route('/')
def home():
    logs_html = "<br>".join(list(logs_buffer)[-50:])  # last 50 logs
    return f"""
    <h2>✅ MQTT Server Running</h2>
    <p>Active Sessions: {len(data_sessions)}</p>
    <h3>📜 Recent Logs:</h3>
    <div style="font-family: monospace; background:#111; color:#0f0; padding:10px; border-radius:8px; max-height:400px; overflow-y:auto;">
        {logs_html}
    </div>
    """

@app.route('/sessions')
def sessions():
    return json.dumps({
        "active_sessions": len(data_sessions),
        "sessions": {
            k: {
                "start_time": v["start_time"].isoformat(),
                "points_count": len(v["points"]),
                "complete": v["complete"]
            } for k, v in data_sessions.items()
        }
    }, indent=2)

@app.route('/drivers')
def drivers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, rank, army_number, unit FROM drivers")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        drivers = [{"id": r.id, "name": r.name, "rank": r.rank, "army_number": r.army_number, "unit": r.unit} for r in
                   rows]
        return json.dumps({"drivers": drivers}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@app.route('/vehicles')
def vehicles():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, ba_number, make, type, model, total_milage, unit FROM vehicles")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        vehicles = [{
            "id": r.id, "ba_number": r.ba_number, "make": r.make, "type": r.type,
            "model": r.model, "total_mileage": r.total_milage, "unit": r.unit
        } for r in rows]
        return json.dumps({"vehicles": vehicles}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@app.route('/mqtt_status')
def mqtt_status():
    return json.dumps({
        "mqtt_connected": mqtt_client.is_connected(),
        "active_sessions": len(data_sessions),
        "server_time": datetime.now().isoformat()
    }, indent=2)

@app.route('/live_data_stats')
def live_data_stats():
    """New endpoint to check live GPS data statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get count of GPS data from today
        cursor.execute("""
            SELECT COUNT(*) FROM GPSData_live 
            WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
        """)
        today_count = cursor.fetchone()[0]

        # Get latest GPS data
        cursor.execute("""
            SELECT TOP 10 vehicle_id, driver_id, timestamp, lat, lon, speed 
            FROM GPSData_live 
            ORDER BY timestamp DESC
        """)
        latest_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return json.dumps({
            "today_gps_points": today_count,
            "latest_data": [
                {
                    "vehicle_id": r.vehicle_id,
                    "driver_id": r.driver_id,
                    "timestamp": r.timestamp.isoformat() if hasattr(r.timestamp, 'isoformat') else str(r.timestamp),
                    "lat": float(r.lat),
                    "lon": float(r.lon),
                    "speed": float(r.speed)
                } for r in latest_data
            ]
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == '__main__':
    print("🚀 Starting MQTT GPS Server with Live Data Support...")
    start_mqtt()
    print("🌐 Starting Flask web server...")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
=======
from datetime import datetime
import sqlite3
import os
import sys


app = Flask(__name__)
app.secret_key = 'your_secret_key'

# === SQLite DB path ===
DB_FILE = os.path.join(os.path.dirname(__file__), 'gps_data.db')

# === Create table if it doesn't exist ===
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
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

# === Route: Home ===
@app.route('/')
def home():
    return "✅ SQLite GPS Server is running."

@app.route('/data', methods=['GET'])
def get_all_data():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM GPSData")
            rows = cursor.fetchall()
            return jsonify(rows)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


# === Route: Receive Data ===
@app.route('/upload', methods=['POST'])
def receive_gps_data():
    try:
        data = request.get_json(force=True)
        print("📥 Received:", data)
        sys.stdout.flush()

        driver_id = data.get('driver_id', 'unknown')
        gps_points = data.get('points', [])

        if not gps_points:
            return jsonify({'status': 'error', 'message': 'No GPS points received'}), 400

        with sqlite3.connect(DB_FILE) as conn:
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

        return jsonify({'status': 'success', 'message': f'{len(gps_points)} points stored.'})

    except Exception as e:
        print("❌ Error:", e)
        sys.stdout.flush()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
>>>>>>> 86e583311a9ee0e72a94d336a742351d30d1fa58
