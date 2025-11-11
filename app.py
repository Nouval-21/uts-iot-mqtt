from flask import Flask, jsonify, render_template, redirect, url_for, request
from flask_cors import CORS
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
import threading
import json
from datetime import datetime


app = Flask(__name__)
CORS(app)


# ==============================
# Konfigurasi Database & MQTT
# ==============================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'uts_sensor'
}

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "sensor/data"


# ==============================
# Inisialisasi Database
# ==============================
def init_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS data_sensor (
                id INT AUTO_INCREMENT PRIMARY KEY,
                suhu FLOAT,
                humidity FLOAT,
                lux FLOAT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()

        print("Database initialized successfully")

    except Exception as e:
        print(f"Database initialization error: {e}")


init_db()


def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


# ==============================
# MQTT Client & Callback
# ==============================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker: {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect to MQTT, return code: {rc}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        print(f"Data diterima dari MQTT: {data}")

        if not all(k in data for k in ("suhu", "humidity", "lux")):
            print("Format data tidak lengkap, dilewati.")
            return

        suhu = float(data["suhu"])
        humidity = float(data["humidity"])
        lux = float(data["lux"])
        waktu = datetime.now()

        conn = get_db_connection()
        if conn is None:
            print("Database connection failed")
            return

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO data_sensor (suhu, humidity, lux, timestamp) VALUES (%s, %s, %s, %s)",
            (suhu, humidity, lux, waktu)
        )

        conn.commit()
        inserted_id = cur.lastrowid
        conn.close()

        print(f"Data tersimpan ke MySQL: ID={inserted_id}, Suhu={suhu}°C, Humidity={humidity}%, Lux={lux}")

    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
    except Exception as e:
        print(f"Error processing MQTT message: {e}")


def mqtt_thread():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Connecting to MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        client.loop_forever()
    except Exception as e:
        print(f"MQTT connection error: {e}")


mqtt_t = threading.Thread(target=mqtt_thread, daemon=True)
mqtt_t.start()


# ==============================
# ROUTES
# ==============================
@app.route('/')
def home():
    return redirect(url_for('dashboard'))


@app.route('/dashboard')
def dashboard():
    return render_template('index.html')


# ==============================
# API ENDPOINTS
# ==============================
@app.route('/api/data', methods=['GET'])
def get_all_data():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 20")
        rows = cur.fetchall()
        conn.close()

        data_list = []
        for row in rows:
            data_list.append({
                "id": row[0],
                "suhu": round(row[1], 2),
                "humidity": round(row[2], 2),
                "lux": round(row[3], 2),
                "timestamp": row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else None
            })

        return jsonify(data_list), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/summary', methods=['GET'])
def get_summary():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        cur.execute("""
            SELECT 
                MAX(suhu), MIN(suhu), AVG(suhu),
                MAX(humidity), MIN(humidity), AVG(humidity),
                COUNT(*)
            FROM data_sensor
        """)
        row = cur.fetchone()
        conn.close()

        summary = {
            "suhu_max": round(row[0], 2) if row[0] else 0,
            "suhu_min": round(row[1], 2) if row[1] else 0,
            "suhu_avg": round(row[2], 2) if row[2] else 0,
            "humid_max": round(row[3], 2) if row[3] else 0,
            "humid_min": round(row[4], 2) if row[4] else 0,
            "humid_avg": round(row[5], 2) if row[5] else 0,
            "total_data": int(row[6]) if row[6] else 0
        }

        return jsonify(summary), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sensor/latest', methods=['GET'])
def get_latest_data():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 1")
        result = cur.fetchone()
        conn.close()

        if result:
            if result['timestamp']:
                result['timestamp'] = result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            return jsonify(result), 200

        return jsonify({'message': 'No data available'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sensor/stats', methods=['GET'])
def get_statistics():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT 
                MAX(suhu) as suhumax,
                MIN(suhu) as suhumin,
                AVG(suhu) as suhurata
            FROM data_sensor
        """)
        suhu_stats = cur.fetchone()

        cur.execute("""
            SELECT id as idx, suhu as suhun, humidity as humid, 
                   lux as kecerahan, timestamp
            FROM data_sensor
            WHERE suhu = (SELECT MAX(suhu) FROM data_sensor)
            ORDER BY timestamp DESC
            LIMIT 2
        """)
        nilai_suhu_max_humid_max = cur.fetchall()

        for row in nilai_suhu_max_humid_max:
            if row['timestamp']:
                row['timestamp'] = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        cur.execute("""
            SELECT CONCAT(MONTH(timestamp), '-', YEAR(timestamp)) as month_year
            FROM data_sensor
            WHERE suhu = (SELECT MAX(suhu) FROM data_sensor)
            GROUP BY YEAR(timestamp), MONTH(timestamp)
            ORDER BY timestamp DESC
            LIMIT 2
        """)
        month_year_max = cur.fetchall()

        conn.close()

        response = {
            'suhumax': round(suhu_stats['suhumax'], 2) if suhu_stats['suhumax'] else 0,
            'suhumin': round(suhu_stats['suhumin'], 2) if suhu_stats['suhumin'] else 0,
            'suhurata': round(suhu_stats['suhurata'], 2) if suhu_stats['suhurata'] else 0,
            'nilai_suhu_max_humid_max': nilai_suhu_max_humid_max,
            'month_year_max': month_year_max
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sensor/filter', methods=['GET'])
def filter_data():
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor(dictionary=True)

        if start_date and end_date:
            query = """
                SELECT * FROM data_sensor 
                WHERE DATE(timestamp) BETWEEN %s AND %s
                ORDER BY timestamp DESC
            """
            cur.execute(query, (start_date, end_date))
        else:
            cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 100")

        results = cur.fetchall()

        for row in results:
            if row['timestamp']:
                row['timestamp'] = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        conn.close()

        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        mqtt_status = mqtt_t.is_alive()

        if conn:
            conn.close()
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'mqtt_thread': 'running' if mqtt_status else 'stopped'
            }), 200

        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'mqtt_thread': 'running' if mqtt_status else 'stopped'
        }), 500

    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


# ==============================
# Main Entry
# ==============================
if __name__ == '__main__':
    print("\n" + "="*60)
    print("IoT Sensor Monitoring System - Backend Server")
    print("="*60)
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"MQTT Topic: {MQTT_TOPIC}")
    print(f"Server: http://localhost:5000")
    print(f"Dashboard: http://localhost:5000/dashboard")
    print("\nAvailable API Endpoints:")
    print("   GET  /api/data           - Get recent sensor data")
    print("   GET  /api/summary        - Get statistics summary")
    print("   GET  /api/sensor/latest  - Get latest reading")
    print("   GET  /api/sensor/stats   - Get detailed statistics")
    print("   GET  /api/sensor/filter  - Filter by date range")
    print("   GET  /api/health         - Health check")
    print("="*60 + "\n")

    app.run(host='0.0.0.0', port=5000, debug=True)
