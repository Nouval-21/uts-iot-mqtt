from flask import Flask, jsonify, render_template, redirect, url_for, request
from flask_cors import CORS
import paho.mqtt.client as mqtt
import mysql.connector
import threading, json
from datetime import datetime

app = Flask(__name__)
CORS(app)

DB_CONFIG = {'host': 'localhost', 'user': 'root', 'password': '', 'database': 'uts_sensor'}
MQTT_BROKER, MQTT_PORT, MQTT_TOPIC = "test.mosquitto.org", 1883, "sensor/data"

def init_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS data_sensor (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    suhu FLOAT, humidity FLOAT, lux FLOAT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit(); conn.close()
init_db()

def get_db_connection():
    try: return mysql.connector.connect(**DB_CONFIG)
    except: return None

def on_connect(client, userdata, flags, rc):
    if rc == 0: client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        if not all(k in data for k in ("suhu", "humidity", "lux")): return
        conn = get_db_connection()
        if not conn: return
        cur = conn.cursor()
        cur.execute("INSERT INTO data_sensor (suhu, humidity, lux, timestamp) VALUES (%s,%s,%s,%s)",
                    (data["suhu"], data["humidity"], data["lux"], datetime.now()))
        conn.commit(); conn.close()
    except: pass

def mqtt_thread():
    client = mqtt.Client()
    client.on_connect, client.on_message = on_connect, on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

threading.Thread(target=mqtt_thread, daemon=True).start()

@app.route('/')
def home(): return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard(): return render_template('index.html')

@app.route('/api/data')
def get_all_data():
    conn = get_db_connection()
    cur = conn.cursor(); cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 20")
    rows = cur.fetchall(); conn.close()
    return jsonify([{
        "id": r[0], "suhu": round(r[1],2), "humidity": round(r[2],2),
        "lux": round(r[3],2), "timestamp": r[4].strftime('%Y-%m-%d %H:%M:%S')
    } for r in rows])

@app.route('/api/summary')
def get_summary():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""SELECT MAX(suhu),MIN(suhu),AVG(suhu),
                   MAX(humidity),MIN(humidity),AVG(humidity),COUNT(*) FROM data_sensor""")
    r = cur.fetchone(); conn.close()
    s = lambda i: round(r[i],2) if r[i] else 0
    return jsonify({
        "suhu_max":s(0),"suhu_min":s(1),"suhu_avg":s(2),
        "humid_max":s(3),"humid_min":s(4),"humid_avg":s(5),
        "total_data":int(r[6]) if r[6] else 0
    })

@app.route('/api/sensor/latest')
def get_latest_data():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 1")
    r = cur.fetchone(); conn.close()
    if r and r['timestamp']: r['timestamp']=r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    return jsonify(r if r else {'message':'No data available'})

@app.route('/api/sensor/stats')
def get_statistics():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT MAX(suhu) suhumax, MIN(suhu) suhumin, AVG(suhu) suhurata FROM data_sensor")
    suhu_stats = cur.fetchone()
    cur.execute("""SELECT id idx,suhu suhun,humidity humid,lux kecerahan,timestamp 
                   FROM data_sensor WHERE suhu=(SELECT MAX(suhu) FROM data_sensor)
                   ORDER BY timestamp DESC LIMIT 2""")
    nmax = cur.fetchall()
    for r in nmax: r['timestamp']=r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""SELECT CONCAT(MONTH(timestamp),'-',YEAR(timestamp)) month_year 
                   FROM data_sensor WHERE suhu=(SELECT MAX(suhu) FROM data_sensor)
                   GROUP BY YEAR(timestamp),MONTH(timestamp) 
                   ORDER BY timestamp DESC LIMIT 2""")
    mymax = cur.fetchall(); conn.close()
    return jsonify({
        'suhumax': round(suhu_stats['suhumax'],2),
        'suhumin': round(suhu_stats['suhumin'],2),
        'suhurata': round(suhu_stats['suhurata'],2),
        'nilai_suhu_max_humid_max': nmax, 'month_year_max': mymax
    })

@app.route('/api/sensor/filter')
def filter_data():
    s,e=request.args.get('start_date'),request.args.get('end_date')
    conn=get_db_connection(); cur=conn.cursor(dictionary=True)
    if s and e: cur.execute("SELECT * FROM data_sensor WHERE DATE(timestamp) BETWEEN %s AND %s ORDER BY timestamp DESC",(s,e))
    else: cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 100")
    rows=cur.fetchall(); [r.update({'timestamp':r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}) for r in rows]
    conn.close(); return jsonify(rows)

@app.route('/api/health')
def health_check():
    conn=get_db_connection(); mqtt_status=threading.active_count()>0
    status={'database':'connected' if conn else 'disconnected','mqtt_thread':'running' if mqtt_status else 'stopped'}
    if conn: conn.close(); return jsonify({'status':'healthy',**status})
    return jsonify({'status':'unhealthy',**status}),500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
