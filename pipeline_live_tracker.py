import requests
import pyodbc
from datetime import datetime, timedelta, timezone
import os
import time
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    conn_str = (
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_HOST2')};"
        f"DATABASE={os.getenv('DB_NAME4')};"
        f"UID={os.getenv('DB_USER2')};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

# Your 14 surveillance stations
AIRPORT_TARGETS = [
    {'code': 'CGK', 'lat': -6.1256, 'lon': 106.6559, 'offset': 7},
    {'code': 'HLP', 'lat': -6.2653, 'lon': 106.8911, 'offset': 7},
    {'code': 'DPS', 'lat': -8.7481, 'lon': 115.1672, 'offset': 8},
    {'code': 'SUB', 'lat': -7.3798, 'lon': 112.7878, 'offset': 7},
    {'code': 'KNO', 'lat': 3.6422,  'lon': 98.8852,  'offset': 7},
    {'code': 'UPG', 'lat': -5.0616, 'lon': 119.5540, 'offset': 8},
    {'code': 'BDJ', 'lat': -3.4472, 'lon': 114.7639, 'offset': 8},
    {'code': 'BPN', 'lat': -1.2683, 'lon': 116.8944, 'offset': 8},
    {'code': 'PKU', 'lat': 0.4608,  'lon': 101.4442, 'offset': 7},
    {'code': 'SOC', 'lat': -7.5156, 'lon': 110.7553, 'offset': 7},
    {'code': 'SRG', 'lat': -6.9722, 'lon': 110.3756, 'offset': 7},
    {'code': 'YIA', 'lat': -7.9016, 'lon': 110.0544, 'offset': 7},
    {'code': 'JOG', 'lat': -7.7881, 'lon': 110.4317, 'offset': 7},
    {'code': 'LOP', 'lat': -8.7583, 'lon': 116.2764, 'offset': 8}
]

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def convert_to_local_time(utc_dt, offset_hours):
    return utc_dt + timedelta(hours=offset_hours)

def fetch_and_store():
    print(f"--- SCAN STARTED: {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"CRITICAL: Connection Error: {e}")
        return

    total_logged = 0
    for airport in AIRPORT_TARGETS:
        code, lat, lon, offset = airport['code'], airport['lat'], airport['lon'], airport['offset']
        url = f"https://api.airplanes.live/v2/point/{lat}/{lon}/50"
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200: continue
            data = response.json()
            aircraft_list = data.get('ac', [])
            now_utc = datetime.now(timezone.utc)
            
            for ac in aircraft_list:
                callsign = ac.get('flight', '').strip()
                if not callsign or callsign.startswith(('QG', 'CTV')): continue

                alt = ac.get('alt_baro')
                speed = ac.get('gs', 0)
                vert_rate = ac.get('baro_rate', 0) or ac.get('geom_rate', 0)
                if not isinstance(alt, (int, float)): continue

                # Standard Takeoff Logic
                if (500 < alt < 6000 and vert_rate > 50 and speed > 100) or \
                   (500 < alt < 2500 and vert_rate >= 0 and speed > 130):
                    
                    local_dep_time = convert_to_local_time(now_utc, offset)
                    month_str = local_dep_time.strftime("%Y-%m")
                    
                    check_sql = "SELECT id FROM flight_log WHERE flight_number = ? AND origin_airport = ? AND actual_departure_time > DATEADD(minute, -45, ?)"
                    cursor.execute(check_sql, (callsign, code, local_dep_time))
                    if cursor.fetchone(): continue 

                    # Back to basics: No arrival field
                    sql = """INSERT INTO flight_log 
                             (flight_number, origin_airport, destination_airport, actual_departure_time, month_period) 
                             VALUES (?, ?, ?, ?, ?)"""
                    cursor.execute(sql, (callsign, code, 'UNK', local_dep_time, month_str))
                    
                    print(f"[{code}] LOGGED: {callsign}")
                    total_logged += 1
                    conn.commit()
        except Exception as e:
            print(f"[{code}] Error: {e}")
        time.sleep(12)
    print(f"--- Cycle Complete. Total New: {total_logged} ---")
    conn.close()

if __name__ == "__main__":
    fetch_and_store()
