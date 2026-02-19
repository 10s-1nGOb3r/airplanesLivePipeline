import pyodbc
import pandas as pd
import os
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

def update_schedule():
    print("--- UPDATING COMPETITOR SCHEDULES ---")
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"CRITICAL: Connection Error: {e}")
        return
    
    query = "SELECT flight_number, origin_airport, month_period, actual_departure_time FROM flight_log"
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No flight data found.")
        conn.close()
        return

    df['dt'] = pd.to_datetime(df['actual_departure_time'])
    df['seconds'] = df['dt'].dt.hour * 3600 + df['dt'].dt.minute * 60
    schedule = df.groupby(['flight_number', 'origin_airport', 'month_period'])['seconds'].median().reset_index()
    
    cursor = conn.cursor()
    count_updated = 0
    for index, row in schedule.iterrows():
        m, s = divmod(row['seconds'], 60)
        h, m = divmod(m, 60)
        time_str = "{:02d}:{:02d}:{:02d}".format(int(h), int(m), int(s))
        
        sql = """
        MERGE monthly_schedule AS target
        USING (SELECT ? AS f_num, ? AS o_air, ? AS m_per) AS source
        ON (target.flight_number = source.f_num AND target.origin_airport = source.o_air AND target.month_period = source.m_per)
        WHEN MATCHED THEN UPDATE SET estimated_std_time = ?
        WHEN NOT MATCHED THEN INSERT (flight_number, origin_airport, month_period, estimated_std_time) VALUES (source.f_num, source.o_air, source.m_per, ?);
        """
        cursor.execute(sql, (row['flight_number'], row['origin_airport'], row['month_period'], time_str, time_str))
        count_updated += 1
        
    conn.commit()
    print(f"Success! Updated {count_updated} schedules.")
    conn.close()

if __name__ == "__main__":
    update_schedule()
