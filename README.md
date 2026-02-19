Flight Tracking & Schedule Estimation Pipeline
This repository contains two Python-based data pipelines designed to track live aircraft departures across 14 major Indonesian airports and calculate estimated competitor flight schedules based on historical takeoff data. It is fully optimized for downstream reporting in Power BI.

🚀 Overview
Pipeline 1: Live Tracker (pipeline_live_tracker.py)
Polls the airplanes.live API to monitor live ADS-B data around target airports. It uses specific altitude, speed, and vertical rate logic to detect actual takeoffs, converting UTC times to local timezones, and logging the events into a SQL database.

Pipeline 2: Schedule Calculator (pipeline_calculate_schedule.py)
Aggregates the logged takeoff data on a monthly basis to calculate the median departure time for each flight number. It then updates a schedule table to provide a highly accurate estimated Scheduled Time of Departure (STD).

🛠️ Prerequisites & Installation
Python Version: Python 3.8+ recommended.

Database: Microsoft SQL Server (or compatible).

Dependencies: Install the required Python packages:

Bash
pip install requests pyodbc pandas python-dotenv
⚙️ Environment Variables
Create a .env file in the root directory to store your database credentials. Both scripts rely on these variables to establish a connection:

Cuplikan kode
DB_DRIVER={ODBC Driver 17 for SQL Server} # Update based on your installed driver
DB_HOST2=your_server_address
DB_NAME4=your_database_name
DB_USER2=your_database_user
# Note: Trusted_Connection=yes is used in the scripts. Ensure your environment supports Windows Authentication or adjust the connection string accordingly.
🗄️ Database Setup
Run the following SQL script in your SQL Server environment to create the necessary tables.

Note on Power BI Integration: Both tables include a persisted computed column (pbi_link_key) and associated indexing. This is specifically designed to act as a highly performant relationship key when importing the data model into Power BI.

SQL
-- ============================================================
-- flight_log table (Stores raw takeoff events)
-- ============================================================
CREATE TABLE flight_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    origin_airport VARCHAR(5) NOT NULL,
    destination_airport VARCHAR(5),
    actual_departure_time DATETIME NOT NULL,
    month_period VARCHAR(7) NOT NULL,
    
    -- Power BI Link Key (Computed & Persisted for performance)
    pbi_link_key AS (CAST(flight_number AS VARCHAR(10)) + '-' + 
                     CAST(origin_airport AS VARCHAR(5)) + '-' + 
                     CAST(month_period AS VARCHAR(7))) PERSISTED,
    
    created_at DATETIME DEFAULT GETDATE()
);

CREATE INDEX IX_flight_log_pbi_link_key ON flight_log(pbi_link_key);

-- ============================================================
-- monthly_schedule table (Stores aggregated schedule estimates)
-- ============================================================
CREATE TABLE monthly_schedule (
    flight_number VARCHAR(10) NOT NULL,
    origin_airport VARCHAR(5) NOT NULL,
    month_period VARCHAR(7) NOT NULL,
    estimated_std_time TIME NOT NULL,
    sample_size INT DEFAULT 0,
    
    -- Power BI Link Key
    pbi_link_key AS (CAST(flight_number AS VARCHAR(10)) + '-' + 
                     CAST(origin_airport AS VARCHAR(5)) + '-' + 
                     CAST(month_period AS VARCHAR(7))) PERSISTED,
    
    CONSTRAINT PK_monthly_schedule PRIMARY KEY (flight_number, origin_airport, month_period)
);

CREATE INDEX IX_monthly_schedule_pbi_link_key ON monthly_schedule(pbi_link_key);
📂 Pipeline Details
1. Live Flight Tracker (pipeline_live_tracker.py)
This script acts as a continuous scanner. It is recommended to run this via a cron job or task scheduler at frequent intervals (e.g., every 2-5 minutes).

Surveillance Stations: Monitors a 50NM radius around 14 Indonesian airports (CGK, HLP, DPS, SUB, KNO, UPG, BDJ, BPN, PKU, SOC, SRG, YIA, JOG, LOP).

Takeoff Detection Logic: Identifies a departure if the aircraft meets either of these conditions:

Altitude between 500 - 6000 ft, Vertical Rate > 50, Ground Speed > 100 knots.

Altitude between 500 - 2500 ft, Vertical Rate >= 0, Ground Speed > 130 knots.

Exclusions: Automatically ignores flights without callsigns and filters out specific operator prefixes (QG, CTV).

Deduplication: Prevents duplicate logging by ensuring the same flight number and origin aren't logged more than once within a 45-minute window.

2. Schedule Calculator (pipeline_calculate_schedule.py)
This script is designed for batch processing. It should ideally be run nightly or weekly to update the estimated schedules based on accumulated data.

Logic: Pulls all historical flights from flight_log. Calculates the median time of departure (in seconds from midnight) for every unique combination of flight_number, origin_airport, and month_period.

Database Operation: Uses a SQL MERGE (Upsert) statement to efficiently insert new schedule estimates or update existing ones in the monthly_schedule table.
