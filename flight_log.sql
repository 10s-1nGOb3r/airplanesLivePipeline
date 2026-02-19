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
