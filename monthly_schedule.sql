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
