USE EnergyTrackerDB;
GO

IF NOT EXISTS (SELECT * FROM sys.tables 
               WHERE name = 'eia_daily_summary' 
                 AND schema_id = SCHEMA_ID('gold'))
BEGIN
    CREATE TABLE gold.eia_daily_summary (
        date DATE NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        region_name VARCHAR(50),
        total_demand_mwh INT,
        average_demand_mwh FLOAT,
        peak_demand_mwh INT,
        peak_hour INT,
        total_forecast_mwh INT,
        forecast_error_mwh INT,
        average_forecast_error_mwh FLOAT,
        total_net_generation_mwh INT,
        total_interchange_mwh INT,
        value_units VARCHAR(20),
        ingested_at DATETIME NOT NULL DEFAULT GETUTCDATE(),
        PRIMARY KEY (date, region_code)
    );
END;
GO