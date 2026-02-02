IF NOT EXISTS (SELECT * FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE t.name = 'eia_hourly_data' AND s.name = 'silver')
BEGIN
    CREATE TABLE silver.eia_hourly_data (
        timestamp DATETIME2 NOT NULL,
        region_code NVARCHAR(10) NOT NULL,
        region_name NVARCHAR(50),
        demand_mwh INT,
        demand_forecast_mwh INT,
        net_generation_mwh INT,
        total_interchange_mwh INT,
        value_units NVARCHAR(20),
        ingested_at DATETIME2 NOT NULL,
        CONSTRAINT PK_eia_hourly_data PRIMARY KEY (timestamp, region_code)
    );
END;