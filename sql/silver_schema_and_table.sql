USE EnergyTrackerDB2;
GO

-- 1. Ensure the schema 'silver' exists
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'silver'
)
BEGIN
    EXEC('CREATE SCHEMA silver');
END;
GO

-- 2. Create the table if it does not exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'eia_hourly_data'
      AND schema_id = SCHEMA_ID('silver')
)
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
GO
