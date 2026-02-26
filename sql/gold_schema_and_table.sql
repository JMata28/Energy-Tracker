USE EnergyTrackerDB2;
GO

-- 1. Ensure the schema 'gold' exists
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'gold'
)
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO

-- 2. Create the table if it does not exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'eia_daily_summary'
      AND schema_id = SCHEMA_ID('gold')
)
BEGIN
    CREATE TABLE gold.eia_daily_summary (
        date DATE NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        region_name VARCHAR(50),
        total_demand_mwh INT,
        average_demand_mwh FLOAT,
        total_forecast_mwh INT,
        total_net_generation_mwh INT,
        daily_interchange_mwh INT,
        value_units VARCHAR(20),
        ingested_at DATETIME NOT NULL DEFAULT GETUTCDATE(),
        PRIMARY KEY (date, region_code)
    );
END;
GO