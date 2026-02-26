import logging
import os
import pyodbc
import azure.functions as func

gold_layer = func.Blueprint()

# ----------------------------
# Azure SQL connection settings
# ----------------------------
SQL_SERVER = os.environ.get("SQL_SERVER")
SQL_DATABASE = os.environ.get("SQL_DATABASE")
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
SQL_DRIVER = "{ODBC Driver 18 for SQL Server}"

# How many past days to reprocess (handles late-arriving data)
REPROCESS_DAYS = 30

# ----------------------------
# Gold aggregation function
# ----------------------------
@gold_layer.timer_trigger(
    schedule="0 21 * * * *",  # run 15 minutes past every hour
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def aggregate_gold(myTimer: func.TimerRequest):
    logging.info("Gold layer aggregation started.")

    conn = None
    cursor = None

    try:
        # ----------------------------
        # Connect to Azure SQL
        # ----------------------------
        conn_str = (
            f"DRIVER={SQL_DRIVER};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # ----------------------------
        # Idempotent + Late-Data-Safe MERGE
        # Recalculates only recent N days
        # ----------------------------
        aggregate_query = f"""
        WITH DailyAggregates AS (
            SELECT
                CAST(s.[timestamp] AS DATE) AS [date],
                s.region_code,
                MAX(s.region_name) AS region_name,
                SUM(s.demand_mwh) AS total_demand_mwh,
                AVG(CAST(s.demand_mwh AS FLOAT)) AS average_demand_mwh,
                SUM(s.demand_forecast_mwh) AS total_forecast_mwh,
                SUM(s.net_generation_mwh) AS total_net_generation_mwh,
                SUM(s.total_interchange_mwh) AS daily_interchange_mwh,
                MAX(s.value_units) AS value_units,
                SYSUTCDATETIME() AS ingested_at
            FROM silver.eia_hourly_data s
            WHERE s.[timestamp] >= DATEADD(DAY, -{REPROCESS_DAYS}, CAST(GETUTCDATE() AS DATE))
            GROUP BY CAST(s.[timestamp] AS DATE), s.region_code
        )

        MERGE gold.eia_daily_summary AS target
        USING DailyAggregates AS source
        ON target.[date] = source.[date]
           AND target.region_code = source.region_code

        WHEN MATCHED THEN
            UPDATE SET
                target.region_name = source.region_name,
                target.total_demand_mwh = source.total_demand_mwh,
                target.average_demand_mwh = source.average_demand_mwh,
                target.total_forecast_mwh = source.total_forecast_mwh,
                target.total_net_generation_mwh = source.total_net_generation_mwh,
                target.daily_interchange_mwh = source.daily_interchange_mwh,
                target.value_units = source.value_units,
                target.ingested_at = source.ingested_at

        WHEN NOT MATCHED THEN
            INSERT (
                [date],
                region_code,
                region_name,
                total_demand_mwh,
                average_demand_mwh,
                total_forecast_mwh,
                total_net_generation_mwh,
                daily_interchange_mwh,
                value_units,
                ingested_at
            )
            VALUES (
                source.[date],
                source.region_code,
                source.region_name,
                source.total_demand_mwh,
                source.average_demand_mwh,
                source.total_forecast_mwh,
                source.total_net_generation_mwh,
                source.daily_interchange_mwh,
                source.value_units,
                source.ingested_at
            );
        """

        cursor.execute(aggregate_query)
        conn.commit()

        logging.info("Gold aggregation completed successfully.")

    except Exception as e:
        logging.error(f"Gold aggregation failed: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()