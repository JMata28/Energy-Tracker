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
SQL_USERNAME = os.environ.get("SQL_USERNAME")       
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")       
SQL_DRIVER = "{ODBC Driver 18 for SQL Server}"     

# ----------------------------
# Gold aggregation function
# ----------------------------
@gold_layer.timer_trigger(
    schedule="0 15 * * * *",  # run 15 min past every hour
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def aggregate_gold(myTimer: func.TimerRequest):
    logging.info("Gold layer aggregation started.")

    try:
        # Connect to Azure SQL
        conn_str = (
            f"DRIVER={SQL_DRIVER};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # ----------------------------
        # Aggregate daily totals per region
        # ----------------------------
        # Only insert rows that don't already exist in gold table
        aggregate_query = """
        INSERT INTO gold.gold_data (timestamp, region_code, total_demand_mwh, total_net_generation_mwh, ingested_at)
        SELECT
            CAST(s.timestamp AS DATE) AS day,
            s.region_code,
            SUM(s.demand_mwh) AS total_demand_mwh,
            SUM(s.net_generation_mwh) AS total_net_generation_mwh,
            GETUTCDATE() AS ingested_at
        FROM silver.silver_data s
        GROUP BY CAST(s.timestamp AS DATE), s.region_code
        HAVING NOT EXISTS (
            SELECT 1
            FROM gold.gold_data g
            WHERE g.timestamp = CAST(s.timestamp AS DATE)
              AND g.region_code = s.region_code
        )
        """

        cursor.execute(aggregate_query)
        conn.commit()
        logging.info("Gold aggregation completed successfully without duplicates.")

    except Exception as e:
        logging.error(f"Gold aggregation failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()