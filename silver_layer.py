import json
import logging
from datetime import datetime, timezone
import os

import azure.functions as func
import pyodbc

silver_layer = func.Blueprint()

# SQL connection info
SQL_SERVER = os.environ.get("SQL_SERVER") 
SQL_DATABASE = os.environ.get("SQL_DATABASE")  
SQL_USER = os.environ.get("SQL_USER")        
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
SQL_DRIVER = "{ODBC Driver 18 for SQL Server}"

@silver_layer.blob_trigger(
    arg_name="myblob",
    path="eia-demand-data/bronze/eia/{name}",
    connection="BLOB_CONNECTION_STRING"
)
def process_data_silver(myblob: func.InputStream):
    logging.info(f"Processing Bronze blob: {myblob.name}")

    try:
        raw_data = json.loads(myblob.read())
        records = raw_data.get("response", {}).get("data", [])
        if not records:
            logging.warning("No records found in Bronze file")
            return

        # Prepare Silver rows
        silver_rows = {}
        for r in records:
            period = r["period"]
            region = r["respondent"]
            key = (period, region)
            if key not in silver_rows:
                silver_rows[key] = {
                    "timestamp": f"{period}:00:00Z",
                    "region_code": region,
                    "region_name": r.get("respondent-name"),
                    "demand_mwh": None,
                    "demand_forecast_mwh": None,
                    "net_generation_mwh": None,
                    "total_interchange_mwh": None,
                    "value_units": r.get("value-units"),
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                }

            value = int(r["value"])
            match r["type"]:
                case "D":
                    silver_rows[key]["demand_mwh"] = value
                case "DF":
                    silver_rows[key]["demand_forecast_mwh"] = value
                case "NG":
                    silver_rows[key]["net_generation_mwh"] = value
                case "TI":
                    silver_rows[key]["total_interchange_mwh"] = value

        silver_data = list(silver_rows.values())

        # Connect to SQL Database
        conn_str = (
            f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
            f"UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for row in silver_data:
                cursor.execute("""
                    MERGE INTO silver.eia_hourly_data AS target
                    USING (SELECT ? AS timestamp, ? AS region_code) AS source
                    ON target.timestamp = source.timestamp AND target.region_code = source.region_code
                    WHEN MATCHED THEN
                        UPDATE SET
                            region_name = ?, demand_mwh = ?, demand_forecast_mwh = ?,
                            net_generation_mwh = ?, total_interchange_mwh = ?, value_units = ?,
                            ingested_at = ?
                    WHEN NOT MATCHED THEN
                        INSERT (timestamp, region_code, region_name, demand_mwh, demand_forecast_mwh,
                                net_generation_mwh, total_interchange_mwh, value_units, ingested_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                row["timestamp"], row["region_code"],
                row["region_name"], row["demand_mwh"], row["demand_forecast_mwh"],
                row["net_generation_mwh"], row["total_interchange_mwh"], row["value_units"],
                row["ingested_at"],
                row["timestamp"], row["region_code"], row["region_name"], row["demand_mwh"],
                row["demand_forecast_mwh"], row["net_generation_mwh"], row["total_interchange_mwh"],
                row["value_units"], row["ingested_at"])
            conn.commit()
        logging.info(f"Silver data successfully written to SQL database ({len(silver_data)} rows)")

    except Exception as e:
        logging.error(f"Silver processing failed: {e}")
        raise