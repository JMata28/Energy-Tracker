import json
import logging
from datetime import datetime, timezone

import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os

silver_layer = func.Blueprint()

BLOB_CONN_STR = os.environ.get("BLOB_CONNECTION_STRING")
SILVER_CONTAINER = os.environ.get("BLOB_CONTAINER")  # same container, different folder


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

        # Write Silver output
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service_client.get_container_client(SILVER_CONTAINER)

        now = datetime.now(timezone.utc)
        silver_path = (
            f"silver/eia/"
            f"{now.year}/{now.month:02}/{now.day:02}/"
            f"{myblob.name.split('/')[-1]}"
        )

        container_client.upload_blob(
            name=silver_path,
            data=json.dumps(silver_data),
            overwrite=True
        )

        logging.info(f"Silver data written to {silver_path}")

    except Exception as e:
        logging.error(f"Silver processing failed: {e}")
        raise