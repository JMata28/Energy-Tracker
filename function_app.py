from datetime import datetime, timedelta, timezone
import json
import logging
import os
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import azure.functions as func
from silver_layer import silver_layer
from gold_layer import gold_layer

app = func.FunctionApp()
app.register_blueprint(silver_layer)
app.register_blueprint(gold_layer)

EIA_API_KEY = os.environ.get("EIA_API_KEY")
BLOB_CONN_STR = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")

EIA_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data"
METADATA_BLOB_PATH = "metadata/last_ingested.json"


def format_eia_datetime(dt: datetime) -> str:
    """Format datetime to EIA expected format: YYYY-MM-DDTHH"""
    return dt.strftime("%Y-%m-%dT%H")


@app.timer_trigger(
    schedule="0 10 * * * *",  # after 10 minutes past every hour
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=False
)

def Request_EIA_API(myTimer: func.TimerRequest) -> None:
    logging.info("Function scheduled to run 10 minutes past the hour to allow upstream data availability.")

    if myTimer.past_due:
        logging.warning("The timer is past due!")

    if not EIA_API_KEY:
        logging.error("EIA_API_KEY is not set")
        return

    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)

    # ---------------------------------------------------
    # 1. Determine incremental time window
    # ---------------------------------------------------
    try:
        metadata_blob = container_client.get_blob_client(METADATA_BLOB_PATH)
        metadata = json.loads(metadata_blob.download_blob().readall())
        last_ingested = datetime.fromisoformat(metadata["last_ingested_utc"])
        start_dt = last_ingested + timedelta(hours=1)
        logging.info(f"Last ingested timestamp found: {last_ingested.isoformat()}")
    except ResourceNotFoundError:
        # First run: bootstrap with last 30 days
        end_bootstrap = datetime.now(timezone.utc)
        start_dt = end_bootstrap - timedelta(days=30)
        logging.info("No metadata found. Bootstrapping with last 30 days of data.")

    end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    if start_dt >= end_dt:
        logging.info("No new data to ingest. Exiting.")
        return

    # ---------------------------------------------------
    # 2. Build API request
    # ---------------------------------------------------
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "ISNE",
        "start": format_eia_datetime(start_dt),
        "end": format_eia_datetime(end_dt)
    }

    logging.info(f"Requesting data from {params['start']} to {params['end']}")

    try:
        response = requests.get(EIA_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        records = data.get("response", {}).get("data", [])
        logging.info(f"EIA API call successful: {len(records)} records")

        if not records:
            logging.info("API returned no new records. Exiting.")
            return

        # ---------------------------------------------------
        # 3. Upload raw data to Bronze layer
        # ---------------------------------------------------
        now = datetime.now(timezone.utc)
        blob_path = (
            f"bronze/eia/"
            f"{now.year}/{now.month:02}/{now.day:02}/{now.hour:02}/"
            f"{now.minute:02}{now.second:02}.json"
        )

        container_client.upload_blob(
            name=blob_path,
            data=json.dumps(data),
            overwrite=False
        )

        logging.info(f"Data uploaded to Blob Storage at {blob_path}")

        # ---------------------------------------------------
        # 4. Update metadata (last ingested timestamp)
        # ---------------------------------------------------
        new_last_ingested = format_eia_datetime(end_dt)

        metadata_payload = {
            "last_ingested_utc": end_dt.isoformat()
        }

        metadata_blob = container_client.get_blob_client(METADATA_BLOB_PATH)
        metadata_blob.upload_blob(
            data=json.dumps(metadata_payload),
            overwrite=True
        )

        logging.info(f"Updated metadata last_ingested_utc to {new_last_ingested}")

    except requests.exceptions.RequestException as e:
        logging.error(f"EIA API request failed: {e}")
    except Exception as e:
        logging.error(f"Pipeline failure: {e}")

