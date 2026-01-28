from datetime import datetime, timezone
import json
import logging
import os
import requests
from azure.storage.blob import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()

EIA_API_KEY = os.environ.get("EIA_API_KEY")
BLOB_CONN_STR = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")
EIA_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data"

@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=False
)
def Request_EIA_API(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("The timer is past due!")

    if not EIA_API_KEY:
        logging.error("EIA_API_KEY is not set")
        return

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "ISNE",
        "start": "2025-01-26T23",
        "end": "2025-02-26T23"
    }

    try:
        response = requests.get(EIA_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logging.info(f"EIA API call successful: {len(data.get('response', {}).get('data', []))} records")

        # Upload to Blob Storage with timestamp including minutes & seconds
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER)

        now = datetime.now(timezone.utc)
        blob_path = f"bronze/eia/{now.year}/{now.month:02}/{now.day:02}/{now.hour:02}/{now.minute:02}{now.second:02}.json"

        container_client.upload_blob(name=blob_path, data=json.dumps(data), overwrite=False)
        logging.info(f"Data uploaded to Blob Storage at {blob_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"EIA API request failed: {e}")
    except Exception as e:
        logging.error(f"Failed to upload to Blob Storage: {e}")