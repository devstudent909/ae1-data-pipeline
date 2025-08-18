import os
import json
import datetime
import requests
from google.cloud import pubsub_v1, storage

# --------------------------
# Environment variables (set at deploy time in Cloud Functions)
# --------------------------
NASA_API_KEY = os.environ.get("NASA_API_KEY", "DEMO_KEY")
PUBSUB_TOPIC = os.environ["PUBSUB_TOPIC"]  # e.g. projects/<PROJECT_ID>/topics/nasa-topic
RAW_BUCKET = os.environ.get("RAW_BUCKET")  # optional backup to GCS

# --------------------------
# Configurable NASA DONKI endpoints
# --------------------------
DONKI_ENDPOINTS = {
    "notifications": "https://api.nasa.gov/DONKI/notifications",
    "CME": "https://api.nasa.gov/DONKI/CME",
    "GST": "https://api.nasa.gov/DONKI/GST",
    "SEP": "https://api.nasa.gov/DONKI/SEP",
    "FLR": "https://api.nasa.gov/DONKI/FLR",
    "HSS": "https://api.nasa.gov/DONKI/HSS",
    "WSAEnlilSimulations": "https://api.nasa.gov/DONKI/WSAEnlilSimulations",
    "IPS": "https://api.nasa.gov/DONKI/IPS"
}

# --------------------------
# Helper: date range utility
# --------------------------
def _dates_last_n_days(n=7):
    today = datetime.datetime.utcnow().date()
    start = today - datetime.timedelta(days=n)
    return start.isoformat(), today.isoformat()

# --------------------------
# Cloud Function entry point
# --------------------------
def fetch_nasa_data(request):
    """
    HTTP-triggered function:
    - For each DONKI endpoint, fetches data for the last N days
    - Publishes each record to Pub/Sub
    - Optionally writes a full JSON dump to GCS Bronze bucket
    """
    publisher = pubsub_v1.PublisherClient()
    startDate, endDate = _dates_last_n_days(7)

    storage_client = storage.Client() if RAW_BUCKET else None
    total_published = 0

    for endpoint_name, base_url in DONKI_ENDPOINTS.items():
        print(f"Fetching endpoint: {endpoint_name}")

        params = {
            "startDate": startDate,
            "endDate": endDate,
            "api_key": NASA_API_KEY
        }

        # Notifications endpoint has an extra 'type' param
        if endpoint_name == "notifications":
            params["type"] = "all"

        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            items = resp.json() or []
            if not isinstance(items, list):
                items = []

            # Publish each record to Pub/Sub
            published_count = 0
            for rec in items:
                # Add endpoint type into the record for downstream identification
                rec["_donki_type"] = endpoint_name
                publisher.publish(PUBSUB_TOPIC, json.dumps(rec).encode("utf-8"))
                published_count += 1

            total_published += published_count
            print(f"Published {published_count} records from {endpoint_name}")

            # Backup JSON to Bronze bucket
            if RAW_BUCKET and storage_client:
                blob_path = f"nasa/{endpoint_name}_{startDate}_{endDate}.json"
                storage_client.bucket(RAW_BUCKET).blob(blob_path).upload_from_string(
                    json.dumps(items, indent=2),
                    content_type="application/json"
                )
                print(f"Backed up {endpoint_name} data to gs://{RAW_BUCKET}/{blob_path}")

        except Exception as e:
            print(f"Error fetching {endpoint_name}: {e}")

    return f"Fetched and published {total_published} records from {len(DONKI_ENDPOINTS)} endpoints, covering {startDate} to {endDate}."

