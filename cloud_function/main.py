import os
import json
import datetime
import requests
from google.cloud import pubsub_v1, storage

# Env vars (set at deploy time)
NASA_API_KEY = os.environ.get("NASA_API_KEY", "DEMO_KEY")
PUBSUB_TOPIC = os.environ["PUBSUB_TOPIC"]  # e.g. projects/<PROJECT_ID>/topics/nasa-topic
RAW_BUCKET   = os.environ.get("RAW_BUCKET")  # optional: ae1-bronze-data-bucket

def _dates_last_n_days(n=7):
    today = datetime.datetime.utcnow().date()
    start = today - datetime.timedelta(days=n)
    return start.isoformat(), today.isoformat()

def fetch_nasa_data(request):
    """HTTP-triggered function: fetches DONKI notifications (last 7 days),
       publishes each notification to Pub/Sub, and (optionally) backs up JSON to GCS."""
    startDate, endDate = _dates_last_n_days(7)
    url = "https://api.nasa.gov/DONKI/notifications"
    params = {"startDate": startDate, "endDate": endDate, "type": "all", "api_key": NASA_API_KEY}

    # Fetch
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    items = resp.json()
    if not isinstance(items, list):
        # DONKI sometimes returns {} when empty
        items = []

    # Publish to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    published = 0
    for rec in items:
        publisher.publish(PUBSUB_TOPIC, json.dumps(rec).encode("utf-8"))
        published += 1

    # Optional backup to Bronze (single file per run)
    if RAW_BUCKET:
        blob_path = f"nasa/notifications_{startDate}_{endDate}.json"
        storage.Client().bucket(RAW_BUCKET).blob(blob_path).upload_from_string(
            json.dumps(items, indent=2), content_type="application/json"
        )

    return f"Published {published} messages from {startDate} to {endDate}."
