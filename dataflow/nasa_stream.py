import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def parse_json(msg_bytes):
    obj = json.loads(msg_bytes.decode("utf-8"))
    # keep a minimal schema that matches your earlier pandas/plots
    return {
        "messageType": obj.get("messageType"),
        "messageIssueTime": obj.get("messageIssueTime"),
        "messageBody": obj.get("messageBody"),
    }

def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_subscription")
    ap.add_argument("--input_topic")
    ap.add_argument("--project", required=True)
    ap.add_argument("--region", default="us-central1")
    ap.add_argument("--dataset", default="nasa_raw")
    ap.add_argument("--table", default="nasa_staging")
    ap.add_argument("--temp_location", required=True)
    args, beam_args = ap.parse_known_args()

    opts = PipelineOptions(
        beam_args,
        streaming=True,
        save_main_session=True,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
    )

    table = f"{args.project}:{args.dataset}.{args.table}"
    schema = "messageType:STRING,messageIssueTime:STRING,messageBody:STRING"

    with beam.Pipeline(options=opts) as p:
        source = (beam.io.ReadFromPubSub(subscription=args.input_subscription)
                  if args.input_subscription else
                  beam.io.ReadFromPubSub(topic=args.input_topic))
        (
            p
            | "Read" >> source
            | "Parse" >> beam.Map(parse_json)
            | "WriteBQ" >> beam.io.WriteToBigQuery(
                table=table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
