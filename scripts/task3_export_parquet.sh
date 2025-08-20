#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-upheld-caldron-468622-n6}"
DEST_BUCKET="${DEST_BUCKET:-ae1-gold-data-bucket}"   # must be US
DATASET="${DATASET:-dw_imdb_agg}"
PREFIX="${PREFIX:-task3}"

bq extract --location=US --destination_format=PARQUET \
  "${PROJECT_ID}:${DATASET}.genre_year"        "gs://${DEST_BUCKET}/${PREFIX}/genre_year/*.parquet"

bq extract --location=US --destination_format=PARQUET \
  "${PROJECT_ID}:${DATASET}.director_stats"    "gs://${DEST_BUCKET}/${PREFIX}/director_stats/*.parquet"

bq extract --location=US --destination_format=PARQUET \
  "${PROJECT_ID}:${DATASET}.top_movie_by_year" "gs://${DEST_BUCKET}/${PREFIX}/top_movie_by_year/*.parquet"
