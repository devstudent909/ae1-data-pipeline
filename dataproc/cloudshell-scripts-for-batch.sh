# Cloud shell operations

# Create a cluster
PROJECT_ID=upheld-caldron-468622-n6
REGION=us-central1
BUCKET=ae1-tmp-data

gcloud config set project $PROJECT_ID

# Create a small single-node cluster (≈4 vCPUs total — under your quota)
gcloud dataproc clusters create imdb-mini \
  --region=$REGION \
  --single-node \
  --master-machine-type=e2-standard-4 \
  --master-boot-disk-size=100GB \
  --image-version=2.2-debian12


# Upload the file from my repo to cloudshell
gsutil cp dataproc/task3_dw_batch_dw_source.py gs://ae1-tmp-data/jobs/task3_dw_batch_dw_source.py

# Sanity check timestamp
gsutil ls -l gs://ae1-tmp-data/jobs/task3_dw_batch_dw_source.py 



# Upload a versioned copy to GCS
STAMP=$(date +%Y%m%d-%H%M%S)
gsutil cp dataproc/task3_dw_batch_dw_source.py gs://ae1-tmp-data/jobs/task3/$STAMP/task3_dw_batch_dw_source.py
gsutil ls -l gs://ae1-tmp-data/jobs/task3/$STAMP/

# Drop the old output (avoid stale rows)
bq rm -t -f upheld-caldron-468622-n6:dw_imdb_agg.top_movie_by_year

# Run the job
gcloud dataproc jobs submit pyspark gs://ae1-tmp-data/jobs/task3/$STAMP/task3_dw_batch_dw_source.py \
  --project=upheld-caldron-468622-n6 \
  --region=us-central1 \
  --cluster=imdb-mini \
  --properties="spark.driverEnv.PROJECT_ID=upheld-caldron-468622-n6,\
spark.executorEnv.PROJECT_ID=upheld-caldron-468622-n6,\
spark.driverEnv.SRC_DATASET=dw_imdb,\
spark.executorEnv.SRC_DATASET=dw_imdb,\
spark.driverEnv.OUT_DATASET=dw_imdb_agg,\
spark.executorEnv.OUT_DATASET=dw_imdb_agg,\
spark.driverEnv.MIN_VOTES=50000,\
spark.executorEnv.MIN_VOTES=50000,\
spark.bigquery.tempGcsBucket=ae1-tmp-data"

# Verify
bq query --use_legacy_sql=false \
'SELECT * FROM `upheld-caldron-468622-n6.dw_imdb_agg.top_movie_by_year`
 ORDER BY start_year DESC LIMIT 15'





