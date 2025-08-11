# Silver Layer Processing – AE1 Data Pipeline

This folder contains documentation and screenshots for **Silver Layer** processing in the AE1 Advanced Data Engineering pipeline.

---

## Overview

The Silver Layer processes cleansed and structured data from the Bronze Layer into optimized formats for analytics.  

### Sources:
- **IMDB Bronze Data** → Processed to Silver via **Dataproc PySpark batch job**.
- **NASA Bronze Data** → Processed to Silver via **Apache Beam / Dataflow** streaming job.

---

## Steps Completed

### 1. **Dataproc Batch Processing (IMDB)**
- Submitted PySpark job (`dataproc/imdb_to_silver.py`) to Dataproc cluster (`imdb-cluster`) in `us-central1`.
- Job processed IMDB `.tsv` data from Bronze and wrote cleaned Parquet output to: gs://ae1-silver-data-bucket/imdb/


**Screenshot – Dataproc job success**  
![Dataproc Job Success](screenshots/dataproc_job_success.png)

---

### 2. **Silver Bucket Verification**
- Verified Parquet files in `ae1-silver-data-bucket/imdb/`.
- Files follow partition naming and are in **Snappy-compressed Parquet** format.

**Screenshot – Parquet files in Silver bucket**  
![Parquet Files in Silver Bucket](screenshots/parquet_files_silver_bucket.png)

---

### 3. **BigQuery External Table (IMDB Silver)**
- Created an external table in BigQuery for querying the IMDB Silver Parquet files without loading them into BigQuery storage.

**SQL Used:**  
```sql
CREATE SCHEMA IF NOT EXISTS `upheld-caldron-468622-n6.imdb_silver`
OPTIONS(location="US");

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_silver.movies`
OPTIONS (
format = 'PARQUET',
uris = ['gs://ae1-silver-data-bucket/imdb/part-*.parquet']
);
```

