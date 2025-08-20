# AE1 Data Pipeline – GCP Implementation

This repository contains the deliverables for **AE1 – Advanced Data Engineering**   
The project implements an **end-to-end data pipeline** in **Google Cloud Platform (GCP)** for ingesting, processing, and visualizing data from multiple sources (IMDB full dump & NASA DONKI API).

---


## Repository Structure


```


ae1-data-pipeline/
│
├── bronze_ingest/                      # Bronze (Raw) layer ingestion steps, README
│   ├── imdb/                            # IMDB raw ingestion
│   ├── nasa/                            # NASA raw ingestion
│   └── README.md
│
├── cloud_function/                      # NASA DONKI API Cloud Function
│   ├── main.py
│   ├── requirements.txt
│   └── nasa.py
│
├── dataflow/                            # Apache Beam / Dataflow streaming pipelines
│   ├── nasa_stream.py
│   └── requirements.txt
│
├── dataproc/                            # Spark jobs for batch transformations
│   └── imdb_to_silver.py
│
├── docs/                                # Centralized diagrams & screenshots
│   ├── Task-1-architecture.png
│   ├── dataproc_job_success.png
│   ├── parquet_files_silver_bucket.png
│   ├── bigquery_external_table.png
│   ├── dataflow_running.png
│   ├── dataflow_success.png
│   └── table_mapping.png
│
├── silver_process/                      # Data processing for the Silver layer
│   └── README.md
│
└── sql/                                 # BigQuery SQL scripts
    ├── nasa/
    │   └── nasa_silver_create.sql
    ├── imdb/
    │   ├── create-all-tables-silver-imdb.sql
    │   ├── create-raw-imdb-dataset.sql
    │   ├── create-raw-imdb-tables-full-final.sql
    │   ├── create-silver-imdb-title-basics.sql
    │   ├── create-tables-from-raw-imdb-dataset.sql
    │   ├── recreate-name-basics-raw-imdb.sql
    │   └── recreate-title-basics-imdb-raw.sql
    └── combined-gold/                  
        ├── create-gold-analytics-schema.sql
        └── gold-analytics.sql


```


---

## Pipeline Overview

### 1. **Ingestion (Bronze Layer)**
- **IMDB** `.tsv.gz` full dump uploaded to GCS (`ae1-bronze-data-bucket/imdb/`).
- **NASA DONKI API** data ingested via GCP Cloud Function → Pub/Sub → Bronze backup in `ae1-bronze-data-bucket/nasa/`.

### 2. **Streaming to Silver (NASA only)**
- NASA raw JSON streamed from Bronze to Silver using Apache Beam / Dataflow pipeline.
- Cleans data (basic field formatting, null handling) before writing to `ae1-silver-data-bucket/nasa/`.
- BigQuery external table + partitioned/clustering table created for efficient querying.

### 3. **Batch Processing (IMDB Bronze → Silver)**
- IMDB `.tsv.gz` processed using PySpark job on Dataproc.
- Writes cleaned data to `ae1-silver-data-bucket/imdb/`.

---

## Links

- **Bronze Layer README:** [`bronze_ingest/README.md`](bronze_ingest/README.md)


---

## Notes

- **Large datasets** (IMDB dump) are **not stored in GitHub**. Only documentation, screenshots, and code are included.
- Code is tested in **GCP us (multiple regions in United States)** regions.
- Bucket naming convention:  
  - `ae1-bronze-data-bucket` (Raw zone)  
  - `ae1-silver-data-bucket` (Cleansed zone)  
  - `ae1-gold-data-bucket` (Curated zone)  
  - `ae1-tmp-data-bucket` (temporary staging)

---

## Change Log

- **First commit:** – Created repo, added Bronze Layer ingestion README & IMDB screenshot
- **Second commit** – Added NASA Cloud Function and Dataflow streaming pipeline code
- **Third commit** - Added NASA ingestion, Dataflow pipeline, IMDB Dataproc batch processing, and update documentation
- **Fourth commit** - Added Silver processing documentation, screenshots, and SQL for IMDB Silver external table
- **Fifth commit** - Restructured and added new screenshots into docs, added SQL script files
- **Sixth commit** - Updated main.py in cloud_function to fetch more data from Nasa DONKI API
