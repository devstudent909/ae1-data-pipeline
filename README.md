# AE1 Data Pipeline – GCP Implementation

This repository contains the deliverables for **AE1 – Advanced Data Engineering**   
The project implements an **end-to-end data pipeline** in **Google Cloud Platform (GCP)** for ingesting, processing, and visualizing data from multiple sources (IMDB full dump & NASA DONKI API).

---


## Repository Structure


```
ae1-data-pipeline/
│
├── bronze_ingest/                      # Bronze (Raw) layer ingestion steps, screenshots, README
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
├── sql_queries/                         # BigQuery SQL scripts
│   ├── nasa_silver_create.sql
│   └── imdb_silver_create.sql
│
└── README.md                            # (this file)
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

- **First commit:** – Created repo, added Bronze Layer ingestion README & IMDB screenshot.
- **Second commit** – Added NASA Cloud Function and Dataflow streaming pipeline code.
- **Third commit** - Add NASA ingestion, Dataflow pipeline, IMDB Dataproc batch processing, and update documentation
