# AE1 Data Pipeline – GCP Implementation

This repository contains the deliverables for **AE1 – Advanced Data Engineering** (Summer-1).  
The project implements an **end-to-end data pipeline** in **Google Cloud Platform (GCP)** for ingesting, processing, and visualizing data from multiple sources (IMDB full dump & NASA DONKI API).

---


## Repository Structure

'''
ae1-data-pipeline/
│
├── bronze_ingest/   # Bronze (Raw) zone ingestion steps, screenshots, README
├── cloud_function/  # NASA DONKI API ingestion Cloud Function code
├── dataflow/        # Dataflow pipeline scripts (streaming & batch)
└── README.md        # (this file)
'''


---

## Pipeline Overview

1. **Ingestion (Bronze Layer)**  
   - IMDB `.tsv.gz` full dump uploaded to GCS (`ae1-bronze-data/imdb/`).  
   - NASA DONKI API data ingested via GCP Cloud Function.
2. **Streaming to Silver (NASA only)**  
   - NASA raw JSON streamed from Bronze to Silver using Apache Beam / Dataflow pipeline.  
   - Cleans data (basic field formatting, null handling) before writing to `ae1-silver-data-bucket/nasa/`.

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

- **2025-8-11** – Created repo, added Bronze Layer ingestion README & IMDB screenshot.
- **2025-08-11** – Added NASA Cloud Function and Dataflow streaming pipeline code.
