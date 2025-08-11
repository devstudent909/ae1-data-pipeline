# AE1 Data Pipeline – GCP Implementation

This repository contains the deliverables for **AE1 – Advanced Data Engineering** (Summer-1).  
The project implements an **end-to-end data pipeline** in **Google Cloud Platform (GCP)** for ingesting, processing, and visualizing data from multiple sources (IMDB full dump & NASA DONKI API).

---

##  Repository Structure


ae1-data-pipeline/
│
├── bronze\_ingest/        # Bronze (Raw) zone ingestion steps, screenshots, README
└── README.md             # (this file)



---

## Pipeline Overview

1. **Ingestion (Bronze Layer)**  
   - IMDB `.tsv.gz` full dump uploaded to GCS (`ae1-bronze-data/imdb/`).  

---

## Links

- **Bronze Layer README:** [`bronze_ingest/README.md`](bronze_ingest/README.md)


---

## Notes

- **Large datasets** (IMDB dump) are **not stored in GitHub**. Only documentation, screenshots, and code are included.
- Code is tested in **GCP us (multiple regions in United States)** regions.

---

## Change Log

- **2025-8-11** – Created repo, added Bronze Layer ingestion README & IMDB screenshot.
