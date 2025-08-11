# Bronze Layer Ingestion (Raw Zone)

This folder documents the **Bronze (Raw)** layer of the AE1 data lake in GCP.  
Bronze stores **immutable, source-format** data exactly as ingested, to support auditability and reprocessing.

- **Bucket (Bronze):** `ae1-bronze-data`
- **Region:** `europe-west2`
- **Structure:**

gs://ae1-bronze-data/
imdb/ # IMDB full dump (.tsv.gz), uploaded via GCP web console

---

## 1) IMDB – Full Dump Upload

**Method:** Using the **GCP web console interface** —  
Cloud Storage → Buckets → `ae1-bronze-data` → **Create folder** `imdb/` → **Upload files** (selected all `.tsv.gz`).

**Files uploaded:**
- `title.basics.tsv.gz` — titles (PK: `tconst`)
- `title.ratings.tsv.gz` — ratings (FK → `tconst`)
- `title.akas.tsv.gz` — alternate titles (FK → `titleId` = `tconst`)
- `title.crew.tsv.gz` — directors/writers (FK → `tconst`)
- `title.principals.tsv.gz` — cast/crew per title (FK → `tconst`, `nconst`)
- `title.episode.tsv.gz` — episodes (FK → `tconst`, `parentTconst`)
- `name.basics.tsv.gz` — people (PK: `nconst`)

**Verification steps:**
1. Confirm all files appear in `gs://ae1-bronze-data/imdb/` with expected sizes.
2. Spot-check headers by previewing a file in the GCP web console (no edits).

**Screenshot evidence:**
- `screenshots/imdb_bronze_list.png` — GCS listing of all IMDB `.tsv.gz` in `imdb/`.

> Note: We **do not** store these large data files in GitHub. This repo contains only documentation, screenshots, and helper scripts.


