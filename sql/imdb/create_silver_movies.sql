-- create_silver_movies.sql
-- Purpose: Transform IMDB Bronze layer data into clean Silver layer tables
-- Runs in BigQuery

CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_basics`
PARTITION BY DATE(SAFE_CAST(startYear AS DATE))
CLUSTER BY titleType, genres AS
SELECT
  tconst,
  titleType,
  primaryTitle,
  originalTitle,
  isAdult,
  SAFE_CAST(startYear AS INT64) AS startYear,
  SAFE_CAST(endYear AS INT64) AS endYear,
  SAFE_CAST(runtimeMinutes AS INT64) AS runtimeMinutes,
  SPLIT(genres, ',') AS genres
FROM
  `upheld-caldron-468622-n6.imdb_raw.title_basics`
WHERE
  primaryTitle IS NOT NULL
  AND titleType IS NOT NULL;
