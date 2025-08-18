-- -- fact_title_ratings
-- CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings`
-- PARTITION BY DATE(_ingest_ts)
-- CLUSTER BY startYear, titleType AS
-- SELECT
--   tconst,
--   primaryTitle,
--   SAFE_CAST(startYear AS INT64) AS startYear,
--   SAFE_CAST(runtimeMinutes AS INT64) AS runtimeMinutes,
--   SAFE_CAST(averageRating AS FLOAT64) AS averageRating,
--   SAFE_CAST(numVotes AS INT64) AS numVotes,
--   titleType,
--   CURRENT_TIMESTAMP() AS _ingest_ts
-- FROM `upheld-caldron-468622-n6.imdb_silver.movies`
-- WHERE titleType = 'movie' AND SAFE_CAST(startYear AS INT64) IS NOT NULL;

-- -- bridge: title ↔ genre (many-to-many)
-- CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.bridge_title_genre` AS
-- SELECT
--   tconst,
--   TRIM(genre) AS genre
-- FROM `upheld-caldron-468622-n6.imdb_silver.movies`,
-- UNNEST(SPLIT(COALESCE(genres,''), '|')) AS genre
-- WHERE genre IS NOT NULL AND genre <> '';

CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings`
PARTITION BY DATE(_ingest_ts)
CLUSTER BY startYear, titleType AS
SELECT
  b.tconst,
  b.primaryTitle,
  SAFE_CAST(b.startYear AS INT64)        AS startYear,
  SAFE_CAST(b.runtimeMinutes AS INT64)   AS runtimeMinutes,
  SAFE_CAST(r.averageRating AS FLOAT64)  AS averageRating,  
  SAFE_CAST(r.numVotes AS INT64)         AS numVotes,       
  b.titleType,
  CURRENT_TIMESTAMP()                    AS _ingest_ts
FROM `upheld-caldron-468622-n6.imdb_silver.movies`        AS b
LEFT JOIN `upheld-caldron-468622-n6.imdb_silver.title_ratings` AS r
USING (tconst)
WHERE b.titleType = 'movie'
  AND SAFE_CAST(b.startYear AS INT64) IS NOT NULL;

