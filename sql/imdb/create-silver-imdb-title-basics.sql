CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_basics`
PARTITION BY start_date
CLUSTER BY titleType, genres_str AS
SELECT
  tconst,
  titleType,
  primaryTitle,
  originalTitle,
  isAdult,
  SAFE_CAST(startYear AS INT64) AS startYear,
  SAFE_CAST(endYear AS INT64) AS endYear,
  SAFE_CAST(runtimeMinutes AS INT64) AS runtimeMinutes,

  -- Scalar version for clustering
  genres AS genres_str,

  -- Array version for analysis
  SPLIT(genres, ',') AS genres,

  -- Make a DATE column from year for partitioning
  SAFE_CAST(CONCAT(startYear, '-01-01') AS DATE) AS start_date

FROM
  `upheld-caldron-468622-n6.imdb_raw.title_basics`
WHERE
  primaryTitle IS NOT NULL
  AND titleType IS NOT NULL
  AND startYear IS NOT NULL
  AND startYear != '\\N';
