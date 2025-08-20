-- 1) Clean dimension (one row per genre, lowercased, trimmed)
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.dim_genre` AS
SELECT DISTINCT LOWER(TRIM(genre)) AS genre
FROM `upheld-caldron-468622-n6.imdb_silver.title_basics` b, UNNEST(b.genres) AS genre
WHERE genre IS NOT NULL AND genre != '\\N';

-- 2) Bridge: one row per (title, atomic genre)
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.bridge_title_genre` AS
SELECT
  b.tconst,
  LOWER(TRIM(g)) AS genre
FROM `upheld-caldron-468622-n6.imdb_silver.title_basics` b, UNNEST(b.genres) AS g
WHERE g IS NOT NULL AND g != '\\N';
