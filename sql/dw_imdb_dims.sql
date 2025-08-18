-- IMDB warehouse dataset
CREATE SCHEMA IF NOT EXISTS `upheld-caldron-468622-n6.dw_imdb`
OPTIONS (location = "US");

-- dim_title_type
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.dim_title_type` AS
SELECT DISTINCT titleType
FROM `upheld-caldron-468622-n6.imdb_silver.movies`;

-- dim_genre (explode multi-valued genres)
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.dim_genre` AS
WITH g AS (
  SELECT DISTINCT TRIM(genre) AS genre
  FROM `upheld-caldron-468622-n6.imdb_silver.movies`,
       UNNEST(SPLIT(COALESCE(genres,''), '|')) AS genre
)
SELECT genre FROM g WHERE genre IS NOT NULL AND genre <> '';

-- dim_year
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.dim_year` AS
SELECT CAST(y AS INT64) AS startYear,
       CONCAT(CAST((y/10)*10 AS STRING), 's') AS decade
FROM UNNEST(GENERATE_ARRAY(1870, 2035)) AS y;


