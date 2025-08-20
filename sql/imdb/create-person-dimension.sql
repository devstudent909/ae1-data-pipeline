-- create a dimension table for people

CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.dim_person` AS
SELECT DISTINCT
  nb.nconst AS person_id,
  nb.primaryName AS primary_name,
  CAST(nb.birthYear AS INT64) AS birth_year,
  CAST(nb.deathYear AS INT64) AS death_year
FROM `upheld-caldron-468622-n6.imdb_silver.name_basics` nb;
