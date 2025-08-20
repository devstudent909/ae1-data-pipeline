CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.bridge_title_person` AS
WITH directors AS (
  SELECT c.tconst, TRIM(n) AS nconst, 'director' AS role
  FROM `upheld-caldron-468622-n6.imdb_silver.title_crew` c, UNNEST(SPLIT(c.directors, ',')) AS n
  WHERE c.directors IS NOT NULL AND c.directors != '\\N'
),
writers AS (
  SELECT c.tconst, TRIM(n) AS nconst, 'writer' AS role
  FROM `upheld-caldron-468622-n6.imdb_silver.title_crew` c, UNNEST(SPLIT(c.writers, ',')) AS n
  WHERE c.writers IS NOT NULL AND c.writers != '\\N'
),
actors AS (
  SELECT p.tconst, p.nconst, 'actor' AS role
  FROM `upheld-caldron-468622-n6.imdb_silver.title_principals` p
  WHERE LOWER(p.category) IN ('actor','actress')
)
SELECT
  x.tconst,
  dp.person_id,
  x.role
FROM (
  SELECT * FROM directors
  UNION ALL
  SELECT * FROM writers
  UNION ALL
  SELECT * FROM actors
) x
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_person` dp
  ON dp.person_id = x.nconst;
