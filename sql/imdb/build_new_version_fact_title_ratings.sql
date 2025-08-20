CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings_by_year`
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1870, 2035, 1))
CLUSTER BY titleType, numVotes
OPTIONS (description="Fact: ratings joined to basics with start_year partitioning")
AS
SELECT
  r.tconst,
  CAST(SAFE_CAST(b.startYear AS INT64) AS INT64) AS start_year,
  b.titleType,
  r.averageRating,
  r.numVotes
FROM `upheld-caldron-468622-n6.imdb_silver.title_ratings` r
JOIN `upheld-caldron-468622-n6.imdb_silver.title_basics` b USING (tconst)
WHERE SAFE_CAST(b.startYear AS INT64) IS NOT NULL;
