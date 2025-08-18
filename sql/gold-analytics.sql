CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.analytics_gold.top_movies_by_genre` AS
SELECT
  g AS genre,
  primaryTitle,
  averageRating,
  numVotes
FROM `upheld-caldron-468622-n6.imdb_silver.title_basics` b
JOIN `upheld-caldron-468622-n6.imdb_silver.title_ratings` r
  USING (tconst),
  UNNEST(genres) AS g
WHERE start_date >= '2015-01-01'
ORDER BY genre, averageRating DESC
LIMIT 10;


CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.analytics_gold.nasa_notifications_counts` AS
SELECT
  start_date,
  messageType,
  COUNT(*) AS total
FROM `upheld-caldron-468622-n6.nasa_silver.nasa_staging`
GROUP BY start_date, messageType
ORDER BY start_date DESC;


CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.analytics_gold.activity_summary` AS
SELECT
  nasa.start_date,
  'NASA_NOTIFICATIONS' AS dataset,
  COUNT(*) AS nasa_events,
  NULL AS imdb_titles
FROM `upheld-caldron-468622-n6.nasa_silver.nasa_staging` nasa
GROUP BY nasa.start_date
UNION ALL
SELECT
  imdb.start_date,
  'IMDB_TITLES' AS dataset,
  NULL AS nasa_events,
  COUNT(*) AS imdb_titles
FROM `upheld-caldron-468622-n6.imdb_silver.title_basics` imdb
GROUP BY imdb.start_date;


