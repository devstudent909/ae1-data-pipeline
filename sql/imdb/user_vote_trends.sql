-- query to get the trend of user votes over time - 3 year mov

WITH yearly AS (
  SELECT
    b.startYear AS movie_year,
    SUM(r.numVotes) AS total_votes
  FROM `upheld-caldron-468622-n6.imdb_silver.title_ratings` r
  JOIN `upheld-caldron-468622-n6.imdb_silver.title_basics` b USING (tconst)
  WHERE b.startYear IS NOT NULL
  GROUP BY movie_year
)
SELECT
  movie_year,
  total_votes,
  ROUND( (total_votes
          + LAG(total_votes, 1) OVER(ORDER BY movie_year)
          + LAG(total_votes, 2) OVER(ORDER BY movie_year)) / 3.0, 2) AS moving_average_3
FROM yearly
ORDER BY movie_year;
