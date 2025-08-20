-- quer

WITH rated AS (
  SELECT
    b.startYear AS movie_year,
    b.primaryTitle AS movie_name,
    r.averageRating AS movie_rating,
    ROW_NUMBER() OVER (PARTITION BY b.startYear ORDER BY r.averageRating DESC, r.numVotes DESC) AS rk
  FROM `upheld-caldron-468622-n6.imdb_silver.title_basics` b
  JOIN `upheld-caldron-468622-n6.imdb_silver.title_ratings` r USING (tconst)
  WHERE b.startYear IS NOT NULL
)
SELECT movie_year, movie_name, movie_rating
FROM rated
WHERE rk = 1
ORDER BY movie_year DESC;


