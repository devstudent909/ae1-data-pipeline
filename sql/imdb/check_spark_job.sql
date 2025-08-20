WITH movies AS (
  SELECT tconst, start_year, averageRating, numVotes
  FROM `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings_by_year`
  WHERE titleType = 'movie' AND start_year IS NOT NULL AND numVotes >= 50000
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY start_year ORDER BY averageRating DESC, numVotes DESC) AS rn
  FROM movies
)
SELECT r.start_year, r.tconst, d.title, r.averageRating, r.numVotes
FROM ranked r
LEFT JOIN `upheld-caldron-468622-n6.dw_imdb.dim_title` d USING (tconst)
WHERE rn = 1
ORDER BY start_year DESC
LIMIT 15;
