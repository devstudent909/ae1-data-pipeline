-- Directors with the most films among the Top 10 movies by rating (>= 50k votes)
-- Uses DW fact + person bridge; counts how many of the Top 10 each director has.

WITH top10 AS (
  SELECT tconst
  FROM `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings_by_year`
  WHERE titleType = 'movie'
    AND numVotes >= 50000
  ORDER BY averageRating DESC, numVotes DESC
  LIMIT 10
)
SELECT
  dp.primary_name AS director_name,
  COUNT(*) AS top_10_movie_count
FROM top10 t
JOIN `upheld-caldron-468622-n6.dw_imdb.bridge_title_person` bp USING (tconst)
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_person` dp ON dp.person_id = bp.person_id
WHERE bp.role = 'director'
GROUP BY director_name
ORDER BY top_10_movie_count DESC, director_name;
