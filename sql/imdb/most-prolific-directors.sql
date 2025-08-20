SELECT
  dp.primary_name AS director_name,
  COUNT(DISTINCT f.tconst) AS movie_count
FROM `upheld-caldron-468622-n6.dw_imdb.bridge_title_person` bp
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_person` dp ON dp.person_id = bp.person_id
JOIN `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings_by_year` f USING (tconst)
WHERE bp.role = 'director'
  AND f.titleType = 'movie'
GROUP BY director_name
ORDER BY movie_count DESC, director_name
LIMIT 25;
