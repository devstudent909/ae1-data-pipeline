

-- query to get the popularity of genres over time  
SELECT
  g.genre AS genre,
  COUNT(*) AS movie_count
FROM `upheld-caldron-468622-n6.dw_imdb.bridge_title_genre` AS bg
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_genre` AS g
  ON LOWER(bg.genre) = LOWER(g.genre)
GROUP BY genre
ORDER BY movie_count DESC;
