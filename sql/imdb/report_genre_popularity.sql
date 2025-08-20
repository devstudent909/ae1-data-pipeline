-- query to get the popularity of genres over time  
SELECT
  d.genre,
  COUNT(*) AS movie_count
FROM `upheld-caldron-468622-n6.dw_imdb.bridge_title_genre` bg
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_genre` d
  ON bg.genre = d.genre
GROUP BY d.genre
ORDER BY movie_count DESC;
