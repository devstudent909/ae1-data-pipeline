-- Top 10 genres by avg rating (min 1k votes, last 30 years)
WITH recent AS (
  SELECT f.*, b.genre
  FROM `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings` f
  JOIN `upheld-caldron-468622-n6.dw_imdb.bridge_title_genre` b USING (tconst)
  WHERE f.startYear >= EXTRACT(YEAR FROM CURRENT_DATE()) - 30
)
SELECT genre,
       ROUND(AVG(averageRating), 2) AS avg_rating,
       SUM(numVotes) AS total_votes
FROM recent
WHERE numVotes >= 1000
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 10;

-- Average runtime by decade
SELECT y.decade, ROUND(AVG(f.runtimeMinutes),1) AS avg_runtime
FROM `upheld-caldron-468622-n6.dw_imdb.fact_title_ratings` f
JOIN `upheld-caldron-468622-n6.dw_imdb.dim_year` y
  ON y.startYear = f.startYear
GROUP BY y.decade
ORDER BY y.decade;

