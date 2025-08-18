/* ---------------------------------------------------------
   IMDb SILVER TABLE CREATION – FULL SET
   Project: upheld-caldron-468622-n6
   Dataset: imdb_silver
   Source:  imdb_raw.*
--------------------------------------------------------- */

/* 1. title_basics */
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_basics`
PARTITION BY start_date
CLUSTER BY titleType, genres_str AS
SELECT
  tconst,
  titleType,
  primaryTitle,
  originalTitle,
  isAdult,
  SAFE_CAST(startYear AS INT64) AS startYear,
  SAFE_CAST(endYear AS INT64) AS endYear,
  SAFE_CAST(runtimeMinutes AS INT64) AS runtimeMinutes,
  genres AS genres_str,
  SPLIT(genres, ',') AS genres,
  SAFE_CAST(CONCAT(startYear, '-01-01') AS DATE) AS start_date
FROM `upheld-caldron-468622-n6.imdb_raw.title_basics`
WHERE primaryTitle IS NOT NULL
  AND titleType IS NOT NULL
  AND startYear IS NOT NULL
  AND startYear != '\\N';

/* 2. name_basics */
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.name_basics`
CLUSTER BY primaryName AS
SELECT
  nconst,
  primaryName,
  SAFE_CAST(birthYear AS INT64) AS birthYear,
  SAFE_CAST(deathYear AS INT64) AS deathYear,
  SPLIT(primaryProfession, ',') AS professions,
  SPLIT(knownForTitles, ',') AS knownForTitles
FROM `upheld-caldron-468622-n6.imdb_raw.name_basics`
WHERE primaryName IS NOT NULL
  AND birthYear IS NOT NULL
  AND birthYear != '\\N';

/* 3. title_principals */
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_principals`
CLUSTER BY category AS
SELECT
  tconst,
  SAFE_CAST(ordering AS INT64) AS ordering,
  nconst,
  category,
  job,
  characters
FROM `upheld-caldron-468622-n6.imdb_raw.title_principals`
WHERE tconst IS NOT NULL
  AND nconst IS NOT NULL
  AND category IS NOT NULL;

/* 4. title_ratings */

CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_ratings`
CLUSTER BY tconst AS
SELECT
  tconst,
  SAFE_CAST(averageRating AS FLOAT64) AS averageRating,
  SAFE_CAST(numVotes AS INT64) AS numVotes
FROM `upheld-caldron-468622-n6.imdb_raw.title_ratings`
WHERE tconst IS NOT NULL;


/* 5. title_crew */
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_crew`
CLUSTER BY has_directors AS
SELECT
  tconst,
  directors,
  SPLIT(directors, ',') AS directors_array,
  writers,
  SPLIT(writers, ',') AS writers_array,
  CASE WHEN directors IS NOT NULL AND directors != '\\N' THEN 'Y' ELSE 'N' END AS has_directors
FROM `upheld-caldron-468622-n6.imdb_raw.title_crew`;

/* 6. title_akas */
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.imdb_silver.title_akas`
CLUSTER BY region AS
SELECT
  titleId,
  SAFE_CAST(ordering AS INT64) AS ordering,
  title,
  region,
  language,
  SPLIT(types, ',') AS types_array,
  attributes,
  isOriginalTitle
FROM `upheld-caldron-468622-n6.imdb_raw.title_akas`
WHERE title IS NOT NULL
  AND title != '\\N';
