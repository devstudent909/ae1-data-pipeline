-- new sql to push for creating tables in raw imdb

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_basics` (
  tconst STRING,
  titleType STRING,
  primaryTitle STRING,
  originalTitle STRING,
  isAdult STRING,
  startYear STRING,
  endYear STRING,
  runtimeMinutes STRING,
  genres STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.basics.tsv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.name_basics` (
  nconst STRING,
  primaryName STRING,
  birthYear STRING,
  deathYear STRING,
  primaryProfession STRING,
  knownForTitles STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/name.basics.tsv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_principals` (
  tconst STRING,
  ordering STRING,
  nconst STRING,
  category STRING,
  job STRING,
  characters STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.principals.tsv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_ratings` (
  tconst STRING,
  averageRating STRING,
  numVotes STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.ratings.tsv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_crew` (
  tconst STRING,
  directors STRING,
  writers STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.crew.tsv.gz']
);


CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_akas` (
  titleId STRING,
  ordering STRING,
  title STRING,
  region STRING,
  language STRING,
  types STRING,
  attributes STRING,
  isOriginalTitle STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.akas.tsv.gz']
);
