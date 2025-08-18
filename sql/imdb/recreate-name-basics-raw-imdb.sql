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
  skip_leading_rows = 1,  -- skip header row in the file
  quote = '',             -- don't treat quotes specially
  uris = ['gs://ae1-bronze-data-bucket/imdb/name.basics.tsv.gz']
);

-- fix column titling error from previous sql table creation script for name_basics