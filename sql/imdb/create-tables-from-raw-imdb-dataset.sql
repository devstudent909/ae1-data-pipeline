-- title_basics
CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_basics`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.basics.tsv.gz'],
  field_delimiter = '\t',
  skip_leading_rows = 1
);

-- name_basics
CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.name_basics`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ae1-bronze-data-bucket/imdb/name.basics.tsv.gz'],
  field_delimiter = '\t',
  skip_leading_rows = 1
);



-- title_principals
CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_principals`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.principals.tsv.gz'],
  field_delimiter = '\t',
  skip_leading_rows = 1
);

-- title_ratings
CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_ratings`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.ratings.tsv.gz'],
  field_delimiter = '\t',
  skip_leading_rows = 1
);
