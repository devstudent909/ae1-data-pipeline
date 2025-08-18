CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_raw.title_basics`
OPTIONS (
  format = 'CSV',
  field_delimiter = '\t',
  skip_leading_rows = 1,
  quote = '',  -- important fix: no special quoting
  uris = ['gs://ae1-bronze-data-bucket/imdb/title.basics.tsv.gz']
);
