-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `upheld-caldron-468622-n6.imdb_silver`
OPTIONS(location="US");

-- Create external table from Parquet files in Silver bucket
CREATE OR REPLACE EXTERNAL TABLE `upheld-caldron-468622-n6.imdb_silver.movies`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ae1-silver-data-bucket/imdb/part-*.parquet']
);
