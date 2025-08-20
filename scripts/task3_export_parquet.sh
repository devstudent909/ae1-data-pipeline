DEST_BUCKET=ae1-gold-data-bucket

bq extract --destination_format=PARQUET \
  upheld-caldron-468622-n6:dw_imdb_agg.genre_year \
  gs://$DEST_BUCKET/task3/genre_year/*.parquet

bq extract --destination_format=PARQUET \
  upheld-caldron-468622-n6:dw_imdb_agg.director_stats \
  gs://$DEST_BUCKET/task3/director_stats/*.parquet

bq extract --destination_format=PARQUET \
  upheld-caldron-468622-n6:dw_imdb_agg.top_movie_by_year \
  gs://$DEST_BUCKET/task3/top_movie_by_year/*.parquet
