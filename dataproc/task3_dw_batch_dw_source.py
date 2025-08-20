import os
from pyspark.sql import SparkSession, functions as F, window as W

# --------- Config (env vars make this reusable) ----------
PROJECT_ID  = os.getenv("PROJECT_ID", "upheld-caldron-468622-n6")
SRC_DATASET = os.getenv("SRC_DATASET", "dw_imdb")
OUT_DATASET = os.getenv("OUT_DATASET", "dw_imdb_agg")  # create this dataset in BQ first

def bq(tbl):  # "project:dataset.table"
    return f"{PROJECT_ID}:{tbl}"

spark = SparkSession.builder.appName("task3_dw_batch_dw_source").getOrCreate()

# --------- Load DW tables ----------
fact = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.fact_title_ratings_by_year")).load()
bp   = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.bridge_title_person")).load()
dp   = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.dim_person")).load()
bg   = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.bridge_title_genre")).load()
dg   = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.dim_genre")).load()
dt   = spark.read.format("bigquery").option("table", bq(f"{SRC_DATASET}.dim_title")).load()

movies = fact.filter((F.col("titleType") == "movie") & F.col("start_year").isNotNull())

# ---------- A) GENRE x YEAR ----------
gxy_raw = (movies
    .join(bg, "tconst", "inner")
    .join(dg, "genre", "inner")
    .groupBy("start_year", "genre")
    .agg(
        F.countDistinct("tconst").alias("movie_count"),
        F.sum(F.col("averageRating") * F.col("numVotes")).alias("weighted_rating_sum"),
        F.sum("numVotes").alias("vote_sum")
    )
)
gxy = (gxy_raw
    .withColumn(
        "weighted_avg_rating",
        F.when(F.col("vote_sum") > 0, F.col("weighted_rating_sum") / F.col("vote_sum"))
         .otherwise(F.lit(None).cast("double"))
    )
    .select("start_year", "genre", "movie_count", "weighted_avg_rating")
)

(gxy.write
 .format("bigquery")
 .option("table", bq(f"{OUT_DATASET}.genre_year"))
 .option("writeMethod", "direct")
 .option("partitionField", "start_year")
 .option("clusteredFields", "genre")
 .mode("overwrite")
 .save())

# ---------- B) DIRECTOR STATS ----------
directors_raw = (movies
    .join(bp.filter(F.col("role") == "director"), "tconst", "inner")
    .join(dp, "person_id", "inner")
    .groupBy("primary_name")
    .agg(
        F.countDistinct("tconst").alias("film_count"),
        F.sum(F.col("averageRating") * F.col("numVotes")).alias("weighted_rating_sum"),
        F.sum("numVotes").alias("vote_sum")
    )
)
directors = (directors_raw
    .withColumn(
        "weighted_avg_rating",
        F.when(F.col("vote_sum") > 0, F.col("weighted_rating_sum") / F.col("vote_sum"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn("total_votes", F.col("vote_sum"))
    .select("primary_name", "film_count", "weighted_avg_rating", "total_votes")
)

(directors.write
 .format("bigquery")
 .option("table", bq(f"{OUT_DATASET}.director_stats"))
 .option("writeMethod", "direct")
 .option("clusteredFields", "primary_name")
 .mode("overwrite")
 .save())

# ---------- C) TOP MOVIE BY YEAR ----------
w = W.Window.partitionBy("start_year").orderBy(F.col("averageRating").desc(), F.col("numVotes").desc())
top_by_year = (movies
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("tconst", "start_year", "averageRating", "numVotes")
    .join(dt, "tconst", "left")
    .select("start_year", "tconst", "title", "averageRating", "numVotes")
)

(top_by_year.write
 .format("bigquery")
 .option("table", bq(f"{OUT_DATASET}.top_movie_by_year"))
 .option("writeMethod", "direct")
 .option("partitionField", "start_year")
 .option("clusteredFields", "start_year")
 .mode("overwrite")
 .save())

spark.stop()
