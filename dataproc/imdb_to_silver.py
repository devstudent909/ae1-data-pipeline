from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IMDB Bronze to Silver").getOrCreate()

# Read from Bronze bucket
df = spark.read.csv("gs://ae1-bronze-data-bucket/imdb/title.basics.tsv.gz",
                    sep="\t", header=True)

# Example cleanup: drop rows with null primaryTitle
df_clean = df.filter(df["primaryTitle"].isNotNull())

# Write to Silver bucket
df_clean.write.mode("overwrite").parquet("gs://ae1-silver-data-bucket/imdb/")

spark.stop()
