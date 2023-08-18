from pyspark.sql import SparkSession
import re

file_path = "../data/Spotify_Dataset_V2.csv"

spark = SparkSession.builder.appName("Data-ETL").getOrCreate()

if __name__ == "__main__":
    csv_df = spark.read.option("delimiter", ";").option("header", "true").csv(file_path)

    # rows = csv_df.show(truncate=False)

    # transformed_df.write.mode("overwrite").parquet(".")

    spark.stop()
