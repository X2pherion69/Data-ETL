from pyspark.sql import SparkSession

from utils import merge_dup_row

file_path = "../data/Spotify_Dataset_V2.csv"
output_path = "../data/Spotify_tranformed.csv"

spark = SparkSession.builder.appName("Data-ETL").getOrCreate()

if __name__ == "__main__":
    csv_df = spark.read.option("delimiter", ";").option("header", "true").csv(file_path)

    rows = merge_dup_row(csv_df).toPandas()

    rows.to_csv(output_path, index=False)

    spark.stop()
