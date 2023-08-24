from pyspark.sql import SparkSession
from utils.hdfs_import import upload_df_to_hdfs
from utils import row_utils

file_path = "../data/Spotify_Dataset_V3.csv"
output_path_csv = "../data/Spotify_tranformed.csv"

spark = (
    SparkSession.builder.appName("Data-ETL")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

if __name__ == "__main__":
    csv_df = spark.read.option("delimiter", ";").option("header", "true").csv(file_path)

    # Transforming data
    merged_rows = row_utils.merge_dup_row(csv_df)
    sorted_rows = row_utils.sort_df(merged_rows)

    # Using pandas for df
    rows = sorted_rows.toPandas()

    upload_df_to_hdfs(rows)

    # Using spark for df
    # rows = merge_dup_row(csv_df)

    # Export csv using pandas
    # rows.to_csv(output_path_csv, index=False)

    # Export using spark
    # rows.coalesce(1).write.option("header", "true").save(output_path)

    spark.stop()
