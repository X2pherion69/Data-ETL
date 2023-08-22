from pyspark.sql import SparkSession

from utils import row_utils

file_path = "../data/Spotify_Dataset_V3.csv"
output_path = "../data/Spotify_tranformed.csv"

spark = (
    SparkSession.builder.appName("Data-ETL")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

if __name__ == "__main__":
    csv_df = spark.read.option("delimiter", ";").option("header", "true").csv(file_path)

    # for row in csv_df.take(100):
    #     print(row)

    # Using pandas for df
    merged_rows = row_utils.merge_dup_row(csv_df)
    sorted_rows = row_utils.sort_df(merged_rows)
    rows = sorted_rows.toPandas()

    # Using spark for df
    # rows = merge_dup_row(csv_df)

    # Export using pandas
    rows.to_csv(output_path, index=False)

    # Export using spark
    # rows.coalesce(1).write.option("header", "true").save(output_path)

    spark.stop()
