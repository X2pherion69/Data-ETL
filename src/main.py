from pyspark.sql import SparkSession

from utils import merge_dup_row

file_path = "../data/Spotify_Dataset_V2.csv"
output_path = "../data/Spotify_tranformed.xlsx"

spark = SparkSession.builder.appName("Data-ETL").getOrCreate()

if __name__ == "__main__":
    csv_df = spark.read.option("delimiter", ";").option("header", "true").csv(file_path)

    # for row in csv_df.take(100):
    #     print(row)

    rows = merge_dup_row(csv_df).toPandas()

    rows.to_excel(output_path, index=False)

    spark.stop()
