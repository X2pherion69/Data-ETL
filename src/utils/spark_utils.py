from typing import List, Tuple
from config.spark import spark_session
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, year, to_date, count, lit, collect_set
from utils import row_utils
from pyspark.sql.window import Window


def transform_csv_to_df(file_path: str):
    csv_df = (
        spark_session.read.option("delimiter", ";")
        .option("header", "true")
        .csv(file_path)
    )
    merged_rows = row_utils.merge_dup_row(csv_df)
    sorted_rows = row_utils.sort_row_df(merged_rows)
    final_df = row_utils.filter_row_df(sorted_rows)
    return final_df


def trans_df_to_chart_top_5_data(df: SparkDataFrame) -> SparkDataFrame:
    # -> List[Tuple[int, SparkDataFrame]]:
    # splitted_df = []

    formatted_df = df.withColumn("Date", year(to_date(col("Date"))))

    top_5_songs = formatted_df.filter(col("Rank") == 1)

    window_spec = Window.partitionBy("Title")

    df_with_count = top_5_songs.withColumn("Count", count(lit(1)).over(window_spec))

    df_with_count = df_with_count.drop_duplicates(subset=["Title"]).orderBy(
        col("Count").desc()
    )

    # unique_dates = top_5_songs.select("Date").distinct().collect()

    # for row in unique_dates:
    #     data_filtered_df = top_5_songs.filter(col("Date") == row["Date"])
    #     splitted_df.append((row["Date"], data_filtered_df))

    # return splitted_df

    return df_with_count
