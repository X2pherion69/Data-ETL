import numpy as np
from config.spark import spark_session
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import (
    col,
    year,
    to_date,
    count,
    lit,
    array_contains,
    format_number,
)
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
    formatted_df = df.withColumn("Date", year(to_date(col("Date"))))

    top_5_songs_df = formatted_df.filter(col("Rank") == 1)

    window_spec = Window.partitionBy("Title")

    df_with_count = top_5_songs_df.withColumn("Count", count(lit(1)).over(window_spec))

    df_with_count = df_with_count.drop_duplicates(subset=["Title"]).orderBy(
        col("Count").desc()
    )

    return df_with_count


def trans_df_to_chart_the_weeknd_songs(df: SparkDataFrame):
    cols = [
        "Rank",
        "Points",
        "Danceability",
        "Energy",
        "Loudness",
        "Speechiness",
        "Instrumentalness",
        "Acousticness",
        "Valence",
        "Nationality",
        "Continent",
        "Date",
    ]

    filtered_df = df.filter(array_contains(df.Artists, "The Weeknd"))

    total_records = filtered_df.count()

    window_spec = Window.partitionBy("Title")
    df_with_count = filtered_df.withColumn("Count", count(lit(1)).over(window_spec))

    top_song_name = df_with_count.orderBy(col("Count").desc()).first().Title

    df_with_top_song = filtered_df.filter(col("Title") == top_song_name)

    df_with_top_song.show(100)

    df_with_count = (
        df_with_count.drop_duplicates(subset=["Title"])
        .orderBy(col("Count").desc())
        .withColumn("Percentage", format_number(col("Count") / total_records * 100, 2))
        .drop(*cols)
        .limit(5)
    )

    col_percent_values = df_with_count.select(col("Percentage")).collect()

    col_percent_values = [float(row[0]) for row in col_percent_values]

    others_percentage = float(np.fix(100 - np.sum(col_percent_values)))

    return (others_percentage, df_with_count)
