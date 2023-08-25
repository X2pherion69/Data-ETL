from pyspark.sql.functions import (
    col,
    collect_set,
    concat_ws,
    to_date,
)
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame

cols = [
    "id",
    "Rank",
    "Date",
    "Points",
    "Danceability",
    "Energy",
    "Loudness",
    "Speechiness",
    "Instrumentalness",
    "Acousticness",
    "Valence",
]


def merge_dup_row(df: DataFrame) -> DataFrame:
    trans_df = df.groupBy(*cols)

    title_col = trans_df.agg(collect_set("Title").alias("Title")).withColumn(
        "Title", concat_ws(" ", col("Title")).cast("string")
    )

    artist_col = trans_df.agg(
        collect_set("Artists").alias("Artists"),
    )

    nation_col = trans_df.agg(collect_set("Nationality").alias("Nationality"))

    continent_col = trans_df.agg(collect_set("Continent").alias("Continent"))

    aggregated_df = (
        (
            title_col.join(artist_col, on=cols)
            .join(nation_col, on=cols)
            .join(continent_col, on=cols)
        )
        .withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
        .withColumn("Rank", col("Rank").cast("int"))
    )

    return aggregated_df


def sort_row_df(df: DataFrame) -> DataFrame:
    sorted_df = df.orderBy(
        [
            col("Date").desc(),
            col("Rank").cast(IntegerType()).asc(),
        ]
    )

    return sorted_df


def filter_row_df(df: DataFrame) -> DataFrame:
    filtered_df = df.filter(col("Date").isNotNull() | col("Rank").isNotNull())

    return filtered_df
