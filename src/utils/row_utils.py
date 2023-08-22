from pyspark.sql.functions import (
    col,
    collect_set,
    unix_timestamp,
    concat_ws,
    udf,
    to_date,
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame, Column

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


# def utf8_encode(s: str):
#     return s.encode("utf-8", "ignore").decode("utf-8")
# utf8_encode_udf = udf(utf8_encode, StringType())


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
        title_col.join(artist_col, on=cols)
        .join(nation_col, on=cols)
        .join(continent_col, on=cols)
    ).withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))

    return aggregated_df


def sort_df(df: DataFrame) -> DataFrame:
    sorted_df = df.orderBy(
        [
            col("Date").desc(),
            col("Rank").cast(IntegerType()).asc(),
        ]
    )

    return sorted_df
