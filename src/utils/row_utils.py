from pyspark.sql.functions import (
    col,
    collect_set,
    concat_ws,
    to_date,
)
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame as SparkDataFrame
from pandas import DataFrame as PdDataFrame, Series as PdSeries

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


def merge_dup_row(df: SparkDataFrame) -> SparkDataFrame:
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


def sort_row_df(df: SparkDataFrame) -> SparkDataFrame:
    sorted_df = df.orderBy(
        [
            col("Date").desc(),
            col("Rank").cast(IntegerType()).asc(),
        ]
    )

    return sorted_df


def filter_row_df(df: SparkDataFrame) -> SparkDataFrame:
    filtered_df = df.filter(col("Date").isNotNull() | col("Rank").isNotNull())

    return filtered_df


def agg_to_set_list_pandas(series: PdSeries) -> PdSeries:
    return set(series)


# def transf_df_to_chart_data(df: PdDataFrame) -> PdDataFrame:
#     # Get top 5 songs that have Rank <= 5
#     df_top_5_rank = df[df["Rank"] <= 5]

#     # Aggregate the top 5 songs after `Title` and make a col named `Count` to count the total quantity of each top 5 songs
#     df_grouped = df_top_5_rank.groupby("Title").size().reset_index(name="Count")

#     # Create a new col named `TotalCount` to store the total quantity of each top 5 songs
#     df_top_5_rank["Count"] = df_top_5_rank["Title"].map(
#         df_grouped.set_index("Title")["Count"]
#     )

#     agged_data = (
#         df_top_5_rank.groupby(cols)
#         .agg(
#             {
#                 "Title": "unique",
#             }
#         )
#         .reset_index()
#     )

#     sort_df = sort_row_df(agged_data)

#     return sort_df
