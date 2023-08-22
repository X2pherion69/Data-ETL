from pyspark.sql.functions import col, collect_set, unix_timestamp, concat_ws, udf
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
    "Nationality",
    "Continent",
]


def utf8_encode(s: str):
    return s.encode("utf-8", "ignore").decode("utf-8")


utf8_encode_udf = udf(utf8_encode, StringType())


def merge_dup_row(df: DataFrame) -> DataFrame:
    trans_df = (
        df.groupBy(*cols)
        .agg(
            collect_set("Artists").alias("Artists"), collect_set("Title").alias("Title")
        )
        .withColumn(
            "timestamp", unix_timestamp(col("Date"), "dd/MM/yyyy").cast(IntegerType())
        )
        .withColumn("Title", concat_ws(" ", col("Title")).cast("string"))
        .withColumn("Title", utf8_encode_udf(col("Title")))
        .orderBy(
            [
                col("timestamp").desc(),
                col("Rank").cast(IntegerType()).asc(),
            ]
        )
    )

    return trans_df
