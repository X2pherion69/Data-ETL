from pyspark.sql.functions import col, collect_set
from pyspark.sql.types import IntegerType, DateType

cols = [
    "Rank",
    # "Danceability",
    "Date",
    # "Danceability",
    # "Energy",
    # "Loudness",
    # "Speechiness",
    # "Instrumentalness",
    # "Acousticness",
    # "Valence",
    # "Nationality",
    # "Continent",
    # "id",
    # "Song URL",
]


def merge_dup_row(df):
    return (
        df.groupBy(*cols).agg(
            collect_set("Artists").alias("Artists"), collect_set("Title").alias("Title")
        )
        # .withColumn("", col("Date").cast(DateType()))
        .orderBy(
            [
                col("Rank").cast(IntegerType()).asc(),
                col("Date").desc(),
            ]
        )
    )
