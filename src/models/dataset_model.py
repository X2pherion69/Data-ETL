from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)

parquet_schema_spotify = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("Rank", IntegerType(), nullable=False),
        StructField("Date", IntegerType(), nullable=False),
        StructField("Points", IntegerType(), nullable=False),
        StructField("Danceability", DoubleType(), nullable=False),
        StructField("Energy", DoubleType(), nullable=False),
        StructField("Loudness", DoubleType(), nullable=False),
        StructField("Speechiness", DoubleType(), nullable=False),
        StructField("Instrumentalness", DoubleType(), nullable=False),
        StructField("Acousticness", DoubleType(), nullable=False),
        StructField("Valence", DoubleType(), nullable=False),
        StructField("Title", StringType(), nullable=False),
        StructField("Artists", ArrayType(StringType()), nullable=False),
        StructField("Nationality", ArrayType(StringType()), nullable=False),
        StructField("Continent", ArrayType(StringType()), nullable=False),
    ]
)
