from pyspark.sql import SparkSession


spark_session: SparkSession = (
    SparkSession.builder.appName("Data-ETL")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
