from pyspark.sql import DataFrame as SparkDataFrame
import matplotlib.pyplot as plt


def bar_chart_top_songs_spotify(df: SparkDataFrame):
    y_axis = df.select("Count").rdd.flatMap(lambda x: x).take(5)
    x_axis = df.select("Title").rdd.flatMap(lambda x: x).take(5)
    min_date = df.select("Date").rdd.flatMap(lambda x: x).min()
    max_date = df.select("Date").rdd.flatMap(lambda x: x).max()
    plt.figure(figsize=(10, 6))
    plt.bar(x_axis, y_axis)
    plt.xlabel("Title")
    plt.ylabel("Count")
    plt.title(f"Top 5 songs from {min_date} to {max_date} on Spotify")
    plt.show()
