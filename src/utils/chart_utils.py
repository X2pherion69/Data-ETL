from typing import Tuple
from pyspark.sql import DataFrame as SparkDataFrame
import matplotlib.pyplot as plt
from matplotlib.patches import ConnectionPatch
import numpy as np
from pyspark.sql.functions import col


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


def pie_bar_chart_top_artist_theweeknd(params: Tuple[float, SparkDataFrame]):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 5))
    fig.subplots_adjust(wspace=0)

    # Pie chart parameters
    ratios = params[1].select(col("Percentage")).collect()
    titles = params[1].select(col("Title")).collect()

    ratios = [float(row[0]) for row in ratios]
    titles = [str(row[0]) for row in titles]

    overall_ratios = [*ratios, params[0]]
    labels = [*titles, "Others"]

    # Bar chart parameters
