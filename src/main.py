from config import HDFS_PATH
from utils.spark_utils import trans_df_to_chart_top_5_data
from utils.chart_utils import bar_chart_top_songs_spotify
from config.spark import spark_session
from config.env import HDFS_SPARK_HOST

csv_file_path = "../data/Spotify_Dataset_V3.csv"
directory_output = "../data/"
file_path_hdfs = f"{HDFS_PATH}sportify_transform_df.parquet"
output_path_csv = "../data/Spotify_tranformed.csv"


def main():
    # df = transform_csv_to_df(file_path=csv_file_path)

    ########################################################################################################
    # Using pandas for df
    # ------------------------------------------------------------------------------------------------------
    # pandas_df = df.toPandas()

    ########################################################################################################
    # Upload transformed data to hdfs
    # ------------------------------------------------------------------------------------------------------
    # upload_df_to_hdfs(df=pandas_df, hdfs_client=hdfs_client)
    # ------------------------------------------------------------------------------------------------------
    # df.write.parquet(
    #     f"{HDFS_SPARK_HOST}/temp/",
    #     mode="overwrite",
    # )

    ########################################################################################################
    # Get parquet from hdfs
    # ------------------------------------------------------------------------------------------------------
    # parquet_from_hdfs = get_df_from_hdfs(
    #     hdfs_path=file_path_hdfs,
    #     local_path_export=directory_output,
    #     hdfs_client=hdfs_client,
    # )
    # ------------------------------------------------------------------------------------------------------

    ########################################################################################################
    # Read file from hdfs
    # ------------------------------------------------------------------------------------------------------
    df_parquet_from_hdfs_spark = spark_session.read.parquet(
        f"{HDFS_SPARK_HOST}/temp/", options={"header": True, "inferSchema": True}
    )
    # ------------------------------------------------------------------------------------------------------
    # df_from_hdfs = pd.read_parquet(parquet_from_hdfs)

    ########################################################################################################
    # Making charts
    # ------------------------------------------------------------------------------------------------------
    chart_df = trans_df_to_chart_top_5_data(df_parquet_from_hdfs_spark)

    bar_chart_top_songs_spotify(chart_df)

    ########################################################################################################
    # Export csv using pandas
    # ------------------------------------------------------------------------------------------------------
    # chart_df.to_csv(output_path_csv, index=False)

    ########################################################################################################
    spark_session.stop()


if __name__ == "__main__":
    main()
