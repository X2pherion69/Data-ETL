from config import hdfs_client, HDFS_PATH
from utils.hdfs_utils import upload_df_to_hdfs, get_df_from_hdfs
from utils.spark_utils import transform_csv_to_df
import pandas as pd
from pandas import DataFrame
from config.spark import spark_session

csv_file_path = "../data/Spotify_Dataset_V3.csv"
directory_output = "../data/"
file_path_hdfs = f"{HDFS_PATH}sportify_transform_df.parquet"
output_path_csv = "../data/Spotify_tranformed.csv"


def main():
    # df = transform_csv_to_df(file_path=csv_file_path)

    # Using pandas for df
    # pandas_df = df.toPandas()

    # Upload transformed data to hdfs
    # upload_df_to_hdfs(df=pandas_df, hdfs_client=hdfs_client)

    # Get parquet from hdfs
    parquet_from_hdfs = get_df_from_hdfs(
        hdfs_path=file_path_hdfs,
        local_path_export=directory_output,
        hdfs_client=hdfs_client,
    )

    df_from_hdfs = pd.read_parquet(parquet_from_hdfs)

    top_5_specific_date = df_from_hdfs[df_from_hdfs["Rank"] <= 5]

    top_5_all_time = top_5_specific_date["Title"].value_counts().head(5)

    for val in top_5_all_time.to_dict().keys():
        x: DataFrame = df_from_hdfs[df_from_hdfs["Title"] == val]

    print(
        x.insert(
            column="Count", value=top_5_all_time.items(), allow_duplicates=False, loc=0
        )
    )
    # Export csv using pandas
    # top_5.to_csv(output_path_csv, index=False)

    spark_session.stop()


if __name__ == "__main__":
    main()
