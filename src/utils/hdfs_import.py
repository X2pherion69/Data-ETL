from hdfs import InsecureClient
from config.env import Env
from pandas import DataFrame


hdfs_client = InsecureClient(url=Env.HDFS_HOST, user=Env.HDFS_HOST_NAME)

file_name = "sportify_transformed.parquet"

output_path = "C:\\Users\\Admin\\Desktop\\EntwProject\\Data-ETL\\data\\"


def upload_df_to_hdfs(df: DataFrame):
    df.to_parquet(output_path + file_name, index=False)

    hdfs_client.upload(hdfs_path="/", local_path=(output_path + file_name))
