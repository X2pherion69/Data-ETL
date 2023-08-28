from pandas import DataFrame
import pyarrow.parquet as ParquetType
from hdfs import InsecureClient
from config import HDFS_PATH

# file_path = "C:\\Users\\olala\\OneDrive\\Desktop\\Personal\\Data-ETL\\data\\sportify_transform_df.parquet"
file_path = "C:\\Users\\Admin\\Desktop\\EntwProject\\Data-ETL\\data\\sportify_transform_df.parquet"


def upload_df_to_hdfs(df: DataFrame, hdfs_client: InsecureClient):
    df.to_parquet(file_path, index=False)
    if hdfs_client.status(hdfs_path=HDFS_PATH, strict=False):
        hdfs_client.delete(hdfs_path=f"{HDFS_PATH}sportify_transform_df.parquet")

    hdfs_client.upload(hdfs_path=HDFS_PATH, local_path=file_path)


def get_df_from_hdfs(
    hdfs_path: str, local_path_export: str, hdfs_client: InsecureClient
) -> ParquetType:
    file: ParquetType = hdfs_client.download(
        hdfs_path,
        local_path=local_path_export,
        overwrite=True,
    )
    return file
