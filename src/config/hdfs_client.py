from hdfs import InsecureClient
from config.env import HDFS_HOST, HDFS_HOST_NAME


hdfs_client = InsecureClient(url=f"http://{HDFS_HOST}", user=HDFS_HOST_NAME)
