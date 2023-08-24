from hdfs import InsecureClient
from config.env import Env


hdfs_client = InsecureClient(url=f"http://{Env.HDFS_HOST}", user=Env.HDFS_HOST_NAME)
