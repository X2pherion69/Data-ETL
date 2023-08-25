import os
from dotenv import load_dotenv

load_dotenv()


HDFS_HOST: str = os.environ.get("HDFS_HOST")
HDFS_HOST_NAME: str = os.environ.get("HDFS_HOST_NAME")
HDFS_PATH: str = os.environ.get("HDFS_PATH")
