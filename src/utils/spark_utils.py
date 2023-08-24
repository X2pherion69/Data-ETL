from config.spark import spark_session
from utils import row_utils


def transform_csv_to_df(file_path: str):
    csv_df = (
        spark_session.read.option("delimiter", ";")
        .option("header", "true")
        .csv(file_path)
    )
    merged_rows = row_utils.merge_dup_row(csv_df)
    sorted_rows = row_utils.sort_row_df(merged_rows)
    return sorted_rows
