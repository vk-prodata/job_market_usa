import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task()
def write_local(df: pd.DataFrame, title: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{title}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gsc")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    csv_dir = f"/Users/kudriavtcevi/Desktop/job_market_usa/files"
    title = "data_engineer"
    for file in os.listdir(csv_dir):
        if file.endswith('.csv'):
            # read the CSV file into a Pandas DataFrame
            df = pd.read_csv(os.path.join(csv_dir, file))
            path = write_local(df, title, file)
            write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
