from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


# @task(retries=3)
# def extract_from_gcs(title: str, file_name: str) -> Path:
#     """Download trip data from GCS"""
#     gcs_path = f"data/business_intelligence_developer_jobs_17_42_00.csv.parquet"
#     gcs_block = GcsBucket.load("zoom-gsc")
#     tst = gcs_block.list_blobs("data")
#     gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
#     return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_parquet(path)
    df = df.drop('description', axis=1)
    df = df.drop('location', axis=1)
    df = df.drop('salaries', axis=1)
    df = df.drop('link', axis=1)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="jobs.stg_job_listing",
        #CREATE TABLE IF NOT EXISTS jobs.stg_job_listing (title	STRING, company STRING, avg_salary FLOAT64, city STRING, salary STRING);
        #NameError("name 'Index' is not defined")
        project_id="dtc-de-375803",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    gcs_block = GcsBucket.load("zoom-gsc")
    title = "senior_data_engineer"
    files = gcs_block.list_blobs(f"data/{title}")

    for gcs_path in files:
        gcs_block.get_directory(from_path=gcs_path.name, local_path=f"./")
        path = Path(f"{gcs_path.name}")#extract_from_gcs(title, file)
        df = transform(path)
        write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
