from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def read_gsheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']

    credentials = Credentials.from_service_account_file('credentials/dtc-de-375803-8dd8e44e7ffd.json', scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)
    sheet_id = '1Vd9U3pNQGguPjmxkDZKq5DD6kTB6Mwy08bpt2WICTiU'
    # open a google sheet
    gs = gc.open_by_key(sheet_id)
    # select a work sheet from its name
    sheet = gs.worksheet('staging')
    print(sheet)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_parquet(path)
    df = df.rename(columns={'positition_type': 'position_type'})
    df[["date_posting"]] = df[["date_posting"]].apply(pd.to_datetime)
    df = df.drop('title', axis=1)
    ##df = df.drop('location', axis=1)
    df = df.drop('salary', axis=1)
    #df = df.drop('link', axis=1)
# CREATE TABLE IF NOT EXISTS jobs.stg_linkedin_job_listing (
#   company STRING, 
#   date_posting DATETIME, 
#   seniority STRING, 
#   position_type STRING, 
#   position_name STRING, 
#   remote_type STRING, 
#   cloud_type STRING, 
#   avg_salary FLOAT64, 
#   city STRING,
#   state STRING
# );
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="jobs.stg_linkedin_job_listing",
        project_id="dtc-de-375803",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    gcs_block = GcsBucket.load("zoom-gsc")
    title = "data_engineer"
    files = gcs_block.list_blobs(f"data/{title}")

    for gcs_path in files:
        gcs_block.get_directory(from_path=gcs_path.name, local_path=f"./")
        path = Path(f"{gcs_path.name}")#extract_from_gcs(title, file)
        df = transform(path)
        write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
    #read_gsheet()