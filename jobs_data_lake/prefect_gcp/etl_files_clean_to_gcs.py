import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import helper


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #df["salary"] = df.apply(lambda row: helper.extract_salaries(row["description"]), axis=1)
    df["seniority"] = df.apply(lambda row: helper.extract_seniority(row["title"]), axis=1)
    df["position_type"] = df.apply(lambda row: helper.extract_positition_type(row["title"]), axis=1)
    df["position_name"] = df.apply(lambda row: helper.remove_extra_info_from_title(row["title"]), axis=1)
    df["remote_type"] = df.apply(lambda row: helper.remote_type(row["location"], row["title"]), axis=1)
    df["cloud_type"] = df.apply(lambda row: helper.cloud_type(row["title"]), axis=1)
    df["avg_salary"] = df.apply(lambda row: helper.average_salary(row["salary"]), axis=1)
    df['city'] = df.apply(lambda row: helper.split_location(row["location"])[0], axis=1) #df.apply(helper.split_location, axis=1)
    df['state'] = df.apply(lambda row: helper.split_location(row["location"])[1], axis=1)
    df['date_posting'] = df.apply(lambda row: helper.fill_date_posting(row["date_posting"]), axis=1)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, title: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{title}/{dataset_file}.parquet")
    path_csv = Path(f"data/{title}/{dataset_file}.csv")
    df.to_csv(path_csv)
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
    title = "data_engineer" #"senior_data_engineer"
    for file in os.listdir(csv_dir):
        if file.endswith('.csv'):
            #if file.find("Washington") >=0:
            # read the CSV file into a Pandas DataFrame
            df = pd.read_csv(os.path.join(csv_dir, file))
            df_clean = clean(df)
            path = write_local(df_clean, title, file)
            write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
