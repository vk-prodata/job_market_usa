
from operator import concat
import os
import pandas as pd
#from prefect import flow, task

#from jobs_data_lake.helper import extract_location_from_filename
#from ..helper import extract_location_from_filename 
import helper




"""
How to Run:
Inttall Prefect
Activate Prefect:
? prefect cloud workspace set --workspace "vkusa87gmailcom/zoomcamp"
conda activate base
prefect cloud login

Run Spark Locally:
Install Spark
Run in terminal: pyspark
conda install pyspark

QA 
1. Why import doesnt work
2. Should open/close spark context every area?
"""

#@task(log_prints=True)
def transform_csv_to_parquet_silver(csv_path, output_csv):

    df = pd.read_csv(csv_path)
    #print(df.printSchema())
    # Transform the DataFrame
    df["search_area"] = df.apply(lambda row: helper.extract_location_from_filename(csv_path))
    df["link"] = df["link"].apply(lambda x: "https://www.linkedin.com/" + str(x))

    #df.apply(lambda row: concat("https://www.linkedin.com/", row["link"]))

    #df.withColumn("search_area", lit(helper.extract_location_from_filename(csv_path)))
    #df.withColumn("link", concat(lit("https://www.linkedin.com/"), col("link")))
    
    #df.write.format("parquet").mode("append").save(output_csv)
    if not os.path.isfile(output_csv):
        df.to_csv(output_csv, header=True)
    else: 
        df.to_csv(output_csv, mode='a', header=False)

# create a list of company names to filter out
agency_names = ["Accenture", "Aerotek", "Allegis Group", "Capgemini", "CGI Group", "Cognizant Technology Solutions", "Collabera", "Datanomics", "Deloitte", "Diverse Lynx", "Eliassen Group", "Egen","Ettain Group", "EY", "Experis", "Genuent Global", "Hays", "HCL Technologies", "Hired", "IBM Global Services", "Infosys Limited", "Insight Global", "Judge Group", "Kelly Services", "Key Business Solutions", "Kforce", "KPMG", "ManpowerGroup", "Mastech Digital", "Mindtree", "Modis", "NTT DATA", "NextGen Global Resources", "PwC", "Procom", "RCG Global Services", "Randstad Technologies", "Robert Half Technology", "Sapphire Digital", "Softura", "SolTech", "Synechron", "Tata Consultancy Services", "TEKsystems", "The Judge Group", "TriCom Technical Services", "UST Global", "Unisys", "V-Soft Consulting", "Wipro Limited", "Workbridge Associates", "Dice", "Talenty"]

#@task(log_prints=True)
def consolidate_clean_parquet_silver(bronze_path, output_csv):
    df = pd.read_csv(bronze_path)
    # Transform the DataFrame
    # drop duplicates
    df['link_core'] = df['link'].str[:22] #compare only first 22 characters for duplicates
    df = df.drop_duplicates(subset=["title", "company", "location", "link_core"])

    # Assuming you have defined equivalent python functions for all your UDFs
    df["seniority"] = df.apply(lambda row: helper.extract_seniority(row["title"]), axis=1)
    df["position_type"] = df.apply(lambda row: helper.extract_positition_type(row["title"]), axis=1)
    df["position_name"] = df.apply(lambda row: helper.remove_extra_info_from_title(row["title"]), axis=1)
    df["remote_type"] = df.apply(lambda row: helper.remote_type(row["location"], row["title"]), axis=1)
    df["cloud_type"] = df.apply(lambda row: helper.cloud_type(row["title"]), axis=1)
    df["avg_salary"] = df.apply(lambda row: helper.average_salary(row["salary"]), axis=1)
    df['city'] = df.apply(lambda row: helper.split_location(row["location"])[0], axis=1) #df.apply(helper.split_location, axis=1)
    df['state'] = df.apply(lambda row: helper.split_location(row["location"])[1], axis=1)
    # df['date_posting'] = df.apply(lambda row: helper.extract_date_from_filename(bronze_path), axis=1)
    # assuming agency_names is a list of agency names
    df['is_agency'] = df['company'].isin(agency_names)

    df = df.drop(columns=['title', 'salary', 'link_core'])
    #df.to_parquet(output_csv, engine='pyarrow')
    if not os.path.isfile(output_csv):
        df.to_csv(output_csv, header=True)
    else: 
        df.to_csv(output_csv, mode='a', header=False)



#@task(log_prints=True)
def clean_jobs_count_to_parquet(csv_path, output_csv):

    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_path)

    # Apply your custom functions to the DataFrame
    df['city'] = df.apply(lambda row: helper.split_location(row["location"])[0], axis=1) #df.apply(helper.split_location, axis=1)
    df['state'] = df.apply(lambda row: helper.split_location(row["location"])[1], axis=1)
    df["date_scrapping"] = df["date_scrapping"].apply(helper.convert_date_to_week_start)  # Assuming extract_date_from_filename is a function that extracts date from filename

    # Group by city, state, and date_scrapping, and take the max position_count
    df_max_position_within_week = df.groupby(["city", "state", "date_scrapping"])["position_count", "position_all"].max().reset_index()

    # Save the DataFrame to a Parquet file
    #df_max_position_within_week.to_parquet(output_csv, engine='pyarrow')
    df_max_position_within_week.to_csv(output_csv)

    
# Create a Prefect flow
#@flow()
def job_count_to_parquet():
    """The main ETL function"""
    csv_path = f"./jobs_count.csv"
    output_csv = f"./data/jobs_count_target.csv" #parquet"
    clean_jobs_count_to_parquet(csv_path, output_csv)
  

# Create a Prefect flow
#@flow()
def stg_csv_job_to_parquet():
    """The main ETL function"""
    partition_ds = "02-12-2023" #CHANGE IT
    output_csv = f"./data/stg_data_engineer_{partition_ds}.csv" #.parquet"

    csv_dir = f"./data/{partition_ds}/"
    for file in os.listdir(csv_dir):
        if file.endswith('.csv'):
            # Transform the CSV to Parquet
            #transform_csv_to_parquet_silver(os.path.join(csv_dir, file), output_csv)
            consolidate_clean_parquet_silver(os.path.join(csv_dir, file), output_csv)


if __name__ == "__main__":
    stg_csv_job_to_parquet()
    job_count_to_parquet()