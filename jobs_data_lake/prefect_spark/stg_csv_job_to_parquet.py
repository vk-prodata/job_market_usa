
from operator import concat
import os
#from pyparsing import col
from pyspark.sql.functions import lit, when, udf, col
from pandas import DataFrame, to_datetime
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DateType

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

@task(log_prints=True)
def transform_csv_to_parquet_silver(csv_path, parquet_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

    # Read the CSV file into a Spark DataFrame
    df = spark.read.format("csv").option("header", "true").schema(apply_schema_csv()).load(csv_path)
    #print(df.printSchema())
    # Transform the DataFrame
    df = df.withColumn("search_area", lit(helper.extract_location_from_filename(csv_path)))
    df = df.withColumn("link", concat(lit("https://www.linkedin.com/"), col("link")))

    #df.withColumn("search_area", lit(helper.extract_location_from_filename(csv_path)))
    #df.withColumn("link", concat(lit("https://www.linkedin.com/"), col("link")))
    
    df.write.format("parquet").mode("append").save(parquet_path)

    # Stop the SparkSession
    spark.stop()

extract_positition_type_udf = udf(helper.extract_positition_type, StringType())
extract_seniority_udf = udf(helper.extract_seniority, StringType())
remove_extra_info_from_title_udf = udf(helper.remove_extra_info_from_title, StringType())
average_salary_udf = udf(helper.average_salary, StringType())
remote_type_udf = udf(helper.remote_type, StringType())
cloud_type_udf = udf(helper.cloud_type, StringType())
split_location_udf = udf(helper.split_location, StringType())
extract_date_from_filename_udf = udf(helper.extract_date_from_filename, DateType())
# create a list of company names to filter out
agency_names = ["Accenture", "Aerotek", "Allegis Group", "Capgemini", "CGI Group", "Cognizant Technology Solutions", "Collabera", "Datanomics", "Deloitte", "Diverse Lynx", "Eliassen Group", "Egen","Ettain Group", "EY", "Experis", "Genuent Global", "Hays", "HCL Technologies", "Hired", "IBM Global Services", "Infosys Limited", "Insight Global", "Judge Group", "Kelly Services", "Key Business Solutions", "Kforce", "KPMG", "ManpowerGroup", "Mastech Digital", "Mindtree", "Modis", "NTT DATA", "NextGen Global Resources", "PwC", "Procom", "RCG Global Services", "Randstad Technologies", "Robert Half Technology", "Sapphire Digital", "Softura", "SolTech", "Synechron", "Tata Consultancy Services", "TEKsystems", "The Judge Group", "TriCom Technical Services", "UST Global", "Unisys", "V-Soft Consulting", "Wipro Limited", "Workbridge Associates", "Dice", "Talenty"]

@task(log_prints=True)
def consolidate_clean_parquet_silver(parquet_bronze_path, parquet_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("silver_layer").getOrCreate()
    df = spark.read.format("parquet").option("header", "true").schema(apply_schema_consolidated_parquet()).load(parquet_bronze_path)
    # Transform the DataFrame
    df = df.dropDuplicates(subset=["title", "company", "location"])
    df = df.withColumn("seniority", extract_seniority_udf("title"))
    df = df.withColumn("position_type", extract_positition_type_udf("title"))
    df = df.withColumn("position_name", extract_positition_type_udf("title"))
    df = df.withColumn("avg_salary", average_salary_udf("title"))
    df = df.withColumn("remote_type", remote_type_udf("title"))
    df = df.withColumn("cloud_type", cloud_type_udf("title"))
    df = df.withColumn("city", split_location_udf("location")[0])
    df = df.withColumn("state", split_location_udf("location")[1])
    df = df.withColumn("date_posting", extract_date_from_filename_udf("date_posting"))
    df = df.withColumn("is_agency", when(col("company").isin(agency_names), True).otherwise(False))
    df = df.drop("title", "salary")

    df.write.format("parquet").mode("overwrite").save(parquet_path)

    # Stop the SparkSession
    spark.stop()

convert_date_to_week_start_udf = udf(helper.convert_date_to_week_start, DateType())

@task(log_prints=True)
def clean_jobs_count_to_parquet(csv_path, parquet_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("job_count").getOrCreate()
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # df = spark.read.format("csv").option("header", "true").schema(apply_schema_consolidated_parquet()).load(csv_path)
    # Transform the DataFrame
    df = df.withColumn("city", split_location_udf(col("location")))
    #df = df.withColumn("state", split_location_udf("location")[1])
    #df = df.withColumn("date_scrapping", udf(helper.convert_date_to_week_start(col("date_scrapping"))))
    df = df.withColumn("date_scrapping",extract_date_from_filename_udf("date_scrapping"))
    df_max_position_within_week = df.groupBy("city", "state", "date_scrapping").agg(max("position_count").alias("position_count"))
    df_max_position_within_week.write.format("parquet").mode("overwrite").save(parquet_path)

    # Stop the SparkSession
    spark.stop()

def apply_schema_csv():
    return StructType([ \
        StructField("title",StringType(),True), \
        StructField("company",StringType(),True), \
        StructField("location",StringType(),True), \
        StructField("date_posting", DateType(), True), \
        StructField("link", StringType(), True), \
        StructField("salary", StringType(), True), \
    ])

def apply_schema_consolidated_parquet():
    return StructType([ \
        StructField("title",StringType(),True), \
        StructField("company",StringType(),True), \
        StructField("location",StringType(),True), \
        StructField("date_posting", DateType(), True), \
        StructField("link", StringType(), True), \
        StructField("salary", StringType(), True), \
        StructField("search_area", StringType(), True), \
    ])

    
# Create a Prefect flow
@flow()
def job_count_to_parquet():
    """The main ETL function"""
    csv_path = f"./job_listing - Sheet4.csv"
    parquet_path = f"./data/jobs_count.parquet"
    clean_jobs_count_to_parquet(csv_path, parquet_path)
  

# Create a Prefect flow
@flow()
def stg_csv_job_to_parquet():
    """The main ETL function"""
    partition_ds = "02-06-2023"
    parquet_path = f"./data/stg_data_engineer_{partition_ds}.parquet"

    csv_dir = f"./data/{partition_ds}/"
    for file in os.listdir(csv_dir):
        if file.endswith('.csv'):
            # Transform the CSV to Parquet
            transform_csv_to_parquet_silver(os.path.join(csv_dir, file), parquet_path)


if __name__ == "__main__":
    #stg_csv_job_to_parquet()
    job_count_to_parquet()