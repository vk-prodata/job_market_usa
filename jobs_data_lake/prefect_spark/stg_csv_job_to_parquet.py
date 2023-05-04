
from operator import concat
import os
from pyparsing import col
from pyspark.sql.functions import lit

from pandas import DataFrame
from prefect import flow, task
from pyspark.sql import SparkSession
#import pyspark
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
def transform_csv_to_parquet(csv_path, parquet_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

    # Read the CSV file into a Spark DataFrame
    #df = spark.read.format("csv").option("header", "true").load(csv_path)
    df = spark.read.csv(csv_path, header=True, schema=apply_schema())
    print(df.printSchema())
    # Write the DataFrame to a Parquet file
    df.withColumn("search_area", lit(helper.extract_location_from_filename(csv_path)))
    df.withColumn("link", concat(lit("https://www.linkedin.com/"), col("link")))
    
    df.write.format("csv").mode("append").save(parquet_path)

    # Stop the SparkSession
    spark.stop()

def apply_schema():
    return StructType([ \
        StructField("title",StringType(),True), \
        StructField("company",StringType(),True), \
        StructField("location",StringType(),True), \
        StructField("date_posting", DateType(), True), \
        StructField("link", StringType(), True), \
        StructField("salary", StringType(), True), \
    ])

    
    

# Create a Prefect flow
@flow()
def stg_csv_job_to_parquet():
    """The main ETL function"""
    partition_ds = "04-04-2023"
    parquet_path = f"./stg_data_engineer_{partition_ds}.parquet"

    csv_dir = f"./{partition_ds}/"
    for file in os.listdir(csv_dir):
        if file.endswith('.csv'):
            # Transform the CSV to Parquet
            transform_csv_to_parquet(os.path.join(csv_dir, file), parquet_path)


if __name__ == "__main__":
    stg_csv_job_to_parquet()