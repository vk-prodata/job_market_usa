from prefect import task, Flow
from pyspark.sql import SparkSession

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
"""

@task
def transform_csv_to_parquet(csv_path, parquet_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

    # Read the CSV file into a Spark DataFrame
    df = spark.read.format("csv").option("header", "true").load(csv_path)

    # Write the DataFrame to a Parquet file
    df.write.format("parquet").mode("overwrite").save(parquet_path)

    # Stop the SparkSession
    spark.stop()

# Create a Prefect flow
with Flow("csv_to_parquet") as flow:
    # Define the CSV and Parquet file paths
    csv_path = "/04-04-2023/data_engineer_Miami, Florida_jobs_1_05-04-2023.csv"
    parquet_path = "/04-04-2023/data_engineer_Miami, Florida_jobs_1_05-04-2023.parquet"

    # Transform the CSV to Parquet
    transform_csv_to_parquet(csv_path, parquet_path)

# Run the flow
flow.run()