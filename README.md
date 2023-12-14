# Job Market USA

I tried to capture data from LinkedIn, but unfortunately, LinkedIn is smarter than me and blocked some requests, so I was only able to capture a part of the information. Nonetheless, I believe it should be enough for the final project of DataZoomCamp.

Tech stack:
Scraping: Python (using Selenium and BeautifulSoup libraries)
Data Lake: Prefect, Google Cloud Storage, Google BigQuery
DWH, DM: Google BigQuery, DBT
Reporting: Google Data Studio

## Deploy Instruction:
1. Intall vs code, python (selenium, pandas, soup etc), Prefect. Login to cloud account and deploy local version
2. Sign up/Logic to GCP, DBT
3. Run scrapping job (any python workflow from the folder "linkedIn_job_scrapper")
4. Deploy with Terraform or manually create Google cloud storage and Bigquery database.
5. Run pipelines in the folder "jobs_data_lake" for populating GCP and clean/enrich data to BigQuery
6. Deploy the folder "dbt_analytics" in dbt. Run pipelines for transforming data for Data Mart
7. Open Google studio. Connect to DM queries and build a dashboard.

## Data Ingestion Schema
![Untitled Diagram drawio](https://user-images.githubusercontent.com/123039991/228688243-7c8eba31-fb2f-4bb1-8d2a-26cc107c14af.png)

## Analytics schema from DBT
<img width="1342" alt="Screenshot 2023-03-29 at 2 28 15 PM" src="https://user-images.githubusercontent.com/123039991/228687724-941464ae-8b02-482c-beeb-cd1a7a90d49f.png">

## Report from Google Data Studio
<img width="1194" alt="Screenshot 2023-04-09 at 8 23 53 PM" src="https://user-images.githubusercontent.com/123039991/230819456-7bdef531-58b8-4400-b716-dff70c14ebc1.png">

[Job_Market_2023.pdf](https://github.com/vk-prodata/job_market_usa/files/11187368/Job_Market_2023.pdf)

Link https://lookerstudio.google.com/s/nTTQAND75yE

### Conclusion
It was an interesting journey. I definitely want to improve the project further by:

- Improving the scraping of LinkedIn
- Investigating other sources
- Adding the ability to manage data from other sources
- Building more sophisticated models and reports
- Creating a bot for Telegram.
